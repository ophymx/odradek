//! Cluster layer: metadata discovery, broker connection pool, and
//! partition-leader routing.
//!
//! A [`Cluster`] bootstraps through any reachable configured broker,
//! learns the cluster's shape from Metadata, and hands out negotiated
//! [`Connection`]s to the broker a request must go to. Version ranges are
//! kept per broker — brokers in a mixed-version cluster may advertise
//! different ranges.

use std::collections::HashMap;

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
use odradek_protocol::messages::metadata_response::MetadataResponse;

use crate::ClientConfig;
use crate::conn::Connection;
use crate::error::ClientError;
use crate::negotiate::ApiVersionRanges;

/// Metadata versions this client speaks: v1+ so an empty topics array
/// means "none" rather than v0's "all".
const METADATA_SUPPORTED: (i16, i16) = (1, MetadataRequest::MAX_VERSION);

/// One broker's advertised endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl BrokerInfo {
    fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// One partition's leadership as last reported by metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    pub index: i32,
    pub leader_id: i32,
    pub error: ErrorCode,
}

/// A negotiated connection to one broker, with the version ranges that
/// broker advertised.
#[derive(Debug, Clone)]
pub struct Broker {
    pub conn: Connection,
    pub ranges: ApiVersionRanges,
}

/// A connected cluster: metadata cache plus per-broker connections.
#[derive(Debug)]
pub struct Cluster {
    config: ClientConfig,
    bootstrap: Broker,
    brokers: HashMap<i32, BrokerInfo>,
    topics: HashMap<String, Vec<PartitionInfo>>,
    conns: HashMap<i32, Broker>,
}

impl Cluster {
    /// Dial the first reachable bootstrap server and negotiate versions.
    /// No metadata is fetched yet; see [`Cluster::refresh_metadata`].
    pub async fn connect(config: ClientConfig) -> Result<Cluster, ClientError> {
        let mut last = String::from("no bootstrap servers configured");
        for addr in &config.bootstrap_servers {
            match dial(addr, &config).await {
                Ok(bootstrap) => {
                    return Ok(Cluster {
                        config,
                        bootstrap,
                        brokers: HashMap::new(),
                        topics: HashMap::new(),
                        conns: HashMap::new(),
                    });
                }
                Err(e) => last = format!("{addr}: {e}"),
            }
        }
        Err(ClientError::Bootstrap(last))
    }

    /// Fetch metadata for `topics` through the bootstrap connection and
    /// update the broker and leadership caches.
    pub async fn refresh_metadata(&mut self, topics: &[&str]) -> Result<(), ClientError> {
        let version = self
            .bootstrap
            .ranges
            .pick(MetadataRequest::API_KEY, METADATA_SUPPORTED)?;
        let request = MetadataRequest {
            topics: Some(
                topics
                    .iter()
                    .map(|name| MetadataRequestTopic {
                        name: Some((*name).to_owned()),
                        ..Default::default()
                    })
                    .collect(),
            ),
            allow_auto_topic_creation: false,
            ..Default::default()
        };
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = self
            .bootstrap
            .conn
            .request(MetadataRequest::API_KEY, version, &body)
            .await?;
        let resp = MetadataResponse::decode(&mut resp, version)?;

        self.brokers = resp
            .brokers
            .iter()
            .map(|b| {
                (
                    b.node_id,
                    BrokerInfo {
                        node_id: b.node_id,
                        host: b.host.clone(),
                        port: b.port,
                    },
                )
            })
            .collect();
        for topic in &resp.topics {
            let Some(name) = &topic.name else { continue };
            let code = ErrorCode(topic.error_code);
            if !code.is_ok() {
                return Err(ClientError::Broker(code));
            }
            self.topics.insert(
                name.clone(),
                topic
                    .partitions
                    .iter()
                    .map(|p| PartitionInfo {
                        index: p.partition_index,
                        leader_id: p.leader_id,
                        error: ErrorCode(p.error_code),
                    })
                    .collect(),
            );
        }
        Ok(())
    }

    /// Known brokers, as of the last metadata refresh.
    pub fn brokers(&self) -> impl Iterator<Item = &BrokerInfo> {
        self.brokers.values()
    }

    /// A topic's partitions, as of the last metadata refresh.
    pub fn partitions(&self, topic: &str) -> Option<&[PartitionInfo]> {
        self.topics.get(topic).map(Vec::as_slice)
    }

    /// The node id currently leading `topic[partition]`.
    pub fn leader_id(&self, topic: &str, partition: i32) -> Option<i32> {
        let info = self
            .topics
            .get(topic)?
            .iter()
            .find(|p| p.index == partition)?;
        (info.leader_id >= 0).then_some(info.leader_id)
    }

    /// A negotiated connection to the broker with `node_id`, dialing on
    /// first use.
    pub async fn broker(&mut self, node_id: i32) -> Result<&Broker, ClientError> {
        if !self.conns.contains_key(&node_id) {
            let info = self
                .brokers
                .get(&node_id)
                .ok_or(ClientError::UnknownLeader {
                    topic: format!("<broker {node_id}>"),
                    partition: -1,
                })?;
            let broker = dial(&info.addr(), &self.config).await?;
            self.conns.insert(node_id, broker);
        }
        Ok(&self.conns[&node_id])
    }

    /// Forget cached leadership for `topic` — e.g. after a
    /// NOT_LEADER_OR_FOLLOWER — so the next lookup refreshes.
    pub fn mark_stale(&mut self, topic: &str) {
        self.topics.remove(topic);
    }

    /// Drop the pooled connection to `node_id` (e.g. after it failed);
    /// the next use redials.
    pub fn forget_broker(&mut self, node_id: i32) {
        self.conns.remove(&node_id);
    }

    /// A negotiated connection to the current leader of
    /// `topic[partition]`, refreshing metadata once if leadership is
    /// unknown.
    pub async fn partition_leader(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<&Broker, ClientError> {
        if self.leader_id(topic, partition).is_none() {
            self.refresh_metadata(&[topic]).await?;
        }
        let leader =
            self.leader_id(topic, partition)
                .ok_or_else(|| ClientError::UnknownLeader {
                    topic: topic.to_owned(),
                    partition,
                })?;
        self.broker(leader).await
    }
}

async fn dial(addr: &str, config: &ClientConfig) -> Result<Broker, ClientError> {
    let conn = Connection::connect(addr, config).await?;
    let ranges = conn.negotiate().await?;
    Ok(Broker { conn, ranges })
}
