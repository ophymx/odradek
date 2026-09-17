//! Cluster layer: metadata discovery, broker connection pool, and
//! partition-leader routing.
//!
//! A [`Cluster`] bootstraps through any reachable configured broker,
//! learns the cluster's shape from Metadata, and hands out negotiated
//! [`Connection`]s to the broker a request must go to. Version ranges are
//! kept per broker — brokers in a mixed-version cluster may advertise
//! different ranges.
//!
//! A `Cluster` is a cheap clonable handle: clones share one connection
//! pool, one metadata cache, and one set of discovered coordinators, so
//! a producer, a consumer, and a group membership in the same process
//! ride the same authenticated connections. Handles are `Send + Sync`;
//! internal locks are never held across I/O.
//!
//! # Connection sharing and blocking requests
//!
//! The pool keeps one connection per broker, and a Kafka broker processes
//! a single connection's requests strictly in order: it will not begin
//! the next request until the current one's response is sent. Fast
//! requests (Produce, Metadata) pipeline freely, but a *blocking* request
//! holds the connection for its whole duration, so concurrent work to the
//! same broker over one shared handle serializes behind it:
//!
//! - A long-poll `Fetch` (up to `max_wait_ms`) delays a concurrent
//!   `Produce` to the same broker until it returns. Lower `max_wait_ms`,
//!   or give the latency-sensitive path its own [`Cluster`], if that
//!   coupling matters.
//! - Two members of the *same* group must not share a `Cluster`: a
//!   parked `JoinGroup` blocks the other member's heartbeats for the
//!   whole rebalance, evicting it. Use one `Cluster` per member (the
//!   natural one-member-per-process topology).
//!
//! A growable per-broker connection pool would lift these; it is future
//! work.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use odradek_protocol::messages::create_topics_response::CreateTopicsResponse;
use odradek_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use odradek_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
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
#[non_exhaustive]
pub struct Broker {
    pub conn: Connection,
    pub ranges: ApiVersionRanges,
}

/// The mutable half of a cluster: caches every handle shares.
#[derive(Debug, Default)]
struct State {
    /// The control-plane connection (metadata refresh, FindCoordinator,
    /// admin). `None` after a failure; the next use redials, failing over
    /// from the configured bootstrap servers to any known broker.
    control: Option<Broker>,
    brokers: HashMap<i32, BrokerInfo>,
    topics: HashMap<String, Vec<PartitionInfo>>,
    conns: HashMap<i32, Broker>,
    /// Group id → coordinator node id, as last discovered.
    coordinators: HashMap<String, i32>,
}

#[derive(Debug)]
struct Inner {
    config: ClientConfig,
    /// The connection the cluster bootstrapped through, frozen for
    /// [`Cluster::bootstrap_broker`]. Control-plane traffic uses the
    /// replaceable [`State::control`] slot instead.
    initial: Broker,
    state: Mutex<State>,
}

/// A connected cluster: metadata cache plus per-broker connections.
/// Cloning is cheap and clones share everything.
#[derive(Debug, Clone)]
pub struct Cluster {
    inner: Arc<Inner>,
}

impl Cluster {
    /// Dial the first reachable bootstrap server and negotiate versions.
    /// No metadata is fetched yet; see [`Cluster::refresh_metadata`].
    pub async fn connect(config: ClientConfig) -> Result<Cluster, ClientError> {
        let mut last = String::from("no bootstrap servers configured");
        for addr in &config.bootstrap_servers {
            match dial(addr, &config).await {
                Ok(bootstrap) => {
                    let state = State {
                        control: Some(bootstrap.clone()),
                        ..State::default()
                    };
                    return Ok(Cluster {
                        inner: Arc::new(Inner {
                            config,
                            initial: bootstrap,
                            state: Mutex::new(state),
                        }),
                    });
                }
                Err(e) => last = format!("{addr}: {e}"),
            }
        }
        Err(ClientError::Bootstrap(last))
    }

    /// The shared state; the guard must never live across an `.await`.
    fn state(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .expect("cluster state lock poisoned")
    }

    /// Fetch metadata for `topics` through the control-plane connection
    /// and update the broker and leadership caches. A dead control
    /// connection fails over: the redial tries the configured bootstrap
    /// servers and every currently-known broker.
    pub async fn refresh_metadata(&self, topics: &[&str]) -> Result<(), ClientError> {
        let broker = self.control_broker().await?;
        match self.refresh_metadata_via(&broker, topics).await {
            Err(e) if is_control_failure(&e) => {
                self.forget_control();
                let broker = self.control_broker().await?;
                self.refresh_metadata_via(&broker, topics).await
            }
            other => other,
        }
    }

    async fn refresh_metadata_via(
        &self,
        broker: &Broker,
        topics: &[&str],
    ) -> Result<(), ClientError> {
        let version = broker
            .ranges
            .pick(MetadataRequest::API_KEY, METADATA_SUPPORTED)?;
        let mut request = MetadataRequest::default();
        request.topics = Some(
            topics
                .iter()
                .map(|name| {
                    let mut topic = MetadataRequestTopic::default();
                    topic.name = Some((*name).to_owned());
                    topic
                })
                .collect(),
        );
        request.allow_auto_topic_creation = false;
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(MetadataRequest::API_KEY, version, &body)
            .await?;
        let resp = MetadataResponse::decode(&mut resp, version)?;

        let brokers: HashMap<i32, BrokerInfo> = resp
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
        let mut refreshed = Vec::new();
        for topic in &resp.topics {
            let Some(name) = &topic.name else { continue };
            let code = ErrorCode(topic.error_code);
            if !code.is_ok() {
                return Err(ClientError::Broker(code));
            }
            refreshed.push((
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
            ));
        }

        let mut state = self.state();
        state.brokers = brokers;
        for (name, partitions) in refreshed {
            state.topics.insert(name, partitions);
        }
        Ok(())
    }

    /// The connection this cluster originally bootstrapped through.
    ///
    /// Frozen at connect time: if that broker dies this handle stays
    /// dead. Prefer [`Cluster::control_broker`], which redials and fails
    /// over, for admin calls and probes.
    pub fn bootstrap_broker(&self) -> &Broker {
        &self.inner.initial
    }

    /// A live control-plane connection, for requests that need no
    /// routing (admin calls, probes). Redials on first use after a
    /// failure, trying the configured bootstrap servers and every
    /// currently-known broker until one answers.
    pub async fn control_broker(&self) -> Result<Broker, ClientError> {
        if let Some(broker) = self.state().control.clone() {
            return Ok(broker);
        }
        // Candidates: the configured bootstrap list, then everything
        // metadata has taught us since — dedup'd, lock dropped before
        // any dialing.
        let mut candidates = self.inner.config.bootstrap_servers.clone();
        candidates.extend(self.state().brokers.values().map(BrokerInfo::addr));
        let mut seen = std::collections::HashSet::new();
        candidates.retain(|addr| seen.insert(addr.clone()));
        let mut last = String::from("no control-plane candidates");
        for addr in &candidates {
            match dial(addr, &self.inner.config).await {
                Ok(broker) => {
                    // A concurrent redial may have won; keep the winner.
                    let mut state = self.state();
                    return Ok(state.control.get_or_insert(broker).clone());
                }
                Err(e) => last = format!("{addr}: {e}"),
            }
        }
        Err(ClientError::Bootstrap(last))
    }

    /// Drop the control-plane connection; the next use redials.
    fn forget_control(&self) {
        self.state().control = None;
    }

    /// Known brokers, as of the last metadata refresh.
    pub fn brokers(&self) -> Vec<BrokerInfo> {
        self.state().brokers.values().cloned().collect()
    }

    /// A topic's partitions, as of the last metadata refresh.
    pub fn partitions(&self, topic: &str) -> Option<Vec<PartitionInfo>> {
        self.state().topics.get(topic).cloned()
    }

    /// The node id currently leading `topic[partition]`.
    pub fn leader_id(&self, topic: &str, partition: i32) -> Option<i32> {
        let state = self.state();
        let info = state
            .topics
            .get(topic)?
            .iter()
            .find(|p| p.index == partition)?;
        (info.leader_id >= 0).then_some(info.leader_id)
    }

    /// A negotiated connection to the broker with `node_id`, dialing on
    /// first use.
    pub async fn broker(&self, node_id: i32) -> Result<Broker, ClientError> {
        let info = {
            let state = self.state();
            if let Some(broker) = state.conns.get(&node_id) {
                return Ok(broker.clone());
            }
            state.brokers.get(&node_id).cloned()
        };
        let info = info.ok_or(ClientError::UnknownLeader {
            topic: format!("<broker {node_id}>"),
            partition: -1,
        })?;
        let broker = dial(&info.addr(), &self.inner.config).await?;
        // A concurrent dial to the same broker may have won the race;
        // keep whichever connection landed first.
        Ok(self.state().conns.entry(node_id).or_insert(broker).clone())
    }

    /// Forget cached leadership for `topic` — e.g. after a
    /// NOT_LEADER_OR_FOLLOWER — so the next lookup refreshes.
    pub fn mark_stale(&self, topic: &str) {
        self.state().topics.remove(topic);
    }

    /// Drop the pooled connection to `node_id` (e.g. after it failed);
    /// the next use redials.
    pub fn forget_broker(&self, node_id: i32) {
        self.state().conns.remove(&node_id);
    }

    /// A negotiated connection to the current leader of
    /// `topic[partition]`, refreshing metadata once if leadership is
    /// unknown.
    pub async fn partition_leader(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Broker, ClientError> {
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

/// FindCoordinator versions this client speaks: the single-key shape
/// (v4+ switches to batched keys).
const FIND_COORDINATOR_SUPPORTED: (i16, i16) = (0, 3);

impl Cluster {
    /// A negotiated connection to `group`'s coordinator, discovering it
    /// via FindCoordinator on first use.
    pub async fn coordinator(&self, group: &str) -> Result<Broker, ClientError> {
        let cached = self.state().coordinators.get(group).copied();
        let node_id = match cached {
            Some(node_id) => node_id,
            None => {
                // A dead control connection fails over like metadata does.
                let broker = self.control_broker().await?;
                match self.find_coordinator(&broker, group).await {
                    Err(e) if is_control_failure(&e) => {
                        self.forget_control();
                        let broker = self.control_broker().await?;
                        self.find_coordinator(&broker, group).await?
                    }
                    other => other?,
                }
            }
        };
        self.broker(node_id).await
    }

    async fn find_coordinator(&self, broker: &Broker, group: &str) -> Result<i32, ClientError> {
        let version = broker
            .ranges
            .pick(FindCoordinatorRequest::API_KEY, FIND_COORDINATOR_SUPPORTED)?;
        let mut request = FindCoordinatorRequest::default();
        request.key = group.to_owned();
        request.key_type = 0; // group coordinator
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(FindCoordinatorRequest::API_KEY, version, &body)
            .await?;
        let resp = FindCoordinatorResponse::decode(&mut resp, version)?;
        let code = ErrorCode(resp.error_code);
        if !code.is_ok() {
            return Err(ClientError::Broker(code));
        }
        let mut state = self.state();
        // The response names the coordinator's endpoint directly;
        // make it dialable even before any metadata refresh.
        state.brokers.insert(
            resp.node_id,
            BrokerInfo {
                node_id: resp.node_id,
                host: resp.host.clone(),
                port: resp.port,
            },
        );
        state.coordinators.insert(group.to_owned(), resp.node_id);
        Ok(resp.node_id)
    }

    /// Forget `group`'s discovered coordinator — e.g. after
    /// NOT_COORDINATOR — so the next use rediscovers it.
    pub fn forget_coordinator(&self, group: &str) {
        let mut state = self.state();
        if let Some(node_id) = state.coordinators.remove(group) {
            state.conns.remove(&node_id);
        }
    }
}

/// CreateTopics versions this client speaks: v2+ for the per-topic error
/// message, v7 the last before topic-id responses.
const CREATE_TOPICS_SUPPORTED: (i16, i16) = (2, 7);

/// Broker-side deadline for a CreateTopics request.
const CREATE_TOPICS_TIMEOUT_MS: i32 = 30_000;

impl Cluster {
    /// Create `name` with `partitions` partitions at `replication_factor`,
    /// through the control-plane connection.
    ///
    /// Any per-topic error — including `TOPIC_ALREADY_EXISTS` — surfaces
    /// as [`ClientError::Broker`]; callers that tolerate an existing
    /// topic can match on the code.
    pub async fn create_topic(
        &self,
        name: &str,
        partitions: i32,
        replication_factor: i16,
    ) -> Result<(), ClientError> {
        let broker = self.control_broker().await?;
        match self
            .create_topic_via(&broker, name, partitions, replication_factor)
            .await
        {
            Err(e) if is_control_failure(&e) => {
                self.forget_control();
                let broker = self.control_broker().await?;
                self.create_topic_via(&broker, name, partitions, replication_factor)
                    .await
            }
            other => other,
        }
    }

    async fn create_topic_via(
        &self,
        broker: &Broker,
        name: &str,
        partitions: i32,
        replication_factor: i16,
    ) -> Result<(), ClientError> {
        let version = broker
            .ranges
            .pick(CreateTopicsRequest::API_KEY, CREATE_TOPICS_SUPPORTED)?;
        let mut creatable = CreatableTopic::default();
        creatable.name = name.to_owned();
        creatable.num_partitions = partitions;
        creatable.replication_factor = replication_factor;
        let mut request = CreateTopicsRequest::default();
        request.topics = vec![creatable];
        request.timeout_ms = CREATE_TOPICS_TIMEOUT_MS;
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(CreateTopicsRequest::API_KEY, version, &body)
            .await?;
        let resp = CreateTopicsResponse::decode(&mut resp, version)?;
        let entry = resp.topics.iter().find(|t| t.name == name).ok_or_else(|| {
            ClientError::ProtocolViolation(format!("create topics response omits {name}"))
        })?;
        let code = ErrorCode(entry.error_code);
        if code.is_ok() {
            Ok(())
        } else {
            Err(ClientError::Broker(code))
        }
    }
}

/// True when the control-plane connection itself failed — closed under
/// us, an I/O error, or a client-side timeout — as opposed to the
/// broker answering with an error. The remedy is dropping the
/// connection and failing over, not resending.
fn is_control_failure(e: &ClientError) -> bool {
    matches!(
        e,
        ClientError::ConnectionClosed | ClientError::Io(_) | ClientError::Timeout(_)
    )
}

/// One full connection establishment — TCP, TLS, ApiVersions, SASL —
/// bounded by [`ClientConfig::connect_timeout`].
async fn dial(addr: &str, config: &ClientConfig) -> Result<Broker, ClientError> {
    tokio::time::timeout(config.connect_timeout, async {
        let conn = Connection::connect(addr, config).await?;
        let ranges = conn.negotiate().await?;
        #[cfg(feature = "sasl")]
        if let Some(sasl) = &config.sasl {
            crate::sasl::authenticate(&conn, &ranges, sasl).await?;
        }
        Ok(Broker { conn, ranges })
    })
    .await
    .unwrap_or(Err(ClientError::Timeout("connect")))
}
