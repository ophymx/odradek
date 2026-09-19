//! Administrative calls: what groups exist, what a group is doing, and
//! what a topic is configured as.
//!
//! These are the questions an operator asks that produce and consume
//! cannot answer. They are separated from [`Cluster`]'s data path
//! because they route differently, and the routing is the part that is
//! easy to get quietly wrong:
//!
//! - **Listing groups asks every broker.** A broker answers only for
//!   the groups it coordinates, so a single-broker query returns a
//!   subset and looks like an answer. This asks them all and merges.
//! - **Describing a group asks its coordinator**, which is a different
//!   broker per group, so a batch is split by coordinator and rejoined.
//! - **Deleting a topic asks the controller**, which moves; the retry
//!   path treats `NOT_CONTROLLER` as "ask again after refetching".
//! - **Reading a topic's configuration** can be asked of anyone.
//!
//! Everything here is read-only except [`Cluster::delete_topics`].

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest};
use odradek_protocol::messages::delete_topics_response::DeleteTopicsResponse;
use odradek_protocol::messages::describe_configs_request::{
    DescribeConfigsRequest, DescribeConfigsResource,
};
use odradek_protocol::messages::describe_configs_response::DescribeConfigsResponse;
use odradek_protocol::messages::describe_groups_request::DescribeGroupsRequest;
use odradek_protocol::messages::describe_groups_response::DescribeGroupsResponse;
use odradek_protocol::messages::list_groups_request::ListGroupsRequest;
use odradek_protocol::messages::list_groups_response::ListGroupsResponse;

use crate::cluster::{Broker, Cluster};
use crate::conn;
use crate::error::ClientError;

/// Versions this client speaks for each admin api.
const LIST_GROUPS_SUPPORTED: (i16, i16) = (0, ListGroupsRequest::MAX_VERSION);
const DESCRIBE_GROUPS_SUPPORTED: (i16, i16) = (0, DescribeGroupsRequest::MAX_VERSION);
const DELETE_TOPICS_SUPPORTED: (i16, i16) = (
    DeleteTopicsRequest::MIN_VERSION,
    DeleteTopicsRequest::MAX_VERSION,
);
const DESCRIBE_CONFIGS_SUPPORTED: (i16, i16) = (
    DescribeConfigsRequest::MIN_VERSION,
    DescribeConfigsRequest::MAX_VERSION,
);

/// `DescribeConfigs` resource types, from the protocol's own numbering.
const RESOURCE_TOPIC: i8 = 2;
const RESOURCE_BROKER: i8 = 4;

/// One group, as a listing names it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct GroupListing {
    pub group_id: String,
    /// `consumer` for consumer groups; empty for groups a broker knows
    /// of but cannot classify.
    pub protocol_type: String,
    /// `Stable`, `PreparingRebalance`, `Empty`, and so on. Empty on
    /// broker versions that predate the field.
    pub state: String,
    /// The node that coordinates this group — which is the only broker
    /// that can describe it.
    pub coordinator_id: i32,
}

/// One group, described.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct GroupDescription {
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    /// The assignor the group settled on, for a classic consumer group.
    pub protocol: String,
    pub members: Vec<GroupMemberDescription>,
}

/// One member of a described group.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct GroupMemberDescription {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    /// The member's assignment, exactly as its leader encoded it.
    ///
    /// Opaque on purpose: the bytes belong to whatever assignor the
    /// group is using, and a broker — or this client — that parsed them
    /// would be guessing at a format it does not own. Decode with
    /// [`odradek_protocol::consumer_protocol`] if the group is using
    /// Kafka's own.
    pub assignment: bytes::Bytes,
}

/// One configuration entry of a topic or broker.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConfigEntry {
    pub name: String,
    /// `None` for a sensitive value the broker refuses to disclose —
    /// which is different from a value that is genuinely empty, and the
    /// distinction is why this is an `Option` rather than a `String`.
    pub value: Option<String>,
    /// Where the value came from, in the protocol's own numbering.
    /// `5` is the built-in default; anything else was set somewhere.
    pub source: i8,
    /// Whether the broker refuses to change this value at runtime.
    pub read_only: bool,
    /// Whether the broker considers this value sensitive; such a value
    /// is reported as `None`.
    pub is_sensitive: bool,
}

impl Cluster {
    /// Every consumer group the cluster knows about.
    ///
    /// Asks **every** broker, because each answers only for the groups
    /// it coordinates: querying one returns a subset that looks exactly
    /// like a complete answer. The results carry the coordinator, since
    /// that is what [`Cluster::describe_groups`] needs and what an
    /// operator chasing a specific group wants next.
    ///
    /// A broker that fails to answer fails the whole call rather than
    /// silently shrinking the list — a partial group listing presented
    /// as a full one is worse than an error.
    pub async fn list_groups(&self) -> Result<Vec<GroupListing>, ClientError> {
        // Refresh first. The broker list is populated by metadata, and
        // on a handle that has only ever created a topic it is empty —
        // so iterating it would ask nobody and return an empty list
        // that looks exactly like "this cluster has no groups". That is
        // the failure this method's own documentation warns about, and
        // it is reachable from inside.
        self.refresh_metadata(&[]).await?;
        let brokers = self.brokers();
        if brokers.is_empty() {
            return Err(ClientError::ProtocolViolation(
                "metadata names no brokers, so no group listing can be complete".into(),
            ));
        }

        let mut listings = Vec::new();
        for info in brokers {
            let broker = self.broker(info.node_id).await?;
            let version = broker
                .ranges
                .pick(ListGroupsRequest::API_KEY, LIST_GROUPS_SUPPORTED)?;
            let request = ListGroupsRequest::default();
            let mut body = BytesMut::new();
            request.encode(&mut body, version)?;
            let mut resp = broker
                .conn
                .request(ListGroupsRequest::API_KEY, version, &body)
                .await?;
            let resp = conn::decode_body::<ListGroupsResponse>(&mut resp, version)?;
            let code = ErrorCode(resp.error_code);
            if !code.is_ok() {
                return Err(ClientError::Broker(code));
            }
            listings.extend(resp.groups.into_iter().map(|g| GroupListing {
                group_id: g.group_id,
                protocol_type: g.protocol_type,
                state: g.group_state,
                coordinator_id: info.node_id,
            }));
        }
        // Brokers do not overlap, but ordering across them is arbitrary
        // and a stable answer is easier to diff.
        listings.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        Ok(listings)
    }

    /// Describe `groups`: state, protocol, and members with their
    /// assignments.
    ///
    /// Each group is asked of its own coordinator, so this batches by
    /// coordinator rather than sending one request per group. A group
    /// the cluster has never heard of comes back described as `Dead`
    /// rather than as an error, which is the broker's answer and not
    /// this client's opinion.
    pub async fn describe_groups(
        &self,
        groups: &[&str],
    ) -> Result<Vec<GroupDescription>, ClientError> {
        // Group ids by coordinator: one request per broker involved.
        let mut by_coordinator: Vec<(i32, Broker, Vec<String>)> = Vec::new();
        for group in groups {
            // Resolve first, then read the cached node id: the lookup is
            // what populates it.
            let broker = self.coordinator(group).await?;
            let node_id = self.coordinator_id(group).unwrap_or(i32::MIN);
            match by_coordinator.iter_mut().find(|(id, _, _)| *id == node_id) {
                Some((_, _, ids)) => ids.push((*group).to_owned()),
                None => by_coordinator.push((node_id, broker, vec![(*group).to_owned()])),
            }
        }

        let mut described = Vec::new();
        for (_, broker, ids) in by_coordinator {
            let version = broker
                .ranges
                .pick(DescribeGroupsRequest::API_KEY, DESCRIBE_GROUPS_SUPPORTED)?;
            let mut request = DescribeGroupsRequest::default();
            request.groups = ids;
            let mut body = BytesMut::new();
            request.encode(&mut body, version)?;
            let mut resp = broker
                .conn
                .request(DescribeGroupsRequest::API_KEY, version, &body)
                .await?;
            let resp = conn::decode_body::<DescribeGroupsResponse>(&mut resp, version)?;
            for group in resp.groups {
                let code = ErrorCode(group.error_code);
                if !code.is_ok() {
                    return Err(ClientError::Broker(code));
                }
                described.push(GroupDescription {
                    group_id: group.group_id,
                    state: group.group_state,
                    protocol_type: group.protocol_type,
                    protocol: group.protocol_data,
                    members: group
                        .members
                        .into_iter()
                        .map(|m| GroupMemberDescription {
                            member_id: m.member_id,
                            client_id: m.client_id,
                            client_host: m.client_host,
                            assignment: m.member_assignment,
                        })
                        .collect(),
                });
            }
        }
        Ok(described)
    }

    /// Delete `topics`, and the data in them.
    ///
    /// Irreversible, and routed to the controller — which moves, so a
    /// `NOT_CONTROLLER` answer is retried through a refreshed control
    /// connection rather than reported. A per-topic failure, including
    /// `UNKNOWN_TOPIC_OR_PARTITION` for a topic that was not there,
    /// surfaces as [`ClientError::Broker`]; callers that tolerate a
    /// missing topic can match on the code.
    pub async fn delete_topics(&self, topics: &[&str]) -> Result<(), ClientError> {
        let broker = self.control_broker().await?;
        match self.delete_topics_via(&broker, topics).await {
            Err(e) if e.is_retriable() => {
                self.forget_control();
                let broker = self.control_broker().await?;
                self.delete_topics_via(&broker, topics).await
            }
            other => other,
        }
    }

    async fn delete_topics_via(&self, broker: &Broker, topics: &[&str]) -> Result<(), ClientError> {
        let version = broker
            .ranges
            .pick(DeleteTopicsRequest::API_KEY, DELETE_TOPICS_SUPPORTED)?;
        let mut request = DeleteTopicsRequest::default();
        // v6 moved names into a `topics` array of (name, id) so a topic
        // can be deleted by either; below that the flat name list is the
        // only form.
        if version >= 6 {
            request.topics = topics
                .iter()
                .map(|name| {
                    let mut state = DeleteTopicState::default();
                    state.name = Some((*name).to_owned());
                    state
                })
                .collect();
        } else {
            request.topic_names = topics.iter().map(|t| (*t).to_owned()).collect();
        }
        request.timeout_ms = 30_000;
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(DeleteTopicsRequest::API_KEY, version, &body)
            .await?;
        let resp = conn::decode_body::<DeleteTopicsResponse>(&mut resp, version)?;
        for result in &resp.responses {
            let code = ErrorCode(result.error_code);
            if !code.is_ok() {
                return Err(ClientError::Broker(code));
            }
        }
        // The topics are gone; anything cached about them is now a lie.
        self.refresh_metadata(topics).await.ok();
        Ok(())
    }

    /// A topic's configuration, as the cluster has it.
    pub async fn describe_topic_config(
        &self,
        topic: &str,
    ) -> Result<Vec<ConfigEntry>, ClientError> {
        let broker = self.control_broker().await?;
        self.describe_config(&broker, RESOURCE_TOPIC, topic).await
    }

    /// One broker's configuration, asked of that broker.
    ///
    /// Broker configuration is per broker, so this must be asked of the
    /// one in question — a different broker would answer about itself.
    pub async fn describe_broker_config(
        &self,
        node_id: i32,
    ) -> Result<Vec<ConfigEntry>, ClientError> {
        let broker = self.broker(node_id).await?;
        self.describe_config(&broker, RESOURCE_BROKER, &node_id.to_string())
            .await
    }

    async fn describe_config(
        &self,
        broker: &Broker,
        resource_type: i8,
        resource_name: &str,
    ) -> Result<Vec<ConfigEntry>, ClientError> {
        let version = broker
            .ranges
            .pick(DescribeConfigsRequest::API_KEY, DESCRIBE_CONFIGS_SUPPORTED)?;
        let mut resource = DescribeConfigsResource::default();
        resource.resource_type = resource_type;
        resource.resource_name = resource_name.to_owned();
        // `None` rather than an empty list: empty means "these named
        // keys", which is none of them.
        resource.configuration_keys = None;
        let mut request = DescribeConfigsRequest::default();
        request.resources = vec![resource];
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(DescribeConfigsRequest::API_KEY, version, &body)
            .await?;
        let resp = conn::decode_body::<DescribeConfigsResponse>(&mut resp, version)?;
        let result = resp.results.into_iter().next().ok_or_else(|| {
            ClientError::ProtocolViolation("DescribeConfigs answered no resource".into())
        })?;
        let code = ErrorCode(result.error_code);
        if !code.is_ok() {
            return Err(ClientError::Broker(code));
        }
        let mut entries: Vec<ConfigEntry> = result
            .configs
            .into_iter()
            .map(|c| ConfigEntry {
                name: c.name,
                // A sensitive value arrives as null, and null is also
                // how a genuinely unset value arrives. The flag is what
                // distinguishes them.
                value: c.value,
                source: c.config_source,
                read_only: c.read_only,
                is_sensitive: c.is_sensitive,
            })
            .collect();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(entries)
    }
}
