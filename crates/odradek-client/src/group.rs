//! Classic consumer-group membership: join, sync, heartbeat, leave.
//!
//! Explicit and caller-driven, like the rest of this crate: [`GroupMember::join`]
//! runs the join → (leader assigns) → sync dance and returns a live
//! membership; the caller heartbeats on its own schedule and calls
//! [`GroupMember::rejoin`] when a heartbeat reports a rebalance. There is
//! no background task to fight over ownership with.
//!
//! The member subscribes with the `range` assignor. When elected leader
//! it computes the whole group's assignment (Kafka brokers treat the
//! embedded subscription/assignment bytes as opaque — assignment is the
//! leader's job in the classic protocol).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::consumer_protocol_assignment::{
    ConsumerProtocolAssignment, TopicPartition,
};
use odradek_protocol::messages::consumer_protocol_subscription::ConsumerProtocolSubscription;
use odradek_protocol::messages::heartbeat_request::HeartbeatRequest;
use odradek_protocol::messages::heartbeat_response::HeartbeatResponse;
use odradek_protocol::messages::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use odradek_protocol::messages::join_group_response::JoinGroupResponse;
use odradek_protocol::messages::leave_group_request::{LeaveGroupRequest, MemberIdentity};
use odradek_protocol::messages::leave_group_response::LeaveGroupResponse;
use odradek_protocol::messages::sync_group_request::{
    SyncGroupRequest, SyncGroupRequestAssignment,
};
use odradek_protocol::messages::sync_group_response::SyncGroupResponse;

use crate::cluster::Cluster;
use crate::error::ClientError;

/// JoinGroup versions this member speaks: v4+ for the MEMBER_ID_REQUIRED
/// handshake.
const JOIN_SUPPORTED: (i16, i16) = (4, JoinGroupRequest::MAX_VERSION);
const SYNC_SUPPORTED: (i16, i16) = (3, SyncGroupRequest::MAX_VERSION);
const HEARTBEAT_SUPPORTED: (i16, i16) = (0, HeartbeatRequest::MAX_VERSION);
const LEAVE_SUPPORTED: (i16, i16) = (0, LeaveGroupRequest::MAX_VERSION);

const PROTOCOL_TYPE: &str = "consumer";
const ASSIGNOR: &str = "range";

/// Membership knobs.
#[derive(Debug, Clone)]
pub struct GroupConfig {
    /// The coordinator evicts a member silent for this long.
    pub session_timeout_ms: i32,
    /// How long a rebalance waits for all members to rejoin.
    pub rebalance_timeout_ms: i32,
    /// Attempts for each membership operation (join, sync, heartbeat).
    pub max_attempts: u32,
    /// Pause between attempts.
    pub retry_backoff: Duration,
}

impl Default for GroupConfig {
    fn default() -> Self {
        GroupConfig {
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            max_attempts: 20,
            retry_backoff: Duration::from_millis(250),
        }
    }
}

/// What a heartbeat learned about the membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatStatus {
    /// All good; keep consuming.
    Stable,
    /// The group is rebalancing: call [`GroupMember::rejoin`] promptly.
    RebalanceInProgress,
    /// The coordinator no longer knows this member (evicted or fenced):
    /// rejoin starts a fresh membership.
    Evicted,
}

/// A live membership in a consumer group.
#[derive(Debug)]
pub struct GroupMember {
    cluster: Cluster,
    config: GroupConfig,
    group_id: String,
    topics: Vec<String>,
    member_id: String,
    generation_id: i32,
    is_leader: bool,
    assignment: Vec<(String, Vec<i32>)>,
}

impl GroupMember {
    /// Join `group_id` subscribed to `topics`, completing the initial
    /// join/sync round before returning.
    pub async fn join(
        cluster: Cluster,
        group_id: &str,
        topics: &[&str],
        config: GroupConfig,
    ) -> Result<GroupMember, ClientError> {
        let mut member = GroupMember {
            cluster,
            config,
            group_id: group_id.to_owned(),
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
            member_id: String::new(),
            generation_id: -1,
            is_leader: false,
            assignment: Vec::new(),
        };
        member.rejoin().await?;
        Ok(member)
    }

    /// The partitions assigned to this member in the current generation.
    pub fn assignment(&self) -> &[(String, Vec<i32>)] {
        &self.assignment
    }

    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    /// The underlying cluster, e.g. for metadata queries.
    pub fn cluster(&mut self) -> &mut Cluster {
        &mut self.cluster
    }

    /// Run one join/sync round, updating generation and assignment.
    pub async fn rejoin(&mut self) -> Result<(), ClientError> {
        let mut last = None;
        for round in 0..self.config.max_attempts {
            if round > 0 {
                tokio::time::sleep(self.config.retry_backoff).await;
            }
            match self.join_round().await {
                Ok(()) => return Ok(()),
                Err(e) if e.is_retriable() => {
                    self.cluster.forget_coordinator(&self.group_id);
                    last = Some(e);
                }
                // The join/sync dance's own transient states.
                Err(ClientError::Broker(code))
                    if code == ErrorCode::REBALANCE_IN_PROGRESS
                        || code == ErrorCode::ILLEGAL_GENERATION =>
                {
                    last = Some(ClientError::Broker(code));
                }
                Err(ClientError::Broker(code)) if code == ErrorCode::UNKNOWN_MEMBER_ID => {
                    self.member_id.clear();
                    last = Some(ClientError::Broker(code));
                }
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or(ClientError::ConnectionClosed))
    }

    async fn join_round(&mut self) -> Result<(), ClientError> {
        let subscription = encode_subscription(&self.topics);
        let broker = self.cluster.coordinator(&self.group_id).await?.clone();
        let join_version = broker
            .ranges
            .pick(JoinGroupRequest::API_KEY, JOIN_SUPPORTED)?;

        let mut join = JoinGroupRequest {
            group_id: self.group_id.clone(),
            session_timeout_ms: self.config.session_timeout_ms,
            rebalance_timeout_ms: self.config.rebalance_timeout_ms,
            member_id: self.member_id.clone(),
            group_instance_id: None,
            protocol_type: PROTOCOL_TYPE.into(),
            protocols: vec![JoinGroupRequestProtocol {
                name: ASSIGNOR.into(),
                metadata: subscription.clone(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut resp = self.join_once(&broker, &join, join_version).await?;
        if ErrorCode(resp.error_code) == ErrorCode::MEMBER_ID_REQUIRED {
            // The coordinator minted us an id; rejoin with it at once.
            self.member_id = resp.member_id.clone();
            join.member_id = resp.member_id.clone();
            resp = self.join_once(&broker, &join, join_version).await?;
        }
        let code = ErrorCode(resp.error_code);
        if !code.is_ok() {
            return Err(ClientError::Broker(code));
        }

        self.member_id = resp.member_id.clone();
        self.generation_id = resp.generation_id;
        self.is_leader = resp.leader == resp.member_id;

        // The leader computes the whole group's assignment.
        let assignments = if self.is_leader {
            let mut subscriptions = Vec::new();
            for m in &resp.members {
                subscriptions.push((m.member_id.clone(), decode_subscription(&m.metadata)?));
            }
            let all_topics: BTreeSet<String> = subscriptions
                .iter()
                .flat_map(|(_, topics)| topics.iter().cloned())
                .collect();
            let topic_refs: Vec<&str> = all_topics.iter().map(String::as_str).collect();
            self.cluster.refresh_metadata(&topic_refs).await?;
            let mut counts = Vec::new();
            for topic in &all_topics {
                let n = self
                    .cluster
                    .partitions(topic)
                    .map_or(0, |p| i32::try_from(p.len()).unwrap_or(i32::MAX));
                counts.push((topic.clone(), n));
            }
            range_assign(&counts, &subscriptions)
                .into_iter()
                .map(|(member_id, parts)| SyncGroupRequestAssignment {
                    member_id,
                    assignment: encode_assignment(&parts),
                    ..Default::default()
                })
                .collect()
        } else {
            Vec::new()
        };

        let sync_version = broker
            .ranges
            .pick(SyncGroupRequest::API_KEY, SYNC_SUPPORTED)?;
        let sync = SyncGroupRequest {
            group_id: self.group_id.clone(),
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: None,
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(ASSIGNOR.into()),
            assignments,
            ..Default::default()
        };
        let mut body = BytesMut::new();
        sync.encode(&mut body, sync_version)?;
        let mut resp = broker
            .conn
            .request(SyncGroupRequest::API_KEY, sync_version, &body)
            .await?;
        let resp = SyncGroupResponse::decode(&mut resp, sync_version)?;
        let code = ErrorCode(resp.error_code);
        if !code.is_ok() {
            return Err(ClientError::Broker(code));
        }
        self.assignment = decode_assignment(&resp.assignment)?;
        Ok(())
    }

    async fn join_once(
        &self,
        broker: &crate::cluster::Broker,
        join: &JoinGroupRequest,
        version: i16,
    ) -> Result<JoinGroupResponse, ClientError> {
        let mut body = BytesMut::new();
        join.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(JoinGroupRequest::API_KEY, version, &body)
            .await?;
        Ok(JoinGroupResponse::decode(&mut resp, version)?)
    }

    /// Tell the coordinator this member is alive; the caller should do
    /// this well within the session timeout.
    pub async fn heartbeat(&mut self) -> Result<HeartbeatStatus, ClientError> {
        let mut last = None;
        for round in 0..self.config.max_attempts {
            if round > 0 {
                tokio::time::sleep(self.config.retry_backoff).await;
            }
            let broker = self.cluster.coordinator(&self.group_id).await?.clone();
            let version = broker
                .ranges
                .pick(HeartbeatRequest::API_KEY, HEARTBEAT_SUPPORTED)?;
            let request = HeartbeatRequest {
                group_id: self.group_id.clone(),
                generation_id: self.generation_id,
                member_id: self.member_id.clone(),
                ..Default::default()
            };
            let mut body = BytesMut::new();
            request.encode(&mut body, version)?;
            let resp = broker
                .conn
                .request(HeartbeatRequest::API_KEY, version, &body)
                .await
                .and_then(|mut b| Ok(HeartbeatResponse::decode(&mut b, version)?));
            let code = match resp {
                Ok(resp) => ErrorCode(resp.error_code),
                Err(e) if e.is_retriable() => {
                    self.cluster.forget_coordinator(&self.group_id);
                    last = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            };
            return Ok(match code {
                c if c.is_ok() => HeartbeatStatus::Stable,
                ErrorCode::REBALANCE_IN_PROGRESS => HeartbeatStatus::RebalanceInProgress,
                ErrorCode::UNKNOWN_MEMBER_ID | ErrorCode::ILLEGAL_GENERATION => {
                    self.member_id.clear();
                    HeartbeatStatus::Evicted
                }
                c if ClientError::Broker(c).is_retriable() => {
                    self.cluster.forget_coordinator(&self.group_id);
                    last = Some(ClientError::Broker(c));
                    continue;
                }
                c => return Err(ClientError::Broker(c)),
            });
        }
        Err(last.unwrap_or(ClientError::ConnectionClosed))
    }

    /// Leave the group cleanly, returning the cluster for reuse.
    pub async fn leave(mut self) -> Result<Cluster, ClientError> {
        if self.member_id.is_empty() {
            return Ok(self.cluster);
        }
        let broker = self.cluster.coordinator(&self.group_id).await?.clone();
        let version = broker
            .ranges
            .pick(LeaveGroupRequest::API_KEY, LEAVE_SUPPORTED)?;
        let request = LeaveGroupRequest {
            group_id: self.group_id.clone(),
            // v0-2 carries the flat member id, v3+ the members array; the
            // encode gates pick whichever the version uses.
            member_id: self.member_id.clone(),
            members: vec![MemberIdentity {
                member_id: self.member_id.clone(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(LeaveGroupRequest::API_KEY, version, &body)
            .await?;
        let resp = LeaveGroupResponse::decode(&mut resp, version)?;
        let code = ErrorCode(resp.error_code);
        // Being already forgotten is as good as having left.
        if !code.is_ok() && code != ErrorCode::UNKNOWN_MEMBER_ID {
            return Err(ClientError::Broker(code));
        }
        Ok(self.cluster)
    }
}

// --- consumer protocol payloads (version-prefixed, non-flexible) ------------

fn encode_subscription(topics: &[String]) -> Bytes {
    let body = ConsumerProtocolSubscription {
        topics: topics.to_vec(),
        ..Default::default()
    };
    let mut out = BytesMut::new();
    out.put_i16(0);
    body.encode(&mut out, 0).expect("v0 subscription encodes");
    out.freeze()
}

fn decode_subscription(data: &[u8]) -> Result<Vec<String>, ClientError> {
    let mut buf = Bytes::copy_from_slice(data);
    if buf.len() < 2 {
        return Err(ClientError::ProtocolViolation(
            "subscription metadata shorter than its version prefix".into(),
        ));
    }
    let version = buf.get_i16().min(ConsumerProtocolSubscription::MAX_VERSION);
    // Newer members may append fields; the prefix decodes, the rest is
    // deliberately ignored.
    let sub = ConsumerProtocolSubscription::decode(&mut buf, version.max(0))?;
    Ok(sub.topics)
}

fn encode_assignment(partitions: &[(String, Vec<i32>)]) -> Bytes {
    let body = ConsumerProtocolAssignment {
        assigned_partitions: partitions
            .iter()
            .map(|(topic, parts)| TopicPartition {
                topic: topic.clone(),
                partitions: parts.clone(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let mut out = BytesMut::new();
    out.put_i16(0);
    body.encode(&mut out, 0).expect("v0 assignment encodes");
    out.freeze()
}

fn decode_assignment(data: &[u8]) -> Result<Vec<(String, Vec<i32>)>, ClientError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let mut buf = Bytes::copy_from_slice(data);
    if buf.len() < 2 {
        return Err(ClientError::ProtocolViolation(
            "assignment shorter than its version prefix".into(),
        ));
    }
    let version = buf.get_i16().min(ConsumerProtocolAssignment::MAX_VERSION);
    let assignment = ConsumerProtocolAssignment::decode(&mut buf, version.max(0))?;
    Ok(assignment
        .assigned_partitions
        .into_iter()
        .map(|tp| (tp.topic, tp.partitions))
        .collect())
}

/// The `range` assignor: per topic, its subscribers (sorted by member
/// id) split the partitions into contiguous ranges, earlier members
/// taking the remainder. Matches Kafka's RangeAssignor so mixed groups
/// agree on the outcome.
fn range_assign(
    topic_partitions: &[(String, i32)],
    subscriptions: &[(String, Vec<String>)],
) -> BTreeMap<String, Vec<(String, Vec<i32>)>> {
    let mut out: BTreeMap<String, Vec<(String, Vec<i32>)>> = subscriptions
        .iter()
        .map(|(member, _)| (member.clone(), Vec::new()))
        .collect();
    for (topic, count) in topic_partitions {
        let mut members: Vec<&String> = subscriptions
            .iter()
            .filter(|(_, topics)| topics.contains(topic))
            .map(|(member, _)| member)
            .collect();
        members.sort();
        if members.is_empty() {
            continue;
        }
        let n = i32::try_from(members.len()).unwrap_or(i32::MAX);
        let per = count / n;
        let extra = count % n;
        let mut next = 0;
        for (i, member) in members.iter().enumerate() {
            let take = per
                + if i32::try_from(i).unwrap_or(i32::MAX) < extra {
                    1
                } else {
                    0
                };
            if take == 0 {
                continue;
            }
            let parts: Vec<i32> = (next..next + take).collect();
            next += take;
            out.get_mut(*member)
                .expect("member seeded above")
                .push((topic.clone(), parts));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_assignor_splits_like_kafka() {
        // 2 members on a 5-partition topic: first takes 3, second 2.
        let assignment = range_assign(
            &[("t".into(), 5)],
            &[
                ("member-b".into(), vec!["t".into()]),
                ("member-a".into(), vec!["t".into()]),
            ],
        );
        assert_eq!(assignment["member-a"], vec![("t".into(), vec![0, 1, 2])]);
        assert_eq!(assignment["member-b"], vec![("t".into(), vec![3, 4])]);
    }

    #[test]
    fn range_assignor_respects_subscriptions() {
        // Only subscribers of a topic share its partitions; a member
        // subscribed to nothing present still appears, empty.
        let assignment = range_assign(
            &[("x".into(), 2), ("y".into(), 1)],
            &[
                ("m1".into(), vec!["x".into()]),
                ("m2".into(), vec!["x".into(), "y".into()]),
                ("m3".into(), vec!["z".into()]),
            ],
        );
        assert_eq!(assignment["m1"], vec![("x".into(), vec![0])]);
        assert_eq!(
            assignment["m2"],
            vec![("x".into(), vec![1]), ("y".into(), vec![0])]
        );
        assert!(assignment["m3"].is_empty());
    }

    #[test]
    fn subscription_and_assignment_payloads_roundtrip() {
        let topics = vec!["alpha".to_owned(), "beta".to_owned()];
        assert_eq!(
            decode_subscription(&encode_subscription(&topics)).unwrap(),
            topics
        );

        let parts = vec![("alpha".to_owned(), vec![0, 2])];
        assert_eq!(
            decode_assignment(&encode_assignment(&parts)).unwrap(),
            parts
        );
        assert!(decode_assignment(b"").unwrap().is_empty());
    }
}
