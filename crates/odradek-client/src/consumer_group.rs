//! KIP-848 consumer-group membership: the next-generation protocol.
//!
//! Where the classic protocol ([`crate::group`]) runs a client-side
//! join → sync dance with leader-side assignment, KIP-848 collapses
//! everything into one API: the member describes itself in a
//! `ConsumerGroupHeartbeat`, and the *coordinator* computes and hands
//! down assignments, addressed by topic id and versioned by a member
//! epoch that fences stale members.
//!
//! Caller-driven, like the rest of this crate: [`ConsumerGroupMember::join`]
//! heartbeats from epoch 0 until the coordinator reconciles an
//! assignment; the caller then heartbeats on its own schedule — the
//! coordinator prescribes the cadence, see
//! [`ConsumerGroupMember::heartbeat_interval`] — and watches each
//! [`GroupEvent`] for assignment changes. Fencing is handled inside
//! [`ConsumerGroupMember::heartbeat`]: a fenced member rejoins from
//! epoch 0 with the same member id and reports what happened.
//!
//! Reconciliation is deliberately simplified for a caller-driven v1:
//! a new target assignment is adopted (and acknowledged on the next
//! heartbeat) immediately. Staged revocation — keep consuming retained
//! partitions while releasing revoked ones before acknowledging — is
//! future work; until then, stop fetching revoked partitions as soon
//! as a heartbeat reports [`GroupEvent::AssignmentChanged`].

use std::time::Duration;

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::consumer_group_heartbeat_request::{
    ConsumerGroupHeartbeatRequest, TopicPartitions,
};
use odradek_protocol::messages::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;

use crate::cluster::Cluster;
use crate::conn;
use crate::error::ClientError;
use crate::retry::{Attempt, or_forget_coordinator, retry_loop};

const HEARTBEAT_SUPPORTED: (i16, i16) = (
    ConsumerGroupHeartbeatRequest::MIN_VERSION,
    ConsumerGroupHeartbeatRequest::MAX_VERSION,
);

/// The member epoch that asks the coordinator to remove this member.
const LEAVE_EPOCH: i32 = -1;

/// Membership knobs for the KIP-848 protocol.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConsumerGroupConfig {
    /// How long a rebalance may wait for this member to reconcile.
    pub rebalance_timeout_ms: i32,
    /// Server-side assignor to request (`"uniform"`, `"range"`);
    /// `None` accepts the broker's default.
    pub server_assignor: Option<String>,
    /// Attempts for each membership operation.
    pub max_attempts: u32,
    /// Pause between attempts.
    pub retry_backoff: Duration,
}

impl Default for ConsumerGroupConfig {
    fn default() -> Self {
        ConsumerGroupConfig {
            rebalance_timeout_ms: 60_000,
            server_assignor: None,
            max_attempts: 20,
            retry_backoff: Duration::from_millis(250),
        }
    }
}

/// What one heartbeat learned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GroupEvent {
    /// Nothing changed; keep consuming.
    Stable,
    /// The coordinator handed down a new target assignment, already
    /// adopted — reread [`ConsumerGroupMember::assignment`] and stop
    /// fetching partitions no longer in it.
    AssignmentChanged,
    /// This member was fenced (its epoch went stale) and has rejoined
    /// from epoch 0; the assignment may have changed with it.
    Rejoined,
}

/// A live membership in a KIP-848 consumer group.
#[derive(Debug)]
pub struct ConsumerGroupMember {
    cluster: Cluster,
    config: ConsumerGroupConfig,
    group_id: String,
    topics: Vec<String>,
    member_id: String,
    member_epoch: i32,
    heartbeat_interval: Duration,
    /// The adopted target, by topic id, echoed to acknowledge.
    owned: Vec<([u8; 16], Vec<i32>)>,
    /// The adopted target, resolved to names for the caller.
    assignment: Vec<(String, Vec<i32>)>,
    /// Owned partitions changed since last heartbeat; echo them.
    ack_pending: bool,
}

impl ConsumerGroupMember {
    /// Join `group_id` subscribed to `topics`, heartbeating from epoch 0
    /// until the coordinator reconciles an assignment (possibly empty —
    /// a group can have more members than partitions).
    pub async fn join(
        cluster: Cluster,
        group_id: &str,
        topics: &[&str],
        config: ConsumerGroupConfig,
    ) -> Result<ConsumerGroupMember, ClientError> {
        let mut member = ConsumerGroupMember {
            cluster,
            config,
            group_id: group_id.to_owned(),
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
            // KIP-1082 (heartbeat v1): the client mints its own id and
            // keeps it across fencing, so the coordinator can correlate
            // reincarnations.
            member_id: generate_member_id()?,
            member_epoch: 0,
            heartbeat_interval: Duration::from_millis(500),
            owned: Vec::new(),
            assignment: Vec::new(),
            ack_pending: false,
        };
        // Epoch 0 heartbeats until the coordinator hands an assignment
        // down; each round waits the coordinator-prescribed interval.
        let mut assigned = false;
        for _ in 0..member.config.max_attempts {
            member.heartbeat_once_with_retries().await?;
            if assigned {
                return Ok(member);
            }
            // The assignment often arrives one beat after the epoch
            // bump; poll once more after each response.
            assigned = !member.owned.is_empty() || member.member_epoch > 0;
            if assigned && member.ack_pending {
                continue; // deliver the ack promptly
            }
            if assigned {
                return Ok(member);
            }
            tokio::time::sleep(member.heartbeat_interval).await;
        }
        Ok(member)
    }

    /// The partitions assigned to this member, by topic name.
    pub fn assignment(&self) -> &[(String, Vec<i32>)] {
        &self.assignment
    }

    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// The current member epoch — the fencing token offset commits
    /// carry (see [`ConsumerGroupMember::commit_offset`]).
    pub fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// The cadence the coordinator prescribed; heartbeat at least this
    /// often to stay in the group.
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// The underlying cluster, e.g. for metadata queries.
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// Tell the coordinator this member is alive, acknowledge any
    /// adopted assignment, and pick up new targets. Fencing rejoins
    /// automatically (same member id, epoch 0) and reports
    /// [`GroupEvent::Rejoined`].
    pub async fn heartbeat(&mut self) -> Result<GroupEvent, ClientError> {
        self.heartbeat_once_with_retries().await
    }

    /// The group this member belongs to.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// The offset last committed for `topic[partition]` under this
    /// member's group, or `None` when nothing was ever committed.
    ///
    /// Reading committed offsets is unchanged by KIP-848 — the same
    /// OffsetFetch, the same answer — but a member that can commit and
    /// cannot read back is an awkward half of an API, and resuming a
    /// partition is the first thing a consumer does with it.
    pub async fn committed_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, ClientError> {
        let (max_attempts, backoff) = (self.config.max_attempts, self.config.retry_backoff);
        retry_loop(&mut &*self, max_attempts, backoff, |this| {
            Box::pin(async move {
                let result =
                    crate::offsets::committed_once(&this.cluster, &this.group_id, topic, partition)
                        .await;
                or_forget_coordinator(&this.cluster, &this.group_id, result)
            })
        })
        .await
    }

    /// Commit `offset` for `topic[partition]` under this group,
    /// fenced by the current member epoch: a stale member's commit
    /// fails with `STALE_MEMBER_EPOCH` instead of clobbering.
    pub async fn commit_offset(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), ClientError> {
        let (max_attempts, backoff) = (self.config.max_attempts, self.config.retry_backoff);
        retry_loop(&mut &*self, max_attempts, backoff, |this| {
            Box::pin(async move {
                let identity = crate::offsets::CommitIdentity {
                    generation_id: this.member_epoch,
                    member_id: &this.member_id,
                };
                let result = crate::offsets::commit_once(
                    &this.cluster,
                    &this.group_id,
                    identity,
                    topic,
                    partition,
                    offset,
                )
                .await;
                or_forget_coordinator(&this.cluster, &this.group_id, result)
            })
        })
        .await
    }

    /// Leave the group cleanly.
    pub async fn leave(self) -> Result<(), ClientError> {
        let broker = self.cluster.coordinator(&self.group_id).await?;
        let version = broker
            .ranges
            .pick(ConsumerGroupHeartbeatRequest::API_KEY, HEARTBEAT_SUPPORTED)?;
        let mut request = ConsumerGroupHeartbeatRequest::default();
        request.group_id = self.group_id.clone();
        request.member_id = self.member_id.clone();
        request.member_epoch = LEAVE_EPOCH;
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(ConsumerGroupHeartbeatRequest::API_KEY, version, &body)
            .await?;
        let resp =
            conn::decode_body::<ConsumerGroupHeartbeatResponse>(&broker.conn, &mut resp, version)?;
        let code = ErrorCode(resp.error_code);
        // Being already forgotten is as good as having left.
        if !code.is_ok() && code != ErrorCode::UNKNOWN_MEMBER_ID {
            return Err(ClientError::Broker(code));
        }
        Ok(())
    }

    async fn heartbeat_once_with_retries(&mut self) -> Result<GroupEvent, ClientError> {
        let (max_attempts, backoff) = (self.config.max_attempts, self.config.retry_backoff);
        retry_loop(&mut *self, max_attempts, backoff, |this| {
            Box::pin(this.heartbeat_attempt())
        })
        .await
    }

    async fn heartbeat_attempt(&mut self) -> Attempt<GroupEvent> {
        let rejoining = self.member_epoch == 0;
        let broker = match self.cluster.coordinator(&self.group_id).await {
            Ok(broker) => broker,
            Err(e) => return or_forget_coordinator(&self.cluster, &self.group_id, Err(e)),
        };
        let sent = async {
            let version = broker
                .ranges
                .pick(ConsumerGroupHeartbeatRequest::API_KEY, HEARTBEAT_SUPPORTED)?;
            let mut request = ConsumerGroupHeartbeatRequest::default();
            request.group_id = self.group_id.clone();
            request.member_id = self.member_id.clone();
            request.member_epoch = self.member_epoch;
            if rejoining {
                // A full heartbeat: (re)state the whole subscription.
                request.rebalance_timeout_ms = self.config.rebalance_timeout_ms;
                request.subscribed_topic_names = Some(self.topics.clone());
                request.server_assignor = self.config.server_assignor.clone();
                request.topic_partitions = Some(Vec::new());
            } else if self.ack_pending {
                // Acknowledge the adopted target by echoing ownership.
                request.topic_partitions = Some(
                    self.owned
                        .iter()
                        .map(|(topic_id, partitions)| {
                            let mut tp = TopicPartitions::default();
                            tp.topic_id = *topic_id;
                            tp.partitions = partitions.clone();
                            tp
                        })
                        .collect(),
                );
            }
            let mut body = BytesMut::new();
            request.encode(&mut body, version)?;
            let mut resp = broker
                .conn
                .request(ConsumerGroupHeartbeatRequest::API_KEY, version, &body)
                .await?;
            conn::decode_body::<ConsumerGroupHeartbeatResponse>(&broker.conn, &mut resp, version)
        }
        .await;
        let resp: ConsumerGroupHeartbeatResponse = match sent {
            Ok(resp) => resp,
            Err(e) => return or_forget_coordinator(&self.cluster, &self.group_id, Err(e)),
        };

        let code = ErrorCode(resp.error_code);
        if code == ErrorCode::FENCED_MEMBER_EPOCH || code == ErrorCode::UNKNOWN_MEMBER_ID {
            // Fenced: restart from epoch 0 with the same member id; the
            // next round sends the full subscription again.
            self.member_epoch = 0;
            self.owned.clear();
            self.assignment.clear();
            self.ack_pending = false;
            return Attempt::Retry(ClientError::Broker(code));
        }
        if !code.is_ok() {
            return or_forget_coordinator(
                &self.cluster,
                &self.group_id,
                Err(ClientError::Broker(code)),
            );
        }

        if let Some(id) = resp
            .member_id
            .filter(|id| !id.is_empty() && *id != self.member_id)
        {
            // v0 coordinators mint the id server-side.
            self.member_id = id;
        }
        let was_rejoining = rejoining;
        self.member_epoch = resp.member_epoch;
        if resp.heartbeat_interval_ms > 0 {
            self.heartbeat_interval =
                Duration::from_millis(u64::from(resp.heartbeat_interval_ms.unsigned_abs()));
        }
        // The previous heartbeat carried our ack (or full state).
        self.ack_pending = false;

        let changed = match resp.assignment {
            // Null assignment = no change since we last heard.
            None => false,
            Some(target) => {
                let mut owned: Vec<([u8; 16], Vec<i32>)> = target
                    .topic_partitions
                    .into_iter()
                    .map(|tp| (tp.topic_id, tp.partitions))
                    .collect();
                owned.sort();
                let changed = owned != self.owned;
                if changed {
                    self.assignment = match self.resolve(&owned).await {
                        Ok(named) => named,
                        Err(e) => return Attempt::Fatal(e),
                    };
                    self.owned = owned;
                    self.ack_pending = true;
                }
                changed
            }
        };
        Attempt::Done(if was_rejoining && self.member_epoch > 0 {
            GroupEvent::Rejoined
        } else if changed {
            GroupEvent::AssignmentChanged
        } else {
            GroupEvent::Stable
        })
    }

    /// Resolve topic-id-addressed partitions to names via metadata.
    async fn resolve(
        &self,
        owned: &[([u8; 16], Vec<i32>)],
    ) -> Result<Vec<(String, Vec<i32>)>, ClientError> {
        let mut refreshed = false;
        let mut named = Vec::with_capacity(owned.len());
        for (topic_id, partitions) in owned {
            let name = match self.cluster.topic_name_by_id(*topic_id) {
                Some(name) => name,
                None if !refreshed => {
                    // The subscription bounds what we can be assigned;
                    // one refresh fills the id map.
                    let topics: Vec<&str> = self.topics.iter().map(String::as_str).collect();
                    self.cluster.refresh_metadata(&topics).await?;
                    refreshed = true;
                    self.cluster.topic_name_by_id(*topic_id).ok_or_else(|| {
                        ClientError::ProtocolViolation(format!(
                            "assigned topic id {topic_id:02x?} resolves to no subscribed topic"
                        ))
                    })?
                }
                None => {
                    return Err(ClientError::ProtocolViolation(format!(
                        "assigned topic id {topic_id:02x?} resolves to no subscribed topic"
                    )));
                }
            };
            named.push((name, partitions.clone()));
        }
        named.sort();
        Ok(named)
    }
}

/// A fresh, unique member id (a type-4 UUID in text form).
fn generate_member_id() -> Result<String, ClientError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|e| ClientError::ProtocolViolation(format!("entropy unavailable: {e}")))?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let b = bytes;
    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0],
        b[1],
        b[2],
        b[3],
        b[4],
        b[5],
        b[6],
        b[7],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15]
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_ids_are_uuid_shaped_and_unique() {
        let a = generate_member_id().unwrap();
        let b = generate_member_id().unwrap();
        assert_ne!(a, b);
        assert_eq!(a.len(), 36);
        assert_eq!(a.as_bytes()[14], b'4'); // version nibble
    }
}
