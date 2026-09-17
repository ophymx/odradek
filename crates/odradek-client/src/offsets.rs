//! Shared OffsetCommit/OffsetFetch plumbing, used by both the simple
//! consumer path (generation -1, empty member id — the coordinator does
//! no fencing) and the group-member path (real generation and member id,
//! so the coordinator fences zombies with ILLEGAL_GENERATION or
//! UNKNOWN_MEMBER_ID).

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use odradek_protocol::messages::offset_commit_response::OffsetCommitResponse;
use odradek_protocol::messages::offset_fetch_request::{
    OffsetFetchRequest, OffsetFetchRequestTopic,
};
use odradek_protocol::messages::offset_fetch_response::OffsetFetchResponse;

use crate::cluster::Cluster;
use crate::error::ClientError;

/// OffsetCommit versions this client speaks: the classic name-addressed
/// shape (v9+ carries member epochs for KIP-848 groups, v10 topic ids).
const OFFSET_COMMIT_SUPPORTED: (i16, i16) = (2, 8);

/// OffsetFetch versions this client speaks: the single-group shape
/// (v8+ switches to batched groups).
const OFFSET_FETCH_SUPPORTED: (i16, i16) = (1, 7);

/// The identity an OffsetCommit is issued under.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CommitIdentity<'a> {
    /// The member's current generation, or -1 for the simple-consumer
    /// path (no fencing).
    pub generation_id: i32,
    /// The member id, empty for the simple-consumer path.
    pub member_id: &'a str,
}

impl CommitIdentity<'static> {
    /// The simple-consumer identity: the coordinator applies no fencing.
    pub const SIMPLE: CommitIdentity<'static> = CommitIdentity {
        generation_id: -1,
        member_id: "",
    };
}

/// One OffsetCommit through `group`'s coordinator, no retries.
pub(crate) async fn commit_once(
    cluster: &Cluster,
    group: &str,
    identity: CommitIdentity<'_>,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<(), ClientError> {
    let broker = cluster.coordinator(group).await?;
    let version = broker
        .ranges
        .pick(OffsetCommitRequest::API_KEY, OFFSET_COMMIT_SUPPORTED)?;
    let mut request_partition = OffsetCommitRequestPartition::default();
    request_partition.partition_index = partition;
    request_partition.committed_offset = offset;
    request_partition.committed_leader_epoch = -1;
    let mut request_topic = OffsetCommitRequestTopic::default();
    request_topic.name = topic.to_owned();
    request_topic.partitions = vec![request_partition];
    let mut request = OffsetCommitRequest::default();
    request.group_id = group.to_owned();
    request.generation_id_or_member_epoch = identity.generation_id;
    request.member_id = identity.member_id.to_owned();
    request.group_instance_id = None;
    request.retention_time_ms = -1;
    request.topics = vec![request_topic];
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = broker
        .conn
        .request(OffsetCommitRequest::API_KEY, version, &body)
        .await?;
    let resp = OffsetCommitResponse::decode(&mut resp, version)?;
    let entry = resp
        .topics
        .iter()
        .find(|t| t.name == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
        .ok_or_else(|| {
            ClientError::ProtocolViolation(format!(
                "offset commit response omits {topic}[{partition}]"
            ))
        })?;
    let code = ErrorCode(entry.error_code);
    if code.is_ok() {
        Ok(())
    } else {
        Err(ClientError::Broker(code))
    }
}

/// One OffsetFetch through `group`'s coordinator, no retries. `None`
/// means nothing was ever committed.
pub(crate) async fn committed_once(
    cluster: &Cluster,
    group: &str,
    topic: &str,
    partition: i32,
) -> Result<Option<i64>, ClientError> {
    let broker = cluster.coordinator(group).await?;
    let version = broker
        .ranges
        .pick(OffsetFetchRequest::API_KEY, OFFSET_FETCH_SUPPORTED)?;
    let mut request_topic = OffsetFetchRequestTopic::default();
    request_topic.name = topic.to_owned();
    request_topic.partition_indexes = vec![partition];
    let mut request = OffsetFetchRequest::default();
    request.group_id = group.to_owned();
    request.topics = Some(vec![request_topic]);
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = broker
        .conn
        .request(OffsetFetchRequest::API_KEY, version, &body)
        .await?;
    let resp = OffsetFetchResponse::decode(&mut resp, version)?;
    let code = ErrorCode(resp.error_code);
    if !code.is_ok() {
        return Err(ClientError::Broker(code));
    }
    let entry = resp
        .topics
        .iter()
        .find(|t| t.name == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
        .ok_or_else(|| {
            ClientError::ProtocolViolation(format!(
                "offset fetch response omits {topic}[{partition}]"
            ))
        })?;
    let code = ErrorCode(entry.error_code);
    if !code.is_ok() {
        return Err(ClientError::Broker(code));
    }
    Ok((entry.committed_offset >= 0).then_some(entry.committed_offset))
}
