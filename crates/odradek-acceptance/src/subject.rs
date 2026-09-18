//! A reference subject server with fault injection.
//!
//! This is the suite's calibration instrument. Run with no faults it is a
//! minimal conformant ApiVersions responder; each [`Fault`] makes it commit
//! exactly one protocol violation. The sensitivity tests assert a 1:1
//! mapping between faults and the checks that claim to detect them — a
//! check that cannot catch its own targeted fault is vacuous, and a check
//! that fails against the compliant subject is wrong.

use std::collections::HashMap;
use std::io;

use bytes::{Bytes, BytesMut};
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use odradek_protocol::messages::consumer_group_heartbeat_response::{
    Assignment, ConsumerGroupHeartbeatResponse, TopicPartitions,
};
use odradek_protocol::messages::create_topics_request::CreateTopicsRequest;
use odradek_protocol::messages::create_topics_response::{
    CreatableTopicResult, CreateTopicsResponse,
};
use odradek_protocol::messages::fetch_request::FetchRequest;
use odradek_protocol::messages::fetch_response::{
    FetchResponse, FetchableTopicResponse, PartitionData,
};
use odradek_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use odradek_protocol::messages::find_coordinator_response::{Coordinator, FindCoordinatorResponse};
use odradek_protocol::messages::heartbeat_request::HeartbeatRequest;
use odradek_protocol::messages::heartbeat_response::HeartbeatResponse;
use odradek_protocol::messages::join_group_request::JoinGroupRequest;
use odradek_protocol::messages::join_group_response::{JoinGroupResponse, JoinGroupResponseMember};
use odradek_protocol::messages::leave_group_request::LeaveGroupRequest;
use odradek_protocol::messages::leave_group_response::LeaveGroupResponse;
use odradek_protocol::messages::list_offsets_request::ListOffsetsRequest;
use odradek_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use odradek_protocol::messages::metadata_request::{self, MetadataRequest};
use odradek_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponseTopic,
};
use odradek_protocol::messages::offset_commit_request::OffsetCommitRequest;
use odradek_protocol::messages::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use odradek_protocol::messages::offset_fetch_request::OffsetFetchRequest;
use odradek_protocol::messages::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
    OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use odradek_protocol::messages::produce_request::ProduceRequest;
use odradek_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use odradek_protocol::messages::sync_group_request::SyncGroupRequest;
use odradek_protocol::messages::sync_group_response::SyncGroupResponse;
use odradek_protocol::records;
use odradek_protocol::{ErrorCode, frame, header};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// The newest ApiVersions version the subject supports.
pub const MAX_SUPPORTED_API_VERSIONS: i16 = ApiVersionsRequest::MAX_VERSION;

/// The single broker this subject presents itself as.
const BROKER_NODE_ID: i32 = 1;
/// ListOffsets sentinel timestamps: the log start and the log end.
const EARLIEST_TIMESTAMP: i64 = -2;
const LATEST_TIMESTAMP: i64 = -1;
/// "This group has committed nothing for this partition" — not an error.
const UNSET_OFFSET: i64 = -1;
/// The version at which FindCoordinator began batching keys.
const FIND_COORDINATOR_BATCHED: i16 = 4;
/// The version at which OffsetFetch began batching groups.
const OFFSET_FETCH_BATCHED: i16 = 8;

/// A single deliberate protocol violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Echo `correlation_id + 1` instead of the request's.
    WrongCorrelationEcho,
    /// Advertise an api key whose range has `min > max`.
    InvertedVersionRange,
    /// Do not advertise the ApiVersions api itself.
    OmitApiVersionsKey,
    /// Answer an unsupported ApiVersions version with error NONE instead of
    /// UNSUPPORTED_VERSION.
    WrongErrorOnUnsupportedVersion,
    /// Encode the UNSUPPORTED_VERSION error body flexibly (v3) instead of
    /// the mandated v0.
    ErrorBodyNotV0,
    /// In the UNSUPPORTED_VERSION error response, advertise a different
    /// ApiVersions max than the one advertised normally.
    AdvertiseWrongMaxInError,
    /// Append junk bytes inside the ApiVersions response frame after the
    /// body.
    TrailingGarbage,
    /// Append junk bytes inside Fetch response frames after the body —
    /// proof the consolidated exchange path polices trailing bytes on the
    /// produce/fetch flows, not just ApiVersions and Metadata.
    FetchTrailingGarbage,
    /// Use response header v1 (with tagged fields) for flexible ApiVersions
    /// requests, violating the always-v0 quirk.
    FlexibleHeaderOnV3,
    /// Answer Metadata with an empty brokers list.
    MetadataEmptyBrokers,
    /// Include a topic in the Metadata response though none was requested.
    MetadataUnrequestedTopic,
    /// Use response header v0 (no tagged fields) for flexible (v9+)
    /// Metadata requests — the inverse of the ApiVersions quirk.
    MetadataNonFlexibleHeader,
    /// Answer a produce with `base_offset` one higher than assigned.
    ProduceWrongBaseOffset,
    /// Corrupt one byte inside stored batches before serving a fetch.
    FetchCorruptBatch,
    /// Answer a topic-id-addressed produce with UNKNOWN_TOPIC_ID even
    /// though the id was minted by this subject's CreateTopics.
    ProduceTopicIdUnknown,
    /// Report a log start that is not 0, though every log in this
    /// subject starts at 0.
    ListOffsetsWrongEarliest,
    /// Echo a coordinator key the request did not ask about.
    FindCoordinatorWrongKey,
    /// Report a committed offset as never-committed.
    OffsetFetchLosesCommit,
    /// Answer a never-committed partition with 0 rather than the -1
    /// sentinel — a plausible offset where "nothing here" was meant.
    OffsetFetchUnsetIsZero,
    /// Never advance a KIP-848 member past epoch 0.
    ConsumerGroupEpochStuck,
    /// Treat an omitted `subscribed_topic_names` as "subscribed to
    /// nothing" rather than "unchanged", revoking a steady-state
    /// member's assignment.
    ConsumerGroupNullSubscriptionRevokes,
    /// Assign nothing, however the member subscribed.
    ConsumerGroupAssignsNothing,
    /// Accept any member epoch.
    ConsumerGroupIgnoresEpoch,
    /// Admit a join with no member id instead of answering
    /// MEMBER_ID_REQUIRED with one to retry with.
    JoinGroupAcceptsEmptyMemberId,
    /// Alter the assignment bytes the leader supplied before handing
    /// them to their member.
    SyncGroupRewritesAssignment,
    /// Accept any generation on a post-join request.
    GroupIgnoresGeneration,
    /// Answer a fetch past the high watermark with an empty batch set
    /// instead of OFFSET_OUT_OF_RANGE.
    FetchPastEndSucceeds,
    /// Omit an unknown topic from a Metadata response instead of naming
    /// it with UNKNOWN_TOPIC_OR_PARTITION — the client cannot tell
    /// "absent" from "never mentioned".
    MetadataUnknownTopicOmitted,
    /// Answer a second CreateTopics for an existing topic with NONE.
    CreateTopicsDuplicateSucceeds,
    /// Create the topic even though the request said validate_only.
    CreateTopicsValidateOnlyCreates,
    /// Corrupt stored batches only when the fetch was made at the
    /// *lowest* version this subject advertises.
    ///
    /// This one exists to test the suite rather than a subject. The
    /// lowest version specifically, rather than "anything below the
    /// maximum": the fetch check caps itself below the advertised
    /// maximum for name addressing, so a fault keyed on that cap would
    /// fire at the check's own top version and prove nothing. Keyed
    /// here, it is invisible to any check that negotiates one version
    /// and stops — and calibration fails the moment the fetch check
    /// stops sweeping the range.
    FetchCorruptOnOldVersions,
    /// Echo a different topic id than the fetch requested.
    FetchWrongTopicId,
}

impl Fault {
    /// Every fault, so calibration tests can assert the fault ↔ check
    /// mapping is exhaustive in both directions.
    pub const ALL: &[Fault] = &[
        Fault::WrongCorrelationEcho,
        Fault::InvertedVersionRange,
        Fault::OmitApiVersionsKey,
        Fault::WrongErrorOnUnsupportedVersion,
        Fault::ErrorBodyNotV0,
        Fault::AdvertiseWrongMaxInError,
        Fault::TrailingGarbage,
        Fault::FetchTrailingGarbage,
        Fault::FlexibleHeaderOnV3,
        Fault::MetadataEmptyBrokers,
        Fault::MetadataUnrequestedTopic,
        Fault::MetadataNonFlexibleHeader,
        Fault::ProduceWrongBaseOffset,
        Fault::FetchCorruptBatch,
        Fault::ProduceTopicIdUnknown,
        Fault::FetchWrongTopicId,
        Fault::ListOffsetsWrongEarliest,
        Fault::FindCoordinatorWrongKey,
        Fault::OffsetFetchLosesCommit,
        Fault::OffsetFetchUnsetIsZero,
        Fault::FetchCorruptOnOldVersions,
        Fault::FetchPastEndSucceeds,
        Fault::MetadataUnknownTopicOmitted,
        Fault::CreateTopicsDuplicateSucceeds,
        Fault::CreateTopicsValidateOnlyCreates,
        Fault::JoinGroupAcceptsEmptyMemberId,
        Fault::SyncGroupRewritesAssignment,
        Fault::GroupIgnoresGeneration,
        Fault::ConsumerGroupEpochStuck,
        Fault::ConsumerGroupNullSubscriptionRevokes,
        Fault::ConsumerGroupAssignsNothing,
        Fault::ConsumerGroupIgnoresEpoch,
    ];
}

/// A running subject server bound to an ephemeral local port.
#[derive(Debug)]
pub struct SubjectServer {
    addr: String,
    handle: JoinHandle<()>,
}

impl SubjectServer {
    /// Spawn a subject exhibiting `faults` (none = conformant).
    pub async fn spawn(faults: Vec<Fault>) -> io::Result<SubjectServer> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?.to_string();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(handle_connection(stream, faults.clone()));
            }
        });
        Ok(SubjectServer { addr, handle })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }
}

impl Drop for SubjectServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn advertised_keys() -> Vec<ApiVersion> {
    // Only versions the subject actually implements — the full schema
    // ranges: its produce/fetch handlers resolve topic ids, so even the
    // id-addressed (v13+) versions are served.
    [
        (
            ApiVersionsRequest::API_KEY,
            ApiVersionsRequest::MIN_VERSION,
            MAX_SUPPORTED_API_VERSIONS,
        ),
        (
            ProduceRequest::API_KEY,
            ProduceRequest::MIN_VERSION,
            ProduceRequest::MAX_VERSION,
        ),
        (
            FetchRequest::API_KEY,
            FetchRequest::MIN_VERSION,
            FetchRequest::MAX_VERSION,
        ),
        (
            MetadataRequest::API_KEY,
            MetadataRequest::MIN_VERSION,
            MetadataRequest::MAX_VERSION,
        ),
        (
            CreateTopicsRequest::API_KEY,
            CreateTopicsRequest::MIN_VERSION,
            CreateTopicsRequest::MAX_VERSION,
        ),
        (
            ListOffsetsRequest::API_KEY,
            ListOffsetsRequest::MIN_VERSION,
            ListOffsetsRequest::MAX_VERSION,
        ),
        (
            FindCoordinatorRequest::API_KEY,
            FindCoordinatorRequest::MIN_VERSION,
            FindCoordinatorRequest::MAX_VERSION,
        ),
        (
            OffsetCommitRequest::API_KEY,
            OffsetCommitRequest::MIN_VERSION,
            OffsetCommitRequest::MAX_VERSION,
        ),
        (
            OffsetFetchRequest::API_KEY,
            OffsetFetchRequest::MIN_VERSION,
            OffsetFetchRequest::MAX_VERSION,
        ),
        (
            JoinGroupRequest::API_KEY,
            JoinGroupRequest::MIN_VERSION,
            JoinGroupRequest::MAX_VERSION,
        ),
        (
            SyncGroupRequest::API_KEY,
            SyncGroupRequest::MIN_VERSION,
            SyncGroupRequest::MAX_VERSION,
        ),
        (
            HeartbeatRequest::API_KEY,
            HeartbeatRequest::MIN_VERSION,
            HeartbeatRequest::MAX_VERSION,
        ),
        (
            LeaveGroupRequest::API_KEY,
            LeaveGroupRequest::MIN_VERSION,
            LeaveGroupRequest::MAX_VERSION,
        ),
        (
            ConsumerGroupHeartbeatRequest::API_KEY,
            ConsumerGroupHeartbeatRequest::MIN_VERSION,
            ConsumerGroupHeartbeatRequest::MAX_VERSION,
        ),
    ]
    .into_iter()
    .map(|(api_key, min_version, max_version)| {
        let mut v = ApiVersion::default();
        v.api_key = api_key;
        v.min_version = min_version;
        v.max_version = max_version;
        v
    })
    .collect()
}

/// One partition's log: appended record sets and the next offset to
/// assign.
#[derive(Debug, Default)]
struct PartitionLog {
    bytes: BytesMut,
    next_offset: i64,
}

/// Connection-scoped broker state — the suite's produce/fetch flow uses
/// a single connection, and cross-connection state would leak between
/// concurrently running checks.
#[derive(Debug, Default)]
struct ConnState {
    logs: HashMap<(String, i32), PartitionLog>,
    /// Topic ids minted by CreateTopics, keyed by id.
    topic_names: HashMap<[u8; 16], String>,
    /// Committed offsets, keyed by (group, topic, partition).
    committed: HashMap<(String, String, i32), i64>,
    /// Consumer groups this connection has joined (classic protocol).
    groups: HashMap<String, GroupState>,
    /// KIP-848 members, keyed by (group, member id).
    members_848: HashMap<(String, String), Member848>,
    /// Topics CreateTopics actually created. Distinct from `logs`, which
    /// only gains an entry once something is produced, and from
    /// `topic_names`, which maps ids: a topic can exist and be empty.
    created: HashMap<String, [u8; 16]>,
}

/// One consumer group, as much of it as the classic protocol needs.
///
/// Writing this down is what surfaced the checks that go with it. Three
/// things the request/response schemas do not say, and an implementer
/// has to decide:
///
/// 1. A join with no member id cannot simply be given one and waved
///    through — from JoinGroup v4 the coordinator answers
///    MEMBER_ID_REQUIRED *and* hands back the id to retry with, so a
///    client always rejoins with an id the coordinator minted. Without
///    that round trip a client that dies mid-join leaves a member
///    nobody can name.
/// 2. The assignment bytes are the group leader's to decide and the
///    coordinator's only to deliver. They are opaque: the coordinator
///    that parses or rewrites them has broken the same guarantee a
///    proxy breaks by re-encoding a record batch.
/// 3. The generation is the fence. Every later request carries it, and
///    one that carries a stale one has to be refused rather than
///    applied, or a member evicted during a rebalance quietly keeps
///    acting on an assignment it no longer owns.
#[derive(Debug, Default)]
struct GroupState {
    generation_id: i32,
    /// Members in join order; the first is the leader.
    members: Vec<String>,
    protocol_type: String,
    protocol_name: String,
    /// Set by the leader at SyncGroup, handed back verbatim.
    assignments: HashMap<String, Bytes>,
    /// Member ids minted for a join that had none, awaiting the rejoin.
    minted: Vec<String>,
}

/// One KIP-848 member.
///
/// The decisions this shape forced, none of which the schema states:
///
/// 1. **The member names itself.** Classic JoinGroup has the coordinator
///    mint the id; here the client generates one and the first heartbeat
///    arrives carrying it at epoch 0. The coordinator accepts it and
///    answers with the epoch it has been admitted at, which is never 0 —
///    a member at epoch 0 has not been admitted yet, so "still 0" is how
///    a client learns nothing happened.
/// 2. **An omitted subscription means unchanged, not empty.** Every
///    field a heartbeat can omit is one the client is saying nothing
///    about. A coordinator that reads `subscribed_topic_names: null` as
///    "subscribed to nothing" revokes the assignment of every member
///    that is simply idling correctly — and the steady-state heartbeat
///    is exactly the one that omits everything.
/// 3. **Assignments are addressed by topic id.** The coordinator has to
///    resolve the names a member subscribed by into ids, which means a
///    subscription naming a topic that does not exist is not an error,
///    it is an assignment that does not mention it.
#[derive(Debug, Default)]
struct Member848 {
    epoch: i32,
    /// Last subscription the member actually stated.
    subscribed: Vec<String>,
    /// The assignment this member was last *told*. The response carries
    /// one only when it differs — the same rule the request follows in
    /// the other direction, and the reason a steady-state heartbeat is
    /// nearly empty in both directions.
    told: Option<Vec<([u8; 16], Vec<i32>)>>,
}

/// A deterministic per-name topic id; never the zero uuid.
fn mint_topic_id(name: &str) -> [u8; 16] {
    let mut id = [0u8; 16];
    let mut acc: u8 = 0x9e;
    for (i, byte) in name.bytes().enumerate() {
        acc = acc.wrapping_mul(31).wrapping_add(byte);
        id[i % 16] ^= acc.rotate_left(u32::try_from(i % 7).unwrap_or(0));
    }
    id[0] |= 1;
    id
}

async fn handle_connection(mut stream: TcpStream, faults: Vec<Fault>) {
    let local_port = match stream.local_addr() {
        Ok(a) => i32::from(a.port()),
        Err(_) => return,
    };
    let mut state = ConnState::default();
    loop {
        let mut len_bytes = [0u8; 4];
        if stream.read_exact(&mut len_bytes).await.is_err() {
            return;
        }
        // A negative or implausibly large length prefix is a wire
        // violation: close the connection, never clamp.
        let Ok(len) = frame::check_len(len_bytes, frame::DEFAULT_MAX_FRAME) else {
            return;
        };
        let mut frame = vec![0u8; len];
        if stream.read_exact(&mut frame).await.is_err() {
            return;
        }
        let frame = Bytes::from(frame);
        if frame.len() < 4 {
            return;
        }
        let api_key = i16::from_be_bytes([frame[0], frame[1]]);
        let api_version = i16::from_be_bytes([frame[2], frame[3]]);
        let out = match api_key {
            ApiVersionsRequest::API_KEY => api_versions_exchange(frame, api_version, &faults),
            MetadataRequest::API_KEY => {
                metadata_exchange(frame, api_version, local_port, &faults, &state)
            }
            CreateTopicsRequest::API_KEY => {
                create_topics_exchange(frame, api_version, &faults, &mut state)
            }
            ProduceRequest::API_KEY => produce_exchange(frame, api_version, &faults, &mut state),
            FetchRequest::API_KEY => fetch_exchange(frame, api_version, &faults, &state),
            ListOffsetsRequest::API_KEY => {
                list_offsets_exchange(frame, api_version, &faults, &state)
            }
            FindCoordinatorRequest::API_KEY => {
                find_coordinator_exchange(frame, api_version, local_port, &faults)
            }
            OffsetCommitRequest::API_KEY => offset_commit_exchange(frame, api_version, &mut state),
            OffsetFetchRequest::API_KEY => {
                offset_fetch_exchange(frame, api_version, &faults, &state)
            }
            JoinGroupRequest::API_KEY => {
                join_group_exchange(frame, api_version, &faults, &mut state)
            }
            SyncGroupRequest::API_KEY => {
                sync_group_exchange(frame, api_version, &faults, &mut state)
            }
            HeartbeatRequest::API_KEY => {
                heartbeat_exchange(frame, api_version, &faults, &mut state)
            }
            LeaveGroupRequest::API_KEY => leave_group_exchange(frame, api_version, &mut state),
            ConsumerGroupHeartbeatRequest::API_KEY => {
                consumer_group_heartbeat_exchange(frame, api_version, &faults, &mut state)
            }
            _ => return,
        };
        let Some(out) = out else {
            return;
        };
        if stream.write_all(&out).await.is_err() {
            return;
        }
    }
}

/// The version-appropriate response header, via the shared quirk-aware
/// helper (the metadata handler keeps its own fault-injectable copy).
fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    header::response_header_version(api_key, api_version).unwrap_or(0)
}

/// Frame a response: length prefix, header at `header_version`, body bytes.
/// The interval this subject tells members to heartbeat at.
const HEARTBEAT_INTERVAL_MS: i32 = 5_000;
/// A heartbeat carrying this epoch is the member leaving.
const LEAVE_EPOCH: i32 = -1;

/// ConsumerGroupHeartbeat: the whole KIP-848 membership in one call.
fn consumer_group_heartbeat_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ConnState,
) -> Option<BytesMut> {
    if !(ConsumerGroupHeartbeatRequest::MIN_VERSION..=ConsumerGroupHeartbeatRequest::MAX_VERSION)
        .contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(ConsumerGroupHeartbeatRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = ConsumerGroupHeartbeatRequest::decode(&mut frame, api_version).ok()?;

    let key = (request.group_id.clone(), request.member_id.clone());
    let mut resp = ConsumerGroupHeartbeatResponse::default();
    resp.member_id = Some(request.member_id.clone());
    resp.heartbeat_interval_ms = HEARTBEAT_INTERVAL_MS;

    // Leaving: acknowledged by echoing the epoch back, and the member is
    // gone. Nothing to assign and nothing to fence against.
    if request.member_epoch == LEAVE_EPOCH {
        state.members_848.remove(&key);
        resp.error_code = 0;
        resp.member_epoch = LEAVE_EPOCH;
        return frame_response(
            req_header.correlation_id,
            response_header_version(ConsumerGroupHeartbeatRequest::API_KEY, api_version),
            |out| resp.encode(out, api_version).unwrap(),
            false,
        );
    }

    let known = state.members_848.contains_key(&key);
    if known {
        let current = state.members_848[&key].epoch;
        if request.member_epoch != current && !faults.contains(&Fault::ConsumerGroupIgnoresEpoch) {
            resp.error_code = ErrorCode::FENCED_MEMBER_EPOCH.0;
            resp.member_epoch = 0;
            return frame_response(
                req_header.correlation_id,
                response_header_version(ConsumerGroupHeartbeatRequest::API_KEY, api_version),
                |out| resp.encode(out, api_version).unwrap(),
                false,
            );
        }
    } else if request.member_epoch != 0 {
        // An unknown member can only be introducing itself, which is
        // epoch 0. Anything else is a member the coordinator forgot.
        resp.error_code = ErrorCode::UNKNOWN_MEMBER_ID.0;
        resp.member_epoch = 0;
        return frame_response(
            req_header.correlation_id,
            response_header_version(ConsumerGroupHeartbeatRequest::API_KEY, api_version),
            |out| resp.encode(out, api_version).unwrap(),
            false,
        );
    }

    let stuck = faults.contains(&Fault::ConsumerGroupEpochStuck);
    let member = state.members_848.entry(key).or_default();
    if !known {
        // Admission is what the epoch records. Leaving it at 0 tells the
        // member it was never admitted, however cheerful the error code.
        member.epoch = if stuck { 0 } else { 1 };
    }
    // Omitted means unchanged. Only a stated subscription replaces the
    // one on file — including a stated empty one, which really is
    // "nothing", unlike an absent one.
    match &request.subscribed_topic_names {
        Some(names) => member.subscribed = names.clone(),
        None if faults.contains(&Fault::ConsumerGroupNullSubscriptionRevokes) => {
            member.subscribed.clear();
        }
        None => {}
    }
    let epoch = member.epoch;
    let subscribed = member.subscribed.clone();

    // Server-side assignment: every partition of every subscribed topic
    // that exists, addressed by id because that is what the wire carries.
    let assigned: Vec<TopicPartitions> = if faults.contains(&Fault::ConsumerGroupAssignsNothing) {
        Vec::new()
    } else {
        subscribed
            .iter()
            .filter_map(|name| state.created.get(name).map(|id| (name, *id)))
            .map(|(_, topic_id)| {
                let mut tp = TopicPartitions::default();
                tp.topic_id = topic_id;
                tp.partitions = vec![0];
                tp
            })
            .collect()
    };
    // Send the assignment only when it is news. An unchanged assignment
    // is reported by saying nothing about it, so a member that hears
    // nothing keeps what it has — and, crucially, a member whose
    // assignment was *revoked* hears an empty one, which is how the two
    // are told apart on the wire.
    let current: Vec<([u8; 16], Vec<i32>)> = assigned
        .iter()
        .map(|tp| (tp.topic_id, tp.partitions.clone()))
        .collect();
    let member = state
        .members_848
        .get_mut(&(request.group_id.clone(), request.member_id.clone()))
        .expect("member was just inserted");
    let changed = member.told.as_ref() != Some(&current);
    member.told = Some(current);

    resp.error_code = 0;
    resp.member_epoch = epoch;
    if changed {
        let mut assignment = Assignment::default();
        assignment.topic_partitions = assigned;
        resp.assignment = Some(assignment);
    }
    frame_response(
        req_header.correlation_id,
        response_header_version(ConsumerGroupHeartbeatRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

/// The version from which a join with no member id must be refused and
/// given one to retry with.
const JOIN_GROUP_MEMBER_ID_REQUIRED: i16 = 4;

/// JoinGroup: mint a member id, or admit the member and name a leader.
fn join_group_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ConnState,
) -> Option<BytesMut> {
    if !(JoinGroupRequest::MIN_VERSION..=JoinGroupRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(JoinGroupRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = JoinGroupRequest::decode(&mut frame, api_version).ok()?;

    let group = state.groups.entry(request.group_id.clone()).or_default();
    let mut resp = JoinGroupResponse::default();

    let needs_id = request.member_id.is_empty()
        && api_version >= JOIN_GROUP_MEMBER_ID_REQUIRED
        && !faults.contains(&Fault::JoinGroupAcceptsEmptyMemberId);
    if needs_id {
        // The id is minted here and the join refused, so the member that
        // comes back is one the coordinator named.
        let minted = format!("odradek-member-{}", group.minted.len() + 1);
        group.minted.push(minted.clone());
        resp.error_code = ErrorCode::MEMBER_ID_REQUIRED.0;
        resp.member_id = minted;
        resp.generation_id = -1;
        resp.leader = String::new();
        resp.protocol_type = Some(request.protocol_type.clone());
        resp.protocol_name = None;
        return frame_response(
            req_header.correlation_id,
            response_header_version(JoinGroupRequest::API_KEY, api_version),
            |out| resp.encode(out, api_version).unwrap(),
            false,
        );
    }

    let member_id = if request.member_id.is_empty() {
        let minted = format!("odradek-member-{}", group.minted.len() + 1);
        group.minted.push(minted.clone());
        minted
    } else {
        request.member_id.clone()
    };
    if !group.members.contains(&member_id) {
        group.members.push(member_id.clone());
        group.generation_id += 1;
        group.assignments.clear();
    }
    group.protocol_type = request.protocol_type.clone();
    group.protocol_name = request
        .protocols
        .first()
        .map(|p| p.name.clone())
        .unwrap_or_default();

    let leader = group.members.first().cloned().unwrap_or_default();
    resp.error_code = 0;
    resp.generation_id = group.generation_id;
    resp.protocol_type = Some(group.protocol_type.clone());
    resp.protocol_name = Some(group.protocol_name.clone());
    resp.leader = leader.clone();
    resp.member_id = member_id.clone();
    // Only the leader is told who else is in the group: it is the one
    // that has to compute an assignment for them.
    resp.members = if member_id == leader {
        group
            .members
            .iter()
            .map(|id| {
                let mut m = JoinGroupResponseMember::default();
                m.member_id = id.clone();
                m.metadata = request
                    .protocols
                    .first()
                    .map(|p| p.metadata.clone())
                    .unwrap_or_default();
                m
            })
            .collect()
    } else {
        Vec::new()
    };
    frame_response(
        req_header.correlation_id,
        response_header_version(JoinGroupRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

/// SyncGroup: take the leader's assignments, hand each member its own.
fn sync_group_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ConnState,
) -> Option<BytesMut> {
    if !(SyncGroupRequest::MIN_VERSION..=SyncGroupRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(SyncGroupRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = SyncGroupRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = SyncGroupResponse::default();
    let group = state.groups.entry(request.group_id.clone()).or_default();
    if let Some(code) = fence(group, request.generation_id, &request.member_id, faults) {
        resp.error_code = code.0;
    } else {
        // The leader's assignments land here; everyone else is told what
        // the leader decided for them.
        if group.members.first() == Some(&request.member_id) {
            for a in &request.assignments {
                let bytes = if faults.contains(&Fault::SyncGroupRewritesAssignment) {
                    let mut mangled = BytesMut::from(&a.assignment[..]);
                    if mangled.is_empty() {
                        mangled.extend_from_slice(b"x");
                    } else {
                        let last = mangled.len() - 1;
                        mangled[last] ^= 0x01;
                    }
                    mangled.freeze()
                } else {
                    a.assignment.clone()
                };
                group.assignments.insert(a.member_id.clone(), bytes);
            }
        }
        resp.error_code = 0;
        resp.protocol_type = Some(group.protocol_type.clone());
        resp.protocol_name = Some(group.protocol_name.clone());
        resp.assignment = group
            .assignments
            .get(&request.member_id)
            .cloned()
            .unwrap_or_default();
    }
    frame_response(
        req_header.correlation_id,
        response_header_version(SyncGroupRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

/// Heartbeat: alive, and still of this generation.
fn heartbeat_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ConnState,
) -> Option<BytesMut> {
    if !(HeartbeatRequest::MIN_VERSION..=HeartbeatRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(HeartbeatRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = HeartbeatRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = HeartbeatResponse::default();
    let group = state.groups.entry(request.group_id.clone()).or_default();
    resp.error_code =
        fence(group, request.generation_id, &request.member_id, faults).map_or(0, |code| code.0);
    frame_response(
        req_header.correlation_id,
        response_header_version(HeartbeatRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

/// LeaveGroup: forget the member.
fn leave_group_exchange(
    mut frame: Bytes,
    api_version: i16,
    state: &mut ConnState,
) -> Option<BytesMut> {
    if !(LeaveGroupRequest::MIN_VERSION..=LeaveGroupRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(LeaveGroupRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = LeaveGroupRequest::decode(&mut frame, api_version).ok()?;

    if let Some(group) = state.groups.get_mut(&request.group_id) {
        group.members.retain(|id| *id != request.member_id);
        group.assignments.remove(&request.member_id);
    }
    let mut resp = LeaveGroupResponse::default();
    resp.error_code = 0;
    frame_response(
        req_header.correlation_id,
        response_header_version(LeaveGroupRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

/// The generation fence every post-join request passes through.
///
/// `None` means the request may proceed. A member the group does not
/// know is UNKNOWN_MEMBER_ID; a known member carrying the wrong
/// generation is ILLEGAL_GENERATION — the distinction matters because a
/// client answers them differently, rejoining from scratch versus
/// rejoining as itself.
fn fence(
    group: &GroupState,
    generation_id: i32,
    member_id: &str,
    faults: &[Fault],
) -> Option<ErrorCode> {
    if faults.contains(&Fault::GroupIgnoresGeneration) {
        return None;
    }
    if !group.members.iter().any(|id| id == member_id) {
        return Some(ErrorCode::UNKNOWN_MEMBER_ID);
    }
    if generation_id != group.generation_id {
        return Some(ErrorCode::ILLEGAL_GENERATION);
    }
    None
}

/// ListOffsets: `-2` is the log start, `-1` the log end, and any other
/// ListOffsets: `-2` is the log start, `-1` the log end, and any other
/// timestamp is a lookup this subject answers "no such message" to.
fn list_offsets_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &ConnState,
) -> Option<BytesMut> {
    if !(ListOffsetsRequest::MIN_VERSION..=ListOffsetsRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(ListOffsetsRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = ListOffsetsRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = ListOffsetsResponse::default();
    resp.topics = request
        .topics
        .iter()
        .map(|topic| {
            let mut out = ListOffsetsTopicResponse::default();
            out.name = topic.name.clone();
            out.partitions = topic
                .partitions
                .iter()
                .map(|p| {
                    let end = state
                        .logs
                        .get(&(topic.name.clone(), p.partition_index))
                        .map_or(0, |log| log.next_offset);
                    let (timestamp, offset) = match p.timestamp {
                        EARLIEST_TIMESTAMP => {
                            let start = if faults.contains(&Fault::ListOffsetsWrongEarliest) {
                                // The log start is always 0 here, so any
                                // other answer is wrong by construction.
                                end.max(1)
                            } else {
                                0
                            };
                            (-1, start)
                        }
                        LATEST_TIMESTAMP => (-1, end),
                        // No message search in this subject: a timestamp
                        // lookup finds nothing, which is `offset: -1`.
                        _ => (-1, -1),
                    };
                    let mut out = ListOffsetsPartitionResponse::default();
                    out.partition_index = p.partition_index;
                    out.error_code = 0;
                    out.timestamp = timestamp;
                    out.offset = offset;
                    out.leader_epoch = -1;
                    out
                })
                .collect();
            out
        })
        .collect();
    frame_response(
        req_header.correlation_id,
        response_header_version(ListOffsetsRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

/// FindCoordinator: this subject is its own coordinator for every group.
///
/// v4 replaced the single `key` with `coordinator_keys`, and the flat
/// node/host/port with a `coordinators` array — so the two shapes are
/// answered separately, which is the whole point of checking it.
fn find_coordinator_exchange(
    mut frame: Bytes,
    api_version: i16,
    local_port: i32,
    faults: &[Fault],
) -> Option<BytesMut> {
    if !(FindCoordinatorRequest::MIN_VERSION..=FindCoordinatorRequest::MAX_VERSION)
        .contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(FindCoordinatorRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = FindCoordinatorRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = FindCoordinatorResponse::default();
    if api_version >= FIND_COORDINATOR_BATCHED {
        resp.coordinators = request
            .coordinator_keys
            .iter()
            .map(|key| {
                let mut c = Coordinator::default();
                c.key = if faults.contains(&Fault::FindCoordinatorWrongKey) {
                    format!("{key}-not-yours")
                } else {
                    key.clone()
                };
                c.node_id = BROKER_NODE_ID;
                c.host = "127.0.0.1".to_owned();
                c.port = local_port;
                c.error_code = 0;
                c
            })
            .collect();
    } else {
        resp.error_code = 0;
        resp.node_id = BROKER_NODE_ID;
        resp.host = "127.0.0.1".to_owned();
        resp.port = local_port;
    }
    frame_response(
        req_header.correlation_id,
        response_header_version(FindCoordinatorRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

/// OffsetCommit: store what was committed, per (group, topic, partition).
fn offset_commit_exchange(
    mut frame: Bytes,
    api_version: i16,
    state: &mut ConnState,
) -> Option<BytesMut> {
    if !(OffsetCommitRequest::MIN_VERSION..=OffsetCommitRequest::MAX_VERSION).contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(OffsetCommitRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = OffsetCommitRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = OffsetCommitResponse::default();
    resp.topics = request
        .topics
        .iter()
        .map(|topic| {
            let name = if topic.name.is_empty() {
                state
                    .topic_names
                    .get(&topic.topic_id)
                    .cloned()
                    .unwrap_or_default()
            } else {
                topic.name.clone()
            };
            let mut out = OffsetCommitResponseTopic::default();
            out.name = name.clone();
            out.topic_id = topic.topic_id;
            out.partitions = topic
                .partitions
                .iter()
                .map(|p| {
                    state.committed.insert(
                        (request.group_id.clone(), name.clone(), p.partition_index),
                        p.committed_offset,
                    );
                    let mut out = OffsetCommitResponsePartition::default();
                    out.partition_index = p.partition_index;
                    out.error_code = 0;
                    out
                })
                .collect();
            out
        })
        .collect();
    frame_response(
        req_header.correlation_id,
        response_header_version(OffsetCommitRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

/// OffsetFetch: report what was committed, or `-1` where nothing was.
///
/// The sentinel is the subtle part and the reason this is checked: a
/// group that never committed is not an error, it is offset `-1` with
/// `error_code` 0. v8 moved the whole exchange into a `groups` array.
fn offset_fetch_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &ConnState,
) -> Option<BytesMut> {
    if !(OffsetFetchRequest::MIN_VERSION..=OffsetFetchRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(OffsetFetchRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = OffsetFetchRequest::decode(&mut frame, api_version).ok()?;

    // From v10 the request names topics only by id, exactly as
    // OffsetCommit does, so both sides have to resolve to the same key or
    // a commit and its read-back silently miss each other.
    let resolve = |name: &str, topic_id: &[u8; 16]| -> String {
        if name.is_empty() {
            state.topic_names.get(topic_id).cloned().unwrap_or_default()
        } else {
            name.to_owned()
        }
    };
    let lookup = |group: &str, topic: &str, partition: i32| -> i64 {
        let stored = state
            .committed
            .get(&(group.to_owned(), topic.to_owned(), partition))
            .copied();
        match stored {
            Some(offset) if faults.contains(&Fault::OffsetFetchLosesCommit) => {
                // Answer as though nothing was ever committed.
                let _ = offset;
                UNSET_OFFSET
            }
            Some(offset) => offset,
            // The sentinel, unless told to report a plausible-looking 0.
            None if faults.contains(&Fault::OffsetFetchUnsetIsZero) => 0,
            None => UNSET_OFFSET,
        }
    };

    let mut resp = OffsetFetchResponse::default();
    if api_version >= OFFSET_FETCH_BATCHED {
        resp.groups = request
            .groups
            .iter()
            .map(|group| {
                let mut out = OffsetFetchResponseGroup::default();
                out.group_id = group.group_id.clone();
                out.error_code = 0;
                out.topics = group
                    .topics
                    .iter()
                    .flatten()
                    .map(|topic| {
                        let mut t = OffsetFetchResponseTopics::default();
                        t.name = topic.name.clone();
                        t.topic_id = topic.topic_id;
                        t.partitions = topic
                            .partition_indexes
                            .iter()
                            .map(|index| {
                                let mut p = OffsetFetchResponsePartitions::default();
                                p.partition_index = *index;
                                p.committed_offset = lookup(
                                    &group.group_id,
                                    &resolve(&topic.name, &topic.topic_id),
                                    *index,
                                );
                                p.committed_leader_epoch = -1;
                                p.error_code = 0;
                                p
                            })
                            .collect();
                        t
                    })
                    .collect();
                out
            })
            .collect();
    } else {
        resp.topics = request
            .topics
            .iter()
            .flatten()
            .map(|topic| {
                let mut t = OffsetFetchResponseTopic::default();
                t.name = topic.name.clone();
                t.partitions = topic
                    .partition_indexes
                    .iter()
                    .map(|index| {
                        let mut p = OffsetFetchResponsePartition::default();
                        p.partition_index = *index;
                        p.committed_offset = lookup(&request.group_id, &topic.name, *index);
                        p.committed_leader_epoch = -1;
                        p.error_code = 0;
                        p
                    })
                    .collect();
                t
            })
            .collect();
    }
    frame_response(
        req_header.correlation_id,
        response_header_version(OffsetFetchRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

fn frame_response(
    correlation_id: i32,
    header_version: i16,
    body: impl FnOnce(&mut BytesMut),
    trailing_garbage: bool,
) -> Option<BytesMut> {
    let mut resp_header = ResponseHeader::default();
    resp_header.correlation_id = correlation_id;
    let mut out = BytesMut::new();
    frame::frame(&mut out, |out| {
        resp_header.encode(out, header_version)?;
        body(out);
        if trailing_garbage {
            out.extend_from_slice(&[0xde, 0xad, 0xbe]);
        }
        Ok(())
    })
    .ok()?;
    Some(out)
}

fn api_versions_exchange(mut frame: Bytes, api_version: i16, faults: &[Fault]) -> Option<BytesMut> {
    let has = |f: Fault| faults.contains(&f);
    let supported = api_version <= MAX_SUPPORTED_API_VERSIONS;

    // For unknown future versions this parses the header at our newest
    // known header version — the same rule real brokers apply, and the
    // shared table's answer for any v3+ request.
    let header_version = header::request_header_version(ApiVersionsRequest::API_KEY, api_version)?;
    let header = RequestHeader::decode(&mut frame, header_version).ok()?;

    let mut keys = advertised_keys();
    if has(Fault::OmitApiVersionsKey) {
        keys.retain(|k| k.api_key != ApiVersionsRequest::API_KEY);
    }
    if has(Fault::InvertedVersionRange) {
        if let Some(k) = keys
            .iter_mut()
            .find(|k| k.api_key == ProduceRequest::API_KEY)
        {
            (k.min_version, k.max_version) = (12, 3);
        }
    }

    let (error_code, encode_at) = if supported {
        (ErrorCode::NONE.0, api_version)
    } else {
        if has(Fault::AdvertiseWrongMaxInError) {
            if let Some(k) = keys
                .iter_mut()
                .find(|k| k.api_key == ApiVersionsRequest::API_KEY)
            {
                k.max_version += 1;
            }
        }
        let error_code = if has(Fault::WrongErrorOnUnsupportedVersion) {
            ErrorCode::NONE.0
        } else {
            ErrorCode::UNSUPPORTED_VERSION.0
        };
        let encode_at = if has(Fault::ErrorBodyNotV0) { 3 } else { 0 };
        (error_code, encode_at)
    };

    let mut resp = ApiVersionsResponse::default();
    resp.error_code = error_code;
    resp.api_keys = keys;
    // ApiVersions responses always use header v0 (the negotiation
    // bootstrap quirk); the fault violates exactly that.
    let resp_header_version = if has(Fault::FlexibleHeaderOnV3) && supported && api_version >= 3 {
        1
    } else {
        0
    };
    let mut correlation_id = header.correlation_id;
    if has(Fault::WrongCorrelationEcho) {
        correlation_id = correlation_id.wrapping_add(1);
    }
    frame_response(
        correlation_id,
        resp_header_version,
        |out| resp.encode(out, encode_at).unwrap(),
        has(Fault::TrailingGarbage),
    )
}

fn metadata_exchange(
    mut frame: Bytes,
    api_version: i16,
    local_port: i32,
    faults: &[Fault],
    state: &ConnState,
) -> Option<BytesMut> {
    let has = |f: Fault| faults.contains(&f);
    if !(MetadataRequest::MIN_VERSION..=MetadataRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let flexible = metadata_request::is_flexible(api_version);
    let header_version = header::request_header_version(MetadataRequest::API_KEY, api_version)?;
    let header = RequestHeader::decode(&mut frame, header_version).ok()?;
    let request = MetadataRequest::decode(&mut frame, api_version).ok()?;

    let brokers = if has(Fault::MetadataEmptyBrokers) {
        Vec::new()
    } else {
        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BROKER_NODE_ID;
        broker.host = "127.0.0.1".into();
        broker.port = local_port;
        vec![broker]
    };
    // A response names exactly the topics the request named: the ones
    // that exist, and the ones that do not with a code saying so. A
    // client cannot distinguish "this topic is absent" from "the server
    // ignored my question" unless the absent one is named.
    let requested: Vec<String> = request
        .topics
        .iter()
        .flatten()
        .filter_map(|t| t.name.clone())
        .collect();
    let topics = if has(Fault::MetadataUnrequestedTopic) && requested.is_empty() {
        let mut topic = MetadataResponseTopic::default();
        topic.name = Some("phantom".into());
        vec![topic]
    } else if has(Fault::MetadataUnknownTopicOmitted) {
        Vec::new()
    } else {
        requested
            .iter()
            .map(|name| {
                let mut topic = MetadataResponseTopic::default();
                topic.name = Some(name.clone());
                match state.created.get(name) {
                    Some(id) => {
                        topic.topic_id = *id;
                        topic.error_code = 0;
                    }
                    None => topic.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.0,
                }
                topic
            })
            .collect()
    };
    let mut resp = MetadataResponse::default();
    resp.brokers = brokers;
    resp.cluster_id = Some("odradek-reference".into());
    resp.controller_id = 1;
    resp.topics = topics;
    // Unlike ApiVersions, flexible Metadata responses use header v1; the
    // fault answers with the non-flexible header anyway.
    let resp_header_version = if flexible && !has(Fault::MetadataNonFlexibleHeader) {
        1
    } else {
        0
    };
    frame_response(
        header.correlation_id,
        resp_header_version,
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

fn create_topics_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ConnState,
) -> Option<BytesMut> {
    if !(CreateTopicsRequest::MIN_VERSION..=CreateTopicsRequest::MAX_VERSION).contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(CreateTopicsRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = CreateTopicsRequest::decode(&mut frame, api_version).ok()?;

    // A topic creates once. A second attempt is TOPIC_ALREADY_EXISTS, and
    // `validate_only` answers the same as a real creation would without
    // performing one — both are the kind of thing that looks like a
    // detail until a client's create-if-absent path depends on it.
    let validate_only =
        request.validate_only && !faults.contains(&Fault::CreateTopicsValidateOnlyCreates);
    let mut resp = CreateTopicsResponse::default();
    resp.topics = request
        .topics
        .iter()
        .map(|t| {
            let topic_id = mint_topic_id(&t.name);
            let exists = state.created.contains_key(&t.name);
            let mut result = CreatableTopicResult::default();
            result.name = t.name.clone();
            result.topic_id = topic_id;
            result.num_partitions = t.num_partitions.max(1);
            result.replication_factor = t.replication_factor.max(1);
            result.error_code = if exists && !faults.contains(&Fault::CreateTopicsDuplicateSucceeds)
            {
                ErrorCode::TOPIC_ALREADY_EXISTS.0
            } else {
                0
            };
            if !exists && result.error_code == 0 && !validate_only {
                state.created.insert(t.name.clone(), topic_id);
                state.topic_names.insert(topic_id, t.name.clone());
            }
            result
        })
        .collect();
    frame_response(
        req_header.correlation_id,
        response_header_version(CreateTopicsRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

fn produce_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ConnState,
) -> Option<BytesMut> {
    if !(ProduceRequest::MIN_VERSION..=ProduceRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(ProduceRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = ProduceRequest::decode(&mut frame, api_version).ok()?;

    let mut responses = Vec::new();
    for topic in &request.topic_data {
        // v13+ addresses by id; earlier versions by name.
        let by_id = topic.name.is_empty();
        let resolved = if by_id {
            state.topic_names.get(&topic.topic_id).cloned()
        } else {
            Some(topic.name.clone())
        };
        let refuse_id = by_id && faults.contains(&Fault::ProduceTopicIdUnknown);

        let mut partition_responses = Vec::new();
        for partition in &topic.partition_data {
            let (Some(name), false) = (&resolved, refuse_id) else {
                let mut entry = PartitionProduceResponse::default();
                entry.index = partition.index;
                entry.error_code = ErrorCode::UNKNOWN_TOPIC_ID.0;
                entry.base_offset = -1;
                entry.log_append_time_ms = -1;
                partition_responses.push(entry);
                continue;
            };
            let log = state
                .logs
                .entry((name.clone(), partition.index))
                .or_default();
            let set = partition.records.clone().unwrap_or_default();
            // Advance the offset by the records just appended.
            let appended: i64 = records::decode_set(&mut set.clone())
                .ok()?
                .iter()
                .map(|b| i64::from(b.last_offset_delta) + 1)
                .sum();
            let mut base_offset = log.next_offset;
            log.bytes.extend_from_slice(&set);
            log.next_offset += appended;
            if faults.contains(&Fault::ProduceWrongBaseOffset) {
                base_offset += 1;
            }
            let mut entry = PartitionProduceResponse::default();
            entry.index = partition.index;
            entry.error_code = 0;
            entry.base_offset = base_offset;
            entry.log_append_time_ms = -1;
            entry.log_start_offset = 0;
            partition_responses.push(entry);
        }
        let mut topic_resp = TopicProduceResponse::default();
        topic_resp.name = topic.name.clone();
        topic_resp.topic_id = topic.topic_id;
        topic_resp.partition_responses = partition_responses;
        responses.push(topic_resp);
    }
    let mut resp = ProduceResponse::default();
    resp.responses = responses;
    frame_response(
        req_header.correlation_id,
        response_header_version(ProduceRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}

fn fetch_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &ConnState,
) -> Option<BytesMut> {
    if !(FetchRequest::MIN_VERSION..=FetchRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(FetchRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = FetchRequest::decode(&mut frame, api_version).ok()?;

    let responses = request
        .topics
        .iter()
        .map(|topic| {
            // v13+ addresses by id; earlier versions by name.
            let resolved = if topic.topic.is_empty() {
                state.topic_names.get(&topic.topic_id).cloned()
            } else {
                Some(topic.topic.clone())
            };
            let mut echoed_id = topic.topic_id;
            if faults.contains(&Fault::FetchWrongTopicId) {
                echoed_id[0] ^= 0x80;
            }
            let mut topic_resp = FetchableTopicResponse::default();
            topic_resp.topic = topic.topic.clone();
            topic_resp.topic_id = echoed_id;
            topic_resp.partitions = topic
                .partitions
                .iter()
                .map(|p| {
                    let log = resolved
                        .as_ref()
                        .and_then(|name| state.logs.get(&(name.clone(), p.partition)));
                    match log {
                        // Reading past the end of the log is a client
                        // mistake the protocol has a code for. Answering
                        // it with an empty batch set instead would look
                        // to a consumer exactly like "caught up".
                        Some(log)
                            if p.fetch_offset > log.next_offset
                                && !faults.contains(&Fault::FetchPastEndSucceeds) =>
                        {
                            let mut data = PartitionData::default();
                            data.partition_index = p.partition;
                            data.error_code = ErrorCode::OFFSET_OUT_OF_RANGE.0;
                            data.high_watermark = log.next_offset;
                            data.last_stable_offset = log.next_offset;
                            data.log_start_offset = 0;
                            data
                        }
                        Some(log) => {
                            let mut bytes = log.bytes.clone();
                            let corrupt = faults.contains(&Fault::FetchCorruptBatch)
                                || (faults.contains(&Fault::FetchCorruptOnOldVersions)
                                    && api_version == FetchRequest::MIN_VERSION);
                            if corrupt && !bytes.is_empty() {
                                let last = bytes.len() - 1;
                                bytes[last] ^= 0x01;
                            }
                            let mut data = PartitionData::default();
                            data.partition_index = p.partition;
                            data.error_code = 0;
                            data.high_watermark = log.next_offset;
                            data.last_stable_offset = log.next_offset;
                            data.log_start_offset = 0;
                            data.records = Some(bytes.freeze());
                            data
                        }
                        None => {
                            let mut data = PartitionData::default();
                            data.partition_index = p.partition;
                            // Unknown id vs unknown name/partition.
                            data.error_code = if topic.topic.is_empty() {
                                ErrorCode::UNKNOWN_TOPIC_ID.0
                            } else {
                                ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.0
                            };
                            data
                        }
                    }
                })
                .collect();
            topic_resp
        })
        .collect();
    let mut resp = FetchResponse::default();
    resp.error_code = 0;
    resp.session_id = 0;
    resp.responses = responses;
    frame_response(
        req_header.correlation_id,
        response_header_version(FetchRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        faults.contains(&Fault::FetchTrailingGarbage),
    )
}
