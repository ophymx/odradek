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
            MetadataRequest::API_KEY => metadata_exchange(frame, api_version, local_port, &faults),
            CreateTopicsRequest::API_KEY => create_topics_exchange(frame, api_version, &mut state),
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
    // The subject hosts no topics: every response names no topics unless
    // the fault invents one the client never asked for.
    let topics = if has(Fault::MetadataUnrequestedTopic) && request.topics == Some(Vec::new()) {
        let mut topic = MetadataResponseTopic::default();
        topic.name = Some("phantom".into());
        vec![topic]
    } else {
        Vec::new()
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
    state: &mut ConnState,
) -> Option<BytesMut> {
    if !(CreateTopicsRequest::MIN_VERSION..=CreateTopicsRequest::MAX_VERSION).contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(CreateTopicsRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = CreateTopicsRequest::decode(&mut frame, api_version).ok()?;

    // Every topic creates successfully (log state itself is lazy) and gets
    // an id, so id-addressed produce/fetch can resolve it later.
    let mut resp = CreateTopicsResponse::default();
    resp.topics = request
        .topics
        .iter()
        .map(|t| {
            let topic_id = mint_topic_id(&t.name);
            state.topic_names.insert(topic_id, t.name.clone());
            let mut result = CreatableTopicResult::default();
            result.name = t.name.clone();
            result.topic_id = topic_id;
            result.error_code = 0;
            result.num_partitions = t.num_partitions.max(1);
            result.replication_factor = t.replication_factor.max(1);
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
