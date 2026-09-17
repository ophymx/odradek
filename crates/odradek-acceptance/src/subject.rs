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

use bytes::{BufMut, Bytes, BytesMut};
use odradek_protocol::header;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::create_topics_request::CreateTopicsRequest;
use odradek_protocol::messages::create_topics_response::{
    CreatableTopicResult, CreateTopicsResponse,
};
use odradek_protocol::messages::fetch_request::FetchRequest;
use odradek_protocol::messages::fetch_response::{
    FetchResponse, FetchableTopicResponse, PartitionData,
};
use odradek_protocol::messages::metadata_request::{self, MetadataRequest};
use odradek_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponseTopic,
};
use odradek_protocol::messages::produce_request::ProduceRequest;
use odradek_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use odradek_protocol::records;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// The newest ApiVersions version the subject supports.
pub const MAX_SUPPORTED_API_VERSIONS: i16 = 4;

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
    // Only versions the subject actually implements.
    [
        (18, 0, MAX_SUPPORTED_API_VERSIONS),
        (0, 3, ProduceRequest::MAX_VERSION),
        (1, 4, FetchRequest::MAX_VERSION),
        (3, 0, 13),
        (19, 2, 7),
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
        let len = i32::from_be_bytes(len_bytes).max(0) as usize;
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
            18 => api_versions_exchange(frame, api_version, &faults),
            3 => metadata_exchange(frame, api_version, local_port, &faults),
            19 => create_topics_exchange(frame, api_version, &mut state),
            0 => produce_exchange(frame, api_version, &faults, &mut state),
            1 => fetch_exchange(frame, api_version, &faults, &state),
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
fn frame_response(
    correlation_id: i32,
    header_version: i16,
    body: impl FnOnce(&mut BytesMut),
    trailing_garbage: bool,
) -> Option<BytesMut> {
    let mut resp_header = ResponseHeader::default();
    resp_header.correlation_id = correlation_id;
    let mut out = BytesMut::new();
    out.put_i32(0);
    resp_header.encode(&mut out, header_version).ok()?;
    body(&mut out);
    if trailing_garbage {
        out.extend_from_slice(&[0xde, 0xad, 0xbe]);
    }
    let frame_len = i32::try_from(out.len() - 4).unwrap();
    out[..4].copy_from_slice(&frame_len.to_be_bytes());
    Some(out)
}

fn api_versions_exchange(mut frame: Bytes, api_version: i16, faults: &[Fault]) -> Option<BytesMut> {
    let has = |f: Fault| faults.contains(&f);
    let supported = api_version <= MAX_SUPPORTED_API_VERSIONS;

    // For unknown future versions, parse the header at our newest known
    // header version — the same rule real brokers apply.
    let header_version = if supported && api_version < 3 { 1 } else { 2 };
    let header = RequestHeader::decode(&mut frame, header_version).ok()?;

    let mut keys = advertised_keys();
    if has(Fault::OmitApiVersionsKey) {
        keys.retain(|k| k.api_key != 18);
    }
    if has(Fault::InvertedVersionRange) {
        if let Some(k) = keys.iter_mut().find(|k| k.api_key == 0) {
            (k.min_version, k.max_version) = (12, 3);
        }
    }

    let (error_code, encode_at) = if supported {
        (0, api_version)
    } else {
        if has(Fault::AdvertiseWrongMaxInError) {
            if let Some(k) = keys.iter_mut().find(|k| k.api_key == 18) {
                k.max_version += 1;
            }
        }
        let error_code = if has(Fault::WrongErrorOnUnsupportedVersion) {
            0
        } else {
            35
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
    let header_version = if flexible { 2 } else { 1 };
    let header = RequestHeader::decode(&mut frame, header_version).ok()?;
    let request = MetadataRequest::decode(&mut frame, api_version).ok()?;

    let brokers = if has(Fault::MetadataEmptyBrokers) {
        Vec::new()
    } else {
        let mut broker = MetadataResponseBroker::default();
        broker.node_id = 1;
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
                entry.error_code = 100; // UNKNOWN_TOPIC_ID
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
                            if faults.contains(&Fault::FetchCorruptBatch) && !bytes.is_empty() {
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
                            data.error_code = if topic.topic.is_empty() { 100 } else { 3 };
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
