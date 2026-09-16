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
    /// Append junk bytes inside the response frame after the body.
    TrailingGarbage,
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
        Fault::FlexibleHeaderOnV3,
        Fault::MetadataEmptyBrokers,
        Fault::MetadataUnrequestedTopic,
        Fault::MetadataNonFlexibleHeader,
        Fault::ProduceWrongBaseOffset,
        Fault::FetchCorruptBatch,
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
    // Only versions the subject actually implements: name-addressed
    // Produce/Fetch (v13+ switch to topic ids).
    [
        (18, 0, MAX_SUPPORTED_API_VERSIONS),
        (0, 3, 12),
        (1, 4, 12),
        (3, 0, 13),
        (19, 2, 7),
    ]
    .into_iter()
    .map(|(api_key, min_version, max_version)| ApiVersion {
        api_key,
        min_version,
        max_version,
        ..Default::default()
    })
    .collect()
}

/// One partition's log: appended record sets and the next offset to
/// assign. State is per connection — the suite's produce/fetch flow uses
/// a single connection, and cross-connection state would leak between
/// concurrently running checks.
#[derive(Debug, Default)]
struct PartitionLog {
    bytes: BytesMut,
    next_offset: i64,
}

type Store = HashMap<(String, i32), PartitionLog>;

async fn handle_connection(mut stream: TcpStream, faults: Vec<Fault>) {
    let local_port = match stream.local_addr() {
        Ok(a) => i32::from(a.port()),
        Err(_) => return,
    };
    let mut store = Store::new();
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
            19 => create_topics_exchange(frame, api_version),
            0 => produce_exchange(frame, api_version, &faults, &mut store),
            1 => fetch_exchange(frame, api_version, &faults, &store),
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
    let resp_header = ResponseHeader {
        correlation_id,
        unknown_tagged_fields: Vec::new(),
    };
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

    let resp = ApiVersionsResponse {
        error_code,
        api_keys: keys,
        ..Default::default()
    };
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
        vec![MetadataResponseBroker {
            node_id: 1,
            host: "127.0.0.1".into(),
            port: local_port,
            ..Default::default()
        }]
    };
    // The subject hosts no topics: every response names no topics unless
    // the fault invents one the client never asked for.
    let topics = if has(Fault::MetadataUnrequestedTopic) && request.topics == Some(Vec::new()) {
        vec![MetadataResponseTopic {
            name: Some("phantom".into()),
            ..Default::default()
        }]
    } else {
        Vec::new()
    };
    let resp = MetadataResponse {
        brokers,
        cluster_id: Some("odradek-reference".into()),
        controller_id: 1,
        topics,
        ..Default::default()
    };
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

fn create_topics_exchange(mut frame: Bytes, api_version: i16) -> Option<BytesMut> {
    if !(CreateTopicsRequest::MIN_VERSION..=CreateTopicsRequest::MAX_VERSION).contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(CreateTopicsRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = CreateTopicsRequest::decode(&mut frame, api_version).ok()?;

    // Every topic creates successfully; produce/fetch state is lazy, so
    // there is nothing to record here.
    let resp = CreateTopicsResponse {
        topics: request
            .topics
            .iter()
            .map(|t| CreatableTopicResult {
                name: t.name.clone(),
                error_code: 0,
                num_partitions: t.num_partitions.max(1),
                replication_factor: t.replication_factor.max(1),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
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
    store: &mut Store,
) -> Option<BytesMut> {
    if !(ProduceRequest::MIN_VERSION..=12).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(ProduceRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = ProduceRequest::decode(&mut frame, api_version).ok()?;

    let mut responses = Vec::new();
    for topic in &request.topic_data {
        let mut partition_responses = Vec::new();
        for partition in &topic.partition_data {
            let log = store
                .entry((topic.name.clone(), partition.index))
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
            partition_responses.push(PartitionProduceResponse {
                index: partition.index,
                error_code: 0,
                base_offset,
                log_append_time_ms: -1,
                log_start_offset: 0,
                ..Default::default()
            });
        }
        responses.push(TopicProduceResponse {
            name: topic.name.clone(),
            partition_responses,
            ..Default::default()
        });
    }
    let resp = ProduceResponse {
        responses,
        ..Default::default()
    };
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
    store: &Store,
) -> Option<BytesMut> {
    if !(FetchRequest::MIN_VERSION..=12).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(FetchRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = FetchRequest::decode(&mut frame, api_version).ok()?;

    let responses = request
        .topics
        .iter()
        .map(|topic| FetchableTopicResponse {
            topic: topic.topic.clone(),
            partitions: topic
                .partitions
                .iter()
                .map(|p| match store.get(&(topic.topic.clone(), p.partition)) {
                    Some(log) => {
                        let mut bytes = log.bytes.clone();
                        if faults.contains(&Fault::FetchCorruptBatch) && !bytes.is_empty() {
                            let last = bytes.len() - 1;
                            bytes[last] ^= 0x01;
                        }
                        PartitionData {
                            partition_index: p.partition,
                            error_code: 0,
                            high_watermark: log.next_offset,
                            last_stable_offset: log.next_offset,
                            log_start_offset: 0,
                            records: Some(bytes.freeze()),
                            ..Default::default()
                        }
                    }
                    None => PartitionData {
                        partition_index: p.partition,
                        error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                        ..Default::default()
                    },
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    let resp = FetchResponse {
        error_code: 0,
        session_id: 0,
        responses,
        ..Default::default()
    };
    frame_response(
        req_header.correlation_id,
        response_header_version(FetchRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version).unwrap(),
        false,
    )
}
