//! A reference subject server with fault injection.
//!
//! This is the suite's calibration instrument. Run with no faults it is a
//! minimal conformant ApiVersions responder; each [`Fault`] makes it commit
//! exactly one protocol violation. The sensitivity tests assert a 1:1
//! mapping between faults and the checks that claim to detect them — a
//! check that cannot catch its own targeted fault is vacuous, and a check
//! that fails against the compliant subject is wrong.

use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::metadata_request::{self, MetadataRequest};
use odradek_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponseTopic,
};
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
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
    [
        (18, 0, MAX_SUPPORTED_API_VERSIONS),
        (0, 3, 12),
        (1, 4, 17),
        (3, 0, 13),
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

async fn handle_connection(mut stream: TcpStream, faults: Vec<Fault>) {
    let local_port = match stream.local_addr() {
        Ok(a) => i32::from(a.port()),
        Err(_) => return,
    };
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
