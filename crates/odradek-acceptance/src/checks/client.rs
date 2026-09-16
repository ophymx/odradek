//! Checks that run against a client under test (the suite acts as server).
//!
//! The harness accepts one client connection, answers just enough of the
//! protocol to keep a real client talking (ApiVersions, Metadata, and empty
//! Produce/Fetch successes), and records every frame the client sends.
//! Checks are then evaluated over the recorded observations.

use std::collections::HashSet;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use odradek_protocol::header::{request_header_version, response_header_version};
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::fetch_request::FetchRequest;
use odradek_protocol::messages::fetch_response::FetchResponse;
use odradek_protocol::messages::metadata_request::MetadataRequest;
use odradek_protocol::messages::metadata_response::{MetadataResponse, MetadataResponseBroker};
use odradek_protocol::messages::produce_request::ProduceRequest;
use odradek_protocol::messages::produce_response::ProduceResponse;
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::report::{CheckOutcome, Report};
use crate::{CheckId, Verdict};

/// (api key, min, max) the harness advertises — exactly the apis it can
/// parse, so version discipline is checkable.
const ADVERTISED: &[(i16, i16, i16)] = &[(18, 0, 4), (0, 3, 12), (1, 4, 17), (3, 0, 13)];

const MAX_API_VERSIONS: i16 = 4;

/// Limits for one observation session.
#[derive(Debug, Clone)]
pub struct ObserveConfig {
    /// Stop after this many requests (the checks need finite input).
    pub max_requests: usize,
    /// Stop when the client goes quiet for this long.
    pub idle_timeout: Duration,
}

impl Default for ObserveConfig {
    fn default() -> Self {
        ObserveConfig {
            max_requests: 32,
            idle_timeout: Duration::from_secs(3),
        }
    }
}

/// What the harness saw in one request frame.
#[derive(Debug)]
struct Observation {
    index: usize,
    api_key: i16,
    api_version: i16,
    /// None when even a salvage parse could not recover a header.
    header: Option<RequestHeader>,
    header_error: Option<String>,
    body_error: Option<String>,
    /// Body checking does not apply (unknown api, or an ApiVersions probe
    /// above our max — the probe dance is legal).
    body_exempt: bool,
}

/// Accept one client connection on `listener`, observe it, and evaluate
/// the client checks.
pub async fn run(listener: &TcpListener, config: &ObserveConfig) -> Report {
    let (stream, peer) = match listener.accept().await {
        Ok(ok) => ok,
        Err(e) => {
            return Report {
                subject: "client <none>".into(),
                outcomes: vec![CheckOutcome {
                    id: CheckId("client/session".into()),
                    requirement: "a client connects to the harness",
                    verdict: Verdict::Fail {
                        details: format!("accept failed: {e}"),
                    },
                }],
            };
        }
    };
    let observations = observe_session(stream, config).await;
    evaluate(&observations, &format!("client {peer}"))
}

async fn observe_session(mut stream: TcpStream, config: &ObserveConfig) -> Vec<Observation> {
    let mut observations = Vec::new();
    while observations.len() < config.max_requests {
        let frame = match tokio::time::timeout(config.idle_timeout, read_frame(&mut stream)).await {
            Ok(Some(frame)) => frame,
            // Idle or closed: the session is over, not an error.
            Ok(None) | Err(_) => break,
        };
        let obs = parse_request(observations.len(), frame);
        if let Some(header) = &obs.header {
            respond(&mut stream, header).await;
        }
        observations.push(obs);
    }
    observations
}

async fn read_frame(stream: &mut TcpStream) -> Option<Bytes> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await.ok()?;
    let len = i32::from_be_bytes(len_bytes);
    if !(0..=crate::raw::MAX_FRAME_SIZE).contains(&len) {
        return None;
    }
    let mut frame = vec![0u8; len as usize];
    stream.read_exact(&mut frame).await.ok()?;
    Some(Bytes::from(frame))
}

fn parse_request(index: usize, frame: Bytes) -> Observation {
    if frame.len() < 8 {
        return Observation {
            index,
            api_key: -1,
            api_version: -1,
            header: None,
            header_error: Some(format!(
                "frame of {} byte(s) cannot hold a header",
                frame.len()
            )),
            body_error: None,
            body_exempt: true,
        };
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);

    let known_probe_version = if api_key == 18 {
        // ApiVersions probes above our max are legal; parse at our newest.
        Some(api_version.min(MAX_API_VERSIONS))
    } else {
        None
    };
    let header_version =
        request_header_version(api_key, known_probe_version.unwrap_or(api_version));

    let (header, header_error, mut body) = match header_version {
        Some(hv) => {
            let mut buf = frame.clone();
            match RequestHeader::decode(&mut buf, hv) {
                Ok(h) => (Some(h), None, Some(buf)),
                Err(e) => (
                    salvage_header(&frame),
                    Some(format!("header (v{hv}): {e}")),
                    None,
                ),
            }
        }
        // Unknown api key: no defined header version. Salvage for the
        // correlation id; the advertised-apis check reports the violation.
        None => (salvage_header(&frame), None, None),
    };

    let mut body_error = None;
    let mut body_exempt = false;
    match (api_key, &mut body) {
        (_, None) => body_exempt = true,
        (18, Some(buf)) if api_version > MAX_API_VERSIONS => {
            let _ = buf;
            body_exempt = true;
        }
        (18, Some(buf)) => body_error = decode_fully::<ApiVersionsRequest>(buf, api_version),
        (0, Some(buf)) => body_error = decode_fully::<ProduceRequest>(buf, api_version),
        (1, Some(buf)) => body_error = decode_fully::<FetchRequest>(buf, api_version),
        (3, Some(buf)) => body_error = decode_fully::<MetadataRequest>(buf, api_version),
        _ => body_exempt = true,
    }

    Observation {
        index,
        api_key,
        api_version,
        header,
        header_error,
        body_error,
        body_exempt,
    }
}

trait DecodeBody: Sized {
    fn decode_body(buf: &mut Bytes, version: i16) -> Result<Self, odradek_protocol::DecodeError>;
}

macro_rules! impl_decode_body {
    ($($ty:ty),+) => {
        $(impl DecodeBody for $ty {
            fn decode_body(
                buf: &mut Bytes,
                version: i16,
            ) -> Result<Self, odradek_protocol::DecodeError> {
                Self::decode(buf, version)
            }
        })+
    };
}
impl_decode_body!(
    ApiVersionsRequest,
    ProduceRequest,
    FetchRequest,
    MetadataRequest
);

fn decode_fully<T: DecodeBody>(buf: &mut Bytes, version: i16) -> Option<String> {
    match T::decode_body(buf, version) {
        Err(e) => Some(format!("body: {e}")),
        Ok(_) if !buf.is_empty() => Some(format!(
            "body leaves {} undecoded trailing byte(s)",
            buf.len()
        )),
        Ok(_) => None,
    }
}

/// Best-effort header recovery so the session can continue: try flexible
/// then classic header encodings.
fn salvage_header(frame: &Bytes) -> Option<RequestHeader> {
    for hv in [2, 1] {
        if let Ok(h) = RequestHeader::decode(&mut frame.clone(), hv) {
            return Some(h);
        }
    }
    None
}

async fn respond(stream: &mut TcpStream, header: &RequestHeader) {
    let api_key = header.request_api_key;
    let api_version = header.request_api_version;

    let (body, header_version) = match api_key {
        18 if api_version > MAX_API_VERSIONS => (encode_api_versions(35, 0), 0),
        18 => (encode_api_versions(0, api_version), 0),
        3 => {
            let v = api_version.clamp(0, 13);
            let resp = MetadataResponse {
                brokers: vec![MetadataResponseBroker {
                    node_id: 0,
                    host: "127.0.0.1".into(),
                    port: 0,
                    rack: None,
                    unknown_tagged_fields: Vec::new(),
                }],
                cluster_id: Some("odradek-harness".into()),
                controller_id: 0,
                ..Default::default()
            };
            let mut buf = BytesMut::new();
            let Ok(()) = resp.encode(&mut buf, v) else {
                return;
            };
            (buf.freeze(), response_header_version(3, v).unwrap_or(0))
        }
        0 => {
            let v = api_version.clamp(3, 12);
            let mut buf = BytesMut::new();
            let Ok(()) = ProduceResponse::default().encode(&mut buf, v) else {
                return;
            };
            (buf.freeze(), response_header_version(0, v).unwrap_or(0))
        }
        1 => {
            let v = api_version.clamp(4, 17);
            let mut buf = BytesMut::new();
            let Ok(()) = FetchResponse::default().encode(&mut buf, v) else {
                return;
            };
            (buf.freeze(), response_header_version(1, v).unwrap_or(0))
        }
        // Unknown api: nothing sensible to say; stay silent.
        _ => return,
    };

    let resp_header = ResponseHeader {
        correlation_id: header.correlation_id,
        unknown_tagged_fields: Vec::new(),
    };
    let mut out = BytesMut::new();
    out.put_i32(0);
    let Ok(()) = resp_header.encode(&mut out, header_version) else {
        return;
    };
    out.extend_from_slice(&body);
    let Ok(len) = i32::try_from(out.len() - 4) else {
        return;
    };
    out[..4].copy_from_slice(&len.to_be_bytes());
    let _ = stream.write_all(&out).await;
}

fn encode_api_versions(error_code: i16, version: i16) -> Bytes {
    let resp = ApiVersionsResponse {
        error_code,
        api_keys: ADVERTISED
            .iter()
            .map(|&(api_key, min_version, max_version)| ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, version)
        .expect("infallible for this shape");
    buf.freeze()
}

fn evaluate(observations: &[Observation], subject: &str) -> Report {
    let mut outcomes = Vec::new();
    let none_observed = observations.is_empty();
    let skip = |reason: &str| Verdict::Skipped {
        reason: reason.into(),
    };

    // client/header-well-formed
    let header_failures: Vec<String> = observations
        .iter()
        .filter_map(|o| {
            o.header_error.as_ref().map(|e| {
                format!(
                    "request #{} (api {} v{}): {e}",
                    o.index, o.api_key, o.api_version
                )
            })
        })
        .collect();
    outcomes.push(CheckOutcome {
        id: CheckId("client/header-well-formed".into()),
        requirement: "every request carries a decodable header at the version \
                      implied by its (api key, api version)",
        verdict: if none_observed {
            skip("client sent no requests")
        } else if header_failures.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail {
                details: header_failures.join("; "),
            }
        },
    });

    // client/starts-with-api-versions
    outcomes.push(CheckOutcome {
        id: CheckId("client/starts-with-api-versions".into()),
        requirement: "the first request on a connection is ApiVersions, so \
                      versions are negotiated before anything else is sent",
        verdict: match observations.first() {
            None => skip("client sent no requests"),
            Some(first) if first.api_key == 18 => Verdict::Pass,
            Some(first) => Verdict::Fail {
                details: format!("first request was api key {}", first.api_key),
            },
        },
    });

    // client/correlation-ids-unique
    let mut seen = HashSet::new();
    let mut duplicates = Vec::new();
    for o in observations {
        if let Some(h) = &o.header {
            if !seen.insert(h.correlation_id) {
                duplicates.push(format!(
                    "request #{} reuses correlation id {}",
                    o.index, h.correlation_id
                ));
            }
        }
    }
    outcomes.push(CheckOutcome {
        id: CheckId("client/correlation-ids-unique".into()),
        requirement: "correlation ids are not reused within a connection, so \
                      responses are unambiguously attributable",
        verdict: if none_observed {
            skip("client sent no requests")
        } else if duplicates.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail {
                details: duplicates.join("; "),
            }
        },
    });

    // client/respects-advertised-versions (ApiVersions itself exempt: the
    // probe-and-downgrade dance happens before ranges are known).
    let mut range_violations = Vec::new();
    let mut applicable = 0usize;
    for o in observations.iter().filter(|o| o.api_key != 18) {
        applicable += 1;
        match ADVERTISED.iter().find(|(k, _, _)| *k == o.api_key) {
            None => range_violations.push(format!(
                "request #{} uses api key {} the harness never advertised",
                o.index, o.api_key
            )),
            Some(&(_, min, max)) if o.api_version < min || o.api_version > max => range_violations
                .push(format!(
                    "request #{} uses api {} v{}, outside advertised {min}-{max}",
                    o.index, o.api_key, o.api_version
                )),
            Some(_) => {}
        }
    }
    outcomes.push(CheckOutcome {
        id: CheckId("client/respects-advertised-versions".into()),
        requirement: "after negotiation the client only sends apis and \
                      versions the server advertised",
        verdict: if applicable == 0 {
            skip("only ApiVersions requests observed")
        } else if range_violations.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail {
                details: range_violations.join("; "),
            }
        },
    });

    // client/body-decodes
    let mut body_failures = Vec::new();
    let mut body_applicable = 0usize;
    for o in observations.iter().filter(|o| !o.body_exempt) {
        body_applicable += 1;
        if let Some(e) = &o.body_error {
            body_failures.push(format!(
                "request #{} (api {} v{}): {e}",
                o.index, o.api_key, o.api_version
            ));
        }
    }
    outcomes.push(CheckOutcome {
        id: CheckId("client/body-decodes".into()),
        requirement: "request bodies decode per the message schema at the \
                      claimed version, with no trailing bytes",
        verdict: if body_applicable == 0 {
            skip("no checkable request bodies observed")
        } else if body_failures.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail {
                details: body_failures.join("; "),
            }
        },
    });

    Report {
        subject: subject.into(),
        outcomes,
    }
}
