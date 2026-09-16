//! Checks that run against a server under test (the suite acts as client).
//!
//! Every check opens its own connection so subjects are validated from a
//! clean state, and failures in one check cannot poison another.

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::metadata_request::{self, MetadataRequest};
use odradek_protocol::messages::metadata_response::MetadataResponse;
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use odradek_protocol::wire;

use crate::raw::RawConnection;
use crate::report::{CheckOutcome, Report};
use crate::{CheckId, Verdict};

const CLIENT_ID: &str = "odradek-acceptance";

/// Run all server-side checks against `addr` and collect a report.
pub async fn run(addr: &str) -> Report {
    let mut outcomes = Vec::new();

    // The basic check doubles as discovery: later checks need the
    // advertised version ranges.
    let (verdict, keys) = v0_basic(addr).await;
    let api_versions_range = advertised_range(&keys, ApiVersionsRequest::API_KEY);
    let metadata_range = advertised_range(&keys, MetadataRequest::API_KEY);
    outcomes.push(CheckOutcome {
        id: CheckId("api-versions/v0-basic".into()),
        requirement: "responds to ApiVersions v0 with error NONE, advertises \
                      ApiVersions itself, and every advertised range has min <= max",
        verdict,
    });

    outcomes.push(CheckOutcome {
        id: CheckId("api-versions/correlation-echo".into()),
        requirement: "echoes the request correlation id, including unusual values",
        verdict: correlation_echo(addr).await,
    });

    outcomes.push(CheckOutcome {
        id: CheckId("api-versions/flexible-v3".into()),
        requirement: "answers a flexible (v3+) ApiVersions request, including \
                      the tagged-field sections, with a v0 response header",
        verdict: flexible_v3(addr, api_versions_range).await,
    });

    outcomes.push(CheckOutcome {
        id: CheckId("api-versions/unsupported-version-error".into()),
        requirement: "rejects an ApiVersions request newer than it supports \
                      with UNSUPPORTED_VERSION in a v0-encoded response that \
                      advertises the supported range",
        verdict: unsupported_version(addr, api_versions_range).await,
    });

    outcomes.push(CheckOutcome {
        id: CheckId("metadata/basic".into()),
        requirement: "answers a Metadata request naming no topics with a \
                      non-empty brokers list (unique node ids, valid ports) \
                      and no topics the client did not ask about",
        verdict: metadata_basic(addr, metadata_range).await,
    });

    outcomes.push(CheckOutcome {
        id: CheckId("metadata/flexible-response-header".into()),
        requirement: "answers a flexible (v9+) Metadata request with a v1 \
                      response header carrying the tagged-fields section — \
                      the ApiVersions always-v0 quirk does not apply to \
                      other apis",
        verdict: metadata_flexible_header(addr, metadata_range).await,
    });

    Report {
        subject: format!("server {addr}"),
        outcomes,
    }
}

fn advertised_range(keys: &[ApiVersion], api_key: i16) -> Option<(i16, i16)> {
    keys.iter()
        .find(|v| v.api_key == api_key)
        .map(|v| (v.min_version, v.max_version))
}

fn header(version: i16, correlation_id: i32) -> RequestHeader {
    RequestHeader {
        request_api_key: ApiVersionsRequest::API_KEY,
        request_api_version: version,
        correlation_id,
        client_id: Some(CLIENT_ID.into()),
        unknown_tagged_fields: Vec::new(),
    }
}

/// One ApiVersions exchange; returns the decoded body after validating the
/// correlation echo and (always-v0) response header.
async fn exchange(
    addr: &str,
    api_version: i16,
    header_version: i16,
    correlation_id: i32,
    decode_at: i16,
) -> Result<ApiVersionsResponse, String> {
    let mut conn = RawConnection::connect(addr)
        .await
        .map_err(|e| e.to_string())?;
    let mut body = BytesMut::new();
    let req = ApiVersionsRequest {
        client_software_name: "odradek-acceptance".into(),
        client_software_version: env!("CARGO_PKG_VERSION").into(),
        unknown_tagged_fields: Vec::new(),
    };
    // Encode the body at the newest shape the schema knows; for a probe of
    // an unknown future version this is the closest well-formed guess.
    req.encode(&mut body, api_version.min(ApiVersionsRequest::MAX_VERSION))
        .map_err(|e| e.to_string())?;

    let mut frame = conn
        .round_trip(&header(api_version, correlation_id), header_version, &body)
        .await
        .map_err(|e| e.to_string())?;

    let echoed = wire::get_i32(&mut frame.clone())
        .map_err(|_| "response frame shorter than a correlation id".to_string())?;
    if echoed != correlation_id {
        return Err(format!(
            "sent correlation id {correlation_id}, response carries {echoed}"
        ));
    }
    // ApiVersions responses always use response header v0.
    ResponseHeader::decode(&mut frame, 0).map_err(|e| format!("response header: {e}"))?;
    let resp = ApiVersionsResponse::decode(&mut frame, decode_at)
        .map_err(|e| format!("response body (decoded as v{decode_at}): {e}"))?;
    if !frame.is_empty() {
        return Err(format!(
            "{} byte(s) of trailing garbage after the response body",
            frame.len()
        ));
    }
    Ok(resp)
}

/// Returns the advertised api keys alongside the verdict; discovery for
/// the version-adaptive checks. The list is kept even on failure so
/// downstream checks can still run (and skip with a precise reason).
async fn v0_basic(addr: &str) -> (Verdict, Vec<ApiVersion>) {
    let resp = match exchange(addr, 0, 1, 1, 0).await {
        Ok(resp) => resp,
        Err(details) => return (Verdict::Fail { details }, Vec::new()),
    };
    let code = ErrorCode(resp.error_code);
    if !code.is_ok() {
        return (
            Verdict::Fail {
                details: format!("error code {code}"),
            },
            resp.api_keys,
        );
    }
    for v in &resp.api_keys {
        if v.min_version > v.max_version {
            return (
                Verdict::Fail {
                    details: format!(
                        "api key {} advertises min {} > max {}",
                        v.api_key, v.min_version, v.max_version
                    ),
                },
                resp.api_keys,
            );
        }
    }
    let advertised = advertised_range(&resp.api_keys, ApiVersionsRequest::API_KEY);
    let verdict = match advertised {
        Some((min, _)) if min <= 0 => Verdict::Pass,
        Some((min, max)) => Verdict::Fail {
            details: format!(
                "ApiVersions advertised as {min}-{max}, but the server just answered v0"
            ),
        },
        None => Verdict::Fail {
            details: "response does not advertise the ApiVersions api itself".into(),
        },
    };
    (verdict, resp.api_keys)
}

async fn correlation_echo(addr: &str) -> Verdict {
    match exchange(addr, 0, 1, i32::MAX - 17, 0).await {
        Ok(_) => Verdict::Pass,
        Err(details) => Verdict::Fail { details },
    }
}

async fn flexible_v3(addr: &str, advertised: Option<(i16, i16)>) -> Verdict {
    let Some((_, max)) = advertised else {
        return Verdict::Skipped {
            reason: "advertised range unknown (v0-basic failed)".into(),
        };
    };
    if max < 3 {
        return Verdict::Skipped {
            reason: format!("server only advertises ApiVersions up to v{max}"),
        };
    }
    let version = max.min(ApiVersionsRequest::MAX_VERSION);
    match exchange(addr, version, 2, 2, version).await {
        Ok(resp) if ErrorCode(resp.error_code).is_ok() => Verdict::Pass,
        Ok(resp) => Verdict::Fail {
            details: format!("error code {}", ErrorCode(resp.error_code)),
        },
        Err(details) => Verdict::Fail { details },
    }
}

async fn unsupported_version(addr: &str, advertised: Option<(i16, i16)>) -> Verdict {
    let Some((_, max)) = advertised else {
        return Verdict::Skipped {
            reason: "advertised range unknown (v0-basic failed)".into(),
        };
    };
    if max < 3 {
        // For pre-flexible servers the header version of a from-the-future
        // request is ambiguous; don't punish the subject for our guess.
        return Verdict::Skipped {
            reason: format!("server only advertises ApiVersions up to v{max}"),
        };
    }
    let probe = max + 7;
    let resp = match exchange(addr, probe, 2, 3, 0).await {
        Ok(resp) => resp,
        Err(details) => return Verdict::Fail { details },
    };
    let code = ErrorCode(resp.error_code);
    if code != ErrorCode::UNSUPPORTED_VERSION {
        return Verdict::Fail {
            details: format!("expected UNSUPPORTED_VERSION (35), got {code}"),
        };
    }
    match resp
        .api_keys
        .iter()
        .find(|v| v.api_key == ApiVersionsRequest::API_KEY)
    {
        Some(range) if range.max_version == max => Verdict::Pass,
        Some(range) => Verdict::Fail {
            details: format!(
                "error response advertises ApiVersions max v{}, but the server \
                 previously advertised v{max}",
                range.max_version
            ),
        },
        None => Verdict::Fail {
            details: "UNSUPPORTED_VERSION response does not advertise the supported \
                      ApiVersions range"
                .into(),
        },
    }
}

/// Pick the newest Metadata version both sides speak, requiring at least
/// `floor`. Returns a skip verdict when there is none.
fn negotiate_metadata(advertised: Option<(i16, i16)>, floor: i16) -> Result<i16, Verdict> {
    let Some((min, max)) = advertised else {
        return Err(Verdict::Skipped {
            reason: "server does not advertise the Metadata api (or discovery failed)".into(),
        });
    };
    let version = max.min(MetadataRequest::MAX_VERSION);
    if version < min {
        return Err(Verdict::Skipped {
            reason: format!(
                "no common Metadata version: server speaks {min}-{max}, suite up to {}",
                MetadataRequest::MAX_VERSION
            ),
        });
    }
    if version < floor {
        return Err(Verdict::Skipped {
            reason: format!("needs Metadata v{floor}+, best common version is v{version}"),
        });
    }
    Ok(version)
}

/// One Metadata exchange naming no topics, validating the correlation
/// echo, the version-appropriate response header, and full body decode.
async fn metadata_exchange(
    addr: &str,
    version: i16,
    correlation_id: i32,
) -> Result<MetadataResponse, String> {
    let mut conn = RawConnection::connect(addr)
        .await
        .map_err(|e| e.to_string())?;
    let req = MetadataRequest {
        // An empty (non-null) topics array means "no topics" from v1 on;
        // the checks only negotiate v1+.
        topics: Some(Vec::new()),
        allow_auto_topic_creation: false,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, version).map_err(|e| e.to_string())?;

    let flexible = metadata_request::is_flexible(version);
    let header = RequestHeader {
        request_api_key: MetadataRequest::API_KEY,
        request_api_version: version,
        correlation_id,
        client_id: Some(CLIENT_ID.into()),
        unknown_tagged_fields: Vec::new(),
    };
    let mut frame = conn
        .round_trip(&header, if flexible { 2 } else { 1 }, &body)
        .await
        .map_err(|e| e.to_string())?;

    let echoed = wire::get_i32(&mut frame.clone())
        .map_err(|_| "response frame shorter than a correlation id".to_string())?;
    if echoed != correlation_id {
        return Err(format!(
            "sent correlation id {correlation_id}, response carries {echoed}"
        ));
    }
    let resp_header_version = if flexible { 1 } else { 0 };
    ResponseHeader::decode(&mut frame, resp_header_version)
        .map_err(|e| format!("response header (decoded as v{resp_header_version}): {e}"))?;
    let resp = MetadataResponse::decode(&mut frame, version)
        .map_err(|e| format!("response body (decoded as v{version}): {e}"))?;
    if !frame.is_empty() {
        return Err(format!(
            "{} byte(s) left over after the response body — wrong response \
             header version or corrupt body encoding",
            frame.len()
        ));
    }
    Ok(resp)
}

async fn metadata_basic(addr: &str, advertised: Option<(i16, i16)>) -> Verdict {
    let version = match negotiate_metadata(advertised, 1) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let resp = match metadata_exchange(addr, version, 4).await {
        Ok(resp) => resp,
        Err(details) => return Verdict::Fail { details },
    };
    if resp.brokers.is_empty() {
        return Verdict::Fail {
            details: "brokers list is empty".into(),
        };
    }
    let mut ids: Vec<i32> = resp.brokers.iter().map(|b| b.node_id).collect();
    ids.sort_unstable();
    ids.dedup();
    if ids.len() != resp.brokers.len() {
        return Verdict::Fail {
            details: "brokers list repeats a node id".into(),
        };
    }
    for b in &resp.brokers {
        if b.host.is_empty() || !(1..=65535).contains(&b.port) {
            return Verdict::Fail {
                details: format!(
                    "broker {} advertises implausible endpoint {:?}:{}",
                    b.node_id, b.host, b.port
                ),
            };
        }
    }
    if !resp.topics.is_empty() {
        return Verdict::Fail {
            details: format!(
                "requested no topics, response names {} topic(s)",
                resp.topics.len()
            ),
        };
    }
    Verdict::Pass
}

async fn metadata_flexible_header(addr: &str, advertised: Option<(i16, i16)>) -> Verdict {
    let version = match negotiate_metadata(advertised, 9) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    // metadata_exchange decodes the response header at v1 for flexible
    // versions and demands the body consume every remaining byte, so a
    // v0-header response cannot pass undetected.
    match metadata_exchange(addr, version, 5).await {
        Ok(_) => Verdict::Pass,
        Err(details) => Verdict::Fail { details },
    }
}
