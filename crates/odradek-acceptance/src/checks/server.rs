//! Checks that run against a server under test (the suite acts as client).
//!
//! Every check opens its own connection so subjects are validated from a
//! clean state, and failures in one check cannot poison another.

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::ApiVersionsResponse;
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
    // advertised ApiVersions range.
    let (verdict, advertised) = v0_basic(addr).await;
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
        verdict: flexible_v3(addr, advertised).await,
    });

    outcomes.push(CheckOutcome {
        id: CheckId("api-versions/unsupported-version-error".into()),
        requirement: "rejects an ApiVersions request newer than it supports \
                      with UNSUPPORTED_VERSION in a v0-encoded response that \
                      advertises the supported range",
        verdict: unsupported_version(addr, advertised).await,
    });

    Report {
        subject: format!("server {addr}"),
        outcomes,
    }
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

async fn v0_basic(addr: &str) -> (Verdict, Option<(i16, i16)>) {
    let resp = match exchange(addr, 0, 1, 1, 0).await {
        Ok(resp) => resp,
        Err(details) => return (Verdict::Fail { details }, None),
    };
    let code = ErrorCode(resp.error_code);
    if !code.is_ok() {
        return (
            Verdict::Fail {
                details: format!("error code {code}"),
            },
            None,
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
                None,
            );
        }
    }
    let advertised = resp
        .api_keys
        .iter()
        .find(|v| v.api_key == ApiVersionsRequest::API_KEY)
        .map(|v| (v.min_version, v.max_version));
    match advertised {
        Some(range) if range.0 <= 0 => (Verdict::Pass, Some(range)),
        Some(range) => (
            Verdict::Fail {
                details: format!(
                    "ApiVersions advertised as {}-{}, but the server just answered v0",
                    range.0, range.1
                ),
            },
            Some(range),
        ),
        None => (
            Verdict::Fail {
                details: "response does not advertise the ApiVersions api itself".into(),
            },
            None,
        ),
    }
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
