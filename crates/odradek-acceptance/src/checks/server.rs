//! Checks that run against a server under test (the suite acts as client).
//!
//! Every check opens its own connection so subjects are validated from a
//! clean state, and failures in one check cannot poison another.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use odradek_protocol::messages::create_topics_response::CreateTopicsResponse;
use odradek_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use odradek_protocol::messages::fetch_response::FetchResponse;
use odradek_protocol::messages::metadata_request::{self, MetadataRequest};
use odradek_protocol::messages::metadata_response::MetadataResponse;
use odradek_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use odradek_protocol::messages::produce_response::ProduceResponse;
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use odradek_protocol::records::{Record, RecordBatch, RecordHeader, Records, decode_set};
use odradek_protocol::{ErrorCode, header, wire};

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

    let create_range = advertised_range(&keys, CreateTopicsRequest::API_KEY);
    let produce_range = advertised_range(&keys, ProduceRequest::API_KEY);
    let fetch_range = advertised_range(&keys, FetchRequest::API_KEY);

    outcomes.push(CheckOutcome {
        id: CheckId("produce/basic".into()),
        requirement: "accepts a produce (acks=-1) of one well-formed record \
                      batch to a freshly created topic with error NONE and \
                      assigns it base offset 0",
        verdict: produce_basic(addr, create_range, produce_range).await,
    });

    outcomes.push(CheckOutcome {
        id: CheckId("fetch/batch-integrity".into()),
        requirement: "a fetch returns the produced record batch byte-identical \
                      from the magic byte onward (crc included) — only \
                      base_offset and partition_leader_epoch, which sit \
                      outside the crc, may be rewritten",
        verdict: fetch_batch_integrity(addr, create_range, produce_range, fetch_range).await,
    });

    outcomes.push(CheckOutcome {
        id: CheckId("produce/topic-id".into()),
        requirement: "accepts a topic-id-addressed produce (v13+) to a fresh \
                      topic, the id learned from CreateTopics, with error NONE",
        verdict: produce_topic_id(addr, create_range, produce_range).await,
    });

    outcomes.push(CheckOutcome {
        id: CheckId("fetch/topic-id".into()),
        requirement: "serves a topic-id-addressed fetch (v13+), echoing the \
                      requested topic id and returning the produced batch \
                      intact",
        verdict: fetch_topic_id(addr, create_range, produce_range, fetch_range).await,
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

/// Pick the newest version of `api` both sides speak, bounded by what the
/// check itself can handle. Returns a skip verdict when there is none.
fn negotiate(
    api: &'static str,
    advertised: Option<(i16, i16)>,
    check_min: i16,
    check_max: i16,
) -> Result<i16, Verdict> {
    let Some((min, max)) = advertised else {
        return Err(Verdict::Skipped {
            reason: format!("server does not advertise the {api} api (or discovery failed)"),
        });
    };
    let version = max.min(check_max);
    if version < min || version < check_min {
        return Err(Verdict::Skipped {
            reason: format!(
                "no usable {api} version: server speaks {min}-{max}, check needs \
                 {check_min}-{check_max}"
            ),
        });
    }
    Ok(version)
}

/// One request/response exchange on an existing connection: send `body`
/// framed with the version-appropriate headers, validate the correlation
/// echo, and return the response body bytes.
async fn request_response(
    conn: &mut RawConnection,
    api_key: i16,
    version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Result<Bytes, String> {
    let req_hv = header::request_header_version(api_key, version)
        .ok_or_else(|| format!("no header version known for api {api_key} v{version}"))?;
    let resp_hv = header::response_header_version(api_key, version)
        .expect("request header version implies response header version");
    let req_header = RequestHeader {
        request_api_key: api_key,
        request_api_version: version,
        correlation_id,
        client_id: Some(CLIENT_ID.into()),
        unknown_tagged_fields: Vec::new(),
    };
    let mut frame = conn
        .round_trip(&req_header, req_hv, body)
        .await
        .map_err(|e| e.to_string())?;
    let echoed = wire::get_i32(&mut frame.clone())
        .map_err(|_| "response frame shorter than a correlation id".to_string())?;
    if echoed != correlation_id {
        return Err(format!(
            "sent correlation id {correlation_id}, response carries {echoed}"
        ));
    }
    ResponseHeader::decode(&mut frame, resp_hv)
        .map_err(|e| format!("response header (decoded as v{resp_hv}): {e}"))?;
    Ok(frame)
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
    let version = match negotiate("Metadata", advertised, 1, MetadataRequest::MAX_VERSION) {
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
    let version = match negotiate("Metadata", advertised, 9, MetadataRequest::MAX_VERSION) {
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

// ---------------------------------------------------------------------------
// Produce / fetch
// ---------------------------------------------------------------------------

/// Newest name-addressed Produce/Fetch versions: v13+ switches to topic
/// ids, which the `*/topic-id` checks exercise separately.
const PRODUCE_NAME_MAX: i16 = 12;
const FETCH_NAME_MAX: i16 = 12;
/// First topic-id-addressed versions.
const PRODUCE_ID_MIN: i16 = 13;
const FETCH_ID_MIN: i16 = 13;

/// How long the flow tolerates a freshly created topic answering with
/// retriable errors before calling it a failure.
const SETTLE_ATTEMPTS: u32 = 50;
const SETTLE_DELAY: Duration = Duration::from_millis(100);

fn retriable(code: ErrorCode) -> bool {
    // The topic or its leadership is still materializing after create;
    // id-addressed requests surface the same lag as UNKNOWN_TOPIC_ID.
    code == ErrorCode::UNKNOWN_TOPIC_OR_PARTITION
        || code == ErrorCode::LEADER_NOT_AVAILABLE
        || code == ErrorCode::NOT_LEADER_OR_FOLLOWER
        || code == ErrorCode::UNKNOWN_TOPIC_ID
}

/// A topic name unique enough to never collide across runs or checks.
fn unique_topic(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("odradek-accept-{tag}-{}-{nanos}", std::process::id())
}

/// The batch every produce/fetch check sends: two records with keys,
/// values, a header, and a tombstone — enough shape to make byte-level
/// integrity meaningful.
fn probe_batch() -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: 1,
        base_timestamp: 1_758_000_000_000,
        max_timestamp: 1_758_000_000_001,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: Records::Plain(vec![
            Record {
                key: Some(Bytes::from_static(b"probe-key")),
                value: Some(Bytes::from_static(b"odradek conformance probe")),
                headers: vec![RecordHeader {
                    key: "origin".into(),
                    value: Some(Bytes::from_static(b"odradek-accept")),
                }],
                ..Default::default()
            },
            Record {
                timestamp_delta: 1,
                offset_delta: 1,
                key: Some(Bytes::from_static(b"probe-tombstone")),
                value: None,
                ..Default::default()
            },
        ]),
        ..Default::default()
    }
}

/// A produced topic: the live connection, its identity, and the exact
/// record set bytes that were sent.
struct ProducedTopic {
    conn: RawConnection,
    topic: String,
    /// From CreateTopics (v7+ returns it); zero-uuid means unknown.
    topic_id: [u8; 16],
    sent: Bytes,
    base_offset: i64,
}

/// How the produce leg of the flow addresses the topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Addressing {
    Name,
    TopicId,
}

/// Create a unique single-partition topic and produce [`probe_batch`] to
/// partition 0 with acks=-1, retrying while the topic materializes.
async fn produce_flow(
    addr: &str,
    tag: &str,
    create_range: Option<(i16, i16)>,
    produce_range: Option<(i16, i16)>,
    addressing: Addressing,
) -> Result<ProducedTopic, Verdict> {
    let create_version = negotiate(
        "CreateTopics",
        create_range,
        CreateTopicsRequest::MIN_VERSION,
        CreateTopicsRequest::MAX_VERSION,
    )?;
    let produce_version = match addressing {
        Addressing::Name => negotiate(
            "Produce",
            produce_range,
            ProduceRequest::MIN_VERSION,
            PRODUCE_NAME_MAX,
        )?,
        Addressing::TopicId => negotiate(
            "Produce",
            produce_range,
            PRODUCE_ID_MIN,
            ProduceRequest::MAX_VERSION,
        )?,
    };
    let fail = |details: String| Verdict::Fail { details };

    let mut conn = RawConnection::connect(addr)
        .await
        .map_err(|e| fail(e.to_string()))?;
    let topic = unique_topic(tag);

    // Create the topic.
    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.clone(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 30_000,
        validate_only: false,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    create
        .encode(&mut body, create_version)
        .map_err(|e| fail(e.to_string()))?;
    let mut resp = request_response(
        &mut conn,
        CreateTopicsRequest::API_KEY,
        create_version,
        10,
        &body,
    )
    .await
    .map_err(|e| fail(format!("CreateTopics: {e}")))?;
    let resp = CreateTopicsResponse::decode(&mut resp, create_version)
        .map_err(|e| fail(format!("CreateTopics response (v{create_version}): {e}")))?;
    let result = resp
        .topics
        .first()
        .ok_or_else(|| fail("CreateTopics response names no topics".into()))?;
    let code = ErrorCode(result.error_code);
    if !code.is_ok() {
        return Err(fail(format!(
            "CreateTopics failed with {code}{}",
            result
                .error_message
                .as_deref()
                .map(|m| format!(": {m}"))
                .unwrap_or_default()
        )));
    }
    let topic_id = result.topic_id;
    if addressing == Addressing::TopicId && topic_id == [0u8; 16] {
        return Err(Verdict::Skipped {
            reason: format!("CreateTopics v{create_version} did not return a topic id (needs v7+)"),
        });
    }

    // Produce the probe batch, riding out post-create leadership settling.
    let mut sent = BytesMut::new();
    probe_batch()
        .encode(&mut sent)
        .map_err(|e| fail(e.to_string()))?;
    let sent = sent.freeze();

    let produce = ProduceRequest {
        transactional_id: None,
        acks: -1,
        timeout_ms: 10_000,
        topic_data: vec![TopicProduceData {
            // v13+ drops the name for the id; encode gates pick per version.
            name: match addressing {
                Addressing::Name => topic.clone(),
                Addressing::TopicId => String::new(),
            },
            topic_id: match addressing {
                Addressing::Name => [0u8; 16],
                Addressing::TopicId => topic_id,
            },
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(sent.clone()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    produce
        .encode(&mut body, produce_version)
        .map_err(|e| fail(e.to_string()))?;

    let mut last_code = ErrorCode(0);
    for _ in 0..SETTLE_ATTEMPTS {
        let mut resp = request_response(
            &mut conn,
            ProduceRequest::API_KEY,
            produce_version,
            11,
            &body,
        )
        .await
        .map_err(|e| fail(format!("Produce: {e}")))?;
        let resp = ProduceResponse::decode(&mut resp, produce_version)
            .map_err(|e| fail(format!("Produce response (v{produce_version}): {e}")))?;
        let partition = resp
            .responses
            .first()
            .and_then(|t| t.partition_responses.first())
            .ok_or_else(|| fail("Produce response names no partitions".into()))?;
        let code = ErrorCode(partition.error_code);
        if code.is_ok() {
            return Ok(ProducedTopic {
                conn,
                topic,
                topic_id,
                sent,
                base_offset: partition.base_offset,
            });
        }
        if !retriable(code) {
            return Err(fail(format!("Produce failed with {code}")));
        }
        last_code = code;
        tokio::time::sleep(SETTLE_DELAY).await;
    }
    Err(fail(format!(
        "topic never became producible: still {last_code} after {SETTLE_ATTEMPTS} attempts"
    )))
}

async fn produce_basic(
    addr: &str,
    create_range: Option<(i16, i16)>,
    produce_range: Option<(i16, i16)>,
) -> Verdict {
    let produced = match produce_flow(
        addr,
        "produce",
        create_range,
        produce_range,
        Addressing::Name,
    )
    .await
    {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if produced.base_offset != 0 {
        return Verdict::Fail {
            details: format!(
                "first batch in a fresh topic was assigned base offset {}, expected 0",
                produced.base_offset
            ),
        };
    }
    Verdict::Pass
}

async fn produce_topic_id(
    addr: &str,
    create_range: Option<(i16, i16)>,
    produce_range: Option<(i16, i16)>,
) -> Verdict {
    match produce_flow(
        addr,
        "produce-id",
        create_range,
        produce_range,
        Addressing::TopicId,
    )
    .await
    {
        Ok(_) => Verdict::Pass,
        Err(verdict) => verdict,
    }
}

/// Fetch partition 0 of the produced topic from offset 0, retrying while
/// the topic settles, and return the record set bytes. When addressing by
/// id, also demands the response echo that id — clients correlate by it.
async fn run_fetch(
    produced: &mut ProducedTopic,
    fetch_version: i16,
    addressing: Addressing,
) -> Result<Bytes, String> {
    let fetch = FetchRequest {
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 8 << 20,
        session_id: 0,
        session_epoch: -1, // sessionless full fetch
        topics: vec![FetchTopic {
            topic: match addressing {
                Addressing::Name => produced.topic.clone(),
                Addressing::TopicId => String::new(),
            },
            topic_id: match addressing {
                Addressing::Name => [0u8; 16],
                Addressing::TopicId => produced.topic_id,
            },
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch: -1,
                fetch_offset: 0,
                last_fetched_epoch: -1,
                log_start_offset: -1,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    fetch
        .encode(&mut body, fetch_version)
        .map_err(|e| e.to_string())?;

    // acks=-1 already committed the batch, but give replication internals
    // a moment anyway rather than failing on an empty first response.
    let mut last = String::from("fetch returned no records");
    for _ in 0..SETTLE_ATTEMPTS {
        let outcome = async {
            let mut resp = request_response(
                &mut produced.conn,
                FetchRequest::API_KEY,
                fetch_version,
                12,
                &body,
            )
            .await
            .map_err(|e| format!("Fetch: {e}"))?;
            let resp = FetchResponse::decode(&mut resp, fetch_version)
                .map_err(|e| format!("Fetch response (v{fetch_version}): {e}"))?;
            let code = ErrorCode(resp.error_code);
            if !code.is_ok() {
                return Err(format!("Fetch failed with top-level {code}"));
            }
            let topic = resp
                .responses
                .first()
                .ok_or_else(|| "Fetch response names no topics".to_string())?;
            if addressing == Addressing::TopicId && topic.topic_id != produced.topic_id {
                return Err(format!(
                    "response echoes topic id {:02x?}, requested {:02x?}",
                    topic.topic_id, produced.topic_id
                ));
            }
            let partition = topic
                .partitions
                .first()
                .ok_or_else(|| "Fetch response names no partitions".to_string())?;
            let code = ErrorCode(partition.error_code);
            if !code.is_ok() {
                return Err(format!("Fetch failed with {code}"));
            }
            Ok(partition.records.clone().unwrap_or_default())
        }
        .await;
        match outcome {
            Ok(got) if !got.is_empty() => return Ok(got),
            Ok(_) => {}
            Err(details) => {
                let transient = [
                    "UNKNOWN_TOPIC_OR_PARTITION",
                    "NOT_LEADER_OR_FOLLOWER",
                    "LEADER_NOT_AVAILABLE",
                    "UNKNOWN_TOPIC_ID",
                ];
                if !transient.iter().any(|t| details.contains(t)) {
                    return Err(details);
                }
                last = details;
            }
        }
        tokio::time::sleep(SETTLE_DELAY).await;
    }
    Err(last)
}

async fn fetch_batch_integrity(
    addr: &str,
    create_range: Option<(i16, i16)>,
    produce_range: Option<(i16, i16)>,
    fetch_range: Option<(i16, i16)>,
) -> Verdict {
    let fetch_version = match negotiate(
        "Fetch",
        fetch_range,
        FetchRequest::MIN_VERSION,
        FETCH_NAME_MAX,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    // The flow tolerates a wrong assigned base offset here — that is
    // produce/basic's finding — and always fetches from offset 0.
    let mut produced =
        match produce_flow(addr, "fetch", create_range, produce_range, Addressing::Name).await {
            Ok(p) => p,
            Err(verdict) => return verdict,
        };
    match run_fetch(&mut produced, fetch_version, Addressing::Name).await {
        Ok(got) => batch_integrity(&produced.sent, &got),
        Err(details) => Verdict::Fail { details },
    }
}

async fn fetch_topic_id(
    addr: &str,
    create_range: Option<(i16, i16)>,
    produce_range: Option<(i16, i16)>,
    fetch_range: Option<(i16, i16)>,
) -> Verdict {
    let fetch_version = match negotiate(
        "Fetch",
        fetch_range,
        FETCH_ID_MIN,
        FetchRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    // Produce by name (that leg has its own checks); the id under test
    // here is the fetch path's.
    let mut produced = match produce_flow(
        addr,
        "fetch-id",
        create_range,
        produce_range,
        Addressing::Name,
    )
    .await
    {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if produced.topic_id == [0u8; 16] {
        return Verdict::Skipped {
            reason: "CreateTopics did not return a topic id (needs v7+)".into(),
        };
    }
    match run_fetch(&mut produced, fetch_version, Addressing::TopicId).await {
        Ok(got) => batch_integrity(&produced.sent, &got),
        Err(details) => Verdict::Fail { details },
    }
}

/// Compare the fetched record set against the produced bytes: identical
/// from the magic byte on. Bytes 0-15 (base_offset, batch_length,
/// partition_leader_epoch) sit outside the crc; base_offset must still be
/// 0 for the first batch of a fresh topic, and batch_length equality is
/// implied by the suffix match.
fn batch_integrity(sent: &Bytes, got: &Bytes) -> Verdict {
    let fail = |details: String| Verdict::Fail { details };
    // Decoding first also verifies the crc still matches the contents.
    let batches = match decode_set(&mut got.clone()) {
        Ok(b) => b,
        Err(e) => return fail(format!("fetched record set does not decode: {e}")),
    };
    if batches.len() != 1 {
        return fail(format!(
            "fetched {} batches where exactly the produced one was expected",
            batches.len()
        ));
    }
    if got.len() != sent.len() {
        return fail(format!(
            "fetched batch is {} byte(s), produced was {}",
            got.len(),
            sent.len()
        ));
    }
    if let Some(at) = (16..sent.len()).find(|&i| got[i] != sent[i]) {
        return fail(format!(
            "stored batch differs from the produced bytes starting at byte {at} \
             (crc-covered region)"
        ));
    }
    let base_offset = i64::from_be_bytes(got[..8].try_into().expect("length checked"));
    if base_offset != 0 {
        return fail(format!(
            "fetched batch carries base offset {base_offset}, expected 0"
        ));
    }
    Verdict::Pass
}
