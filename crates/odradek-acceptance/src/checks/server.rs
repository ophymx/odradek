//! Checks that run against a server under test (the suite acts as client).
//!
//! Every check opens its own connection so subjects are validated from a
//! clean state, and failures in one check cannot poison another. The
//! catalog [`SERVER_CHECKS`] is the single source of truth: [`run`]
//! executes exactly the Server-role checks it lists, in order.
//!
//! All traffic goes through one exchange path (`checked_call`) that
//! always validates the correlation echo, decodes the response header and
//! body, and rejects trailing bytes — no response gets a lighter
//! inspection than any other.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use odradek_protocol::messages::create_topics_response::CreateTopicsResponse;
use odradek_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use odradek_protocol::messages::fetch_response::FetchResponse;
use odradek_protocol::messages::metadata_request::MetadataRequest;
use odradek_protocol::messages::metadata_response::MetadataResponse;
use odradek_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use odradek_protocol::messages::produce_response::ProduceResponse;
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use odradek_protocol::records::{Record, RecordBatch, RecordHeader, Records, decode_set};
use odradek_protocol::{ErrorCode, Message, frame, header};

use crate::checks::{Check, Runner};
use crate::raw::{RawConnection, WireError};
use crate::report::{CheckOutcome, Report};
use crate::{CheckId, SubjectRole, Verdict};

const CLIENT_ID: &str = "odradek-acceptance";

/// Every server-side check, in run order. Ids are stable; baselines and
/// the calibration registry cite them verbatim.
pub static SERVER_CHECKS: &[Check] = &[
    Check {
        id: "api-versions/v0-basic",
        requirement: "responds to ApiVersions v0 with error NONE, advertises \
                      ApiVersions itself, and every advertised range has min <= max",
        runner: Runner::Server(|ctx| Box::pin(v0_basic(ctx))),
    },
    Check {
        id: "api-versions/correlation-echo",
        requirement: "echoes the request correlation id, including unusual values",
        runner: Runner::Server(|ctx| Box::pin(correlation_echo(ctx))),
    },
    Check {
        id: "api-versions/flexible-v3",
        requirement: "answers a flexible (v3+) ApiVersions request, including \
                      the tagged-field sections, with a v0 response header",
        runner: Runner::Server(|ctx| Box::pin(flexible_v3(ctx))),
    },
    Check {
        id: "api-versions/unsupported-version-error",
        requirement: "rejects an ApiVersions request newer than it supports \
                      with UNSUPPORTED_VERSION in a v0-encoded response that \
                      advertises the supported range",
        runner: Runner::Server(|ctx| Box::pin(unsupported_version(ctx))),
    },
    Check {
        id: "metadata/basic",
        requirement: "answers a Metadata request naming no topics with a \
                      non-empty brokers list (unique node ids, valid ports) \
                      and no topics the client did not ask about",
        runner: Runner::Server(|ctx| Box::pin(metadata_basic(ctx))),
    },
    Check {
        id: "metadata/flexible-response-header",
        requirement: "answers a flexible (v9+) Metadata request with a v1 \
                      response header carrying the tagged-fields section — \
                      the ApiVersions always-v0 quirk does not apply to \
                      other apis",
        runner: Runner::Server(|ctx| Box::pin(metadata_flexible_header(ctx))),
    },
    Check {
        id: "produce/basic",
        requirement: "accepts a produce (acks=-1) of one well-formed record \
                      batch to a freshly created topic with error NONE and \
                      assigns it base offset 0",
        runner: Runner::Server(|ctx| Box::pin(produce_basic(ctx))),
    },
    Check {
        id: "fetch/batch-integrity",
        requirement: "a fetch returns the produced record batch byte-identical \
                      from the magic byte onward (crc included) — only \
                      base_offset and partition_leader_epoch, which sit \
                      outside the crc, may be rewritten",
        runner: Runner::Server(|ctx| Box::pin(fetch_batch_integrity(ctx))),
    },
    Check {
        id: "produce/topic-id",
        requirement: "accepts a topic-id-addressed produce (v13+) to a fresh \
                      topic, the id learned from CreateTopics, with error NONE",
        runner: Runner::Server(|ctx| Box::pin(produce_topic_id(ctx))),
    },
    Check {
        id: "fetch/topic-id",
        requirement: "serves a topic-id-addressed fetch (v13+), echoing the \
                      requested topic id and returning the produced batch \
                      intact",
        runner: Runner::Server(|ctx| Box::pin(fetch_topic_id(ctx))),
    },
];

/// Run the catalogued Server-role checks against `addr` and collect a
/// report.
pub async fn run(addr: &str) -> Report {
    let ctx = ServerCtx::discover(addr).await;
    let mut outcomes = Vec::new();
    for check in crate::checks::catalog() {
        if check.role() != SubjectRole::Server {
            continue;
        }
        let Runner::Server(runner) = check.runner else {
            continue;
        };
        outcomes.push(CheckOutcome::new(
            CheckId(check.id.into()),
            check.requirement,
            runner(&ctx).await,
        ));
    }
    Report::new(format!("server {addr}"), outcomes)
}

/// Why an exchange did not yield a validated response: the suite could
/// not run it (infrastructure) or the subject misbehaved on the wire.
/// The distinction is what keeps a flaky network from reading as
/// nonconformance.
#[derive(Debug)]
enum CheckError {
    /// The check could not run: connection refused, i/o failure, timeout.
    Infra(String),
    /// The subject violated the requirement under test.
    Violation(String),
}

impl CheckError {
    fn context(self, what: &str) -> CheckError {
        match self {
            CheckError::Infra(d) => CheckError::Infra(format!("{what}: {d}")),
            CheckError::Violation(d) => CheckError::Violation(format!("{what}: {d}")),
        }
    }

    fn into_verdict(self) -> Verdict {
        match self {
            CheckError::Infra(details) => Verdict::Error { details },
            CheckError::Violation(details) => Verdict::Fail { details },
        }
    }
}

impl From<WireError> for CheckError {
    fn from(e: WireError) -> CheckError {
        match e {
            // An implausible frame length is the subject talking garbage.
            WireError::BadFrameLength(_) => CheckError::Violation(e.to_string()),
            // I/o trouble, timeouts, and our own encode failures mean the
            // exchange never got a fair chance to observe the subject.
            WireError::Io(_) | WireError::Timeout | WireError::Encode(_) => {
                CheckError::Infra(e.to_string())
            }
        }
    }
}

async fn connect(addr: &str) -> Result<RawConnection, CheckError> {
    RawConnection::connect(addr)
        .await
        .map_err(|e| CheckError::Infra(format!("connect {addr}: {e}")))
}

/// Discovery and shared state for one server run: the subject's address
/// plus the api ranges learned from an up-front ApiVersions v0 exchange.
#[derive(Debug)]
pub(crate) struct ServerCtx {
    addr: String,
    /// `Err` when discovery could not run at all (infrastructure); an
    /// empty list when the exchange ran but yielded nothing usable — a
    /// protocol problem `api-versions/v0-basic` reports, which the other
    /// checks answer with skips exactly as before.
    discovery: Result<Vec<ApiVersion>, String>,
}

impl ServerCtx {
    async fn discover(addr: &str) -> ServerCtx {
        let discovery = match exchange(addr, 0, 1, 9, 0).await {
            Ok(resp) => Ok(resp.api_keys),
            Err(CheckError::Violation(_)) => Ok(Vec::new()),
            Err(CheckError::Infra(details)) => {
                Err(format!("discovery (ApiVersions v0): {details}"))
            }
        };
        ServerCtx {
            addr: addr.into(),
            discovery,
        }
    }

    /// The advertised range for `api_key`, or an infra [`Verdict::Error`]
    /// when discovery never ran.
    fn range(&self, api_key: i16) -> Result<Option<(i16, i16)>, Verdict> {
        match &self.discovery {
            Ok(keys) => Ok(advertised_range(keys, api_key)),
            Err(details) => Err(Verdict::Error {
                details: details.clone(),
            }),
        }
    }
}

fn advertised_range(keys: &[ApiVersion], api_key: i16) -> Option<(i16, i16)> {
    keys.iter()
        .find(|v| v.api_key == api_key)
        .map(|v| (v.min_version, v.max_version))
}

/// The wire coordinates of one exchange.
struct Call {
    api_key: i16,
    api_version: i16,
    request_header_version: i16,
    response_header_version: i16,
    correlation_id: i32,
    /// The version to decode the response body at (differs from
    /// `api_version` only for from-the-future ApiVersions probes).
    decode_at: i16,
}

/// The one exchange path every server-side check goes through: frame the
/// request, validate the correlation echo, decode the response header and
/// body, and reject trailing bytes. Produce, Fetch, and CreateTopics
/// responses get exactly the same scrutiny as ApiVersions and Metadata.
async fn checked_call<T: Message>(
    conn: &mut RawConnection,
    call: Call,
    body: &[u8],
) -> Result<T, CheckError> {
    let mut req_header = RequestHeader::default();
    req_header.request_api_key = call.api_key;
    req_header.request_api_version = call.api_version;
    req_header.correlation_id = call.correlation_id;
    req_header.client_id = Some(CLIENT_ID.into());

    let mut frame = conn
        .round_trip(&req_header, call.request_header_version, body)
        .await?;
    let echoed = frame::peek_correlation_id(&frame).map_err(|_| {
        CheckError::Violation("response frame shorter than a correlation id".into())
    })?;
    if echoed != call.correlation_id {
        return Err(CheckError::Violation(format!(
            "sent correlation id {}, response carries {echoed}",
            call.correlation_id
        )));
    }
    let hv = call.response_header_version;
    ResponseHeader::decode(&mut frame, hv)
        .map_err(|e| CheckError::Violation(format!("response header (decoded as v{hv}): {e}")))?;
    let resp = T::decode(&mut frame, call.decode_at).map_err(|e| {
        CheckError::Violation(format!(
            "response body (decoded as v{}): {e}",
            call.decode_at
        ))
    })?;
    if !frame.is_empty() {
        return Err(CheckError::Violation(format!(
            "{} byte(s) of trailing garbage after the response body",
            frame.len()
        )));
    }
    Ok(resp)
}

/// One exchange at a negotiated version on an existing connection, header
/// versions derived from the api tables.
async fn api_call<T: Message>(
    conn: &mut RawConnection,
    api_key: i16,
    version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Result<T, CheckError> {
    let request_header_version =
        header::request_header_version(api_key, version).ok_or_else(|| {
            CheckError::Infra(format!(
                "no header version known for api {api_key} v{version}"
            ))
        })?;
    let response_header_version = header::response_header_version(api_key, version)
        .expect("request header version implies response header version");
    checked_call(
        conn,
        Call {
            api_key,
            api_version: version,
            request_header_version,
            response_header_version,
            correlation_id,
            decode_at: version,
        },
        body,
    )
    .await
}

/// One ApiVersions exchange on a fresh connection. The response header is
/// always decoded at v0 (the negotiation-bootstrap quirk).
async fn exchange(
    addr: &str,
    api_version: i16,
    header_version: i16,
    correlation_id: i32,
    decode_at: i16,
) -> Result<ApiVersionsResponse, CheckError> {
    let mut conn = connect(addr).await?;
    let mut body = BytesMut::new();
    let mut req = ApiVersionsRequest::default();
    req.client_software_name = "odradek-acceptance".into();
    req.client_software_version = env!("CARGO_PKG_VERSION").into();
    // Encode the body at the newest shape the schema knows; for a probe of
    // an unknown future version this is the closest well-formed guess.
    req.encode(&mut body, api_version.min(ApiVersionsRequest::MAX_VERSION))
        .map_err(|e| CheckError::Infra(e.to_string()))?;
    checked_call(
        &mut conn,
        Call {
            api_key: ApiVersionsRequest::API_KEY,
            api_version,
            request_header_version: header_version,
            response_header_version: 0,
            correlation_id,
            decode_at,
        },
        &body,
    )
    .await
}

async fn v0_basic(ctx: &ServerCtx) -> Verdict {
    let resp = match exchange(&ctx.addr, 0, 1, 1, 0).await {
        Ok(resp) => resp,
        Err(e) => return e.into_verdict(),
    };
    let code = ErrorCode(resp.error_code);
    if !code.is_ok() {
        return Verdict::Fail {
            details: format!("error code {code}"),
        };
    }
    for v in &resp.api_keys {
        if v.min_version > v.max_version {
            return Verdict::Fail {
                details: format!(
                    "api key {} advertises min {} > max {}",
                    v.api_key, v.min_version, v.max_version
                ),
            };
        }
    }
    match advertised_range(&resp.api_keys, ApiVersionsRequest::API_KEY) {
        Some((min, _)) if min <= 0 => Verdict::Pass,
        Some((min, max)) => Verdict::Fail {
            details: format!(
                "ApiVersions advertised as {min}-{max}, but the server just answered v0"
            ),
        },
        None => Verdict::Fail {
            details: "response does not advertise the ApiVersions api itself".into(),
        },
    }
}

async fn correlation_echo(ctx: &ServerCtx) -> Verdict {
    match exchange(&ctx.addr, 0, 1, i32::MAX - 17, 0).await {
        Ok(_) => Verdict::Pass,
        Err(e) => e.into_verdict(),
    }
}

async fn flexible_v3(ctx: &ServerCtx) -> Verdict {
    let advertised = match ctx.range(ApiVersionsRequest::API_KEY) {
        Ok(a) => a,
        Err(v) => return v,
    };
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
    match exchange(&ctx.addr, version, 2, 2, version).await {
        Ok(resp) if ErrorCode(resp.error_code).is_ok() => Verdict::Pass,
        Ok(resp) => Verdict::Fail {
            details: format!("error code {}", ErrorCode(resp.error_code)),
        },
        Err(e) => e.into_verdict(),
    }
}

async fn unsupported_version(ctx: &ServerCtx) -> Verdict {
    let advertised = match ctx.range(ApiVersionsRequest::API_KEY) {
        Ok(a) => a,
        Err(v) => return v,
    };
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
    let resp = match exchange(&ctx.addr, probe, 2, 3, 0).await {
        Ok(resp) => resp,
        Err(e) => return e.into_verdict(),
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

/// One Metadata exchange naming no topics, on a fresh connection, with
/// the version-appropriate headers.
async fn metadata_exchange(
    addr: &str,
    version: i16,
    correlation_id: i32,
) -> Result<MetadataResponse, CheckError> {
    let mut conn = connect(addr).await?;
    let mut req = MetadataRequest::default();
    // An empty (non-null) topics array means "no topics" from v1 on;
    // the checks only negotiate v1+.
    req.topics = Some(Vec::new());
    req.allow_auto_topic_creation = false;
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .map_err(|e| CheckError::Infra(e.to_string()))?;

    // Header versions come from the shared tables: request header v2 and
    // response header v1 for flexible (v9+) versions, v1/v0 below.
    api_call(
        &mut conn,
        MetadataRequest::API_KEY,
        version,
        correlation_id,
        &body,
    )
    .await
}

async fn metadata_basic(ctx: &ServerCtx) -> Verdict {
    let advertised = match ctx.range(MetadataRequest::API_KEY) {
        Ok(a) => a,
        Err(v) => return v,
    };
    let version = match negotiate("Metadata", advertised, 1, MetadataRequest::MAX_VERSION) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let resp = match metadata_exchange(&ctx.addr, version, 4).await {
        Ok(resp) => resp,
        Err(e) => return e.into_verdict(),
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

async fn metadata_flexible_header(ctx: &ServerCtx) -> Verdict {
    let advertised = match ctx.range(MetadataRequest::API_KEY) {
        Ok(a) => a,
        Err(v) => return v,
    };
    let version = match negotiate("Metadata", advertised, 9, MetadataRequest::MAX_VERSION) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    // metadata_exchange decodes the response header at v1 for flexible
    // versions and demands the body consume every remaining byte, so a
    // v0-header response cannot pass undetected.
    match metadata_exchange(&ctx.addr, version, 5).await {
        Ok(_) => Verdict::Pass,
        Err(e) => e.into_verdict(),
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
    ctx: &ServerCtx,
    tag: &str,
    addressing: Addressing,
) -> Result<ProducedTopic, Verdict> {
    let create_version = negotiate(
        "CreateTopics",
        ctx.range(CreateTopicsRequest::API_KEY)?,
        CreateTopicsRequest::MIN_VERSION,
        CreateTopicsRequest::MAX_VERSION,
    )?;
    let produce_range = ctx.range(ProduceRequest::API_KEY)?;
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
    // Failing to encode our own request means the check never ran.
    let infra = |details: String| Verdict::Error { details };

    let mut conn = connect(&ctx.addr).await.map_err(CheckError::into_verdict)?;
    let topic = unique_topic(tag);

    // Create the topic.
    let mut creatable = CreatableTopic::default();
    creatable.name = topic.clone();
    creatable.num_partitions = 1;
    creatable.replication_factor = 1;
    let mut create = CreateTopicsRequest::default();
    create.topics = vec![creatable];
    create.timeout_ms = 30_000;
    create.validate_only = false;
    let mut body = BytesMut::new();
    create
        .encode(&mut body, create_version)
        .map_err(|e| infra(e.to_string()))?;
    let resp: CreateTopicsResponse = api_call(
        &mut conn,
        CreateTopicsRequest::API_KEY,
        create_version,
        10,
        &body,
    )
    .await
    .map_err(|e| e.context("CreateTopics").into_verdict())?;
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
        .map_err(|e| infra(e.to_string()))?;
    let sent = sent.freeze();

    let mut partition_data = PartitionProduceData::default();
    partition_data.index = 0;
    partition_data.records = Some(sent.clone());
    let mut topic_data = TopicProduceData::default();
    // v13+ drops the name for the id; encode gates pick per version.
    topic_data.name = match addressing {
        Addressing::Name => topic.clone(),
        Addressing::TopicId => String::new(),
    };
    topic_data.topic_id = match addressing {
        Addressing::Name => [0u8; 16],
        Addressing::TopicId => topic_id,
    };
    topic_data.partition_data = vec![partition_data];
    let mut produce = ProduceRequest::default();
    produce.transactional_id = None;
    produce.acks = -1;
    produce.timeout_ms = 10_000;
    produce.topic_data = vec![topic_data];
    let mut body = BytesMut::new();
    produce
        .encode(&mut body, produce_version)
        .map_err(|e| infra(e.to_string()))?;

    let mut last_code = ErrorCode(0);
    for _ in 0..SETTLE_ATTEMPTS {
        let resp: ProduceResponse = api_call(
            &mut conn,
            ProduceRequest::API_KEY,
            produce_version,
            11,
            &body,
        )
        .await
        .map_err(|e| e.context("Produce").into_verdict())?;
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

async fn produce_basic(ctx: &ServerCtx) -> Verdict {
    let produced = match produce_flow(ctx, "produce", Addressing::Name).await {
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

async fn produce_topic_id(ctx: &ServerCtx) -> Verdict {
    match produce_flow(ctx, "produce-id", Addressing::TopicId).await {
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
) -> Result<Bytes, CheckError> {
    let mut fetch_partition = FetchPartition::default();
    fetch_partition.partition = 0;
    fetch_partition.current_leader_epoch = -1;
    fetch_partition.fetch_offset = 0;
    fetch_partition.last_fetched_epoch = -1;
    fetch_partition.log_start_offset = -1;
    fetch_partition.partition_max_bytes = 1 << 20;
    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = match addressing {
        Addressing::Name => produced.topic.clone(),
        Addressing::TopicId => String::new(),
    };
    fetch_topic.topic_id = match addressing {
        Addressing::Name => [0u8; 16],
        Addressing::TopicId => produced.topic_id,
    };
    fetch_topic.partitions = vec![fetch_partition];
    let mut fetch = FetchRequest::default();
    fetch.max_wait_ms = 500;
    fetch.min_bytes = 1;
    fetch.max_bytes = 8 << 20;
    fetch.session_id = 0;
    fetch.session_epoch = -1; // sessionless full fetch
    fetch.topics = vec![fetch_topic];
    let mut body = BytesMut::new();
    fetch
        .encode(&mut body, fetch_version)
        .map_err(|e| CheckError::Infra(e.to_string()))?;

    // acks=-1 already committed the batch, but give replication internals
    // a moment anyway rather than failing on an empty first response.
    let mut last = String::from("fetch returned no records");
    for _ in 0..SETTLE_ATTEMPTS {
        let outcome = async {
            let resp: FetchResponse = api_call(
                &mut produced.conn,
                FetchRequest::API_KEY,
                fetch_version,
                12,
                &body,
            )
            .await
            .map_err(|e| e.context("Fetch"))?;
            let code = ErrorCode(resp.error_code);
            if !code.is_ok() {
                return Err(CheckError::Violation(format!(
                    "Fetch failed with top-level {code}"
                )));
            }
            let topic = resp
                .responses
                .first()
                .ok_or_else(|| CheckError::Violation("Fetch response names no topics".into()))?;
            if addressing == Addressing::TopicId && topic.topic_id != produced.topic_id {
                return Err(CheckError::Violation(format!(
                    "response echoes topic id {:02x?}, requested {:02x?}",
                    topic.topic_id, produced.topic_id
                )));
            }
            let partition = topic.partitions.first().ok_or_else(|| {
                CheckError::Violation("Fetch response names no partitions".into())
            })?;
            let code = ErrorCode(partition.error_code);
            if !code.is_ok() {
                return Err(CheckError::Violation(format!("Fetch failed with {code}")));
            }
            Ok(partition.records.clone().unwrap_or_default())
        }
        .await;
        match outcome {
            Ok(got) if !got.is_empty() => return Ok(got),
            Ok(_) => {}
            // Infrastructure trouble is not going to settle; surface it.
            Err(CheckError::Infra(details)) => return Err(CheckError::Infra(details)),
            Err(CheckError::Violation(details)) => {
                let transient = [
                    "UNKNOWN_TOPIC_OR_PARTITION",
                    "NOT_LEADER_OR_FOLLOWER",
                    "LEADER_NOT_AVAILABLE",
                    "UNKNOWN_TOPIC_ID",
                ];
                if !transient.iter().any(|t| details.contains(t)) {
                    return Err(CheckError::Violation(details));
                }
                last = details;
            }
        }
        tokio::time::sleep(SETTLE_DELAY).await;
    }
    Err(CheckError::Violation(last))
}

async fn fetch_batch_integrity(ctx: &ServerCtx) -> Verdict {
    let fetch_range = match ctx.range(FetchRequest::API_KEY) {
        Ok(r) => r,
        Err(v) => return v,
    };
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
    let mut produced = match produce_flow(ctx, "fetch", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    match run_fetch(&mut produced, fetch_version, Addressing::Name).await {
        Ok(got) => batch_integrity(&produced.sent, &got),
        Err(e) => e.into_verdict(),
    }
}

async fn fetch_topic_id(ctx: &ServerCtx) -> Verdict {
    let fetch_range = match ctx.range(FetchRequest::API_KEY) {
        Ok(r) => r,
        Err(v) => return v,
    };
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
    let mut produced = match produce_flow(ctx, "fetch-id", Addressing::Name).await {
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
        Err(e) => e.into_verdict(),
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
