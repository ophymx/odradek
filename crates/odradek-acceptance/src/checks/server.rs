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
use odradek_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use odradek_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use odradek_protocol::messages::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use odradek_protocol::messages::list_offsets_response::ListOffsetsResponse;
use odradek_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
use odradek_protocol::messages::metadata_response::MetadataResponse;
use odradek_protocol::messages::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use odradek_protocol::messages::offset_commit_response::OffsetCommitResponse;
use odradek_protocol::messages::offset_fetch_request::{
    OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use odradek_protocol::messages::offset_fetch_response::OffsetFetchResponse;
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
    Check {
        id: "list-offsets/earliest-latest",
        requirement: "answers timestamp -2 with the log start and -1 with the \
                      log end, so that the span between them is exactly the \
                      records produced",
        runner: Runner::Server(|ctx| Box::pin(list_offsets_earliest_latest(ctx))),
    },
    Check {
        id: "find-coordinator/group",
        requirement: "names a reachable coordinator for a group key, answering \
                      in the shape the negotiated version defines (v4+ echoes \
                      each requested key in `coordinators`)",
        runner: Runner::Server(|ctx| Box::pin(find_coordinator_group(ctx))),
    },
    Check {
        id: "offsets/commit-fetch-roundtrip",
        requirement: "returns from OffsetFetch exactly the offset OffsetCommit \
                      was given for that group, topic and partition",
        runner: Runner::Server(|ctx| Box::pin(offsets_commit_fetch_roundtrip(ctx))),
    },
    Check {
        id: "offsets/unset-is-sentinel",
        requirement: "reports a partition a group never committed as offset -1 \
                      with no error, rather than as 0 or as a failure",
        runner: Runner::Server(|ctx| Box::pin(offsets_unset_is_sentinel(ctx))),
    },
    Check {
        id: "fetch/offset-out-of-range",
        requirement: "answers a fetch past the high watermark with \
                      OFFSET_OUT_OF_RANGE rather than with an empty batch set",
        runner: Runner::Server(|ctx| Box::pin(fetch_offset_out_of_range(ctx))),
    },
    Check {
        id: "metadata/unknown-topic",
        requirement: "names a topic it does not have in the response, carrying \
                      UNKNOWN_TOPIC_OR_PARTITION, rather than omitting it",
        runner: Runner::Server(|ctx| Box::pin(metadata_unknown_topic(ctx))),
    },
    Check {
        id: "create-topics/duplicate",
        requirement: "refuses a second CreateTopics for an existing topic with \
                      TOPIC_ALREADY_EXISTS",
        runner: Runner::Server(|ctx| Box::pin(create_topics_duplicate(ctx))),
    },
    Check {
        id: "create-topics/validate-only",
        requirement: "a validate_only request reports what would happen without \
                      creating the topic",
        runner: Runner::Server(|ctx| Box::pin(create_topics_validate_only(ctx))),
    },
];

/// Limits for one server-side run.
///
/// The settle budget is the only knob that costs wall-clock time: a
/// freshly created topic on a real broker genuinely takes seconds to
/// elect a leader, but a subject that answers instantly (the calibration
/// [`crate::subject`], or any in-process stub) never needs the wait — and
/// a subject that answers a *permanent* error the flow treats as
/// retriable burns the whole budget before reaching the right verdict.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProbeConfig {
    /// How long the produce/fetch flows tolerate retriable errors from a
    /// freshly created topic before calling it a failure.
    pub settle_budget: Duration,
    /// How long to wait between attempts within that budget.
    pub settle_delay: Duration,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        ProbeConfig {
            settle_budget: Duration::from_secs(5),
            settle_delay: Duration::from_millis(100),
        }
    }
}

impl ProbeConfig {
    /// How many attempts the budget affords, `settle_delay` apart. Always
    /// at least one: a zero budget still gets a single try, so the flow
    /// can never skip the exchange it is there to make.
    fn settle_attempts(&self) -> u32 {
        let delay = self.settle_delay.as_millis().max(1);
        let attempts = self.settle_budget.as_millis() / delay;
        u32::try_from(attempts).unwrap_or(u32::MAX).max(1)
    }
}

/// Run the catalogued Server-role checks against `addr` with the default
/// [`ProbeConfig`] and collect a report.
pub async fn run(addr: &str) -> Report {
    run_with(addr, &ProbeConfig::default()).await
}

/// Run the catalogued Server-role checks against `addr` under `config`.
pub async fn run_with(addr: &str, config: &ProbeConfig) -> Report {
    let ctx = ServerCtx::discover(addr, config.clone()).await;
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
    config: ProbeConfig,
}

impl ServerCtx {
    async fn discover(addr: &str, config: ProbeConfig) -> ServerCtx {
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
            config,
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

/// ListOffsets sentinels: `-2` is the log start, `-1` the log end.
const EARLIEST_TIMESTAMP: i64 = -2;
const LATEST_TIMESTAMP: i64 = -1;
/// OffsetFetch reports a never-committed partition as this, not as an error.
const UNSET_OFFSET: i64 = -1;
/// FindCoordinator batched keys from v4; OffsetFetch batched groups from v8.
const FIND_COORDINATOR_BATCHED: i16 = 4;
const OFFSET_FETCH_BATCHED: i16 = 8;
/// From v10 both offset APIs address topics by id instead of by name —
/// the same migration Produce and Fetch made at v13.
const OFFSETS_BY_TOPIC_ID: i16 = 10;
/// The group this suite commits under. Named per run so a rerun against a
/// live cluster never reads a previous run's commits.
fn check_group(topic: &str) -> String {
    format!("{topic}-odradek-acceptance")
}

/// Ask for one partition's offset at `timestamp`.
async fn list_offsets_at(
    conn: &mut RawConnection,
    version: i16,
    topic: &str,
    timestamp: i64,
    correlation_id: i32,
) -> Result<i64, CheckError> {
    let mut partition = ListOffsetsPartition::default();
    partition.partition_index = 0;
    partition.current_leader_epoch = -1;
    partition.timestamp = timestamp;
    let mut req_topic = ListOffsetsTopic::default();
    req_topic.name = topic.to_owned();
    req_topic.partitions = vec![partition];
    let mut request = ListOffsetsRequest::default();
    request.replica_id = -1;
    request.isolation_level = 0;
    request.topics = vec![req_topic];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding ListOffsets: {e}")))?;
    let resp: ListOffsetsResponse = api_call(
        conn,
        ListOffsetsRequest::API_KEY,
        version,
        correlation_id,
        &body,
    )
    .await?;
    let partition = resp
        .topics
        .iter()
        .find(|t| t.name == topic)
        .and_then(|t| t.partitions.first())
        .ok_or_else(|| CheckError::Violation(format!("ListOffsets response omits {topic}[0]")))?;
    let code = ErrorCode(partition.error_code);
    if !code.is_ok() {
        return Err(CheckError::Violation(format!(
            "ListOffsets for {topic}[0] at timestamp {timestamp} failed: {code}"
        )));
    }
    Ok(partition.offset)
}

/// The log start and log end bracket exactly what was produced.
///
/// Checking the two together is what makes this more than a liveness
/// probe: either alone can be faked by a constant, but their difference
/// has to equal the record count, and the flow knows that count.
async fn list_offsets_earliest_latest(ctx: &ServerCtx) -> Verdict {
    let advertised = match ctx.range(ListOffsetsRequest::API_KEY) {
        Ok(a) => a,
        Err(v) => return v,
    };
    let versions = match negotiate_all(
        "ListOffsets",
        advertised,
        ListOffsetsRequest::MIN_VERSION,
        ListOffsetsRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let mut produced = match produce_flow(ctx, "listoffsets", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let sent_records = match decode_set(&mut produced.sent.clone()) {
        Ok(batches) => batches.iter().map(batch_record_count).sum::<i64>(),
        Err(e) => {
            return Verdict::Error {
                details: format!("suite produced a record set it cannot decode: {e}"),
            };
        }
    };

    // The same log, asked at every version the subject serves: the answer
    // cannot depend on which version was used to ask it.
    for (i, version) in versions.iter().copied().enumerate() {
        let base = 41 + i32::try_from(i).unwrap_or(0) * 2;
        let earliest = match list_offsets_at(
            &mut produced.conn,
            version,
            &produced.topic,
            EARLIEST_TIMESTAMP,
            base,
        )
        .await
        {
            Ok(o) => o,
            Err(e) => return e.into_verdict().at_version(version),
        };
        let latest = match list_offsets_at(
            &mut produced.conn,
            version,
            &produced.topic,
            LATEST_TIMESTAMP,
            base + 1,
        )
        .await
        {
            Ok(o) => o,
            Err(e) => return e.into_verdict().at_version(version),
        };

        if earliest != 0 {
            return Verdict::Fail {
                details: format!(
                    "v{version}: log start of a freshly created topic is {earliest}, expected 0"
                ),
            };
        }
        if latest - earliest != sent_records {
            return Verdict::Fail {
                details: format!(
                    "v{version}: log spans {} offset(s) ({earliest}..{latest}) after producing \
                     {sent_records} record(s)",
                    latest - earliest
                ),
            };
        }
    }
    Verdict::Pass
}

/// Records in a batch, from whichever representation it decoded to.
fn batch_record_count(batch: &RecordBatch) -> i64 {
    match &batch.records {
        Records::Plain(records) => records.len() as i64,
        Records::Compressed { count, .. } => i64::from(*count),
    }
}

/// A group has a coordinator, and v4+ says which key it answered for.
async fn find_coordinator_group(ctx: &ServerCtx) -> Verdict {
    let advertised = match ctx.range(FindCoordinatorRequest::API_KEY) {
        Ok(a) => a,
        Err(v) => return v,
    };
    let versions = match negotiate_all(
        "FindCoordinator",
        advertised,
        FindCoordinatorRequest::MIN_VERSION,
        FindCoordinatorRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let group = check_group("find-coordinator");
    // Sweeping matters more here than almost anywhere: v4 is where the
    // single key became `coordinator_keys` and the flat endpoint became a
    // `coordinators` array, so a suite that only ever negotiates the
    // maximum never exercises the older shape at all.
    for (i, version) in versions.iter().copied().enumerate() {
        let correlation = 51 + i32::try_from(i).unwrap_or(0) * 32;
        if let Verdict::Fail { details } =
            find_coordinator_at(ctx, version, &group, correlation).await
        {
            return Verdict::Fail {
                details: format!("v{version}: {details}"),
            };
        }
    }
    Verdict::Pass
}

/// One FindCoordinator exchange at `version`, checked.
async fn find_coordinator_at(
    ctx: &ServerCtx,
    version: i16,
    group: &str,
    correlation_base: i32,
) -> Verdict {
    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };

    let mut request = FindCoordinatorRequest::default();
    request.key_type = 0; // group
    if version >= FIND_COORDINATOR_BATCHED {
        request.coordinator_keys = vec![group.to_owned()];
    } else {
        request.key = group.to_owned();
    }
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, version) {
        return Verdict::Error {
            details: format!("encoding FindCoordinator: {e}"),
        };
    }

    // A cluster that has never hosted a group creates `__consumer_offsets`
    // on the first ask, and says COORDINATOR_NOT_AVAILABLE until its
    // partitions have leaders. That is a retriable error, not a wrong
    // answer, and a client that treated it as final would be the broken
    // one — so the suite waits it out on the same budget the produce flow
    // uses for a freshly created topic.
    let mut resp = FindCoordinatorResponse::default();
    let mut last = ErrorCode(0);
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let correlation = correlation_base + i32::try_from(attempt).unwrap_or(0);
        resp = match api_call(
            &mut conn,
            FindCoordinatorRequest::API_KEY,
            version,
            correlation,
            &body,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => return e.into_verdict(),
        };
        last = coordinator_error(&resp, version);
        if !is_coordinator_settling(last) {
            break;
        }
    }
    if is_coordinator_settling(last) {
        return Verdict::Fail {
            details: format!(
                "coordinator for {group:?} still {last} after {:?}",
                ctx.config.settle_budget
            ),
        };
    }

    // The two shapes are genuinely different messages wearing one name.
    let (error_code, key, host, port) = if version >= FIND_COORDINATOR_BATCHED {
        if resp.coordinators.len() != 1 {
            return Verdict::Fail {
                details: format!(
                    "asked about 1 coordinator key, response carries {}",
                    resp.coordinators.len()
                ),
            };
        }
        let c = &resp.coordinators[0];
        (c.error_code, Some(c.key.clone()), c.host.clone(), c.port)
    } else {
        (resp.error_code, None, resp.host.clone(), resp.port)
    };

    let code = ErrorCode(error_code);
    if !code.is_ok() {
        return Verdict::Fail {
            details: format!("no coordinator for group {group:?}: {code}"),
        };
    }
    // No let-chain: this crate builds on the declared MSRV, which predates
    // them.
    if key.as_deref().is_some_and(|key| key != group) {
        return Verdict::Fail {
            details: format!(
                "asked about group {group:?}, response answers for {:?}",
                key.unwrap_or_default()
            ),
        };
    }
    if host.is_empty() || !(1..=65535).contains(&port) {
        return Verdict::Fail {
            details: format!("coordinator endpoint is implausible: {host:?}:{port}"),
        };
    }
    Verdict::Pass
}

/// The error a FindCoordinator response reports, from whichever shape
/// the negotiated version used.
fn coordinator_error(resp: &FindCoordinatorResponse, version: i16) -> ErrorCode {
    if version >= FIND_COORDINATOR_BATCHED {
        ErrorCode(resp.coordinators.first().map_or(0, |c| c.error_code))
    } else {
        ErrorCode(resp.error_code)
    }
}

/// Errors that mean "ask again shortly", not "no".
fn is_coordinator_settling(code: ErrorCode) -> bool {
    code == ErrorCode::COORDINATOR_NOT_AVAILABLE || code == ErrorCode::COORDINATOR_LOAD_IN_PROGRESS
}

/// Wait for the group coordinator to exist before asking it anything.
///
/// The offsets checks need this for the same reason and would otherwise
/// pass or fail on whether they happened to run after
/// `find-coordinator/group` warmed the cluster — an order dependency
/// between checks is a bug in the suite, not a property of the subject.
async fn await_coordinator(ctx: &ServerCtx, group: &str) -> Result<(), CheckError> {
    let advertised = match ctx.range(FindCoordinatorRequest::API_KEY) {
        Ok(a) => a,
        // No FindCoordinator advertised: let the offsets exchange itself
        // report whatever it reports.
        Err(_) => return Ok(()),
    };
    let Ok(version) = negotiate(
        "FindCoordinator",
        advertised,
        FindCoordinatorRequest::MIN_VERSION,
        FindCoordinatorRequest::MAX_VERSION,
    ) else {
        return Ok(());
    };
    let mut request = FindCoordinatorRequest::default();
    request.key_type = 0;
    if version >= FIND_COORDINATOR_BATCHED {
        request.coordinator_keys = vec![group.to_owned()];
    } else {
        request.key = group.to_owned();
    }
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding FindCoordinator: {e}")))?;

    let mut conn = connect(&ctx.addr).await?;
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let resp: FindCoordinatorResponse = api_call(
            &mut conn,
            FindCoordinatorRequest::API_KEY,
            version,
            80 + i32::try_from(attempt).unwrap_or(0),
            &body,
        )
        .await?;
        if !is_coordinator_settling(coordinator_error(&resp, version)) {
            return Ok(());
        }
    }
    Ok(())
}

/// Commit an offset for one partition of `topic` under `group`.
async fn commit_offset(
    conn: &mut RawConnection,
    version: i16,
    group: &str,
    topic: &str,
    topic_id: [u8; 16],
    offset: i64,
    correlation_id: i32,
) -> Result<(), CheckError> {
    let mut partition = OffsetCommitRequestPartition::default();
    partition.partition_index = 0;
    partition.committed_offset = offset;
    partition.committed_leader_epoch = -1;
    let mut req_topic = OffsetCommitRequestTopic::default();
    // v10 addresses by id and drops the name from the wire entirely, so
    // sending the name there would name nothing.
    if version >= OFFSETS_BY_TOPIC_ID {
        req_topic.topic_id = topic_id;
    } else {
        req_topic.name = topic.to_owned();
    }
    req_topic.partitions = vec![partition];
    let mut request = OffsetCommitRequest::default();
    request.group_id = group.to_owned();
    // A simple (non-member) commit: no generation, no member id. This is
    // the path a consumer that manages its own partitions uses.
    request.generation_id_or_member_epoch = -1;
    request.member_id = String::new();
    request.retention_time_ms = -1;
    request.topics = vec![req_topic];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding OffsetCommit: {e}")))?;
    let resp: OffsetCommitResponse = api_call(
        conn,
        OffsetCommitRequest::API_KEY,
        version,
        correlation_id,
        &body,
    )
    .await?;
    let partition = resp
        .topics
        .iter()
        .find(|t| {
            if version >= OFFSETS_BY_TOPIC_ID {
                t.topic_id == topic_id
            } else {
                t.name == topic
            }
        })
        .and_then(|t| t.partitions.first())
        .ok_or_else(|| CheckError::Violation(format!("OffsetCommit response omits {topic}[0]")))?;
    let code = ErrorCode(partition.error_code);
    if !code.is_ok() {
        return Err(CheckError::Violation(format!(
            "committing {offset} for {topic}[0] failed: {code}"
        )));
    }
    Ok(())
}

/// Read back what a group committed for one partition.
async fn fetch_committed(
    conn: &mut RawConnection,
    version: i16,
    group: &str,
    topic: &str,
    topic_id: [u8; 16],
    correlation_id: i32,
) -> Result<i64, CheckError> {
    let mut request = OffsetFetchRequest::default();
    if version >= OFFSET_FETCH_BATCHED {
        let mut topics = OffsetFetchRequestTopics::default();
        if version >= OFFSETS_BY_TOPIC_ID {
            topics.topic_id = topic_id;
        } else {
            topics.name = topic.to_owned();
        }
        topics.partition_indexes = vec![0];
        let mut req_group = OffsetFetchRequestGroup::default();
        req_group.group_id = group.to_owned();
        req_group.member_epoch = -1;
        req_group.topics = Some(vec![topics]);
        request.groups = vec![req_group];
    } else {
        let mut req_topic = OffsetFetchRequestTopic::default();
        req_topic.name = topic.to_owned();
        req_topic.partition_indexes = vec![0];
        request.group_id = group.to_owned();
        request.topics = Some(vec![req_topic]);
    }
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding OffsetFetch: {e}")))?;
    let resp: OffsetFetchResponse = api_call(
        conn,
        OffsetFetchRequest::API_KEY,
        version,
        correlation_id,
        &body,
    )
    .await?;

    let (error_code, committed) = if version >= OFFSET_FETCH_BATCHED {
        let group_resp = resp
            .groups
            .iter()
            .find(|g| g.group_id == group)
            .ok_or_else(|| {
                CheckError::Violation(format!("OffsetFetch response omits group {group:?}"))
            })?;
        let partition = group_resp
            .topics
            .iter()
            .find(|t| {
                if version >= OFFSETS_BY_TOPIC_ID {
                    t.topic_id == topic_id
                } else {
                    t.name == topic
                }
            })
            .and_then(|t| t.partitions.first())
            .ok_or_else(|| {
                CheckError::Violation(format!("OffsetFetch response omits {topic}[0]"))
            })?;
        (
            if group_resp.error_code != 0 {
                group_resp.error_code
            } else {
                partition.error_code
            },
            partition.committed_offset,
        )
    } else {
        let partition = resp
            .topics
            .iter()
            .find(|t| t.name == topic)
            .and_then(|t| t.partitions.first())
            .ok_or_else(|| {
                CheckError::Violation(format!("OffsetFetch response omits {topic}[0]"))
            })?;
        (partition.error_code, partition.committed_offset)
    };
    let code = ErrorCode(error_code);
    if !code.is_ok() {
        return Err(CheckError::Violation(format!(
            "reading the committed offset for {topic}[0] failed: {code}"
        )));
    }
    Ok(committed)
}

/// What OffsetCommit stored is what OffsetFetch returns.
///
/// The round trip is the assertion. A server that accepts commits and
/// loses them answers every commit with success, so only reading the
/// value back distinguishes the two.
async fn offsets_commit_fetch_roundtrip(ctx: &ServerCtx) -> Verdict {
    // Commit once at the newest version, read back at every one.
    let (commit_version, fetch_versions) = match offsets_commit_and_fetch_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let mut produced = match produce_flow(ctx, "offsets", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if let Some(skip) = skip_without_topic_id(&produced, commit_version) {
        return skip;
    }
    let group = check_group(&produced.topic);
    if let Err(e) = await_coordinator(ctx, &group).await {
        return e.into_verdict();
    }
    // Not 0, and not the log end either: a number nothing else would
    // produce by accident.
    let committed = 7;

    if let Err(e) = commit_offset(
        &mut produced.conn,
        commit_version,
        &group,
        &produced.topic,
        produced.topic_id,
        committed,
        61,
    )
    .await
    {
        return e.into_verdict();
    }
    // One commit, read back at every OffsetFetch version on offer. A
    // durable position that only survives being read at one version is
    // not durable: v8 moved the exchange into a `groups` array and v10
    // switched to topic ids, and both shapes must see the same number.
    for (i, version) in fetch_versions.iter().copied().enumerate() {
        if version >= OFFSETS_BY_TOPIC_ID && produced.topic_id == [0u8; 16] {
            continue;
        }
        let correlation = 62 + i32::try_from(i).unwrap_or(0);
        match fetch_committed(
            &mut produced.conn,
            version,
            &group,
            &produced.topic,
            produced.topic_id,
            correlation,
        )
        .await
        {
            Ok(got) if got == committed => {}
            Ok(got) => {
                return Verdict::Fail {
                    details: format!("v{version}: committed offset {committed}, read back {got}"),
                };
            }
            Err(e) => return e.into_verdict().at_version(version),
        }
    }
    Verdict::Pass
}

/// A partition a group never committed reads as -1, not 0 and not an error.
///
/// Worth its own check because the wrong answer here is plausible: 0 is a
/// valid offset, so a server that reports 0 for "nothing committed" sends
/// a resuming consumer back to the start of the log instead of to wherever
/// its configured default says.
async fn offsets_unset_is_sentinel(ctx: &ServerCtx) -> Verdict {
    let (_, fetch_version) = match offsets_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let mut produced = match produce_flow(ctx, "unset", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if let Some(skip) = skip_without_topic_id(&produced, fetch_version) {
        return skip;
    }
    // A group that has never existed, let alone committed.
    let group = format!("{}-never-committed", check_group(&produced.topic));
    if let Err(e) = await_coordinator(ctx, &group).await {
        return e.into_verdict();
    }
    match fetch_committed(
        &mut produced.conn,
        fetch_version,
        &group,
        &produced.topic,
        produced.topic_id,
        71,
    )
    .await
    {
        Ok(got) if got == UNSET_OFFSET => Verdict::Pass,
        Ok(got) => Verdict::Fail {
            details: format!(
                "a group that never committed reads as offset {got}, expected \
                 {UNSET_OFFSET}"
            ),
        },
        Err(e) => e.into_verdict(),
    }
}

/// From v10 the offset APIs name topics only by id, so a subject whose
/// CreateTopics did not hand one back cannot be asked the question.
fn skip_without_topic_id(produced: &ProducedTopic, version: i16) -> Option<Verdict> {
    if version >= OFFSETS_BY_TOPIC_ID && produced.topic_id == [0u8; 16] {
        return Some(Verdict::Skipped {
            reason: format!(
                "offsets v{version} addresses topics by id, and CreateTopics \
                 returned none for {}",
                produced.topic
            ),
        });
    }
    None
}

/// The newest OffsetCommit, and every OffsetFetch worth reading back at.
///
/// Both halves have to be present for the pair to mean anything, so a
/// subject missing either skips rather than half-running.
fn offsets_commit_and_fetch_versions(ctx: &ServerCtx) -> Result<(i16, Vec<i16>), Verdict> {
    let commit = negotiate(
        "OffsetCommit",
        ctx.range(OffsetCommitRequest::API_KEY)?,
        OffsetCommitRequest::MIN_VERSION,
        OffsetCommitRequest::MAX_VERSION,
    )?;
    let fetch = negotiate_all(
        "OffsetFetch",
        ctx.range(OffsetFetchRequest::API_KEY)?,
        OffsetFetchRequest::MIN_VERSION,
        OffsetFetchRequest::MAX_VERSION,
    )?;
    Ok((commit, fetch))
}

/// Negotiate both halves of the offsets pair, since either can be absent.
fn offsets_versions(ctx: &ServerCtx) -> Result<(i16, i16), Verdict> {
    let commit = negotiate(
        "OffsetCommit",
        ctx.range(OffsetCommitRequest::API_KEY)?,
        OffsetCommitRequest::MIN_VERSION,
        OffsetCommitRequest::MAX_VERSION,
    )?;
    let fetch = negotiate(
        "OffsetFetch",
        ctx.range(OffsetFetchRequest::API_KEY)?,
        OffsetFetchRequest::MIN_VERSION,
        OffsetFetchRequest::MAX_VERSION,
    )?;
    Ok((commit, fetch))
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

/// A fetch past the end of the log is an error, not silence.
///
/// The wrong answer is quiet: an empty batch set is what a caught-up
/// consumer sees, so a server that answers an impossible offset that way
/// leaves a client polling forever at a position that will never exist.
async fn fetch_offset_out_of_range(ctx: &ServerCtx) -> Verdict {
    let fetch_range = match ctx.range(FetchRequest::API_KEY) {
        Ok(r) => r,
        Err(v) => return v,
    };
    let version = match negotiate(
        "Fetch",
        fetch_range,
        FetchRequest::MIN_VERSION,
        FETCH_NAME_MAX,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let mut produced = match produce_flow(ctx, "outofrange", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };

    // Far past anything the probe batch could have written.
    let beyond = 1_000_000;
    let mut fetch_partition = FetchPartition::default();
    fetch_partition.partition = 0;
    fetch_partition.current_leader_epoch = -1;
    fetch_partition.fetch_offset = beyond;
    fetch_partition.last_fetched_epoch = -1;
    fetch_partition.log_start_offset = -1;
    fetch_partition.partition_max_bytes = 1 << 20;
    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = produced.topic.clone();
    fetch_topic.partitions = vec![fetch_partition];
    let mut request = FetchRequest::default();
    request.replica_id = -1;
    request.max_wait_ms = 500;
    request.min_bytes = 0;
    request.max_bytes = 1 << 20;
    request.session_epoch = -1;
    request.topics = vec![fetch_topic];
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, version) {
        return Verdict::Error {
            details: format!("encoding Fetch: {e}"),
        };
    }
    let resp: FetchResponse = match api_call(
        &mut produced.conn,
        FetchRequest::API_KEY,
        version,
        91,
        &body,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return e.into_verdict(),
    };
    let Some(partition) = resp.responses.first().and_then(|t| t.partitions.first()) else {
        return Verdict::Fail {
            details: format!("fetch response omits {}[0]", produced.topic),
        };
    };
    let code = ErrorCode(partition.error_code);
    if code == ErrorCode::OFFSET_OUT_OF_RANGE {
        return Verdict::Pass;
    }
    if code.is_ok() {
        return Verdict::Fail {
            details: format!(
                "fetch at offset {beyond} of a log with high watermark {} answered \
                 NONE with {} record byte(s) — a consumer cannot tell this from \
                 being caught up",
                partition.high_watermark,
                partition.records.as_ref().map_or(0, |r| r.len())
            ),
        };
    }
    Verdict::Fail {
        details: format!("fetch at offset {beyond} answered {code}, expected OFFSET_OUT_OF_RANGE"),
    }
}

/// An unknown topic is named in the response, not left out of it.
async fn metadata_unknown_topic(ctx: &ServerCtx) -> Verdict {
    let advertised = match ctx.range(MetadataRequest::API_KEY) {
        Ok(a) => a,
        Err(v) => return v,
    };
    let version = match negotiate("Metadata", advertised, 1, MetadataRequest::MAX_VERSION) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let topic = unique_topic("nosuch");
    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };

    let mut requested = MetadataRequestTopic::default();
    requested.name = Some(topic.clone());
    let mut request = MetadataRequest::default();
    request.topics = Some(vec![requested]);
    // The flag is the point: without it a broker configured to
    // auto-create would answer by creating the topic, and the check would
    // be testing configuration rather than the protocol.
    request.allow_auto_topic_creation = false;
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, version) {
        return Verdict::Error {
            details: format!("encoding Metadata: {e}"),
        };
    }
    let resp: MetadataResponse =
        match api_call(&mut conn, MetadataRequest::API_KEY, version, 95, &body).await {
            Ok(r) => r,
            Err(e) => return e.into_verdict(),
        };

    let Some(entry) = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic.as_str()))
    else {
        return Verdict::Fail {
            details: format!(
                "asked about {topic:?}, which does not exist; response names {} topic(s) \
                 and not that one, so a client cannot tell absent from ignored",
                resp.topics.len()
            ),
        };
    };
    let code = ErrorCode(entry.error_code);
    if code == ErrorCode::UNKNOWN_TOPIC_OR_PARTITION {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: format!("{topic:?} does not exist but is reported with {code}"),
        }
    }
}

/// One CreateTopics exchange, returning the per-topic result.
async fn create_topic_call(
    ctx: &ServerCtx,
    conn: &mut RawConnection,
    version: i16,
    topic: &str,
    validate_only: bool,
    correlation_id: i32,
) -> Result<(ErrorCode, [u8; 16]), CheckError> {
    let _ = ctx;
    let mut creatable = CreatableTopic::default();
    creatable.name = topic.to_owned();
    creatable.num_partitions = 1;
    creatable.replication_factor = 1;
    let mut request = CreateTopicsRequest::default();
    request.topics = vec![creatable];
    request.timeout_ms = 30_000;
    request.validate_only = validate_only;
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding CreateTopics: {e}")))?;
    let resp: CreateTopicsResponse = api_call(
        conn,
        CreateTopicsRequest::API_KEY,
        version,
        correlation_id,
        &body,
    )
    .await?;
    let result = resp
        .topics
        .first()
        .ok_or_else(|| CheckError::Violation("CreateTopics response names no topics".into()))?;
    Ok((ErrorCode(result.error_code), result.topic_id))
}

/// Creating a topic that exists is refused, and says why.
async fn create_topics_duplicate(ctx: &ServerCtx) -> Verdict {
    let version = match negotiate(
        "CreateTopics",
        match ctx.range(CreateTopicsRequest::API_KEY) {
            Ok(a) => a,
            Err(v) => return v,
        },
        CreateTopicsRequest::MIN_VERSION,
        CreateTopicsRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let topic = unique_topic("dup");

    match create_topic_call(ctx, &mut conn, version, &topic, false, 96).await {
        Ok((code, _)) if code.is_ok() => {}
        Ok((code, _)) => {
            return Verdict::Fail {
                details: format!("creating a fresh topic failed with {code}"),
            };
        }
        Err(e) => return e.into_verdict(),
    }
    match create_topic_call(ctx, &mut conn, version, &topic, false, 97).await {
        Ok((code, _)) if code == ErrorCode::TOPIC_ALREADY_EXISTS => Verdict::Pass,
        Ok((code, _)) if code.is_ok() => Verdict::Fail {
            details: "creating the same topic twice succeeded both times".into(),
        },
        Ok((code, _)) => Verdict::Fail {
            details: format!(
                "recreating an existing topic answered {code}, expected TOPIC_ALREADY_EXISTS"
            ),
        },
        Err(e) => e.into_verdict(),
    }
}

/// `validate_only` answers the question without doing the thing.
///
/// Checked by asking twice: a validate_only create, then a real one. If
/// the first actually created the topic, the second reports
/// TOPIC_ALREADY_EXISTS and gives the game away.
async fn create_topics_validate_only(ctx: &ServerCtx) -> Verdict {
    let version = match negotiate(
        "CreateTopics",
        match ctx.range(CreateTopicsRequest::API_KEY) {
            Ok(a) => a,
            Err(v) => return v,
        },
        CreateTopicsRequest::MIN_VERSION,
        CreateTopicsRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let topic = unique_topic("validate");

    match create_topic_call(ctx, &mut conn, version, &topic, true, 98).await {
        Ok((code, _)) if code.is_ok() => {}
        Ok((code, _)) => {
            return Verdict::Fail {
                details: format!("a validate_only create of a fresh topic reported {code}"),
            };
        }
        Err(e) => return e.into_verdict(),
    }
    match create_topic_call(ctx, &mut conn, version, &topic, false, 99).await {
        Ok((code, _)) if code.is_ok() => Verdict::Pass,
        Ok((code, _)) if code == ErrorCode::TOPIC_ALREADY_EXISTS => Verdict::Fail {
            details: "validate_only created the topic: the real create that \
                      followed it reported TOPIC_ALREADY_EXISTS"
                .into(),
        },
        Ok((code, _)) => Verdict::Fail {
            details: format!("creating the topic after validating it answered {code}"),
        },
        Err(e) => e.into_verdict(),
    }
}

/// Name the version a swept check was on when it failed.
///
/// Without this a sweep reports "the batch came back different" and
/// leaves the reader to guess which of fifteen versions did it.
trait AtVersion {
    fn at_version(self, version: i16) -> Verdict;
}

impl AtVersion for Verdict {
    fn at_version(self, version: i16) -> Verdict {
        match self {
            Verdict::Fail { details } => Verdict::Fail {
                details: format!("v{version}: {details}"),
            },
            Verdict::Error { details } => Verdict::Error {
                details: format!("v{version}: {details}"),
            },
            other => other,
        }
    }
}

/// Every version a check and a subject both speak, lowest first.
///
/// [`negotiate`] answers "can this run?"; this answers "on how many
/// versions?". A conformance claim about a range the subject advertises
/// is only as good as the versions actually exercised, and testing one
/// of them tests one of them — a broker that mishandles v7 while serving
/// v18 correctly looks perfect to a suite that always negotiates the
/// maximum.
///
/// Skips carry the same reason [`negotiate`] would have given, so a
/// subject that speaks none of the range reads identically either way.
fn negotiate_all(
    api: &'static str,
    advertised: Option<(i16, i16)>,
    check_min: i16,
    check_max: i16,
) -> Result<Vec<i16>, Verdict> {
    // Reuse the single-version path for the "can this run at all"
    // question, so the skip reasons stay one sentence in one place.
    let highest = negotiate(api, advertised, check_min, check_max)?;
    let (min, _) = advertised.expect("negotiate succeeded, so a range was advertised");
    let lowest = min.max(check_min);
    Ok((lowest..=highest).collect())
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
    let versions = match negotiate_all("Metadata", advertised, 1, MetadataRequest::MAX_VERSION) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    for (i, version) in versions.iter().copied().enumerate() {
        let correlation = 4 + i32::try_from(i).unwrap_or(0);
        let resp = match metadata_exchange(&ctx.addr, version, correlation).await {
            Ok(resp) => resp,
            Err(e) => return e.into_verdict().at_version(version),
        };
        if let Verdict::Fail { details } = metadata_shape(&resp) {
            return Verdict::Fail {
                details: format!("v{version}: {details}"),
            };
        }
    }
    Verdict::Pass
}

/// What a Metadata response must look like, at any version.
fn metadata_shape(resp: &MetadataResponse) -> Verdict {
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
    let attempts = ctx.config.settle_attempts();
    for _ in 0..attempts {
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
        tokio::time::sleep(ctx.config.settle_delay).await;
    }
    Err(fail(format!(
        "topic never became producible: still {last_code} after {attempts} attempts \
         over {:?}",
        ctx.config.settle_budget
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
    config: &ProbeConfig,
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
    for _ in 0..config.settle_attempts() {
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
        tokio::time::sleep(config.settle_delay).await;
    }
    Err(CheckError::Violation(last))
}

async fn fetch_batch_integrity(ctx: &ServerCtx) -> Verdict {
    let fetch_range = match ctx.range(FetchRequest::API_KEY) {
        Ok(r) => r,
        Err(v) => return v,
    };
    let fetch_versions = match negotiate_all(
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
    // Produce once, fetch at every version the subject offers. The batch
    // that comes back must be the same bytes each time: a broker that
    // re-encodes on one older version and not on others is exactly the
    // bug this check exists for, and it is invisible if only the newest
    // version is ever asked.
    for version in fetch_versions {
        match run_fetch(&mut produced, version, Addressing::Name, &ctx.config).await {
            Ok(got) => {
                if let Verdict::Fail { details } = batch_integrity(&produced.sent, &got) {
                    return Verdict::Fail {
                        details: format!("v{version}: {details}"),
                    };
                }
            }
            Err(e) => return e.into_verdict().at_version(version),
        }
    }
    Verdict::Pass
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
    match run_fetch(
        &mut produced,
        fetch_version,
        Addressing::TopicId,
        &ctx.config,
    )
    .await
    {
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
