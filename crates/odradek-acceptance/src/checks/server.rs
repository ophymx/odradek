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
use odradek_protocol::messages::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use odradek_protocol::messages::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
use odradek_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use odradek_protocol::messages::create_topics_response::CreateTopicsResponse;
use odradek_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use odradek_protocol::messages::fetch_response::FetchResponse;
use odradek_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use odradek_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use odradek_protocol::messages::heartbeat_request::HeartbeatRequest;
use odradek_protocol::messages::heartbeat_response::HeartbeatResponse;
use odradek_protocol::messages::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use odradek_protocol::messages::join_group_response::JoinGroupResponse;
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
use odradek_protocol::messages::sasl_authenticate_request::SaslAuthenticateRequest;
use odradek_protocol::messages::sasl_authenticate_response::SaslAuthenticateResponse;
use odradek_protocol::messages::sasl_handshake_request::SaslHandshakeRequest;
use odradek_protocol::messages::sasl_handshake_response::SaslHandshakeResponse;
use odradek_protocol::messages::sync_group_request::{
    SyncGroupRequest, SyncGroupRequestAssignment,
};
use odradek_protocol::messages::sync_group_response::SyncGroupResponse;
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
    Check {
        id: "groups/member-id-required",
        requirement: "refuses a JoinGroup (v4+) that carries no member id, \
                      answering MEMBER_ID_REQUIRED with an id to rejoin with",
        runner: Runner::Server(|ctx| Box::pin(groups_member_id_required(ctx))),
    },
    Check {
        id: "groups/assignment-round-trips",
        requirement: "hands a member the assignment bytes its leader supplied, \
                      unexamined and unchanged",
        runner: Runner::Server(|ctx| Box::pin(groups_assignment_round_trips(ctx))),
    },
    Check {
        id: "groups/stale-generation-fenced",
        requirement: "refuses a Heartbeat carrying a generation the group has \
                      moved past, with ILLEGAL_GENERATION",
        runner: Runner::Server(|ctx| Box::pin(groups_stale_generation_fenced(ctx))),
    },
    Check {
        id: "consumer-group/epoch-advances",
        requirement: "admits a KIP-848 member that names itself at epoch 0, \
                      answering with a non-zero epoch and a usable heartbeat \
                      interval",
        runner: Runner::Server(|ctx| Box::pin(consumer_group_epoch_advances(ctx))),
    },
    Check {
        id: "consumer-group/assigns-subscription",
        requirement: "assigns the partitions of a subscribed topic, addressed by \
                      topic id",
        runner: Runner::Server(|ctx| Box::pin(consumer_group_assigns_subscription(ctx))),
    },
    Check {
        id: "consumer-group/omitted-subscription-is-unchanged",
        requirement: "treats a heartbeat that omits subscribed_topic_names as \
                      saying nothing about the subscription, not as unsubscribing",
        runner: Runner::Server(|ctx| Box::pin(consumer_group_omitted_subscription(ctx))),
    },
    Check {
        id: "consumer-group/fenced-epoch",
        requirement: "refuses a heartbeat carrying an epoch the member has moved \
                      past, with FENCED_MEMBER_EPOCH",
        runner: Runner::Server(|ctx| Box::pin(consumer_group_fenced_epoch(ctx))),
    },
    Check {
        id: "sasl/authenticate-requires-handshake",
        requirement: "refuses a SASL token on a connection that negotiated no \
                      mechanism, as a state error rather than as bad credentials",
        runner: Runner::Server(|ctx| Box::pin(sasl_authenticate_requires_handshake(ctx))),
    },
    Check {
        id: "sasl/refusal-names-mechanisms",
        requirement: "answers an unsupported mechanism with \
                      UNSUPPORTED_SASL_MECHANISM and the mechanisms it does \
                      support, so a client has something to fall back to",
        runner: Runner::Server(|ctx| Box::pin(sasl_refusal_names_mechanisms(ctx))),
    },
    Check {
        id: "sasl/scram-nonce-extends-client",
        requirement: "answers a SCRAM client-first with a nonce that begins with \
                      the client's own, rather than replacing it",
        runner: Runner::Server(|ctx| Box::pin(scram_nonce_extends_client(ctx))),
    },
    Check {
        id: "sasl/scram-iteration-floor",
        requirement: "states a salt and an iteration count at or above RFC 7677's \
                      floor of 4096 for SCRAM-SHA-256",
        runner: Runner::Server(|ctx| Box::pin(scram_iteration_floor(ctx))),
    },
    Check {
        id: "sasl/scram-server-proves-itself",
        requirement: "completes a SCRAM exchange with a server signature that \
                      verifies, proving it holds the account's key material",
        runner: Runner::Server(|ctx| Box::pin(scram_server_proves_itself(ctx))),
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

/// Run the Server-role checks, with a second address that has SASL
/// configured on it.
///
/// A listener with no SASL answers ILLEGAL_SASL_STATE to every SASL
/// request — correctly, since there is no session to negotiate within —
/// so mechanism negotiation simply cannot be asked about there. Given a
/// SASL address, the checks that need one use it; without, they skip and
/// say why.
pub async fn run_with_sasl(addr: &str, sasl_addr: Option<&str>, config: &ProbeConfig) -> Report {
    let mut ctx = ServerCtx::discover(addr, config.clone()).await;
    ctx.sasl_addr = sasl_addr.map(str::to_owned);
    run_ctx(ctx).await
}

/// Run the catalogued Server-role checks against `addr` under `config`.
pub async fn run_with(addr: &str, config: &ProbeConfig) -> Report {
    run_ctx(ServerCtx::discover(addr, config.clone()).await).await
}

async fn run_ctx(ctx: ServerCtx) -> Report {
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
    Report::new(format!("server {}", ctx.addr), outcomes)
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
    /// A second address with SASL configured, when one was supplied.
    sasl_addr: Option<String>,
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
            sasl_addr: None,
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

/// The account the suite authenticates as. The reference subject knows
/// it, and the conformance harness provisions it on the brokers that get
/// a SASL listener.
pub const SCRAM_USER: &str = "conformance";
pub const SCRAM_PASSWORD: &str = "conformance";

/// One `k=v` attribute of a SCRAM message.
fn scram_attr(message: &str, key: char) -> Option<String> {
    message.split(',').find_map(|part| {
        let mut chars = part.chars();
        let found = chars.next()?;
        let rest = chars.as_str().strip_prefix('=')?;
        (found == key).then(|| rest.to_owned())
    })
}

fn b64_decode(value: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(value).ok()
}

/// The client-side keys of a SCRAM exchange.
struct ScramKeys {
    client_key: [u8; 32],
    stored_key: [u8; 32],
    server_key: [u8; 32],
}

fn scram_hmac(key: &[u8], msg: &[u8]) -> [u8; 32] {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac takes any key length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

fn scram_keys(password: &str, salt: &[u8], iterations: u32) -> ScramKeys {
    use hmac::SimpleHmac;
    use sha2::{Digest, Sha256};
    let mut salted = [0u8; 32];
    pbkdf2::pbkdf2::<SimpleHmac<Sha256>>(password.as_bytes(), salt, iterations, &mut salted)
        .expect("pbkdf2 accepts any output length");
    let client_key = scram_hmac(&salted, b"Client Key");
    ScramKeys {
        client_key,
        stored_key: Sha256::digest(client_key).into(),
        server_key: scram_hmac(&salted, b"Server Key"),
    }
}

fn scram_client_proof(keys: &ScramKeys, auth_message: &str) -> String {
    use base64::Engine as _;
    let signature = scram_hmac(&keys.stored_key, auth_message.as_bytes());
    let mut proof = [0u8; 32];
    for (i, byte) in proof.iter_mut().enumerate() {
        *byte = keys.client_key[i] ^ signature[i];
    }
    base64::engine::general_purpose::STANDARD.encode(proof)
}

fn scram_server_signature(keys: &ScramKeys, auth_message: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .encode(scram_hmac(&keys.server_key, auth_message.as_bytes()))
}

/// The SCRAM mechanism these checks speak.
const SCRAM_MECHANISM: &str = "SCRAM-SHA-256";

/// Begin a SCRAM exchange: handshake, then client-first.
///
/// Returns `(connection, client nonce, client-first-bare, server-first)`,
/// or a verdict — `Skipped` when this subject has no SASL listener or
/// does not offer SCRAM, which is a capability statement rather than a
/// failure.
async fn scram_begin(
    ctx: &ServerCtx,
    correlation_base: i32,
) -> Result<(RawConnection, String, String, String), Verdict> {
    let Some(sasl_addr) = ctx.sasl_addr.clone() else {
        return Err(Verdict::Skipped {
            reason: "no SASL listener given (--sasl-server)".into(),
        });
    };
    let handshake_version = negotiate(
        "SaslHandshake",
        ctx.range(SaslHandshakeRequest::API_KEY)?,
        SaslHandshakeRequest::MIN_VERSION,
        SaslHandshakeRequest::MAX_VERSION,
    )?;
    let auth_version = negotiate(
        "SaslAuthenticate",
        ctx.range(SaslAuthenticateRequest::API_KEY)?,
        SaslAuthenticateRequest::MIN_VERSION,
        SaslAuthenticateRequest::MAX_VERSION,
    )?;
    let mut conn = connect(&sasl_addr)
        .await
        .map_err(CheckError::into_verdict)?;

    let mut handshake = SaslHandshakeRequest::default();
    handshake.mechanism = SCRAM_MECHANISM.to_owned();
    let mut body = BytesMut::new();
    handshake
        .encode(&mut body, handshake_version)
        .map_err(|e| Verdict::Error {
            details: format!("encoding SaslHandshake: {e}"),
        })?;
    let resp: SaslHandshakeResponse = api_call(
        &mut conn,
        SaslHandshakeRequest::API_KEY,
        handshake_version,
        correlation_base,
        &body,
    )
    .await
    .map_err(CheckError::into_verdict)?;
    let code = ErrorCode(resp.error_code);
    if code != ErrorCode(0) {
        return Err(Verdict::Skipped {
            reason: format!("{sasl_addr} does not offer {SCRAM_MECHANISM} ({code})"),
        });
    }

    // A nonce this exchange has never used. Printable, per RFC 5802,
    // and unique enough that a recorded answer could not contain it.
    let nonce = format!(
        "odradekNonce{}{}",
        std::process::id(),
        correlation_base as u32
    );
    let client_first_bare = format!("n={SCRAM_USER},r={nonce}");
    let client_first = format!("n,,{client_first_bare}");
    let server_first = scram_token(
        &mut conn,
        auth_version,
        client_first.as_bytes(),
        correlation_base + 1,
    )
    .await?;
    Ok((conn, nonce, client_first_bare, server_first))
}

/// Send one SASL token and return the server's, as text.
async fn scram_token(
    conn: &mut RawConnection,
    version: i16,
    token: &[u8],
    correlation_id: i32,
) -> Result<String, Verdict> {
    let mut request = SaslAuthenticateRequest::default();
    request.auth_bytes = Bytes::copy_from_slice(token);
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| Verdict::Error {
            details: format!("encoding SaslAuthenticate: {e}"),
        })?;
    let resp: SaslAuthenticateResponse = api_call(
        conn,
        SaslAuthenticateRequest::API_KEY,
        version,
        correlation_id,
        &body,
    )
    .await
    .map_err(CheckError::into_verdict)?;
    let code = ErrorCode(resp.error_code);
    if !code.is_ok() {
        return Err(Verdict::Fail {
            details: format!(
                "SCRAM exchange answered {code}{}",
                resp.error_message
                    .as_deref()
                    .map(|m| format!(": {m}"))
                    .unwrap_or_default()
            ),
        });
    }
    Ok(String::from_utf8_lossy(&resp.auth_bytes).into_owned())
}

/// The server's nonce must begin with the client's.
///
/// The client picked a nonce it has never used. A server answer that
/// does not contain it might be a recording of an earlier exchange, and
/// the nonce is the only thing in the protocol that could tell the
/// client otherwise — so a server that replaces it rather than extending
/// it has removed the client's only replay defence, while still looking
/// like it is working.
async fn scram_nonce_extends_client(ctx: &ServerCtx) -> Verdict {
    let (_conn, nonce, _bare, server_first) = match scram_begin(ctx, 210).await {
        Ok(v) => v,
        Err(verdict) => return verdict,
    };
    let Some(server_nonce) = scram_attr(&server_first, 'r') else {
        return Verdict::Fail {
            details: format!("server-first carries no nonce: {server_first:?}"),
        };
    };
    if server_nonce.starts_with(&nonce) {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: format!(
                "client sent nonce {nonce:?}; server answered {server_nonce:?}, which does \
                 not extend it, so the client cannot tell this exchange from a replay"
            ),
        }
    }
}

/// The stated cost of the key derivation has a floor.
///
/// The client must run the KDF at whatever cost the server names, before
/// it has learned anything at all. A server naming a low count has
/// quietly weakened the password hashing of every client that talks to
/// it, and the client cannot refuse without failing to connect.
async fn scram_iteration_floor(ctx: &ServerCtx) -> Verdict {
    let (_conn, _nonce, _bare, server_first) = match scram_begin(ctx, 220).await {
        Ok(v) => v,
        Err(verdict) => return verdict,
    };
    let salt = scram_attr(&server_first, 's').unwrap_or_default();
    if salt.is_empty() {
        return Verdict::Fail {
            details: format!("server-first states no salt: {server_first:?}"),
        };
    }
    let Some(iterations) = scram_attr(&server_first, 'i').and_then(|i| i.parse::<u32>().ok())
    else {
        return Verdict::Fail {
            details: format!("server-first states no iteration count: {server_first:?}"),
        };
    };
    if iterations >= SCRAM_MIN_ITERATIONS {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: format!(
                "server asks for {iterations} iterations; RFC 7677 makes \
                 {SCRAM_MIN_ITERATIONS} the floor for {SCRAM_MECHANISM}, and a client \
                 cannot refuse a low one without failing to connect"
            ),
        }
    }
}

/// RFC 7677 §4: 4096 is the minimum for SCRAM-SHA-256.
const SCRAM_MIN_ITERATIONS: u32 = 4096;

/// The server signs the exchange too, or the client authenticated to
/// nobody in particular.
///
/// `v=` is derived from key material only a holder of the account can
/// produce. Without it, a client has proved itself to whatever answered
/// the socket and has no way to notice.
async fn scram_server_proves_itself(ctx: &ServerCtx) -> Verdict {
    let auth_version = match negotiate(
        "SaslAuthenticate",
        match ctx.range(SaslAuthenticateRequest::API_KEY) {
            Ok(a) => a,
            Err(v) => return v,
        },
        SaslAuthenticateRequest::MIN_VERSION,
        SaslAuthenticateRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let (mut conn, _nonce, client_first_bare, server_first) = match scram_begin(ctx, 230).await {
        Ok(v) => v,
        Err(verdict) => return verdict,
    };
    let (Some(server_nonce), Some(salt), Some(iterations)) = (
        scram_attr(&server_first, 'r'),
        scram_attr(&server_first, 's').and_then(|s| b64_decode(&s)),
        scram_attr(&server_first, 'i').and_then(|i| i.parse::<u32>().ok()),
    ) else {
        return Verdict::Fail {
            details: format!("server-first is not a SCRAM message: {server_first:?}"),
        };
    };

    let without_proof = format!("c=biws,r={server_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let keys = scram_keys(SCRAM_PASSWORD, &salt, iterations);
    let proof = scram_client_proof(&keys, &auth_message);
    let client_final = format!("{without_proof},p={proof}");

    let server_final =
        match scram_token(&mut conn, auth_version, client_final.as_bytes(), 232).await {
            Ok(t) => t,
            Err(verdict) => return verdict,
        };
    let Some(signature) = scram_attr(&server_final, 'v') else {
        return Verdict::Fail {
            details: format!(
                "server-final carries no signature ({server_final:?}), so a client has \
                 authenticated itself to something it cannot identify"
            ),
        };
    };
    if signature == scram_server_signature(&keys, &auth_message) {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: "server signature does not verify against the account's key material".into(),
        }
    }
}

/// A SASL token on a connection that negotiated nothing is refused.
///
/// The interesting part is *which* refusal. A token arriving before a
/// mechanism has been chosen cannot be interpreted at all — there is no
/// mechanism to interpret it under — so the answer has to say "your
/// sequence is wrong", not "your credentials are wrong". A client told
/// the latter retries with the same broken sequence forever, and an
/// operator reading the logs goes looking for a password problem that
/// does not exist.
///
/// This one needs no credentials and no SASL listener, which is why it
/// runs everywhere: the question is about state, and a connection that
/// has done nothing is in the same state either way.
async fn sasl_authenticate_requires_handshake(ctx: &ServerCtx) -> Verdict {
    let version = match negotiate(
        "SaslAuthenticate",
        match ctx.range(SaslAuthenticateRequest::API_KEY) {
            Ok(a) => a,
            Err(v) => return v,
        },
        SaslAuthenticateRequest::MIN_VERSION,
        SaslAuthenticateRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    // A fresh connection: nothing negotiated on it, by construction.
    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };

    let mut request = SaslAuthenticateRequest::default();
    request.auth_bytes = Bytes::from_static(b"not-a-token");
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, version) {
        return Verdict::Error {
            details: format!("encoding SaslAuthenticate: {e}"),
        };
    }
    let resp: SaslAuthenticateResponse = match api_call(
        &mut conn,
        SaslAuthenticateRequest::API_KEY,
        version,
        200,
        &body,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return e.into_verdict(),
    };
    let code = ErrorCode(resp.error_code);
    if code == ErrorCode::ILLEGAL_SASL_STATE {
        Verdict::Pass
    } else if code.is_ok() {
        Verdict::Fail {
            details: "a SASL token was accepted on a connection that negotiated no \
                      mechanism"
                .into(),
        }
    } else {
        Verdict::Fail {
            details: format!(
                "an out-of-sequence SASL token answered {code}; ILLEGAL_SASL_STATE is \
                 what tells a client its sequence is wrong rather than its credentials"
            ),
        }
    }
}

/// A refused mechanism comes with the list of ones that would work.
///
/// Skipped rather than failed on a listener with no SASL configured:
/// such a listener answers ILLEGAL_SASL_STATE to every SASL request,
/// which is correct — there is no SASL session to negotiate within — and
/// reporting that as nonconformance would be reporting the operator's
/// listener configuration.
async fn sasl_refusal_names_mechanisms(ctx: &ServerCtx) -> Verdict {
    let Some(sasl_addr) = ctx.sasl_addr.clone() else {
        return Verdict::Skipped {
            reason: "no SASL listener given (--sasl-server); a listener without SASL \
                     answers ILLEGAL_SASL_STATE to every SASL request, so mechanism \
                     negotiation cannot be observed there"
                .into(),
        };
    };
    let version = match negotiate(
        "SaslHandshake",
        match ctx.range(SaslHandshakeRequest::API_KEY) {
            Ok(a) => a,
            Err(v) => return v,
        },
        SaslHandshakeRequest::MIN_VERSION,
        SaslHandshakeRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let mut conn = match connect(&sasl_addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };

    let mut request = SaslHandshakeRequest::default();
    // A mechanism no registry will ever contain.
    request.mechanism = "ODRADEK-NOSUCH-MECHANISM".to_owned();
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, version) {
        return Verdict::Error {
            details: format!("encoding SaslHandshake: {e}"),
        };
    }
    let resp: SaslHandshakeResponse = match api_call(
        &mut conn,
        SaslHandshakeRequest::API_KEY,
        version,
        202,
        &body,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return e.into_verdict(),
    };
    let code = ErrorCode(resp.error_code);
    if code == ErrorCode::ILLEGAL_SASL_STATE {
        return Verdict::Skipped {
            reason: format!("{sasl_addr} has no SASL configured after all"),
        };
    }
    if code != ErrorCode::UNSUPPORTED_SASL_MECHANISM {
        return Verdict::Fail {
            details: format!(
                "an unknown mechanism answered {code}, expected \
                 UNSUPPORTED_SASL_MECHANISM"
            ),
        };
    }
    if resp.mechanisms.is_empty() {
        return Verdict::Fail {
            details: "mechanism refused without naming a supported one, so a client \
                      has nothing to fall back to and must guess"
                .into(),
        };
    }
    Verdict::Pass
}

/// The assignment shape a heartbeat response carries.
type AssignedPartitions =
    odradek_protocol::messages::consumer_group_heartbeat_response::TopicPartitions;

/// A KIP-848 member names itself; this is the shape of that name.
///
/// Kafka takes any string here, but a uuid is what every real client
/// sends and what the field was designed around.
fn mint_member_id(tag: &str) -> String {
    format!("odradek-acceptance-{tag}-{}", std::process::id())
}

/// One ConsumerGroupHeartbeat exchange.
///
/// `subscribed` distinguishes the three states the field has, and the
/// distinction is the point: `None` says nothing about the subscription,
/// `Some(&[])` says there is none, and `Some(names)` states one.
async fn consumer_group_heartbeat(
    conn: &mut RawConnection,
    version: i16,
    group: &str,
    member_id: &str,
    epoch: i32,
    subscribed: Option<&[String]>,
    correlation_id: i32,
) -> Result<ConsumerGroupHeartbeatResponse, CheckError> {
    let mut request = ConsumerGroupHeartbeatRequest::default();
    request.group_id = group.to_owned();
    request.member_id = member_id.to_owned();
    request.member_epoch = epoch;
    request.rebalance_timeout_ms = 30_000;
    request.subscribed_topic_names = subscribed.map(<[String]>::to_vec);
    // A heartbeat that states a subscription is a member (re)introducing
    // itself, and it must also state what it currently owns — nothing,
    // as an empty list. Omitting the field is not the same as an empty
    // one here either: Kafka answers INVALID_REQUEST for the silence.
    if subscribed.is_some() {
        request.topic_partitions = Some(Vec::new());
    }
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding ConsumerGroupHeartbeat: {e}")))?;
    api_call(
        conn,
        ConsumerGroupHeartbeatRequest::API_KEY,
        version,
        correlation_id,
        &body,
    )
    .await
}

/// The KIP-848 version this subject and these checks share.
fn consumer_group_version(ctx: &ServerCtx) -> Result<i16, Verdict> {
    negotiate(
        "ConsumerGroupHeartbeat",
        ctx.range(ConsumerGroupHeartbeatRequest::API_KEY)?,
        ConsumerGroupHeartbeatRequest::MIN_VERSION,
        ConsumerGroupHeartbeatRequest::MAX_VERSION,
    )
}

/// A member that introduces itself is admitted at a non-zero epoch.
///
/// Epoch 0 is what a member says on the way in, so it cannot also be
/// what the coordinator says back: a client that is told 0 has no way to
/// distinguish having joined from having been ignored, and the epoch it
/// must echo on every later heartbeat is the one thing it cannot guess.
async fn consumer_group_epoch_advances(ctx: &ServerCtx) -> Verdict {
    let version = match consumer_group_version(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let group = check_group("epoch");
    if let Err(e) = await_coordinator(ctx, &group).await {
        return e.into_verdict();
    }
    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let member_id = mint_member_id("epoch");
    let topics = vec![unique_topic("cgnone")];

    let resp = match consumer_group_heartbeat(
        &mut conn,
        version,
        &group,
        &member_id,
        0,
        Some(&topics),
        150,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return e.into_verdict(),
    };
    let code = ErrorCode(resp.error_code);
    if !code.is_ok() {
        return Verdict::Fail {
            details: format!("a member introducing itself at epoch 0 was answered {code}"),
        };
    }
    if resp.member_epoch == 0 {
        return Verdict::Fail {
            details: "member was admitted at epoch 0, which is the epoch it arrived \
                      with: nothing distinguishes joining from being ignored"
                .into(),
        };
    }
    if resp.heartbeat_interval_ms <= 0 {
        return Verdict::Fail {
            details: format!(
                "heartbeat interval is {}ms, so a member has no pace to keep",
                resp.heartbeat_interval_ms
            ),
        };
    }
    // The coordinator may rename a member; it may not silently drop the id.
    if resp.member_id.as_deref().unwrap_or_default().is_empty() {
        return Verdict::Fail {
            details: "response carries no member id".into(),
        };
    }
    let _ = leave_consumer_group(&mut conn, version, &group, &member_id, 151).await;
    Verdict::Pass
}

/// Leaving is epoch -1, and the suite does it so a check leaves no member
/// behind to be rebalanced against on a live cluster.
async fn leave_consumer_group(
    conn: &mut RawConnection,
    version: i16,
    group: &str,
    member_id: &str,
    correlation_id: i32,
) -> Result<(), CheckError> {
    consumer_group_heartbeat(conn, version, group, member_id, -1, None, correlation_id)
        .await
        .map(|_| ())
}

/// A subscription produces an assignment, addressed by topic id.
async fn consumer_group_assigns_subscription(ctx: &ServerCtx) -> Verdict {
    let version = match consumer_group_version(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    // A real topic, so there is something to assign.
    let mut produced = match produce_flow(ctx, "cgassign", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if produced.topic_id == [0u8; 16] {
        return Verdict::Skipped {
            reason: "assignments are addressed by topic id and CreateTopics returned none".into(),
        };
    }
    let group = check_group("cgassign");
    if let Err(e) = await_coordinator(ctx, &group).await {
        return e.into_verdict();
    }
    let member_id = mint_member_id("assign");
    let topics = vec![produced.topic.clone()];

    let (assigned, _epoch) = match settle_assignment(
        ctx,
        &mut produced.conn,
        version,
        &group,
        &member_id,
        &topics,
        160,
    )
    .await
    {
        Ok(a) => a,
        Err(verdict) => return verdict,
    };
    let _ = leave_consumer_group(&mut produced.conn, version, &group, &member_id, 169).await;

    match assigned.iter().find(|tp| tp.topic_id == produced.topic_id) {
        Some(tp) if tp.partitions.contains(&0) => Verdict::Pass,
        Some(tp) => Verdict::Fail {
            details: format!(
                "subscribed to a 1-partition topic; assignment names it with partitions {:?}",
                tp.partitions
            ),
        },
        None => Verdict::Fail {
            details: format!(
                "subscribed to {:?} and was assigned {} topic(s), none of them that one",
                produced.topic,
                assigned.len()
            ),
        },
    }
}

/// Heartbeat until the coordinator has an assignment to give, or the
/// settle budget runs out.
///
/// A real coordinator computes assignments asynchronously, so the first
/// heartbeat legitimately returns nothing. That is reconciliation, not
/// nonconformance.
async fn settle_assignment(
    ctx: &ServerCtx,
    conn: &mut RawConnection,
    version: i16,
    group: &str,
    member_id: &str,
    topics: &[String],
    correlation_base: i32,
) -> Result<(Vec<AssignedPartitions>, i32), Verdict> {
    let mut epoch = 0;
    let mut subscribed = Some(topics);
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let correlation = correlation_base + i32::try_from(attempt).unwrap_or(0);
        let resp = match consumer_group_heartbeat(
            conn,
            version,
            group,
            member_id,
            epoch,
            subscribed,
            correlation,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => return Err(e.into_verdict()),
        };
        let code = ErrorCode(resp.error_code);
        if !code.is_ok() {
            return Err(Verdict::Fail {
                details: format!("heartbeat at epoch {epoch} answered {code}"),
            });
        }
        epoch = resp.member_epoch;
        // Stated once; from here the member is saying nothing new.
        subscribed = None;
        let assigned = resp
            .assignment
            .map(|a| a.topic_partitions)
            .unwrap_or_default();
        if !assigned.is_empty() {
            return Ok((assigned, epoch));
        }
    }
    Err(Verdict::Fail {
        details: format!(
            "no assignment for a subscribed topic after {:?}",
            ctx.config.settle_budget
        ),
    })
}

/// Omitting the subscription says nothing; it does not unsubscribe.
///
/// This is the steady state: a member that has settled sends heartbeats
/// carrying only its id and epoch. A coordinator that reads the absent
/// field as an empty subscription revokes the assignment of every member
/// that is idling correctly.
async fn consumer_group_omitted_subscription(ctx: &ServerCtx) -> Verdict {
    let version = match consumer_group_version(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let mut produced = match produce_flow(ctx, "cgsteady", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if produced.topic_id == [0u8; 16] {
        return Verdict::Skipped {
            reason: "assignments are addressed by topic id and CreateTopics returned none".into(),
        };
    }
    let group = check_group("cgsteady");
    if let Err(e) = await_coordinator(ctx, &group).await {
        return e.into_verdict();
    }
    let member_id = mint_member_id("steady");
    let topics = vec![produced.topic.clone()];

    let (_assigned, epoch) = match settle_assignment(
        ctx,
        &mut produced.conn,
        version,
        &group,
        &member_id,
        &topics,
        170,
    )
    .await
    {
        Ok(a) => a,
        Err(verdict) => return verdict,
    };

    // Now heartbeat the way a settled member does: its id and the epoch
    // it was last told, and nothing else. No re-reading the epoch first —
    // a known member arriving at epoch 0 is a rejoin, and being fenced
    // for it is correct.
    let quiet = match consumer_group_heartbeat(
        &mut produced.conn,
        version,
        &group,
        &member_id,
        epoch,
        None,
        180,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return e.into_verdict(),
    };
    let code = ErrorCode(quiet.error_code);
    let _ = leave_consumer_group(&mut produced.conn, version, &group, &member_id, 181).await;
    if !code.is_ok() {
        return Verdict::Fail {
            details: format!("a heartbeat stating nothing new was answered {code}"),
        };
    }
    // An *absent* assignment means nothing changed, which is the whole
    // point of the steady-state heartbeat: the response omits what has
    // not moved exactly as the request does. So absence is the passing
    // case, and so is being told the same assignment again.
    //
    // Revocation is what the broken behaviour looks like on the wire,
    // and it is distinguishable: dropping the subscription *changes* the
    // assignment to nothing, so the coordinator has to say so — an
    // assignment that is present and empty.
    match quiet.assignment {
        None => Verdict::Pass,
        Some(a)
            if a.topic_partitions
                .iter()
                .any(|tp| tp.topic_id == produced.topic_id) =>
        {
            Verdict::Pass
        }
        Some(a) if a.topic_partitions.is_empty() => Verdict::Fail {
            details: "a heartbeat that omitted subscribed_topic_names was answered with \
                      an empty assignment: the absent field was read as unsubscribing"
                .into(),
        },
        Some(a) => Verdict::Fail {
            details: format!(
                "a heartbeat that stated nothing new was reassigned to {} other topic(s)",
                a.topic_partitions.len()
            ),
        },
    }
}

/// An epoch the member has moved past is fenced.
async fn consumer_group_fenced_epoch(ctx: &ServerCtx) -> Verdict {
    let version = match consumer_group_version(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let group = check_group("cgfence");
    if let Err(e) = await_coordinator(ctx, &group).await {
        return e.into_verdict();
    }
    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let member_id = mint_member_id("fence");
    let topics = vec![unique_topic("cgfence")];

    let joined = match consumer_group_heartbeat(
        &mut conn,
        version,
        &group,
        &member_id,
        0,
        Some(&topics),
        190,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return e.into_verdict(),
    };
    let code = ErrorCode(joined.error_code);
    if !code.is_ok() {
        return Verdict::Fail {
            details: format!("joining answered {code}"),
        };
    }
    // An epoch beyond anything the coordinator has issued: a member
    // claiming to be further ahead than the group.
    let ahead = joined.member_epoch + 99;
    let fenced =
        match consumer_group_heartbeat(&mut conn, version, &group, &member_id, ahead, None, 191)
            .await
        {
            Ok(r) => r,
            Err(e) => return e.into_verdict(),
        };
    let code = ErrorCode(fenced.error_code);
    let _ = leave_consumer_group(&mut conn, version, &group, &member_id, 192).await;
    if code == ErrorCode::FENCED_MEMBER_EPOCH || code == ErrorCode::UNKNOWN_MEMBER_ID {
        Verdict::Pass
    } else if code.is_ok() {
        Verdict::Fail {
            details: format!(
                "heartbeat claiming epoch {ahead} (the group issued {}) was accepted",
                joined.member_epoch
            ),
        }
    } else {
        Verdict::Fail {
            details: format!("a bogus epoch answered {code}, expected FENCED_MEMBER_EPOCH"),
        }
    }
}

/// The version from which a join with no member id must be refused.
const JOIN_GROUP_MEMBER_ID_REQUIRED: i16 = 4;
/// The consumer protocol name these checks join under. The bytes are
/// opaque to the coordinator, so the suite does not have to speak it.
const GROUP_PROTOCOL_TYPE: &str = "consumer";

/// One JoinGroup exchange.
async fn join_group(
    conn: &mut RawConnection,
    version: i16,
    group: &str,
    member_id: &str,
    correlation_id: i32,
) -> Result<JoinGroupResponse, CheckError> {
    let mut protocol = JoinGroupRequestProtocol::default();
    protocol.name = "range".to_owned();
    protocol.metadata = Bytes::from_static(b"\x00\x01");
    let mut request = JoinGroupRequest::default();
    request.group_id = group.to_owned();
    request.session_timeout_ms = 30_000;
    request.rebalance_timeout_ms = 30_000;
    request.member_id = member_id.to_owned();
    request.protocol_type = GROUP_PROTOCOL_TYPE.to_owned();
    request.protocols = vec![protocol];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding JoinGroup: {e}")))?;
    api_call(
        conn,
        JoinGroupRequest::API_KEY,
        version,
        correlation_id,
        &body,
    )
    .await
}

/// Join a group and become its leader, returning (member id, generation).
async fn join_as_leader(
    ctx: &ServerCtx,
    conn: &mut RawConnection,
    version: i16,
    group: &str,
    correlation_base: i32,
) -> Result<(String, i32), CheckError> {
    let _ = ctx;
    let first = join_group(conn, version, group, "", correlation_base).await?;
    let code = ErrorCode(first.error_code);
    // v4+ answers the first join with an id to come back with; below
    // that the coordinator simply assigns one.
    let (member_id, joined) = if code == ErrorCode::MEMBER_ID_REQUIRED {
        let minted = first.member_id.clone();
        let second = join_group(conn, version, group, &minted, correlation_base + 1).await?;
        (minted, second)
    } else if code.is_ok() {
        (first.member_id.clone(), first)
    } else {
        return Err(CheckError::Violation(format!(
            "joining a fresh group answered {code}"
        )));
    };
    let code = ErrorCode(joined.error_code);
    if !code.is_ok() {
        return Err(CheckError::Violation(format!(
            "rejoining with the coordinator's own member id answered {code}"
        )));
    }
    if joined.leader != member_id {
        return Err(CheckError::Violation(format!(
            "sole member {member_id:?} was not made leader (leader is {:?})",
            joined.leader
        )));
    }
    Ok((member_id, joined.generation_id))
}

/// A join with no member id is refused, and told what to come back as.
///
/// Handing an anonymous join a membership instead leaves a member the
/// coordinator named but the client never acknowledged: if the client
/// dies before it learns its own id, nothing can name that member to
/// remove it, and the group waits out the session timeout.
async fn groups_member_id_required(ctx: &ServerCtx) -> Verdict {
    let version = match negotiate(
        "JoinGroup",
        match ctx.range(JoinGroupRequest::API_KEY) {
            Ok(a) => a,
            Err(v) => return v,
        },
        JOIN_GROUP_MEMBER_ID_REQUIRED,
        JoinGroupRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let group = check_group("memberid");
    if let Err(e) = await_coordinator(ctx, &group).await {
        return e.into_verdict();
    }

    let resp = match join_group(&mut conn, version, &group, "", 120).await {
        Ok(r) => r,
        Err(e) => return e.into_verdict(),
    };
    let code = ErrorCode(resp.error_code);
    if code != ErrorCode::MEMBER_ID_REQUIRED {
        return Verdict::Fail {
            details: format!(
                "JoinGroup v{version} with an empty member id answered {code}, expected \
                 MEMBER_ID_REQUIRED"
            ),
        };
    }
    if resp.member_id.is_empty() {
        return Verdict::Fail {
            details: "MEMBER_ID_REQUIRED carried no member id, so there is nothing to \
                      rejoin with"
                .into(),
        };
    }
    // The id it gave has to actually work.
    match join_group(&mut conn, version, &group, &resp.member_id, 121).await {
        Ok(second) if ErrorCode(second.error_code).is_ok() => Verdict::Pass,
        Ok(second) => Verdict::Fail {
            details: format!(
                "rejoining with the id MEMBER_ID_REQUIRED supplied answered {}",
                ErrorCode(second.error_code)
            ),
        },
        Err(e) => e.into_verdict(),
    }
}

/// The leader's assignment bytes reach their member unchanged.
///
/// The same guarantee the record-batch codec makes: the coordinator is
/// delivering an opaque payload it has no business reading. A
/// coordinator that parses assignments is one that breaks the day a
/// client uses an assignor it has never heard of.
async fn groups_assignment_round_trips(ctx: &ServerCtx) -> Verdict {
    let (join_version, sync_version) = match group_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let group = check_group("assignment");
    if let Err(e) = await_coordinator(ctx, &group).await {
        return e.into_verdict();
    }
    let (member_id, generation) =
        match join_as_leader(ctx, &mut conn, join_version, &group, 130).await {
            Ok(v) => v,
            Err(e) => return e.into_verdict(),
        };

    // Deliberately not a valid consumer-protocol assignment: the
    // coordinator has no business knowing the difference.
    let payload = Bytes::from_static(&[0x00, 0x03, 0xff, 0x7f, 0x00, 0xde, 0xad, 0xbe, 0xef]);
    let mut assignment = SyncGroupRequestAssignment::default();
    assignment.member_id = member_id.clone();
    assignment.assignment = payload.clone();
    let mut request = SyncGroupRequest::default();
    request.group_id = group.clone();
    request.generation_id = generation;
    request.member_id = member_id.clone();
    request.protocol_type = Some(GROUP_PROTOCOL_TYPE.to_owned());
    request.protocol_name = Some("range".to_owned());
    request.assignments = vec![assignment];
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, sync_version) {
        return Verdict::Error {
            details: format!("encoding SyncGroup: {e}"),
        };
    }
    let resp: SyncGroupResponse = match api_call(
        &mut conn,
        SyncGroupRequest::API_KEY,
        sync_version,
        132,
        &body,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return e.into_verdict(),
    };
    let code = ErrorCode(resp.error_code);
    if !code.is_ok() {
        return Verdict::Fail {
            details: format!("SyncGroup as the group's leader answered {code}"),
        };
    }
    if resp.assignment == payload {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: format!(
                "leader supplied {} assignment byte(s), member received {}: {:?} vs {:?}",
                payload.len(),
                resp.assignment.len(),
                payload.as_ref(),
                resp.assignment.as_ref()
            ),
        }
    }
}

/// A heartbeat from a generation the group has left is refused.
async fn groups_stale_generation_fenced(ctx: &ServerCtx) -> Verdict {
    let (join_version, _) = match group_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let heartbeat_version = match negotiate(
        "Heartbeat",
        match ctx.range(HeartbeatRequest::API_KEY) {
            Ok(a) => a,
            Err(v) => return v,
        },
        HeartbeatRequest::MIN_VERSION,
        HeartbeatRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let group = check_group("fencing");
    if let Err(e) = await_coordinator(ctx, &group).await {
        return e.into_verdict();
    }
    let (member_id, generation) =
        match join_as_leader(ctx, &mut conn, join_version, &group, 140).await {
            Ok(v) => v,
            Err(e) => return e.into_verdict(),
        };

    // One generation behind: a member that missed a rebalance.
    let stale = generation - 1;
    let mut request = HeartbeatRequest::default();
    request.group_id = group.clone();
    request.generation_id = stale;
    request.member_id = member_id;
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, heartbeat_version) {
        return Verdict::Error {
            details: format!("encoding Heartbeat: {e}"),
        };
    }
    let resp: HeartbeatResponse = match api_call(
        &mut conn,
        HeartbeatRequest::API_KEY,
        heartbeat_version,
        142,
        &body,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return e.into_verdict(),
    };
    let code = ErrorCode(resp.error_code);
    if code == ErrorCode::ILLEGAL_GENERATION {
        Verdict::Pass
    } else if code.is_ok() {
        Verdict::Fail {
            details: format!(
                "heartbeat carrying generation {stale} (the group is at {generation}) was \
                 accepted, so a member that missed a rebalance keeps its old assignment"
            ),
        }
    } else {
        Verdict::Fail {
            details: format!("stale heartbeat answered {code}, expected ILLEGAL_GENERATION"),
        }
    }
}

/// JoinGroup and SyncGroup versions, since either can be absent.
fn group_versions(ctx: &ServerCtx) -> Result<(i16, i16), Verdict> {
    let join = negotiate(
        "JoinGroup",
        ctx.range(JoinGroupRequest::API_KEY)?,
        JoinGroupRequest::MIN_VERSION,
        JoinGroupRequest::MAX_VERSION,
    )?;
    let sync = negotiate(
        "SyncGroup",
        ctx.range(SyncGroupRequest::API_KEY)?,
        SyncGroupRequest::MIN_VERSION,
        SyncGroupRequest::MAX_VERSION,
    )?;
    Ok((join, sync))
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
