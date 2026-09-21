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
use odradek_protocol::messages::add_offsets_to_txn_request::AddOffsetsToTxnRequest;
use odradek_protocol::messages::add_offsets_to_txn_response::AddOffsetsToTxnResponse;
use odradek_protocol::messages::add_partitions_to_txn_request::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnTopic,
};
use odradek_protocol::messages::add_partitions_to_txn_response::AddPartitionsToTxnResponse;
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use odradek_protocol::messages::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
use odradek_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use odradek_protocol::messages::create_topics_response::CreateTopicsResponse;
use odradek_protocol::messages::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest};
use odradek_protocol::messages::delete_topics_response::DeleteTopicsResponse;
use odradek_protocol::messages::describe_groups_request::DescribeGroupsRequest;
use odradek_protocol::messages::describe_groups_response::DescribeGroupsResponse;
use odradek_protocol::messages::end_txn_request::EndTxnRequest;
use odradek_protocol::messages::end_txn_response::EndTxnResponse;
use odradek_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use odradek_protocol::messages::fetch_response::FetchResponse;
use odradek_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use odradek_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use odradek_protocol::messages::heartbeat_request::HeartbeatRequest;
use odradek_protocol::messages::heartbeat_response::HeartbeatResponse;
use odradek_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use odradek_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use odradek_protocol::messages::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use odradek_protocol::messages::join_group_response::JoinGroupResponse;
use odradek_protocol::messages::leave_group_request::{LeaveGroupRequest, MemberIdentity};
use odradek_protocol::messages::leave_group_response::LeaveGroupResponse;
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
use odradek_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use odradek_protocol::messages::txn_offset_commit_response::TxnOffsetCommitResponse;
use odradek_protocol::records::{Record, RecordBatch, RecordHeader, Records, decode_set};
use odradek_protocol::{ErrorCode, Message, frame, header};

use crate::checks::{BoxFuture, Check, Runner};
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
    Check {
        id: "produce/acks-zero-is-silent",
        requirement: "sends no response at all to an acks=0 produce, rather than \
                      a frame the client has no correlation id outstanding for",
        runner: Runner::Server(|ctx| Box::pin(produce_acks_zero_is_silent(ctx))),
    },
    Check {
        id: "metadata/topic-id-is-stable",
        requirement: "reports the same topic id for a topic that has not gone \
                      away, so id-addressed requests keep working",
        runner: Runner::Server(|ctx| Box::pin(metadata_topic_id_is_stable(ctx))),
    },
    Check {
        id: "produce/compressed-batch-passthrough",
        requirement: "returns a gzip-compressed batch exactly as it was \
                      produced, rather than recompressing it and rewriting \
                      bytes the producer's crc covered",
        runner: Runner::Server(|ctx| Box::pin(produce_compressed_passthrough(ctx))),
    },
    Check {
        id: "fetch/long-poll-contract",
        requirement: "waits out max_wait_ms for a fetch it cannot yet satisfy, \
                      and answers one it can without spending the wait",
        runner: Runner::Server(|ctx| Box::pin(fetch_long_poll_contract(ctx))),
    },
    Check {
        id: "metadata/leader-is-a-known-broker",
        requirement: "names every partition leader in the same response's broker \
                      list, so a client has somewhere to route to",
        runner: Runner::Server(|ctx| Box::pin(metadata_leader_is_a_known_broker(ctx))),
    },
    Check {
        id: "admin/describe-groups-reports-members",
        requirement: "reports the members a live group has, rather than \
                      describing it as empty",
        runner: Runner::Server(|ctx| Box::pin(describe_groups_reports_members(ctx))),
    },
    Check {
        id: "offsets/metadata-round-trips",
        requirement: "returns the metadata string a commit carried exactly as it \
                      was given, rather than dropping or altering it beside an \
                      offset that reads back fine",
        runner: Runner::Server(|ctx| Box::pin(offsets_metadata_round_trips(ctx))),
    },
    Check {
        id: "create-topics/impossible-replication",
        requirement: "refuses a replication factor the cluster cannot satisfy \
                      rather than creating the topic with fewer replicas than \
                      were asked for",
        runner: Runner::Server(|ctx| Box::pin(create_topics_impossible_replication(ctx))),
    },
    Check {
        id: "groups/leave-unregisters-the-member",
        requirement: "stops accepting heartbeats from a member that has left, so \
                      its partitions are reassigned instead of being held by a \
                      member that is gone",
        runner: Runner::Server(|ctx| Box::pin(groups_leave_unregisters_the_member(ctx))),
    },
    Check {
        id: "list-offsets/by-timestamp",
        requirement: "answers a timestamp with the first offset at or after it, \
                      and a timestamp past every record with -1 rather than \
                      with either end of the log",
        runner: Runner::Server(|ctx| Box::pin(list_offsets_by_timestamp(ctx))),
    },
    Check {
        id: "admin/delete-topics-removes-the-topic",
        requirement: "a topic it reports deleted stops existing, rather than \
                      being acknowledged and left in place",
        runner: Runner::Server(|ctx| Box::pin(delete_topics_removes_the_topic(ctx))),
    },
    Check {
        id: "produce/idempotent-retry-is-deduped",
        requirement: "stores one copy of a batch sent twice under the same \
                      producer id, epoch and sequence, however it answers the \
                      second one",
        runner: Runner::Server(|ctx| Box::pin(produce_idempotent_retry_is_deduped(ctx))),
    },
    Check {
        id: "produce/sequence-gap-is-refused",
        requirement: "refuses a stamped batch whose sequence skips past what \
                      it last accepted, with OUT_OF_ORDER_SEQUENCE_NUMBER \
                      rather than by silently accepting the gap",
        runner: Runner::Server(|ctx| Box::pin(produce_sequence_gap_is_refused(ctx))),
    },
    Check {
        id: "txn/init-bumps-the-epoch",
        requirement: "hands a producer re-taking a transactional id a higher \
                      epoch than its predecessor, which is the only thing that \
                      tells the two apart",
        runner: Runner::Server(|ctx| Box::pin(txn_init_bumps_the_epoch(ctx))),
    },
    Check {
        id: "txn/stale-epoch-is-fenced",
        requirement: "refuses a transaction request carrying a superseded \
                      producer epoch, rather than letting a fenced producer \
                      write into its successor's transaction",
        runner: Runner::Server(|ctx| Box::pin(txn_stale_epoch_is_fenced(ctx))),
    },
    Check {
        id: "txn/unannounced-write-stays-in-the-transaction",
        requirement: "either refuses a transactional produce to a partition the \
                      client never announced, or brings that partition into the \
                      transaction itself — never writes the records outside it",
        runner: Runner::Server(|ctx| Box::pin(txn_unannounced_write_stays_in_the_transaction(ctx))),
    },
    Check {
        id: "txn/open-transaction-holds-the-stable-offset",
        requirement: "keeps the last stable offset below the high watermark \
                      while a transaction is open, so read_committed consumers \
                      are not shown records that may yet be aborted",
        runner: Runner::Server(|ctx| Box::pin(txn_open_transaction_holds_the_stable_offset(ctx))),
    },
    Check {
        id: "txn/fenced-producer-cannot-produce",
        requirement: "refuses a transactional produce carrying a superseded \
                      epoch at the partition leader, not only at the \
                      coordinator",
        runner: Runner::Server(|ctx| Box::pin(txn_fenced_producer_cannot_produce(ctx))),
    },
    Check {
        id: "txn/commit-is-visible-to-readers",
        requirement: "moves the stable offset past a committed transaction's \
                      records and does not name it in the aborted list, so \
                      read_committed consumers can see it",
        runner: Runner::Server(|ctx| Box::pin(txn_commit_is_visible_to_readers(ctx))),
    },
    Check {
        id: "txn/offsets-wait-for-the-commit",
        requirement: "holds offsets committed inside a transaction back until it \
                      commits, so the input is never marked processed while the \
                      output can still be thrown away",
        runner: Runner::Server(|ctx| Box::pin(txn_offsets_wait_for_the_commit(ctx))),
    },
    Check {
        id: "txn/abort-is-reported-to-readers",
        requirement: "names the aborted transaction in a read_committed fetch \
                      over its records, which is the only way a client can tell \
                      they were thrown away",
        runner: Runner::Server(|ctx| Box::pin(txn_abort_is_reported_to_readers(ctx))),
    },
    Check {
        id: "cluster/brokers-agree-on-the-leader",
        requirement: "every broker names the same leader for a partition, so a                       client that refreshes metadata against a different broker                       than last time is not sent somewhere else",
        runner: Runner::Server(|ctx| Box::pin(cluster_brokers_agree_on_the_leader(ctx))),
    },
    Check {
        id: "cluster/brokers-agree-on-the-coordinator",
        requirement: "every broker names the same coordinator for a group, so a                       group does not end up with as many coordinators as it has                       bootstrap addresses",
        runner: Runner::Server(|ctx| Box::pin(cluster_brokers_agree_on_the_coordinator(ctx))),
    },
    Check {
        id: "cluster/replicas-span-brokers",
        requirement: "places a topic asked for n replicas on n distinct brokers,                       with the leader among them and the in-sync set drawn from                       them",
        runner: Runner::Server(|ctx| Box::pin(cluster_replicas_span_brokers(ctx))),
    },
    Check {
        id: "cluster/writes-go-to-the-leader",
        requirement: "refuses a produce to a broker that does not lead the                       partition, rather than appending to a log the leader knows                       nothing about",
        runner: Runner::Server(|ctx| Box::pin(cluster_writes_go_to_the_leader(ctx))),
    },
    Check {
        id: "cluster/group-offsets-need-the-coordinator",
        requirement: "refuses an offset commit on a broker that does not                       coordinate the group, rather than storing it where the                       coordinator will never see it",
        runner: Runner::Server(|ctx| Box::pin(cluster_group_offsets_need_the_coordinator(ctx))),
    },
    Check {
        id: "cluster/leadership-moves-when-a-broker-stops",
        requirement: "moves a partition's leadership to one of its replicas when                       the leader stops, and the new leader accepts writes —                       otherwise the replicas were decoration",
        runner: Runner::Server(|ctx| Box::pin(cluster_leadership_moves_when_a_broker_stops(ctx))),
    },
    Check {
        id: "cluster/committed-offsets-outlive-the-coordinator",
        requirement: "gives the group a new coordinator when its own stops, still                       reporting the offset it acknowledged — a commit is only as                       durable as what answers after the failure",
        runner: Runner::Server(|ctx| {
            Box::pin(cluster_committed_offsets_outlive_the_coordinator(ctx))
        }),
    },
    Check {
        id: "versions/advertised-versions-are-speakable",
        requirement: "answers every version of every api it advertises, in the \
                      shape that version specifies — an advertised range the \
                      server cannot serve sends clients to a version that fails",
        runner: Runner::Server(|ctx| Box::pin(versions_advertised_are_speakable(ctx))),
    },
];

/// How to take one of the subject's brokers away, and give it back.
///
/// Nothing in the Kafka protocol says "stop". A check that needs a
/// broker to go away therefore has to ask something outside the
/// protocol, and what that something is belongs entirely to the
/// deployment: `docker stop`, `kubectl delete pod`, `systemctl stop`, a
/// cloud API. The suite refuses to guess. It is handed a way to do it
/// or, given none, skips the checks that need one and says so — which
/// is the same bargain `--sasl-server` strikes.
///
/// Implementations must be idempotent. A check restores the cluster on
/// every path out, including the ones where it has already decided the
/// subject is wrong, so `start` is routinely called on a node that was
/// never stopped.
pub trait ClusterControl: std::fmt::Debug + Send + Sync {
    /// Take a broker out of the cluster and do not return until it is
    /// gone. A check that proceeded while the broker was still
    /// answering would be testing nothing.
    fn stop(&self, broker: Broker<'_>) -> BoxFuture<'_, Result<(), String>>;

    /// Put it back, and do not return until it is serving again.
    /// Whatever runs next is entitled to a whole cluster.
    fn start(&self, broker: Broker<'_>) -> BoxFuture<'_, Result<(), String>>;
}

/// Which broker a [`ClusterControl`] is being asked about, named both
/// ways the suite knows it.
///
/// Both, because neither alone suits every deployment. The node id is
/// the cluster's own name for the broker and the one an operator thinks
/// in — but not every implementation lets you choose it: Kafka takes a
/// configured `node.id`, while Redpanda assigns its own, so a control
/// script that mapped id to container would be guessing. The advertised
/// address is what the suite actually connected to, and is unambiguous
/// by construction.
#[derive(Debug, Clone, Copy)]
pub struct Broker<'a> {
    /// The node id Metadata gave for this broker.
    pub node_id: i32,
    /// The `host:port` Metadata advertised for it.
    pub addr: &'a str,
}

/// Limits for one server-side run.
///
/// The settle budget is the only knob that costs wall-clock time: a
/// freshly created topic on a real broker genuinely takes seconds to
/// elect a leader, but a subject that answers instantly (the calibration
/// [`crate::subject`], or any in-process stub) never needs the wait — and
/// a subject that answers a *permanent* error the flow treats as
/// retriable burns the whole budget before reaching the right verdict.
///
/// It covers every wait on work a broker does *after* acknowledging
/// something: electing a leader for a new topic, propagating a topic to
/// another broker, writing a transaction marker and advancing the
/// stable offset past it. Those are all "when it gets round to it", and
/// how long that is depends on how busy the machine is rather than on
/// anything the suite controls — a matrix that runs several brokers at
/// once on a two-core runner is a different proposition from one broker
/// on an idle laptop. The default is therefore set by what a loaded
/// machine needs, not by what a quiet one does; every loop that uses it
/// exits as soon as the work lands, so patience costs nothing when
/// things work.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProbeConfig {
    /// How long the produce/fetch flows tolerate retriable errors from a
    /// freshly created topic before calling it a failure.
    pub settle_budget: Duration,
    /// How long to wait between attempts within that budget.
    pub settle_delay: Duration,
    /// How to stop and start the subject's brokers. Without one, the
    /// checks that need a broker to fail skip.
    pub control: Option<std::sync::Arc<dyn ClusterControl>>,
    /// How long to let a cluster notice a broker has gone and finish
    /// electing around it.
    ///
    /// Separate from the settle budget, and much larger, because it is
    /// bounded by the subject's own failure detection rather than by
    /// anything the suite does: Kafka's KRaft controller waits out a
    /// broker session timeout before declaring it gone, which is seconds
    /// by default. A budget tight enough for topic creation would report
    /// every healthy cluster as one that never recovers.
    pub recovery_budget: Duration,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        ProbeConfig {
            settle_budget: Duration::from_secs(20),
            settle_delay: Duration::from_millis(100),
            control: None,
            recovery_budget: Duration::from_secs(60),
        }
    }
}

impl ProbeConfig {
    /// How many attempts the budget affords, `settle_delay` apart. Always
    /// at least one: a zero budget still gets a single try, so the flow
    /// can never skip the exchange it is there to make.
    fn settle_attempts(&self) -> u32 {
        Self::attempts(self.settle_budget, self.settle_delay)
    }

    /// The same, over the recovery budget.
    fn recovery_attempts(&self) -> u32 {
        Self::attempts(self.recovery_budget, self.settle_delay)
    }

    fn attempts(budget: Duration, delay: Duration) -> u32 {
        let delay = delay.as_millis().max(1);
        let attempts = budget.as_millis() / delay;
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

    /// What went wrong, without saying whose fault it was.
    fn details(&self) -> &str {
        match self {
            CheckError::Infra(d) | CheckError::Violation(d) => d,
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
/// The group this suite commits under, stamped with [`run_id`] so a
/// rerun against a live cluster never meets its own leavings.
///
/// It is not only stale *commits* that matter. A group whose members did
/// not leave — because the check that made them was about fencing, or
/// failed halfway — keeps them until the session times out, and the next
/// run's JoinGroup then parks behind a rebalance waiting for members
/// that will never rejoin. That parks for the rebalance timeout, which
/// is far longer than any check's read deadline, so the whole thing
/// surfaces as an infrastructure error rather than as anything about the
/// broker. Fresh names per run cost nothing and make the failure
/// impossible; per-check cleanup could not, since the checks that most
/// need it are the ones that end badly.
fn check_group(tag: &str) -> String {
    format!("{tag}-odradek-acceptance-{}", run_id())
}

/// A string distinguishing this process from every other run of the
/// suite, for naming things a broker keeps.
///
/// Process id alone is not enough — they are reused, and a container
/// that starts the suite twice can plausibly see the same one — so it is
/// paired with the clock at first use. Computed once, because names
/// derived from it have to agree across the calls within a single check.
fn run_id() -> &'static str {
    static RUN_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    RUN_ID.get_or_init(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{}-{nanos}", std::process::id())
    })
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

/// The endpoint a FindCoordinator response names, from whichever shape
/// the negotiated version used, or `None` if it named nothing usable.
fn coordinator_endpoint(resp: &FindCoordinatorResponse, version: i16) -> Option<String> {
    let (host, port) = if version >= FIND_COORDINATOR_BATCHED {
        let c = resp.coordinators.first()?;
        (c.host.as_str(), c.port)
    } else {
        (resp.host.as_str(), resp.port)
    };
    if host.is_empty() || !(1..=65535).contains(&port) {
        return None;
    }
    Some(format!("{host}:{port}"))
}

/// A connection to the broker that coordinates `group`.
///
/// Group requests are answered only by the coordinator; every other
/// broker replies `NOT_COORDINATOR` and expects the client to go and ask
/// the right one. On a single-broker cluster the two are the same
/// machine, which is how the suite got as far as it did while sending
/// group requests down whichever connection it already had. On a real
/// cluster that is a one-in-`n` guess.
///
/// Falls back to the bootstrap address when the cluster names no
/// coordinator — the check that follows then reports whatever the
/// cluster says, rather than this helper inventing a verdict about it.
async fn coordinator_conn(ctx: &ServerCtx, group: &str) -> Result<RawConnection, CheckError> {
    let addr = await_coordinator(ctx, group).await?;
    connect(addr.as_deref().unwrap_or(&ctx.addr)).await
}

/// Wait for the group coordinator to exist, and say where it is.
///
/// The offsets checks need this for the same reason and would otherwise
/// pass or fail on whether they happened to run after
/// `find-coordinator/group` warmed the cluster — an order dependency
/// between checks is a bug in the suite, not a property of the subject.
async fn await_coordinator(ctx: &ServerCtx, group: &str) -> Result<Option<String>, CheckError> {
    await_coordinator_at(ctx, &ctx.addr, group).await
}

/// The same, asking a broker of the caller's choosing.
///
/// Which broker is asked is normally nobody's business — every broker
/// gives the same answer, and `cluster/brokers-agree-on-the-coordinator`
/// is the check that says so. It matters in exactly one situation: when
/// the bootstrap broker is the one that has just been stopped, and
/// asking it would fail for the obvious reason rather than the
/// interesting one.
async fn await_coordinator_at(
    ctx: &ServerCtx,
    addr: &str,
    group: &str,
) -> Result<Option<String>, CheckError> {
    let advertised = match ctx.range(FindCoordinatorRequest::API_KEY) {
        Ok(a) => a,
        // No FindCoordinator advertised: let the offsets exchange itself
        // report whatever it reports.
        Err(_) => return Ok(None),
    };
    let Ok(version) = negotiate(
        "FindCoordinator",
        advertised,
        FindCoordinatorRequest::MIN_VERSION,
        FindCoordinatorRequest::MAX_VERSION,
    ) else {
        return Ok(None);
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

    let mut conn = connect(addr).await?;
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
            return Ok(coordinator_endpoint(&resp, version));
        }
    }
    Ok(None)
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

/// Wait until the broker on the other end of `conn` has heard of
/// `topic`.
///
/// A cluster propagates metadata between its brokers asynchronously, so
/// a topic created a moment ago and already written to on its leader is
/// briefly unknown to the group coordinator that is about to be asked to
/// commit an offset for it. UNKNOWN_TOPIC_OR_PARTITION from that broker
/// is a wait, not an answer — a client retries it and so must a suite
/// that means to test clusters. On a single broker there is nothing to
/// propagate and the first ask succeeds, which is why this was never
/// needed before.
///
/// Best effort by design: if the topic never shows up, say nothing and
/// let the call the caller actually cares about report what it gets.
/// Turning a propagation delay into a verdict of its own would hide the
/// question being asked behind the plumbing.
async fn await_topic_known(
    ctx: &ServerCtx,
    conn: &mut RawConnection,
    topic: &str,
    correlation_base: i32,
) -> Result<(), CheckError> {
    let Ok(range) = ctx.range(MetadataRequest::API_KEY) else {
        return Ok(());
    };
    let Ok(version) = negotiate("Metadata", range, 1, MetadataRequest::MAX_VERSION) else {
        return Ok(());
    };
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let correlation = correlation_base + i32::try_from(attempt).unwrap_or(0);
        let resp = metadata_of_topic(conn, version, topic, correlation).await?;
        let known = resp
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic))
            .is_some_and(|t| ErrorCode(t.error_code).is_ok() && !t.partitions.is_empty());
        if known {
            return Ok(());
        }
    }
    Ok(())
}

/// Commit an offset, following the cluster when it says the group's
/// coordinator is somewhere else, and hand back the connection that
/// took it.
///
/// `NOT_COORDINATOR` is a redirect, not a refusal: coordinators move,
/// and they move most while a cluster is still settling, which is
/// exactly when the suite is asking. A client re-reads FindCoordinator
/// and asks the broker it names; a check that treated the first answer
/// as final would report a healthy cluster for having changed its mind
/// between two requests. The connection comes back so the read-back
/// goes to the same broker the write did.
async fn commit_offset_settled(
    ctx: &ServerCtx,
    group: &str,
    produced: &ProducedTopic,
    offset: i64,
    version: i16,
    correlation: i32,
) -> Result<RawConnection, CheckError> {
    let mut last = CheckError::Infra("no commit attempts were made".into());
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let mut conn = coordinator_conn(ctx, group).await?;
        await_topic_known(ctx, &mut conn, &produced.topic, correlation).await?;
        match commit_offset(
            &mut conn,
            version,
            group,
            &produced.topic,
            produced.topic_id,
            offset,
            correlation + 1,
        )
        .await
        {
            Ok(()) => return Ok(conn),
            Err(CheckError::Violation(details))
                if details.contains(&format!("{}", ErrorCode::NOT_COORDINATOR)) =>
            {
                last = CheckError::Violation(details);
            }
            Err(e) => return Err(e),
        }
    }
    Err(last)
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
    let produced = match produce_flow(ctx, "offsets", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if let Some(skip) = skip_without_topic_id(&produced, commit_version) {
        return skip;
    }
    let group = check_group(&produced.topic);
    // Not 0, and not the log end either: a number nothing else would
    // produce by accident.
    let committed = 7;
    // The offsets live with the group's coordinator, which is not in
    // general the broker that leads the topic just produced to — and
    // which may move while the cluster settles.
    let mut conn =
        match commit_offset_settled(ctx, &group, &produced, committed, commit_version, 55).await {
            Ok(c) => c,
            Err(e) => return e.into_verdict(),
        };
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
            &mut conn,
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
    let produced = match produce_flow(ctx, "unset", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if let Some(skip) = skip_without_topic_id(&produced, fetch_version) {
        return skip;
    }
    // A group that has never existed, let alone committed.
    let group = format!("{}-never-committed", check_group(&produced.topic));
    let mut conn = match coordinator_conn(ctx, &group).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    if let Err(e) = await_topic_known(ctx, &mut conn, &produced.topic, 66).await {
        return e.into_verdict();
    }
    match fetch_committed(
        &mut conn,
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

/// The SCRAM mechanism these checks speak.
const SCRAM_MECHANISM: &str = "SCRAM-SHA-256";

/// Negotiate SCRAM on a connection, or say why it cannot be.
async fn scram_handshake(
    conn: &mut RawConnection,
    version: i16,
    sasl_addr: &str,
    correlation_id: i32,
) -> Result<(), Verdict> {
    let mut handshake = SaslHandshakeRequest::default();
    handshake.mechanism = SCRAM_MECHANISM.to_owned();
    let mut body = BytesMut::new();
    handshake
        .encode(&mut body, version)
        .map_err(|e| Verdict::Error {
            details: format!("encoding SaslHandshake: {e}"),
        })?;
    let resp: SaslHandshakeResponse = api_call(
        conn,
        SaslHandshakeRequest::API_KEY,
        version,
        correlation_id,
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
    Ok(())
}

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
    let Some(sasl_addr) = ctx.sasl_addr.clone() else {
        return Verdict::Skipped {
            reason: "no SASL listener given (--sasl-server)".into(),
        };
    };
    let handshake_version = match negotiate(
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

    // The exchange is driven by odradek-sasl, which is the same code the
    // client crate uses and is checked against the RFC vectors. A suite
    // that reimplemented it here would be testing its own arithmetic
    // against itself.
    let mut client = match odradek_sasl::ScramClient::new(
        odradek_sasl::Mechanism::ScramSha256,
        SCRAM_USER,
        SCRAM_PASSWORD,
        odradek_sasl::Limits::default(),
    ) {
        Ok(c) => c,
        Err(e) => {
            return Verdict::Error {
                details: format!("building a SCRAM client: {e}"),
            };
        }
    };

    let mut conn = match connect(&sasl_addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    match scram_handshake(&mut conn, handshake_version, &sasl_addr, 230).await {
        Ok(()) => {}
        Err(verdict) => return verdict,
    }

    let server_first = match scram_token(
        &mut conn,
        auth_version,
        client.client_first().as_bytes(),
        231,
    )
    .await
    {
        Ok(t) => t,
        Err(verdict) => return verdict,
    };
    let client_final = match client.client_final(&server_first) {
        Ok(m) => m,
        Err(e) => {
            return Verdict::Fail {
                details: format!("server-first is not usable: {e}"),
            };
        }
    };
    let server_final =
        match scram_token(&mut conn, auth_version, client_final.as_bytes(), 232).await {
            Ok(t) => t,
            Err(verdict) => return verdict,
        };
    match client.verify_server_final(&server_final) {
        Ok(()) => Verdict::Pass,
        Err(odradek_sasl::SaslError::NoServerSignature) => Verdict::Fail {
            details: format!(
                "server-final carries no signature ({server_final:?}), so a client has \
                 authenticated itself to something it cannot identify"
            ),
        },
        Err(e) => Verdict::Fail {
            details: format!("server signature does not verify: {e}"),
        },
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
    let mut conn = match coordinator_conn(ctx, &group).await {
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
    let produced = match produce_flow(ctx, "cgassign", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if produced.topic_id == [0u8; 16] {
        return Verdict::Skipped {
            reason: "assignments are addressed by topic id and CreateTopics returned none".into(),
        };
    }
    let group = check_group("cgassign");
    // ConsumerGroupHeartbeat is a group call: the coordinator answers it,
    // and the broker leading the topic only refers you onwards.
    let mut conn = match coordinator_conn(ctx, &group).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let member_id = mint_member_id("assign");
    let topics = vec![produced.topic.clone()];

    let (assigned, _epoch) =
        match settle_assignment(ctx, &mut conn, version, &group, &member_id, &topics, 160).await {
            Ok(a) => a,
            Err(verdict) => return verdict,
        };
    let _ = leave_consumer_group(&mut conn, version, &group, &member_id, 169).await;

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
    let produced = match produce_flow(ctx, "cgsteady", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if produced.topic_id == [0u8; 16] {
        return Verdict::Skipped {
            reason: "assignments are addressed by topic id and CreateTopics returned none".into(),
        };
    }
    let group = check_group("cgsteady");
    let mut conn = match coordinator_conn(ctx, &group).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let member_id = mint_member_id("steady");
    let topics = vec![produced.topic.clone()];

    let (_assigned, epoch) =
        match settle_assignment(ctx, &mut conn, version, &group, &member_id, &topics, 170).await {
            Ok(a) => a,
            Err(verdict) => return verdict,
        };

    // Now heartbeat the way a settled member does: its id and the epoch
    // it was last told, and nothing else. No re-reading the epoch first —
    // a known member arriving at epoch 0 is a rejoin, and being fenced
    // for it is correct.
    let quiet =
        match consumer_group_heartbeat(&mut conn, version, &group, &member_id, epoch, None, 180)
            .await
        {
            Ok(r) => r,
            Err(e) => return e.into_verdict(),
        };
    let code = ErrorCode(quiet.error_code);
    let _ = leave_consumer_group(&mut conn, version, &group, &member_id, 181).await;
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
    let mut conn = match coordinator_conn(ctx, &group).await {
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
    let group = check_group("memberid");
    let mut conn = match coordinator_conn(ctx, &group).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };

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
    let group = check_group("assignment");
    let mut conn = match coordinator_conn(ctx, &group).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
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
    let group = check_group("fencing");
    let mut conn = match coordinator_conn(ctx, &group).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
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
    // Creating a topic changes cluster metadata, which is the
    // controller's to change. Every broker names the controller in its
    // Metadata response so a client can go there, and an implementation
    // is free to answer NOT_CONTROLLER rather than forward — Redpanda
    // does. That only shows up on a cluster, and only once something
    // has moved the controller off whichever broker the suite
    // bootstrapped from, which is exactly what the recovery checks do.
    let mut controller = admin_conn(ctx, conn, correlation_id.wrapping_sub(1)).await?;
    let conn = &mut controller;
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
/// The first Produce version that carries a v2 record batch.
///
/// Below it the request body is a *message set* — the pre-KIP-98
/// format, with its own framing and no producer id — which this
/// workspace does not model and does not intend to: the record batch
/// and its crc are the thing the protocol crate exists to get right.
/// So v0-v2 are unspeakable here whatever the server does, and a sweep
/// that sent a modern batch at them would be reporting its own
/// limitation as the subject's fault. Apache Kafka 4.1 still advertises
/// Produce from v0.
const PRODUCE_RECORD_BATCH_MIN: i16 = 3;
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
///
/// Stamped per call rather than per [`run_id`]: two checks can share a
/// tag, and a topic one of them deleted is not one the other should be
/// handed.
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
    /// Where `conn` points: the partition leader, for callers that need
    /// a second connection to the same broker.
    addr: String,
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
    // Metadata is how the leader is found; without it the flow can only
    // address the broker it bootstrapped from and hope.
    let metadata_version = negotiate(
        "Metadata",
        ctx.range(MetadataRequest::API_KEY)?,
        1,
        MetadataRequest::MAX_VERSION,
    )?;
    let fail = |details: String| Verdict::Fail { details };
    // Failing to encode our own request means the check never ran.
    let infra = |details: String| Verdict::Error { details };

    let mut bootstrap = connect(&ctx.addr).await.map_err(CheckError::into_verdict)?;
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
    // To the controller, not to whichever broker we bootstrapped from.
    let mut controller = admin_conn(ctx, &mut bootstrap, 9)
        .await
        .map_err(|e| e.context("locating the controller").into_verdict())?;
    let resp: CreateTopicsResponse = api_call(
        &mut controller,
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

    // Address the leader, not whichever broker answered CreateTopics.
    let mut conn = connect_to_leader(ctx, &mut bootstrap, metadata_version, &topic, 12)
        .await
        .map_err(|e| e.context("locating the partition leader").into_verdict())?;

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
                addr: conn.peer().to_owned(),
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
        // Every retriable code here is the cluster saying this broker is
        // the wrong one to ask — either not yet, or not any more. Asking
        // Metadata again between attempts is the difference between
        // waiting for leadership to settle and waiting for it to settle
        // *here*, and only the first is something a cluster will ever do.
        conn = connect_to_leader(ctx, &mut bootstrap, metadata_version, &topic, 13)
            .await
            .map_err(|e| e.context("relocating the partition leader").into_verdict())?;
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

// ---- transactions ---------------------------------------------------
//
// A transaction is an agreement between a producer and its coordinator
// that a set of writes lands all together or not at all. Four things
// have to hold for that to mean anything, and each is a check below:
// the id can be taken over (fencing), a partition cannot be written to
// without being announced, an open transaction withholds its records
// from committed readers, and an abandoned one is reported to them as
// abandoned rather than quietly left in the log looking like data.

/// AddPartitionsToTxn versions a client may speak; v4+ is the
/// broker-to-broker batching shape, which the schema reserves outright.
const ADD_PARTITIONS_CLIENT_MAX: i16 = 3;
/// InitProducerId below v6 (2PC, marked unstable upstream).
const TXN_INIT_MAX: i16 = 5;
/// EndTxn below v5, which bumps the epoch on every transaction and
/// hands back a new one the client must adopt.
const END_TXN_MAX: i16 = 4;
/// The fetch isolation level that filters uncommitted data.
const READ_COMMITTED: i8 = 1;
/// The default: every record in the log, decided or not.
const READ_UNCOMMITTED: i8 = 0;
/// The record batch attribute marking a batch as transactional.
const TRANSACTIONAL_ATTR: i16 = 1 << 4;

/// Negotiated versions for one transaction check.
struct TxnVersions {
    init: i16,
    add: i16,
    end: i16,
    produce: i16,
    fetch: i16,
}

fn txn_versions(ctx: &ServerCtx) -> Result<TxnVersions, Verdict> {
    Ok(TxnVersions {
        init: negotiate(
            "InitProducerId",
            ctx.range(InitProducerIdRequest::API_KEY)?,
            InitProducerIdRequest::MIN_VERSION,
            TXN_INIT_MAX,
        )?,
        add: negotiate(
            "AddPartitionsToTxn",
            ctx.range(AddPartitionsToTxnRequest::API_KEY)?,
            AddPartitionsToTxnRequest::MIN_VERSION,
            ADD_PARTITIONS_CLIENT_MAX,
        )?,
        end: negotiate(
            "EndTxn",
            ctx.range(EndTxnRequest::API_KEY)?,
            EndTxnRequest::MIN_VERSION,
            END_TXN_MAX,
        )?,
        produce: negotiate(
            "Produce",
            ctx.range(ProduceRequest::API_KEY)?,
            ProduceRequest::MIN_VERSION,
            PRODUCE_NAME_MAX,
        )?,
        fetch: negotiate(
            "Fetch",
            ctx.range(FetchRequest::API_KEY)?,
            FetchRequest::MIN_VERSION,
            FETCH_NAME_MAX,
        )?,
    })
}

/// Who is acting, and on which transaction.
///
/// The three travel together because they are checked together: the id
/// names the transaction, and the producer id and epoch are what prove
/// this is the producer entitled to it.
#[derive(Debug, Clone, Copy)]
struct TxnActor<'a> {
    transactional_id: &'a str,
    producer_id: i64,
    producer_epoch: i16,
}

impl<'a> TxnActor<'a> {
    fn new(transactional_id: &'a str, identity: &InitProducerIdResponse) -> TxnActor<'a> {
        TxnActor {
            transactional_id,
            producer_id: identity.producer_id,
            producer_epoch: identity.producer_epoch,
        }
    }

    /// The same actor at a different epoch — a superseded one, in the
    /// only check that wants it.
    fn at_epoch(self, producer_epoch: i16) -> TxnActor<'a> {
        TxnActor {
            producer_epoch,
            ..self
        }
    }
}

/// A transactional id nothing else will use.
fn unique_transactional_id(tag: &str) -> String {
    format!("odradek-txn-{tag}-{}", unique_topic(tag))
}

/// Discover the transaction coordinator for `transactional_id`.
///
/// Not optional, and not only because a multi-broker cluster needs to
/// know which broker to ask. Brokers materialize their transaction log
/// on this call: Redpanda 25.2 answers InitProducerId with *silence* —
/// no response frame at all — until a FindCoordinator has created
/// `kafka_internal/tx`, while Kafka 4.1 answers either way. A suite
/// that skipped the step would report Redpanda as broken rather than
/// report itself as impatient.
async fn await_txn_coordinator(
    ctx: &ServerCtx,
    conn: &mut RawConnection,
    transactional_id: &str,
    correlation: i32,
) -> Result<Option<String>, CheckError> {
    let advertised = match ctx.range(FindCoordinatorRequest::API_KEY) {
        Ok(a) => a,
        // Nothing to ask; let InitProducerId report what it reports.
        Err(_) => return Ok(None),
    };
    let Ok(version) = negotiate(
        "FindCoordinator",
        advertised,
        FindCoordinatorRequest::MIN_VERSION,
        FindCoordinatorRequest::MAX_VERSION,
    ) else {
        return Ok(None);
    };
    let mut request = FindCoordinatorRequest::default();
    request.key_type = 1; // transaction coordinator
    if version >= FIND_COORDINATOR_BATCHED {
        request.coordinator_keys = vec![transactional_id.to_owned()];
    } else {
        request.key = transactional_id.to_owned();
    }
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding FindCoordinator: {e}")))?;

    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let resp: FindCoordinatorResponse = api_call(
            conn,
            FindCoordinatorRequest::API_KEY,
            version,
            correlation + i32::try_from(attempt).unwrap_or(0),
            &body,
        )
        .await?;
        if !is_coordinator_settling(coordinator_error(&resp, version)) {
            return Ok(coordinator_endpoint(&resp, version));
        }
    }
    Ok(None)
}

/// Claim a transactional id, riding out a coordinator that is still
/// loading, and hand back the connection to the coordinator that granted
/// it.
///
/// The connection is the point. Every later call in the transaction —
/// AddPartitionsToTxn, AddOffsetsToTxn, EndTxn — is answered by this one
/// broker and by no other, while the writes inside the transaction go to
/// the partition leaders instead, and any offsets go to the *group*
/// coordinator. Three roles that a single-broker cluster collapses onto
/// one machine and a real one does not. Handing the connection back
/// makes each caller say which of the three it means.
///
/// A cluster that has never hosted a transaction creates its
/// transaction log on the first ask and answers
/// COORDINATOR_LOAD_IN_PROGRESS until that log's partitions have
/// leaders — retriable, not a wrong answer, and a suite that treated it
/// as one would fail every transaction check on a fresh broker.
async fn init_producer_id(
    ctx: &ServerCtx,
    version: i16,
    transactional_id: &str,
    correlation: i32,
) -> Result<(RawConnection, InitProducerIdResponse), CheckError> {
    let mut bootstrap = connect(&ctx.addr).await?;
    let located =
        await_txn_coordinator(ctx, &mut bootstrap, transactional_id, correlation - 1).await?;
    let mut conn = connect(located.as_deref().unwrap_or(&ctx.addr)).await?;
    let mut request = InitProducerIdRequest::default();
    request.transactional_id = Some(transactional_id.to_owned());
    request.transaction_timeout_ms = 60_000;
    request.producer_id = -1;
    request.producer_epoch = -1;
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding InitProducerId: {e}")))?;

    let mut last = ErrorCode(0);
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
            // Ask again where the coordinator is: on a broker that has
            // never hosted a transaction, this is the call that creates
            // the log, and a NOT_COORDINATOR means the partition it
            // lives on has no leader yet. On a cluster it can equally
            // mean the coordinator is another broker, so follow the
            // answer rather than merely waiting on it.
            if let Some(addr) =
                await_txn_coordinator(ctx, &mut bootstrap, transactional_id, correlation - 1)
                    .await?
            {
                conn = connect(&addr).await?;
            }
        }
        let resp: InitProducerIdResponse = api_call(
            &mut conn,
            InitProducerIdRequest::API_KEY,
            version,
            correlation + i32::try_from(attempt).unwrap_or(0),
            &body,
        )
        .await?;
        last = ErrorCode(resp.error_code);
        if last.is_ok() {
            return Ok((conn, resp));
        }
        // Coordinator-settling codes are a wait, not an answer: the
        // transaction log is being created and its partitions are
        // electing leaders. A suite that stopped here would report
        // every broker as broken on its first ever transaction.
        if !retriable(last)
            && !txn_coordinator_settling(last)
            && last != ErrorCode::CONCURRENT_TRANSACTIONS
        {
            break;
        }
    }
    Err(CheckError::Violation(format!(
        "InitProducerId for {transactional_id} never succeeded: {last}"
    )))
}

/// Codes that mean the transaction log is still coming up, rather than
/// an answer about this request.
///
/// NOT_COORDINATOR normally means a client asked the wrong broker,
/// which is a real answer worth reporting — which is why the group
/// path's [`is_coordinator_settling`] excludes it. On the first
/// transaction against a fresh cluster it means something else: the
/// transaction log has only just been created, the partition this id
/// hashes to has no leader yet, and the broker that is about to
/// coordinate it does not know that yet either.
fn txn_coordinator_settling(code: ErrorCode) -> bool {
    is_coordinator_settling(code) || code == ErrorCode::NOT_COORDINATOR
}

/// Announce one partition to the transaction, returning the per-partition
/// error code the coordinator answered with.
async fn add_partitions_to_txn(
    conn: &mut RawConnection,
    version: i16,
    actor: TxnActor<'_>,
    topic: &str,
    partition: i32,
    correlation: i32,
) -> Result<ErrorCode, CheckError> {
    let mut topic_entry = AddPartitionsToTxnTopic::default();
    topic_entry.name = topic.to_owned();
    topic_entry.partitions = vec![partition];
    let mut request = AddPartitionsToTxnRequest::default();
    request.v3_and_below_transactional_id = actor.transactional_id.to_owned();
    request.v3_and_below_producer_id = actor.producer_id;
    request.v3_and_below_producer_epoch = actor.producer_epoch;
    request.v3_and_below_topics = vec![topic_entry];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding AddPartitionsToTxn: {e}")))?;
    let resp: AddPartitionsToTxnResponse = api_call(
        conn,
        AddPartitionsToTxnRequest::API_KEY,
        version,
        correlation,
        &body,
    )
    .await?;
    // At v3 and below the per-partition results are the whole answer;
    // the top-level error code does not exist until v4.
    Ok(resp
        .results_by_topic_v3_and_below
        .first()
        .and_then(|t| t.results_by_partition.first())
        .map_or(ErrorCode::NONE, |p| ErrorCode(p.partition_error_code)))
}

/// Finish the transaction, returning the coordinator's answer.
async fn end_txn(
    conn: &mut RawConnection,
    version: i16,
    actor: TxnActor<'_>,
    committed: bool,
    correlation: i32,
) -> Result<ErrorCode, CheckError> {
    let mut request = EndTxnRequest::default();
    request.transactional_id = actor.transactional_id.to_owned();
    request.producer_id = actor.producer_id;
    request.producer_epoch = actor.producer_epoch;
    request.committed = committed;
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding EndTxn: {e}")))?;
    let resp: EndTxnResponse =
        api_call(conn, EndTxnRequest::API_KEY, version, correlation, &body).await?;
    Ok(ErrorCode(resp.error_code))
}

/// Produce one transactional batch, returning the partition's error code
/// and base offset.
/// What a produce stamps its batch with, and whether it belongs to a
/// transaction.
///
/// The triple is what the broker dedupes on; `transactional_id` is what
/// turns an idempotent write into a transactional one, on the batch's
/// attributes and on the request alike.
#[derive(Debug, Clone, Copy)]
struct ProduceStamp<'a> {
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    transactional_id: Option<&'a str>,
}

impl<'a> ProduceStamp<'a> {
    fn transactional(actor: TxnActor<'a>, base_sequence: i32) -> ProduceStamp<'a> {
        ProduceStamp {
            producer_id: actor.producer_id,
            producer_epoch: actor.producer_epoch,
            base_sequence,
            transactional_id: Some(actor.transactional_id),
        }
    }

    /// An ordinary write: no producer identity, no transaction. The
    /// sentinels are what a producer that has never called
    /// InitProducerId sends.
    fn plain() -> ProduceStamp<'static> {
        ProduceStamp {
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            transactional_id: None,
        }
    }

    fn idempotent(identity: &InitProducerIdResponse, base_sequence: i32) -> ProduceStamp<'static> {
        ProduceStamp {
            producer_id: identity.producer_id,
            producer_epoch: identity.producer_epoch,
            base_sequence,
            transactional_id: None,
        }
    }
}

async fn produce_stamped(
    conn: &mut RawConnection,
    version: i16,
    stamp: ProduceStamp<'_>,
    topic: &str,
    partition: i32,
    correlation: i32,
) -> Result<(ErrorCode, i64), CheckError> {
    let mut batch = probe_batch();
    if stamp.transactional_id.is_some() {
        batch.attributes |= TRANSACTIONAL_ATTR;
    }
    batch.producer_id = stamp.producer_id;
    batch.producer_epoch = stamp.producer_epoch;
    batch.base_sequence = stamp.base_sequence;
    let mut set = BytesMut::new();
    batch
        .encode(&mut set)
        .map_err(|e| CheckError::Infra(format!("encoding transactional batch: {e}")))?;

    let mut partition_data = PartitionProduceData::default();
    partition_data.index = partition;
    partition_data.records = Some(set.freeze());
    let mut topic_data = TopicProduceData::default();
    topic_data.name = topic.to_owned();
    topic_data.partition_data = vec![partition_data];
    let mut request = ProduceRequest::default();
    request.transactional_id = stamp.transactional_id.map(str::to_owned);
    // Idempotent and transactional produce both require the full ISR;
    // anything less and the broker refuses on grounds that have nothing
    // to do with the check.
    request.acks = -1;
    request.timeout_ms = 10_000;
    request.topic_data = vec![topic_data];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding Produce: {e}")))?;
    let resp: ProduceResponse =
        api_call(conn, ProduceRequest::API_KEY, version, correlation, &body).await?;
    let entry = resp
        .responses
        .first()
        .and_then(|t| t.partition_responses.first())
        .ok_or_else(|| CheckError::Violation("Produce response names no partitions".into()))?;
    Ok((ErrorCode(entry.error_code), entry.base_offset))
}

/// One partition's fetch response at the given isolation level.
/// Which partition to read, from where, and at what isolation.
#[derive(Clone, Copy)]
struct FetchAt<'a> {
    topic: &'a str,
    partition: i32,
    offset: i64,
    isolation_level: i8,
}

/// One fetch of one partition, refusing to pass off an error as data.
///
/// A partition response that carries an error code carries no offsets
/// with it: `high_watermark` and `last_stable_offset` come back as -1,
/// and a caller that reads those as numbers concludes something false
/// about the log. The stable-offset check did exactly that on a
/// cluster — it compared -1 to -1, found the stable offset had "caught
/// up to" the high watermark, and reported a broker for showing
/// uncommitted records when what had really happened was that this
/// broker had not answered about the partition at all.
async fn fetch_partition(
    conn: &mut RawConnection,
    version: i16,
    at: FetchAt<'_>,
    correlation: i32,
) -> Result<odradek_protocol::messages::fetch_response::PartitionData, CheckError> {
    let mut fetch_partition = FetchPartition::default();
    fetch_partition.partition = at.partition;
    fetch_partition.current_leader_epoch = -1;
    fetch_partition.fetch_offset = at.offset;
    fetch_partition.last_fetched_epoch = -1;
    fetch_partition.log_start_offset = -1;
    fetch_partition.partition_max_bytes = 1 << 20;
    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = at.topic.to_owned();
    fetch_topic.partitions = vec![fetch_partition];
    let mut request = FetchRequest::default();
    request.max_wait_ms = 500;
    request.min_bytes = 0;
    request.max_bytes = 1 << 22;
    request.isolation_level = at.isolation_level;
    request.session_id = 0;
    request.session_epoch = -1;
    request.topics = vec![fetch_topic];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding Fetch: {e}")))?;
    let resp: FetchResponse =
        api_call(conn, FetchRequest::API_KEY, version, correlation, &body).await?;
    let data = resp
        .responses
        .first()
        .and_then(|t| t.partitions.first())
        .cloned()
        .ok_or_else(|| CheckError::Violation("Fetch response names no partitions".into()))?;
    let code = ErrorCode(data.error_code);
    if !code.is_ok() {
        // The offset and isolation are in the message because for this
        // family of codes they *are* the explanation — OFFSET_OUT_OF_RANGE
        // means nothing without saying which offset was asked for, and
        // under read_committed it usually means the stable offset has
        // not reached it rather than that the log has not.
        return Err(CheckError::Violation(format!(
            "Fetch for {}[{}] at offset {} (isolation {}) answered {code}",
            at.topic, at.partition, at.offset, at.isolation_level
        )));
    }
    Ok(data)
}

/// The same fetch, waiting out the codes that mean "not from me, not
/// yet".
///
/// On a cluster the broker leading a partition can change, and a broker
/// that has just learned of a topic has not necessarily caught up on
/// it. Both answer with a retriable code, which is a client's cue to
/// look again rather than to conclude anything.
async fn fetch_partition_settled(
    ctx: &ServerCtx,
    conn: &mut RawConnection,
    version: i16,
    at: FetchAt<'_>,
    correlation: i32,
) -> Result<odradek_protocol::messages::fetch_response::PartitionData, CheckError> {
    let mut last = None;
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let correlation = correlation + i32::try_from(attempt).unwrap_or(0);
        match fetch_partition(conn, version, at, correlation).await {
            Ok(data) => return Ok(data),
            // Under read_committed, OFFSET_OUT_OF_RANGE is the stable
            // offset saying "not this far yet" — the same wait as an
            // unmoved stable offset, seen from the other side, and the
            // reason `still_resolving` exists. It is not a wait under
            // read_uncommitted, where the log really does end there.
            Err(CheckError::Violation(details))
                if mentions_retriable(&details)
                    || (at.isolation_level == READ_COMMITTED && still_resolving(&details)) =>
            {
                last = Some(CheckError::Violation(details));
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| CheckError::Infra("no fetch attempts were made".into())))
}

/// Whether a fetch refusal reported by [`fetch_partition`] names one of
/// `codes`.
///
/// Matched on the rendered code because that is what `fetch_partition`
/// reports; the alternative is a second error type threaded through
/// every caller to carry one `i16`.
fn fetch_refusal_names(details: &str, codes: &[ErrorCode]) -> bool {
    codes
        .iter()
        .any(|code| details.contains(&format!("answered {code}")))
}

/// Refusals that mean "not from me, not yet" rather than an answer.
const FETCH_SETTLING: &[ErrorCode] = &[
    ErrorCode::NOT_LEADER_OR_FOLLOWER,
    ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
    ErrorCode::LEADER_NOT_AVAILABLE,
    ErrorCode::UNKNOWN_TOPIC_ID,
    ErrorCode::REPLICA_NOT_AVAILABLE,
];

fn mentions_retriable(details: &str) -> bool {
    fetch_refusal_names(details, FETCH_SETTLING)
}

/// The same, for a loop waiting on a transaction marker.
///
/// A `read_committed` fetch at an offset the stable offset has not
/// reached yet is refused with OFFSET_OUT_OF_RANGE: the records are
/// written, but they are not decided, so as far as a committed reader is
/// concerned the log does not go that far. That is the same wait as a
/// stable offset that has not moved, seen from the other side, and it is
/// the answer for as long as the marker is still being written.
fn still_resolving(details: &str) -> bool {
    mentions_retriable(details) || fetch_refusal_names(details, &[ErrorCode::OFFSET_OUT_OF_RANGE])
}

/// Taking over a transactional id must hand out a *newer* epoch.
///
/// The epoch is the only thing that distinguishes the producer holding
/// the id now from the one that held it before. If a second
/// InitProducerId returns the same epoch, a half-dead predecessor's
/// writes are indistinguishable from its successor's and can be
/// interleaved into a transaction the successor commits — the failure
/// transactions exist to prevent.
async fn txn_init_bumps_the_epoch(ctx: &ServerCtx) -> Verdict {
    let versions = match txn_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let id = unique_transactional_id("epoch");

    let (_, first) = match init_producer_id(ctx, versions.init, &id, 400).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId (first)").into_verdict(),
    };
    let (_, second) = match init_producer_id(ctx, versions.init, &id, 420).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId (second)").into_verdict(),
    };

    if second.producer_id != first.producer_id {
        // Not automatically wrong — a coordinator may issue a new id —
        // but then the old one must be fenced, which the epoch check
        // below cannot speak to. Report rather than guess.
        return Verdict::Fail {
            details: format!(
                "re-initializing {id} changed the producer id ({} to {}); the epoch is what \
                 should have moved",
                first.producer_id, second.producer_id
            ),
        };
    }
    if second.producer_epoch <= first.producer_epoch {
        return Verdict::Fail {
            details: format!(
                "re-initializing {id} returned epoch {} again (was {}), so the previous \
                 producer is not fenced",
                second.producer_epoch, first.producer_epoch
            ),
        };
    }
    Verdict::Pass
}

/// The fenced producer must actually be refused.
///
/// A higher epoch handed to the successor is only half of fencing; the
/// other half is that the predecessor's requests stop working. A
/// coordinator that bumps the epoch and then keeps honouring the old one
/// has described fencing without doing it.
async fn txn_stale_epoch_is_fenced(ctx: &ServerCtx) -> Verdict {
    let versions = match txn_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "txnfence", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let id = unique_transactional_id("fence");

    let (_, first) = match init_producer_id(ctx, versions.init, &id, 440).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId (first)").into_verdict(),
    };
    let (mut conn, second) = match init_producer_id(ctx, versions.init, &id, 460).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId (second)").into_verdict(),
    };
    if second.producer_epoch <= first.producer_epoch {
        return Verdict::Skipped {
            reason: "the epoch never advanced, so there is no stale epoch to refuse \
                     (txn/init-bumps-the-epoch reports that)"
                .into(),
        };
    }

    match add_partitions_to_txn(
        &mut conn,
        versions.add,
        TxnActor::new(&id, &second).at_epoch(first.producer_epoch),
        &produced.topic,
        0,
        480,
    )
    .await
    {
        Ok(code) if is_fenced(code) => Verdict::Pass,
        Ok(code) if code.is_ok() => Verdict::Fail {
            details: format!(
                "AddPartitionsToTxn at the superseded epoch {} was accepted; the producer \
                 holding {id} is epoch {}",
                first.producer_epoch, second.producer_epoch
            ),
        },
        Ok(code) => Verdict::Fail {
            details: format!(
                "AddPartitionsToTxn at the superseded epoch {} answered {code}, which does \
                 not tell the caller it has been fenced",
                first.producer_epoch
            ),
        },
        Err(e) => e.context("AddPartitionsToTxn").into_verdict(),
    }
}

/// A transactional write must never land outside its transaction.
///
/// The coordinator finishes a transaction by writing a marker into every
/// partition it knows the transaction touched. A record written to a
/// partition it does not know about would be covered by no marker at
/// all: visible to committed readers even when the transaction is
/// thrown away, or blocking the partition's stable offset forever.
///
/// There are two safe answers and the check accepts either, because
/// brokers really do differ: refuse the write, or bring the partition
/// into the transaction on the producer's behalf. Apache Kafka 4.1 does
/// the latter — the partition leader verifies membership with the
/// coordinator and adds what is missing, so an unannounced write is
/// simply part of the transaction and an abort disowns it like any
/// other. What the check rules out is the third answer, where the
/// record is written and belongs to nothing.
async fn txn_unannounced_write_stays_in_the_transaction(ctx: &ServerCtx) -> Verdict {
    let versions = match txn_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "txnunannounced", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut leader = produced.conn;
    let id = unique_transactional_id("unannounced");

    let (mut txn, identity) = match init_producer_id(ctx, versions.init, &id, 500).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId").into_verdict(),
    };

    // No AddPartitionsToTxn: straight to the write.
    let actor = TxnActor::new(&id, &identity);
    let base_offset = match produce_stamped(
        &mut leader,
        versions.produce,
        ProduceStamp::transactional(actor, 0),
        &topic,
        0,
        520,
    )
    .await
    {
        // Refusing is the other safe answer: nothing was written, so
        // nothing can be orphaned.
        Ok((code, _)) if !code.is_ok() => return Verdict::Pass,
        Ok((_, base_offset)) => base_offset,
        Err(e) => return e.context("Produce").into_verdict(),
    };

    // It was accepted. Then the transaction had better own it — which
    // an abort is the sharpest way to ask: if these records survive
    // being thrown away, they were never in the transaction.
    match end_txn(&mut txn, versions.end, actor, false, 530).await {
        Ok(code) if code.is_ok() => {}
        Ok(code) => {
            return Verdict::Fail {
                details: format!(
                    "the unannounced write was accepted at offset {base_offset}, then aborting \
                     the transaction answered {code}; those records are covered by nothing"
                ),
            };
        }
        Err(e) => return e.context("EndTxn").into_verdict(),
    }

    match await_abort_marker(
        ctx,
        &mut leader,
        versions.fetch,
        AbortedRun {
            topic: &topic,
            first_offset: base_offset,
            producer_id: identity.producer_id,
        },
        "the unannounced write was accepted, but",
        540,
    )
    .await
    {
        Ok(()) => Verdict::Pass,
        Err(verdict) => verdict,
    }
}

/// An open transaction must withhold its records from committed readers.
///
/// The last stable offset is the promise: a `read_committed` fetch stops
/// there, because nothing past it is decided yet. A broker that lets the
/// stable offset run up to the high watermark while a transaction is
/// open shows uncommitted records to every consumer that asked not to
/// see them — and the records may still be aborted.
async fn txn_open_transaction_holds_the_stable_offset(ctx: &ServerCtx) -> Verdict {
    let versions = match txn_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "txnlso", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut leader = produced.conn;
    let id = unique_transactional_id("lso");

    let (mut txn, identity) = match init_producer_id(ctx, versions.init, &id, 540).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId").into_verdict(),
    };
    let actor = TxnActor::new(&id, &identity);
    let conns = TxnConns {
        txn: &mut txn,
        leader: &mut leader,
    };
    if let Err(verdict) = open_transaction(ctx, conns, &versions, actor, &topic, 0, 560).await {
        return verdict;
    }

    let at = FetchAt {
        topic: &topic,
        partition: 0,
        offset: 0,
        isolation_level: READ_COMMITTED,
    };
    let data = match fetch_partition_settled(ctx, &mut leader, versions.fetch, at, 580).await {
        Ok(d) => d,
        Err(e) => return e.context("Fetch (read_committed)").into_verdict(),
    };
    // Whatever else happens, the transaction stays open only as long as
    // this check needs it; an abandoned one would hold the partition
    // back from every later check on the same broker.
    let cleanup = end_txn(&mut txn, versions.end, actor, false, 599).await;

    if data.high_watermark <= data.last_stable_offset {
        return Verdict::Fail {
            details: format!(
                "with a transaction open, the stable offset ({}) has caught up to the high \
                 watermark ({}); uncommitted records are being offered to read_committed \
                 consumers",
                data.last_stable_offset, data.high_watermark
            ),
        };
    }
    if let Err(e) = cleanup {
        return e.context("EndTxn (cleanup)").into_verdict();
    }
    Verdict::Pass
}

/// An aborted transaction must be reported as aborted.
///
/// The records stay in the log — aborting does not unwrite them — so a
/// `read_committed` fetch returns them alongside a list of the
/// transactions that disowned them, and the reading client drops what
/// the list names. A broker that omits the list hands every client data
/// somebody threw away, and the client cannot tell.
async fn txn_abort_is_reported_to_readers(ctx: &ServerCtx) -> Verdict {
    let versions = match txn_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "txnabort", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut leader = produced.conn;
    let id = unique_transactional_id("abort");

    let (mut txn, identity) = match init_producer_id(ctx, versions.init, &id, 600).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId").into_verdict(),
    };
    let actor = TxnActor::new(&id, &identity);
    let conns = TxnConns {
        txn: &mut txn,
        leader: &mut leader,
    };
    let first_offset = match open_transaction(ctx, conns, &versions, actor, &topic, 0, 620).await {
        Ok(offset) => offset,
        Err(verdict) => return verdict,
    };
    match end_txn(&mut txn, versions.end, actor, false, 640).await {
        Ok(code) if code.is_ok() => {}
        Ok(code) => {
            return Verdict::Fail {
                details: format!("aborting the transaction answered {code}"),
            };
        }
        Err(e) => return e.context("EndTxn").into_verdict(),
    }

    match await_abort_marker(
        ctx,
        &mut leader,
        versions.fetch,
        AbortedRun {
            topic: &topic,
            first_offset,
            producer_id: identity.producer_id,
        },
        "the abort was acknowledged, but",
        660,
    )
    .await
    {
        Ok(()) => Verdict::Pass,
        Err(verdict) => verdict,
    }
}

/// Wait for an abort marker to land, then confirm a `read_committed`
/// fetch over the aborted records names the transaction that disowned
/// them.
///
/// Two claims, and they are the same claim from two sides. The stable
/// offset must move past the records, or the partition stays blocked on
/// a transaction that has already finished. And the aborted list must
/// name the producer, or a reading client has no way to tell those
/// records from data — the broker still returns them, because aborting
/// does not unwrite anything.
///
/// The marker is written after EndTxn answers, so both happen a moment
/// later; reading immediately would find nothing and prove nothing.
async fn await_abort_marker(
    ctx: &ServerCtx,
    conn: &mut RawConnection,
    fetch_version: i16,
    run: AbortedRun<'_>,
    context: &str,
    correlation: i32,
) -> Result<(), Verdict> {
    let AbortedRun {
        topic,
        first_offset,
        producer_id,
    } = run;
    let mut data = None;
    // The recovery budget, not the settle one. Ending a transaction is
    // acknowledged by the coordinator, but the marker that unblocks the
    // partition is written afterwards and, on a replicated transaction
    // log, has to reach enough replicas first. How long that takes is
    // the cluster's business rather than the suite's — the same kind of
    // wait as an election, and the same reason for a budget sized by
    // what a working cluster needs rather than by what a single idle
    // broker manages.
    for attempt in 0..ctx.config.recovery_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let at = FetchAt {
            topic,
            partition: 0,
            offset: first_offset,
            isolation_level: READ_COMMITTED,
        };
        let got = match fetch_partition(
            conn,
            fetch_version,
            at,
            correlation + i32::try_from(attempt).unwrap_or(0),
        )
        .await
        {
            Ok(d) => d,
            // Still settling: this attempt says nothing, and there are
            // more of them.
            Err(CheckError::Violation(details)) if still_resolving(&details) => continue,
            Err(e) => return Err(e.context("Fetch (read_committed)").into_verdict()),
        };
        let settled = got.last_stable_offset > first_offset;
        data = Some(got);
        if settled {
            break;
        }
    }
    // No attempt ever got an answer: every one was refused for a reason
    // that meant "not yet". That is the failure this check is about —
    // the marker never landed — and saying so beats reporting the last
    // refusal as if the fetch itself were the problem.
    let data = data.ok_or_else(|| Verdict::Fail {
        details: format!(
            "{context} nothing was readable at {first_offset} within {:?}: every \
             read_committed fetch was refused, so the transaction was never resolved",
            ctx.config.recovery_budget
        ),
    })?;

    if data.last_stable_offset <= first_offset {
        return Err(Verdict::Fail {
            details: format!(
                "{context} the stable offset never moved past {first_offset} (still {}), so the \
                 partition stays blocked on a transaction that has already finished",
                data.last_stable_offset
            ),
        });
    }
    let aborted = data.aborted_transactions.unwrap_or_default();
    if !aborted.iter().any(|entry| entry.producer_id == producer_id) {
        return Err(Verdict::Fail {
            details: format!(
                "{context} a read_committed fetch over the aborted records named {} aborted \
                 transaction(s), none of them producer {}; a client reading this partition has \
                 no way to know those records were thrown away",
                aborted.len(),
                producer_id
            ),
        });
    }
    Ok(())
}

/// One aborted transaction's footprint on one partition.
#[derive(Debug, Clone, Copy)]
struct AbortedRun<'a> {
    topic: &'a str,
    first_offset: i64,
    producer_id: i64,
}

/// Announce a partition and write one transactional batch to it,
/// returning the base offset the transaction starts at.
/// The two brokers a transaction is carried out against.
///
/// The coordinator owns the transaction — it grants the id, records
/// which partitions are in, and writes the markers that end it — while
/// the records themselves go to whichever broker leads the partition.
/// A single-broker cluster collapses the pair onto one machine, which is
/// how a suite can go a long way sending both down the same connection
/// without noticing.
struct TxnConns<'a> {
    txn: &'a mut RawConnection,
    leader: &'a mut RawConnection,
}

async fn open_transaction(
    ctx: &ServerCtx,
    conns: TxnConns<'_>,
    versions: &TxnVersions,
    actor: TxnActor<'_>,
    topic: &str,
    partition: i32,
    correlation: i32,
) -> Result<i64, Verdict> {
    let TxnConns { txn, leader } = conns;
    match add_partitions_to_txn(txn, versions.add, actor, topic, partition, correlation).await {
        Ok(code) if code.is_ok() => {}
        Ok(code) => {
            return Err(Verdict::Fail {
                details: format!("AddPartitionsToTxn answered {code}"),
            });
        }
        Err(e) => return Err(e.context("AddPartitionsToTxn").into_verdict()),
    }
    // A transaction coordinator that has just accepted the partition may
    // not have told the partition leader yet; the leader answers
    // CONCURRENT_TRANSACTIONS until it has.
    let mut last = ErrorCode(0);
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        match produce_stamped(
            leader,
            versions.produce,
            ProduceStamp::transactional(actor, 0),
            topic,
            partition,
            correlation + 10 + i32::try_from(attempt).unwrap_or(0),
        )
        .await
        {
            Ok((code, base_offset)) if code.is_ok() => return Ok(base_offset),
            Ok((code, _)) => {
                last = code;
                if !retriable(code) && code != ErrorCode::CONCURRENT_TRANSACTIONS {
                    break;
                }
            }
            Err(e) => return Err(e.context("Produce (transactional)").into_verdict()),
        }
    }
    Err(Verdict::Fail {
        details: format!("a transactional produce to an announced partition failed with {last}"),
    })
}

/// True when this code tells a producer it has been superseded.
fn is_fenced(code: ErrorCode) -> bool {
    code == ErrorCode::PRODUCER_FENCED || code == ErrorCode::INVALID_PRODUCER_EPOCH
}

// ---- idempotent produce ---------------------------------------------
//
// A producer that retries a batch whose acknowledgement was lost has no
// way to tell "the broker never got it" from "the broker got it and the
// reply went missing". Idempotence is the broker's half of that deal:
// it remembers the last sequences per (producer id, partition) and
// recognizes a repeat. Both checks below are guarantees the client's
// producer is built on, and both fail silently when they are not kept —
// a duplicated batch and a dropped one look like ordinary data.

/// Take a producer id with no transactional id attached.
///
/// The plain idempotent case: no coordinator to find, no fencing, just
/// an id scoped to this session that the broker will dedupe against.
async fn init_idempotent_producer_id(
    ctx: &ServerCtx,
    conn: &mut RawConnection,
    version: i16,
    correlation: i32,
) -> Result<InitProducerIdResponse, CheckError> {
    let mut request = InitProducerIdRequest::default();
    request.transactional_id = None;
    request.transaction_timeout_ms = -1;
    request.producer_id = -1;
    request.producer_epoch = -1;
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding InitProducerId: {e}")))?;

    let mut last = ErrorCode(0);
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let resp: InitProducerIdResponse = api_call(
            conn,
            InitProducerIdRequest::API_KEY,
            version,
            correlation + i32::try_from(attempt).unwrap_or(0),
            &body,
        )
        .await?;
        last = ErrorCode(resp.error_code);
        if last.is_ok() {
            return Ok(resp);
        }
        // A broker still loading its transaction log says so even for
        // an id it will not coordinate.
        if !retriable(last) && !txn_coordinator_settling(last) {
            break;
        }
    }
    Err(CheckError::Violation(format!(
        "InitProducerId never succeeded: {last}"
    )))
}

/// The number of records in a partition, from the offsets at both ends.
async fn log_span(
    ctx: &ServerCtx,
    conn: &mut RawConnection,
    topic: &str,
    correlation: i32,
) -> Result<i64, CheckError> {
    let version = negotiate(
        "ListOffsets",
        ctx.range(ListOffsetsRequest::API_KEY)
            .map_err(|_| CheckError::Infra("ListOffsets is not advertised".into()))?,
        ListOffsetsRequest::MIN_VERSION,
        ListOffsetsRequest::MAX_VERSION,
    )
    .map_err(|_| CheckError::Infra("no common ListOffsets version".into()))?;
    let earliest = list_offset_of(conn, version, topic, EARLIEST_TIMESTAMP, correlation).await?;
    let latest = list_offset_of(conn, version, topic, LATEST_TIMESTAMP, correlation + 1).await?;
    Ok(latest - earliest)
}

/// One ListOffsets lookup for partition 0 of `topic`.
async fn list_offset_of(
    conn: &mut RawConnection,
    version: i16,
    topic: &str,
    timestamp: i64,
    correlation: i32,
) -> Result<i64, CheckError> {
    let mut request_partition = ListOffsetsPartition::default();
    request_partition.partition_index = 0;
    request_partition.current_leader_epoch = -1;
    request_partition.timestamp = timestamp;
    let mut request_topic = ListOffsetsTopic::default();
    request_topic.name = topic.to_owned();
    request_topic.partitions = vec![request_partition];
    let mut request = ListOffsetsRequest::default();
    request.replica_id = -1;
    request.isolation_level = 0;
    request.topics = vec![request_topic];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding ListOffsets: {e}")))?;
    let resp: ListOffsetsResponse = api_call(
        conn,
        ListOffsetsRequest::API_KEY,
        version,
        correlation,
        &body,
    )
    .await?;
    let entry = resp
        .topics
        .first()
        .and_then(|t| t.partitions.first())
        .ok_or_else(|| CheckError::Violation("ListOffsets names no partitions".into()))?;
    let code = ErrorCode(entry.error_code);
    if !code.is_ok() {
        return Err(CheckError::Violation(format!(
            "ListOffsets answered {code}"
        )));
    }
    Ok(entry.offset)
}

/// Versions for a stamped produce and the offsets lookups around it.
fn idempotence_versions(ctx: &ServerCtx) -> Result<(i16, i16), Verdict> {
    Ok((
        negotiate(
            "InitProducerId",
            ctx.range(InitProducerIdRequest::API_KEY)?,
            InitProducerIdRequest::MIN_VERSION,
            TXN_INIT_MAX,
        )?,
        negotiate(
            "Produce",
            ctx.range(ProduceRequest::API_KEY)?,
            ProduceRequest::MIN_VERSION,
            PRODUCE_NAME_MAX,
        )?,
    ))
}

/// A batch sent twice under the same stamp must be stored once.
///
/// This is the entire point of idempotent produce. The producer cannot
/// tell a lost request from a lost acknowledgement, so it retries; if
/// the broker appends the retry as a new batch the log quietly holds
/// the records twice, and nothing downstream can tell which duplicates
/// were meant. Answering the retry with the original offset and
/// answering it with DUPLICATE_SEQUENCE_NUMBER are both fine — what
/// matters is the count in the log.
async fn produce_idempotent_retry_is_deduped(ctx: &ServerCtx) -> Verdict {
    let (init_version, produce_version) = match idempotence_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "dedupe", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut conn = produced.conn;

    let identity = match init_idempotent_producer_id(ctx, &mut conn, init_version, 700).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId").into_verdict(),
    };
    let before = match log_span(ctx, &mut conn, &topic, 710).await {
        Ok(span) => span,
        Err(e) => return e.context("ListOffsets (before)").into_verdict(),
    };

    let stamp = ProduceStamp::idempotent(&identity, 0);
    let first = match produce_stamped(&mut conn, produce_version, stamp, &topic, 0, 720).await {
        Ok((code, offset)) if code.is_ok() => offset,
        Ok((code, _)) => {
            return Verdict::Fail {
                details: format!("the first stamped produce answered {code}"),
            };
        }
        Err(e) => return e.context("Produce (first)").into_verdict(),
    };
    // Byte for byte the same batch, the way a producer retrying a lost
    // acknowledgement sends it.
    let retry = match produce_stamped(&mut conn, produce_version, stamp, &topic, 0, 730).await {
        Ok(outcome) => outcome,
        Err(e) => return e.context("Produce (retry)").into_verdict(),
    };
    let after = match log_span(ctx, &mut conn, &topic, 740).await {
        Ok(span) => span,
        Err(e) => return e.context("ListOffsets (after)").into_verdict(),
    };

    let records = batch_record_count(&probe_batch());
    let appended = after - before;
    if appended != records {
        return Verdict::Fail {
            details: format!(
                "a batch of {records} record(s) sent twice under producer {} sequence 0 grew the \
                 log by {appended}; the retry was appended rather than recognized (first at \
                 offset {first}, retry answered {}, offset {})",
                identity.producer_id, retry.0, retry.1
            ),
        };
    }
    Verdict::Pass
}

/// A sequence that skips ahead must be refused.
///
/// The broker's dedupe window is the last few sequences per producer and
/// partition, so a gap is not something it can paper over: it cannot
/// tell "the batch you skipped never existed" from "the batch you
/// skipped is still in flight and will arrive out of order". Accepting
/// the gap silently abandons the ordering the producer was promised, so
/// OUT_OF_ORDER_SEQUENCE_NUMBER is the answer, and it is what tells a
/// client its stamp has drifted.
async fn produce_sequence_gap_is_refused(ctx: &ServerCtx) -> Verdict {
    let (init_version, produce_version) = match idempotence_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "seqgap", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut conn = produced.conn;

    let identity = match init_idempotent_producer_id(ctx, &mut conn, init_version, 750).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId").into_verdict(),
    };
    // Establish the sequence: this batch is 0..count.
    match produce_stamped(
        &mut conn,
        produce_version,
        ProduceStamp::idempotent(&identity, 0),
        &topic,
        0,
        760,
    )
    .await
    {
        Ok((code, _)) if code.is_ok() => {}
        Ok((code, _)) => {
            return Verdict::Fail {
                details: format!("the first stamped produce answered {code}"),
            };
        }
        Err(e) => return e.context("Produce (first)").into_verdict(),
    }

    // Skip a long way past what comes next, so no dedupe window can
    // plausibly contain it.
    let gap = i32::try_from(batch_record_count(&probe_batch())).unwrap_or(2) + 50;
    match produce_stamped(
        &mut conn,
        produce_version,
        ProduceStamp::idempotent(&identity, gap),
        &topic,
        0,
        770,
    )
    .await
    {
        Ok((code, _)) if code == ErrorCode::OUT_OF_ORDER_SEQUENCE_NUMBER => Verdict::Pass,
        Ok((code, offset)) if code.is_ok() => Verdict::Fail {
            details: format!(
                "a batch stamped with sequence {gap} was accepted at offset {offset} after a \
                 batch ending well below it; the gap was neither filled nor refused"
            ),
        },
        Ok((code, _)) => Verdict::Fail {
            details: format!(
                "a batch stamped with sequence {gap} was refused with {code} rather than \
                 OUT_OF_ORDER_SEQUENCE_NUMBER, which is the code that tells a producer its \
                 stamp has drifted"
            ),
        },
        Err(e) => e.context("Produce (gap)").into_verdict(),
    }
}

/// DeleteTopics versions this suite speaks: v6+ moved the request from
/// a name list to a `topics` array that can address by id.
const DELETE_TOPICS_BY_STATE: i16 = 6;

/// Seeking by time must land on the first record at or after the
/// timestamp, and report *no offset* for a time after the last one.
///
/// A consumer that seeks by time has no other way to find its place, so
/// both halves matter and both have a plausible wrong answer. Returning
/// the log end for a future timestamp reads as "start here", which
/// looks exactly like being caught up; returning the log *start* reads
/// as "read everything", which silently reprocesses the topic.
async fn list_offsets_by_timestamp(ctx: &ServerCtx) -> Verdict {
    let version = match negotiate(
        "ListOffsets",
        match ctx.range(ListOffsetsRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        // v1 is the first with the timestamp/offset response shape.
        1,
        ListOffsetsRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "bytime", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut conn = produced.conn;

    let batch = probe_batch();
    let first = batch.base_timestamp;
    let last = batch.max_timestamp;
    let records = batch_record_count(&batch);

    // The batch's own first timestamp: the first record, offset 0.
    match list_offset_of(&mut conn, version, &topic, first, 780).await {
        Ok(0) => {}
        Ok(other) => {
            return Verdict::Fail {
                details: format!(
                    "the batch's first timestamp ({first}) answered offset {other}, not 0"
                ),
            };
        }
        Err(e) => return e.context("ListOffsets (first timestamp)").into_verdict(),
    }

    // One past the last record's timestamp is after every record here,
    // so there is no offset to name.
    match list_offset_of(&mut conn, version, &topic, last + 1, 790).await {
        Ok(UNSET_OFFSET) => {}
        Ok(other) if other == records => {
            return Verdict::Fail {
                details: format!(
                    "a timestamp after every record answered the log end ({other}) instead of \
                     {UNSET_OFFSET}; a consumer seeking forward in time reads that as being \
                     caught up"
                ),
            };
        }
        Ok(other) => {
            return Verdict::Fail {
                details: format!(
                    "a timestamp after every record answered offset {other} instead of \
                     {UNSET_OFFSET}"
                ),
            };
        }
        Err(e) => return e.context("ListOffsets (future timestamp)").into_verdict(),
    }
    Verdict::Pass
}

/// A deleted topic must actually be gone.
///
/// The plausible wrong answer is the one `create-topics/validate-only`
/// guards from the other side: reporting success without doing the
/// work. A caller has no way to see the difference except by asking
/// again, which is what this does — the topic must stop being named in
/// Metadata as one that exists.
async fn delete_topics_removes_the_topic(ctx: &ServerCtx) -> Verdict {
    let delete_version = match negotiate(
        "DeleteTopics",
        match ctx.range(DeleteTopicsRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        DeleteTopicsRequest::MIN_VERSION,
        DeleteTopicsRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let metadata_version = match negotiate(
        "Metadata",
        match ctx.range(MetadataRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        1,
        MetadataRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "delete", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut conn = produced.conn;
    // Deleting a topic is the controller's business, not the partition
    // leader's, and on a cluster those are rarely the same broker.
    // Redpanda answers NOT_CONTROLLER to anyone else; Kafka forwards.
    // Both are within their rights — the protocol names the controller
    // in every Metadata response precisely so a client can go there —
    // so the suite goes there.
    let mut admin = match connect_to_controller(ctx, &mut conn, metadata_version, 861).await {
        Ok(c) => c,
        Err(e) => return e.context("locating the controller").into_verdict(),
    };

    let mut request = DeleteTopicsRequest::default();
    if delete_version >= DELETE_TOPICS_BY_STATE {
        let mut state = DeleteTopicState::default();
        state.name = Some(topic.clone());
        request.topics = vec![state];
    } else {
        request.topic_names = vec![topic.clone()];
    }
    request.timeout_ms = 30_000;
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, delete_version) {
        return Verdict::Error {
            details: format!("encoding DeleteTopics: {e}"),
        };
    }
    let resp: DeleteTopicsResponse = match api_call(
        &mut admin,
        DeleteTopicsRequest::API_KEY,
        delete_version,
        800,
        &body,
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => return e.context("DeleteTopics").into_verdict(),
    };
    let code = resp
        .responses
        .first()
        .map_or(ErrorCode::NONE, |r| ErrorCode(r.error_code));
    if code == ErrorCode::TOPIC_DELETION_DISABLED {
        return Verdict::Skipped {
            reason: "the subject has topic deletion disabled".into(),
        };
    }
    if !code.is_ok() {
        return Verdict::Fail {
            details: format!("deleting {topic} answered {code}"),
        };
    }

    // Deletion is asynchronous on a real broker, so the answer is "gone
    // shortly", not "gone by the time this returns".
    let mut last = ErrorCode::NONE;
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let resp = match metadata_of_topic(
            &mut conn,
            metadata_version,
            &topic,
            810 + i32::try_from(attempt).unwrap_or(0),
        )
        .await
        {
            Ok(resp) => resp,
            Err(e) => return e.context("Metadata").into_verdict(),
        };
        last = resp
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic.as_str()))
            .map_or(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, |t| {
                ErrorCode(t.error_code)
            });
        if last == ErrorCode::UNKNOWN_TOPIC_OR_PARTITION || last == ErrorCode::UNKNOWN_TOPIC_ID {
            return Verdict::Pass;
        }
    }
    Verdict::Fail {
        details: format!(
            "{topic} was reported deleted, but Metadata still answers {last} for it after \
             {:?}; the deletion was acknowledged without being done",
            ctx.config.settle_budget
        ),
    }
}

/// A Metadata request naming one topic, with auto-creation refused.
///
/// The flag matters here as much as it does in
/// `metadata/unknown-topic`: a broker configured to auto-create would
/// answer a question about a deleted topic by making it again, and the
/// check would report a pass for the wrong reason.
async fn metadata_of_topic(
    conn: &mut RawConnection,
    version: i16,
    topic: &str,
    correlation: i32,
) -> Result<MetadataResponse, CheckError> {
    let mut requested = MetadataRequestTopic::default();
    requested.name = Some(topic.to_owned());
    let mut request = MetadataRequest::default();
    request.topics = Some(vec![requested]);
    request.allow_auto_topic_creation = false;
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding Metadata: {e}")))?;
    api_call(conn, MetadataRequest::API_KEY, version, correlation, &body).await
}

/// The address of the broker leading partition 0 of `topic`, or `None`
/// while leadership is still settling.
///
/// Read out of the cluster's own Metadata, which is the only place a
/// client could learn it either.
async fn leader_endpoint(
    conn: &mut RawConnection,
    version: i16,
    topic: &str,
    correlation: i32,
) -> Result<Option<String>, CheckError> {
    let resp = metadata_of_topic(conn, version, topic, correlation).await?;
    let Some(entry) = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
    else {
        return Ok(None);
    };
    if !ErrorCode(entry.error_code).is_ok() {
        return Ok(None);
    }
    let Some(partition) = entry.partitions.iter().find(|p| p.partition_index == 0) else {
        return Ok(None);
    };
    if !ErrorCode(partition.error_code).is_ok() {
        return Ok(None);
    }
    Ok(resp
        .brokers
        .iter()
        .find(|b| b.node_id == partition.leader_id)
        .filter(|b| !b.host.is_empty() && (1..=65535).contains(&b.port))
        .map(|b| format!("{}:{}", b.host, b.port)))
}

/// The address of the broker this cluster's Metadata names as its
/// controller, or `None` when it names none this response knows about.
async fn controller_endpoint(
    conn: &mut RawConnection,
    version: i16,
    correlation: i32,
) -> Result<Option<String>, CheckError> {
    let mut request = MetadataRequest::default();
    request.topics = Some(Vec::new());
    request.allow_auto_topic_creation = false;
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding Metadata: {e}")))?;
    let resp: MetadataResponse =
        api_call(conn, MetadataRequest::API_KEY, version, correlation, &body).await?;
    Ok(resp
        .brokers
        .iter()
        .find(|b| b.node_id == resp.controller_id)
        .filter(|b| !b.host.is_empty() && (1..=65535).contains(&b.port))
        .map(|b| format!("{}:{}", b.host, b.port)))
}

/// A connection to the cluster's controller.
///
/// Topic administration belongs to the controller: it owns the cluster
/// metadata that creating and deleting topics changes. Other brokers
/// may forward the request or may answer `NOT_CONTROLLER` and expect
/// the client to consult Metadata, which names the controller in every
/// response — Kafka does the first, Redpanda the second, and both are
/// within their rights. A suite that only ever talked to one broker
/// could not tell them apart, because on a cluster of one the broker
/// you have is the controller.
///
/// Falls back to the bootstrap address when Metadata names no
/// controller, so the request the caller cares about reports what the
/// cluster says rather than this helper inventing a verdict.
async fn connect_to_controller(
    ctx: &ServerCtx,
    bootstrap: &mut RawConnection,
    version: i16,
    correlation: i32,
) -> Result<RawConnection, CheckError> {
    let located = controller_endpoint(bootstrap, version, correlation).await?;
    connect(located.as_deref().unwrap_or(&ctx.addr)).await
}

/// The same, negotiating Metadata for itself — for the callers that
/// only want somewhere to send an administrative request and have no
/// other use for the version.
///
/// A subject that does not advertise Metadata gets a connection to the
/// bootstrap: there is then no way to find the controller, and the
/// request will report whatever it reports.
async fn admin_conn(
    ctx: &ServerCtx,
    bootstrap: &mut RawConnection,
    correlation: i32,
) -> Result<RawConnection, CheckError> {
    let Ok(range) = ctx.range(MetadataRequest::API_KEY) else {
        return connect(&ctx.addr).await;
    };
    let Ok(version) = negotiate("Metadata", range, 1, MetadataRequest::MAX_VERSION) else {
        return connect(&ctx.addr).await;
    };
    connect_to_controller(ctx, bootstrap, version, correlation).await
}

/// A connection to the broker leading partition 0 of `topic`, waiting
/// out the window in which a freshly created topic has no leader yet.
///
/// Writes are answered only by the leader; a follower says
/// `NOT_LEADER_OR_FOLLOWER` and expects the client to consult Metadata
/// and go elsewhere. Retrying against the same broker — which is what
/// treating that code as merely retriable amounts to — can only work on
/// a cluster of one, where the leader is the only broker there is.
///
/// Returns the bootstrap connection's own address when Metadata names no
/// usable leader, so that a cluster in that state is reported by the
/// check rather than by this helper.
async fn connect_to_leader(
    ctx: &ServerCtx,
    bootstrap: &mut RawConnection,
    version: i16,
    topic: &str,
    correlation_base: i32,
) -> Result<RawConnection, CheckError> {
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let correlation = correlation_base + i32::try_from(attempt).unwrap_or(0);
        if let Some(addr) = leader_endpoint(bootstrap, version, topic, correlation).await? {
            return connect(&addr).await;
        }
    }
    connect(&ctx.addr).await
}

/// Commit metadata must come back exactly as it was given.
///
/// The string is the client's, and the broker has no business in it:
/// consumers put processing state, a schema version, a shard id in
/// there. Same property the group assignment check guards — opaque
/// bytes reach their owner unexamined — but on the durable path, where
/// a broker that drops or truncates it loses something no retry will
/// bring back, and the offset beside it comes back fine so nothing
/// looks wrong.
async fn offsets_metadata_round_trips(ctx: &ServerCtx) -> Verdict {
    let (commit_version, fetch_version) = match offsets_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "commitmeta", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if let Some(skip) = skip_without_topic_id(&produced, commit_version) {
        return skip;
    }
    let topic = produced.topic.clone();
    let topic_id = produced.topic_id;
    let group = check_group("commitmeta");
    // The produce leg is over; what follows is all coordinator traffic.
    let mut conn = match coordinator_conn(ctx, &group).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    if let Err(e) = await_topic_known(ctx, &mut conn, &topic, 815).await {
        return e.into_verdict();
    }

    // Shaped to catch the ways a string gets mangled in transit:
    // non-ASCII, an embedded quote, and enough length to notice a
    // truncation.
    let metadata = "odradek:\u{e9}\"state\"/shard-7";

    let mut partition = OffsetCommitRequestPartition::default();
    partition.partition_index = 0;
    partition.committed_offset = 11;
    partition.committed_leader_epoch = -1;
    partition.committed_metadata = Some(metadata.to_owned());
    let mut req_topic = OffsetCommitRequestTopic::default();
    if commit_version >= OFFSETS_BY_TOPIC_ID {
        req_topic.topic_id = topic_id;
    } else {
        req_topic.name = topic.clone();
    }
    req_topic.partitions = vec![partition];
    let mut request = OffsetCommitRequest::default();
    request.group_id = group.clone();
    request.generation_id_or_member_epoch = -1;
    request.member_id = String::new();
    request.retention_time_ms = -1;
    request.topics = vec![req_topic];
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, commit_version) {
        return Verdict::Error {
            details: format!("encoding OffsetCommit: {e}"),
        };
    }
    let resp: OffsetCommitResponse = match api_call(
        &mut conn,
        OffsetCommitRequest::API_KEY,
        commit_version,
        820,
        &body,
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => return e.context("OffsetCommit").into_verdict(),
    };
    let code = resp
        .topics
        .first()
        .and_then(|t| t.partitions.first())
        .map_or(ErrorCode::NONE, |p| ErrorCode(p.error_code));
    if !code.is_ok() {
        return Verdict::Fail {
            details: format!("committing an offset with metadata answered {code}"),
        };
    }

    match fetch_committed_metadata(&mut conn, fetch_version, &group, &topic, topic_id, 830).await {
        Ok(Some(got)) if got == metadata => Verdict::Pass,
        Ok(Some(got)) => Verdict::Fail {
            details: format!(
                "committed metadata {metadata:?} came back as {got:?}; the string is the \
                 client's and nothing in the round trip should touch it"
            ),
        },
        Ok(None) => Verdict::Fail {
            details: format!(
                "committed metadata {metadata:?} came back absent, while the offset beside \
                 it came back fine — so nothing about the read looks wrong"
            ),
        },
        Err(e) => e.context("OffsetFetch").into_verdict(),
    }
}

/// The committed metadata for partition 0, as OffsetFetch reports it.
async fn fetch_committed_metadata(
    conn: &mut RawConnection,
    version: i16,
    group: &str,
    topic: &str,
    topic_id: [u8; 16],
    correlation: i32,
) -> Result<Option<String>, CheckError> {
    let mut request = OffsetFetchRequest::default();
    if version >= OFFSET_FETCH_BATCHED {
        let mut req_topic = OffsetFetchRequestTopics::default();
        if version >= OFFSETS_BY_TOPIC_ID {
            req_topic.topic_id = topic_id;
        } else {
            req_topic.name = topic.to_owned();
        }
        req_topic.partition_indexes = vec![0];
        let mut req_group = OffsetFetchRequestGroup::default();
        req_group.group_id = group.to_owned();
        req_group.topics = Some(vec![req_topic]);
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
        correlation,
        &body,
    )
    .await?;
    if version >= OFFSET_FETCH_BATCHED {
        Ok(resp
            .groups
            .first()
            .and_then(|g| g.topics.first())
            .and_then(|t| t.partitions.first())
            .and_then(|p| p.metadata.clone()))
    } else {
        Ok(resp
            .topics
            .first()
            .and_then(|t| t.partitions.first())
            .and_then(|p| p.metadata.clone()))
    }
}

/// A replication factor the cluster cannot satisfy must be refused.
///
/// The wrong answer here is not an error, it is a *success*: creating
/// the topic with however many replicas are available. The caller asked
/// for a durability level and would be told it got one, and the
/// difference only ever surfaces as data loss on a broker failure that
/// the topic was supposed to survive.
async fn create_topics_impossible_replication(ctx: &ServerCtx) -> Verdict {
    let version = match negotiate(
        "CreateTopics",
        match ctx.range(CreateTopicsRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
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
    // More replicas than any test cluster has brokers, and more than
    // the protocol's own i16 could describe a plausible cluster of.
    let asked = 32_767i16;
    let topic = unique_topic("overreplicated");
    let mut creatable = CreatableTopic::default();
    creatable.name = topic.clone();
    creatable.num_partitions = 1;
    creatable.replication_factor = asked;
    let mut request = CreateTopicsRequest::default();
    request.topics = vec![creatable];
    request.timeout_ms = 30_000;
    request.validate_only = false;
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, version) {
        return Verdict::Error {
            details: format!("encoding CreateTopics: {e}"),
        };
    }
    let resp: CreateTopicsResponse =
        match api_call(&mut conn, CreateTopicsRequest::API_KEY, version, 840, &body).await {
            Ok(resp) => resp,
            Err(e) => return e.context("CreateTopics").into_verdict(),
        };
    let code = resp
        .topics
        .first()
        .map_or(ErrorCode::NONE, |t| ErrorCode(t.error_code));
    if code == ErrorCode::INVALID_REPLICATION_FACTOR {
        return Verdict::Pass;
    }
    if !code.is_ok() {
        // Some other refusal is still a refusal; the point is that the
        // topic was not quietly created with fewer replicas.
        return Verdict::Pass;
    }
    Verdict::Fail {
        details: format!(
            "asked for {asked} replicas of {topic} and was told it was created; a caller \
             that asked for a durability level it cannot have should be refused, not given \
             a weaker one under the same name"
        ),
    }
}

/// A member that has left must stop being a member.
///
/// Leaving is how a consumer hands its partitions back without waiting
/// out the session timeout. A coordinator that keeps honouring the
/// departed member's heartbeats believes it still owns them, so they
/// are not reassigned and the partitions go unread — quietly, because
/// every request involved succeeds.
async fn groups_leave_unregisters_the_member(ctx: &ServerCtx) -> Verdict {
    let (join_version, sync_version) = match group_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let leave_version = match negotiate(
        "LeaveGroup",
        match ctx.range(LeaveGroupRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        LeaveGroupRequest::MIN_VERSION,
        LeaveGroupRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let heartbeat_version = match negotiate(
        "Heartbeat",
        match ctx.range(HeartbeatRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        HeartbeatRequest::MIN_VERSION,
        HeartbeatRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let group = check_group("leaving");
    let mut conn = match coordinator_conn(ctx, &group).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let (member_id, generation) =
        match join_as_leader(ctx, &mut conn, join_version, &group, 850).await {
            Ok(v) => v,
            Err(e) => return e.into_verdict(),
        };

    // Sync first, so the group is settled rather than mid-rebalance:
    // a heartbeat into an unsettled group is answered
    // REBALANCE_IN_PROGRESS by some brokers and NONE by others, and
    // either way it says nothing about membership. Without this the
    // check could only pass vacuously.
    if let Err(verdict) =
        sync_as_leader(&mut conn, sync_version, &group, &member_id, generation, 855).await
    {
        return verdict;
    }

    // Now the precondition means something: the member is live before
    // it leaves, so a later refusal is the leave taking effect rather
    // than the join never having.
    match heartbeat_code(
        &mut conn,
        heartbeat_version,
        &group,
        &member_id,
        generation,
        860,
    )
    .await
    {
        Ok(code) if code.is_ok() => {}
        Ok(code) => {
            return Verdict::Fail {
                details: format!(
                    "a heartbeat from a synced member answered {code} before it left, so \
                     nothing after the leave would prove anything"
                ),
            };
        }
        Err(e) => return e.context("Heartbeat (before leaving)").into_verdict(),
    }

    let mut identity = MemberIdentity::default();
    identity.member_id = member_id.clone();
    let mut request = LeaveGroupRequest::default();
    request.group_id = group.clone();
    request.member_id = member_id.clone();
    request.members = vec![identity];
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, leave_version) {
        return Verdict::Error {
            details: format!("encoding LeaveGroup: {e}"),
        };
    }
    let resp: LeaveGroupResponse = match api_call(
        &mut conn,
        LeaveGroupRequest::API_KEY,
        leave_version,
        870,
        &body,
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => return e.context("LeaveGroup").into_verdict(),
    };
    let code = ErrorCode(resp.error_code);
    if !code.is_ok() {
        return Verdict::Fail {
            details: format!("leaving the group answered {code}"),
        };
    }

    match heartbeat_code(
        &mut conn,
        heartbeat_version,
        &group,
        &member_id,
        generation,
        880,
    )
    .await
    {
        Ok(code) if code == ErrorCode::UNKNOWN_MEMBER_ID => Verdict::Pass,
        Ok(code) if code.is_ok() => Verdict::Fail {
            details: format!(
                "a heartbeat from {member_id} was still accepted after it left; the \
                 coordinator believes a departed member owns its partitions, so nothing \
                 reassigns them"
            ),
        },
        Ok(code) => Verdict::Fail {
            details: format!(
                "a heartbeat after leaving answered {code} rather than UNKNOWN_MEMBER_ID"
            ),
        },
        Err(e) => e.context("Heartbeat (after leaving)").into_verdict(),
    }
}

/// One Heartbeat, reduced to the code it answered with.
async fn heartbeat_code(
    conn: &mut RawConnection,
    version: i16,
    group: &str,
    member_id: &str,
    generation: i32,
    correlation: i32,
) -> Result<ErrorCode, CheckError> {
    let mut request = HeartbeatRequest::default();
    request.group_id = group.to_owned();
    request.generation_id = generation;
    request.member_id = member_id.to_owned();
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding Heartbeat: {e}")))?;
    let resp: HeartbeatResponse =
        api_call(conn, HeartbeatRequest::API_KEY, version, correlation, &body).await?;
    Ok(ErrorCode(resp.error_code))
}

/// SyncGroup as the group's only member, supplying itself an
/// assignment, to settle the group out of its post-join rebalance.
async fn sync_as_leader(
    conn: &mut RawConnection,
    version: i16,
    group: &str,
    member_id: &str,
    generation: i32,
    correlation: i32,
) -> Result<(), Verdict> {
    let mut assignment = SyncGroupRequestAssignment::default();
    assignment.member_id = member_id.to_owned();
    assignment.assignment = Bytes::from_static(&[0x00]);
    let mut request = SyncGroupRequest::default();
    request.group_id = group.to_owned();
    request.generation_id = generation;
    request.member_id = member_id.to_owned();
    request.protocol_type = Some(GROUP_PROTOCOL_TYPE.to_owned());
    request.protocol_name = Some("range".to_owned());
    request.assignments = vec![assignment];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| Verdict::Error {
            details: format!("encoding SyncGroup: {e}"),
        })?;
    let resp: SyncGroupResponse =
        match api_call(conn, SyncGroupRequest::API_KEY, version, correlation, &body).await {
            Ok(resp) => resp,
            Err(e) => return Err(e.context("SyncGroup").into_verdict()),
        };
    let code = ErrorCode(resp.error_code);
    if code.is_ok() {
        Ok(())
    } else {
        Err(Verdict::Fail {
            details: format!("SyncGroup as the group's only member answered {code}"),
        })
    }
}

/// A fetch must block when there is nothing, and return at once when
/// there is.
///
/// Both halves are the same contract and both fail quietly. A broker
/// that answers an empty long poll immediately turns every caught-up
/// consumer into a busy loop — correct data, burned CPU and network, no
/// error anywhere. A broker that sits on a fetch it could already
/// satisfy adds its whole wait to the latency of every record.
async fn fetch_long_poll_contract(ctx: &ServerCtx) -> Verdict {
    let version = match negotiate(
        "Fetch",
        match ctx.range(FetchRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        FetchRequest::MIN_VERSION,
        FETCH_NAME_MAX,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "longpoll", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut conn = produced.conn;
    let end = batch_record_count(&probe_batch());

    // Nothing at the log end, and a byte is asked for: the broker has
    // to wait for one.
    // Long enough that a broker which does not wait at all is
    // unmistakable — those answer in microseconds — and short enough
    // that the fault matrix, which runs the whole catalogue once per
    // fault, does not pay a second for it every time.
    let wait_ms = 500;
    let started = std::time::Instant::now();
    if let Err(e) = fetch_with_wait(&mut conn, version, &topic, end, 1, wait_ms, 890).await {
        return e.context("Fetch (empty, long poll)").into_verdict();
    }
    let waited = started.elapsed();
    // Half the window, so a broker that rounds or wakes early still
    // passes while one that does not wait at all cannot.
    let floor = Duration::from_millis(u64::try_from(wait_ms).unwrap_or(0) / 2);
    if waited < floor {
        return Verdict::Fail {
            details: format!(
                "a fetch at the log end asking for 1 byte with max_wait_ms={wait_ms} came \
                 back in {waited:?}; a caught-up consumer polling this broker spins instead \
                 of waiting"
            ),
        };
    }

    // Data already there, the same long wait: it must not be spent.
    let started = std::time::Instant::now();
    if let Err(e) = fetch_with_wait(&mut conn, version, &topic, 0, 1, wait_ms, 900).await {
        return e.context("Fetch (satisfiable, long poll)").into_verdict();
    }
    let waited = started.elapsed();
    if waited >= floor {
        return Verdict::Fail {
            details: format!(
                "a fetch that could be answered from existing records still took {waited:?} \
                 with max_wait_ms={wait_ms}; the wait is for data that is not there yet, not \
                 for data that is"
            ),
        };
    }
    Verdict::Pass
}

/// One fetch with an explicit `min_bytes` and `max_wait_ms`.
async fn fetch_with_wait(
    conn: &mut RawConnection,
    version: i16,
    topic: &str,
    offset: i64,
    min_bytes: i32,
    max_wait_ms: i32,
    correlation: i32,
) -> Result<(), CheckError> {
    let mut fetch_partition = FetchPartition::default();
    fetch_partition.partition = 0;
    fetch_partition.current_leader_epoch = -1;
    fetch_partition.fetch_offset = offset;
    fetch_partition.last_fetched_epoch = -1;
    fetch_partition.log_start_offset = -1;
    fetch_partition.partition_max_bytes = 1 << 20;
    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic = topic.to_owned();
    fetch_topic.partitions = vec![fetch_partition];
    let mut request = FetchRequest::default();
    request.max_wait_ms = max_wait_ms;
    request.min_bytes = min_bytes;
    request.max_bytes = 1 << 22;
    request.isolation_level = 0;
    request.session_id = 0;
    request.session_epoch = -1;
    request.topics = vec![fetch_topic];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding Fetch: {e}")))?;
    let _: FetchResponse =
        api_call(conn, FetchRequest::API_KEY, version, correlation, &body).await?;
    Ok(())
}

/// Every partition leader must be a broker the same response names.
///
/// Metadata is the only thing a client has to route by, and a leader id
/// it cannot resolve leaves it with nowhere to send and nothing to say
/// about why. The failure is quiet in a particular way: the response
/// carries no error, so a client sees a healthy topic it simply cannot
/// write to.
async fn metadata_leader_is_a_known_broker(ctx: &ServerCtx) -> Verdict {
    let version = match negotiate(
        "Metadata",
        match ctx.range(MetadataRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        1,
        MetadataRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "leaderref", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut conn = produced.conn;

    let resp = match metadata_of_topic(&mut conn, version, &topic, 910).await {
        Ok(resp) => resp,
        Err(e) => return e.context("Metadata").into_verdict(),
    };
    let known: Vec<i32> = resp.brokers.iter().map(|b| b.node_id).collect();
    if known.is_empty() {
        // Nothing to be consistent with. A response that names no
        // brokers at all is a different and larger failure, and
        // `metadata/basic` is the check that reports it.
        return Verdict::Skipped {
            reason: "the response names no brokers at all (metadata/basic reports that)".into(),
        };
    }
    let Some(entry) = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic.as_str()))
    else {
        return Verdict::Fail {
            details: format!("Metadata does not name {topic}, which was just produced to"),
        };
    };
    let code = ErrorCode(entry.error_code);
    if !code.is_ok() {
        return Verdict::Fail {
            details: format!("Metadata answered {code} for a topic that exists"),
        };
    }
    if entry.partitions.is_empty() {
        return Verdict::Fail {
            details: format!("{topic} exists but Metadata gives it no partitions"),
        };
    }
    for partition in &entry.partitions {
        let code = ErrorCode(partition.error_code);
        if !code.is_ok() {
            continue;
        }
        if !known.contains(&partition.leader_id) {
            return Verdict::Fail {
                details: format!(
                    "{topic}[{}] names leader {} and the response's brokers are {known:?}; a \
                     client is given a healthy-looking topic it has nowhere to send to",
                    partition.partition_index, partition.leader_id
                ),
            };
        }
    }
    Verdict::Pass
}

/// A described group must name the members it has.
///
/// This is the operator's and the admin client's only view of who holds
/// what. A group reported with no members reads as idle — the state a
/// tool uses to decide a group is safe to delete, or that a consumer
/// has died and its lag is nobody's.
async fn describe_groups_reports_members(ctx: &ServerCtx) -> Verdict {
    let (join_version, sync_version) = match group_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let describe_version = match negotiate(
        "DescribeGroups",
        match ctx.range(DescribeGroupsRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        DescribeGroupsRequest::MIN_VERSION,
        DescribeGroupsRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let group = check_group("described");
    let mut conn = match coordinator_conn(ctx, &group).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let (member_id, generation) =
        match join_as_leader(ctx, &mut conn, join_version, &group, 920).await {
            Ok(v) => v,
            Err(e) => return e.into_verdict(),
        };
    if let Err(verdict) =
        sync_as_leader(&mut conn, sync_version, &group, &member_id, generation, 930).await
    {
        return verdict;
    }

    let mut request = DescribeGroupsRequest::default();
    request.groups = vec![group.clone()];
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, describe_version) {
        return Verdict::Error {
            details: format!("encoding DescribeGroups: {e}"),
        };
    }
    let resp: DescribeGroupsResponse = match api_call(
        &mut conn,
        DescribeGroupsRequest::API_KEY,
        describe_version,
        940,
        &body,
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => return e.context("DescribeGroups").into_verdict(),
    };
    let Some(described) = resp.groups.iter().find(|g| g.group_id == group) else {
        return Verdict::Fail {
            details: format!("DescribeGroups was asked about {group} and answered about neither"),
        };
    };
    let code = ErrorCode(described.error_code);
    if !code.is_ok() {
        return Verdict::Fail {
            details: format!("describing a live group answered {code}"),
        };
    }
    if !described.members.iter().any(|m| m.member_id == member_id) {
        return Verdict::Fail {
            details: format!(
                "{group} has a synced member {member_id}, and DescribeGroups reports {} \
                 member(s) (state {:?}); a group that reads as empty reads as safe to \
                 delete",
                described.members.len(),
                described.group_state
            ),
        };
    }
    Verdict::Pass
}

/// The compression codec bits of a record batch's attributes.
const GZIP_ATTR: i16 = 1;

/// A compressed batch must come back exactly as it was sent.
///
/// The record set is the producer's bytes, and a broker storing a topic
/// at the default `compression.type=producer` has no business in them.
/// Recompressing — even to the same codec — rewrites the batch and
/// breaks every consumer that verified the crc it was given, which
/// includes anything proxying or mirroring the log. It is the same
/// promise the uncompressed case makes in `fetch/batch-integrity`, on
/// the path where a broker is most tempted to intervene, and the
/// give-away is not an error: the records decode fine, they are simply
/// not the bytes anybody wrote.
async fn produce_compressed_passthrough(ctx: &ServerCtx) -> Verdict {
    let create_version = match negotiate(
        "CreateTopics",
        match ctx.range(CreateTopicsRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        CreateTopicsRequest::MIN_VERSION,
        CreateTopicsRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produce_version = match negotiate(
        "Produce",
        match ctx.range(ProduceRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        ProduceRequest::MIN_VERSION,
        PRODUCE_NAME_MAX,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let fetch_version = match negotiate(
        "Fetch",
        match ctx.range(FetchRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        FetchRequest::MIN_VERSION,
        FETCH_NAME_MAX,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };

    // This check builds its own batch rather than going through
    // `produce_flow`, so it has to find the leader for itself too.
    let metadata_version = match negotiate(
        "Metadata",
        match ctx.range(MetadataRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        1,
        MetadataRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };

    let mut bootstrap = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let topic = unique_topic("gzip");
    match create_topic_call(ctx, &mut bootstrap, create_version, &topic, false, 950).await {
        Ok((code, _)) if code.is_ok() => {}
        Ok((code, _)) => {
            return Verdict::Fail {
                details: format!("creating {topic} answered {code}"),
            };
        }
        Err(e) => return e.context("CreateTopics").into_verdict(),
    }
    let mut conn = match connect_to_leader(ctx, &mut bootstrap, metadata_version, &topic, 951).await
    {
        Ok(c) => c,
        Err(e) => return e.context("locating the partition leader").into_verdict(),
    };

    let sent = match gzip_batch() {
        Ok(bytes) => bytes,
        Err(details) => return Verdict::Error { details },
    };

    // The produce rides out post-create leadership settling the way
    // produce_flow does.
    let mut last = ErrorCode::NONE;
    let mut produced = false;
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let mut partition_data = PartitionProduceData::default();
        partition_data.index = 0;
        partition_data.records = Some(sent.clone());
        let mut topic_data = TopicProduceData::default();
        topic_data.name = topic.clone();
        topic_data.partition_data = vec![partition_data];
        let mut request = ProduceRequest::default();
        request.acks = -1;
        request.timeout_ms = 10_000;
        request.topic_data = vec![topic_data];
        let mut body = BytesMut::new();
        if let Err(e) = request.encode(&mut body, produce_version) {
            return Verdict::Error {
                details: format!("encoding Produce: {e}"),
            };
        }
        let resp: ProduceResponse = match api_call(
            &mut conn,
            ProduceRequest::API_KEY,
            produce_version,
            960 + i32::try_from(attempt).unwrap_or(0),
            &body,
        )
        .await
        {
            Ok(resp) => resp,
            Err(e) => return e.context("Produce").into_verdict(),
        };
        last = resp
            .responses
            .first()
            .and_then(|t| t.partition_responses.first())
            .map_or(ErrorCode::NONE, |p| ErrorCode(p.error_code));
        if last.is_ok() {
            produced = true;
            break;
        }
        if last == ErrorCode::UNSUPPORTED_COMPRESSION_TYPE {
            return Verdict::Skipped {
                reason: "the subject does not accept gzip-compressed batches".into(),
            };
        }
        if !retriable(last) {
            return Verdict::Fail {
                details: format!("producing a gzip batch answered {last}"),
            };
        }
        // As in `produce_flow`: a retriable code here means this broker
        // is the wrong one to ask, so ask Metadata again rather than
        // asking the same broker again.
        conn = match connect_to_leader(ctx, &mut bootstrap, metadata_version, &topic, 952).await {
            Ok(c) => c,
            Err(e) => return e.context("relocating the partition leader").into_verdict(),
        };
    }
    if !produced {
        return Verdict::Fail {
            details: format!("a gzip batch never became producible: still {last}"),
        };
    }

    let at = FetchAt {
        topic: &topic,
        partition: 0,
        offset: 0,
        isolation_level: READ_UNCOMMITTED,
    };
    let data = match fetch_partition_settled(ctx, &mut conn, fetch_version, at, 970).await {
        Ok(data) => data,
        Err(e) => return e.context("Fetch").into_verdict(),
    };
    let got = data.records.unwrap_or_default();
    // Everything from the attributes on is inside the crc; only
    // base_offset and partition_leader_epoch, which sit outside it, may
    // legitimately be rewritten.
    const OUTSIDE_CRC: usize = 12 + 4 + 4;
    if got.len() < OUTSIDE_CRC || sent.len() < OUTSIDE_CRC {
        return Verdict::Fail {
            details: format!(
                "produced {} byte(s) of gzip batch and got {} back",
                sent.len(),
                got.len()
            ),
        };
    }
    if got[OUTSIDE_CRC..] != sent[OUTSIDE_CRC..] {
        return Verdict::Fail {
            details: format!(
                "a gzip batch came back rewritten: {} byte(s) produced, {} returned, and the \
                 crc-covered bytes differ — the records still decode, they are simply not \
                 the ones anybody wrote",
                sent.len(),
                got.len()
            ),
        };
    }
    Verdict::Pass
}

/// [`probe_batch`]'s records, gzipped, as a batch declaring the codec.
///
/// Built rather than pasted so it stays honest if the probe batch
/// changes; gzip because it is the one codec every Kafka-protocol
/// implementation has had since the beginning.
fn gzip_batch() -> Result<Bytes, String> {
    use std::io::Write as _;

    let batch = probe_batch();
    let Records::Plain(records) = &batch.records else {
        return Err("the probe batch is not plain records".into());
    };
    let mut plain = BytesMut::new();
    for record in records {
        record
            .encode(&mut plain)
            .map_err(|e| format!("encoding a record: {e}"))?;
    }
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(&plain)
        .map_err(|e| format!("gzipping the record set: {e}"))?;
    let payload = encoder
        .finish()
        .map_err(|e| format!("finishing the gzip stream: {e}"))?;

    let mut compressed = batch.clone();
    compressed.attributes |= GZIP_ATTR;
    compressed.records = Records::Compressed {
        count: i32::try_from(records.len()).unwrap_or(0),
        payload: Bytes::from(payload),
    };
    let mut out = BytesMut::new();
    compressed
        .encode(&mut out)
        .map_err(|e| format!("encoding the compressed batch: {e}"))?;
    Ok(out.freeze())
}

/// A committed transaction must become readable, and not read as
/// aborted.
///
/// The counterpart to `txn/abort-is-reported-to-readers`, and the half
/// a producer is actually waiting on. Two ways to get it wrong and both
/// are quiet: a stable offset that never moves past the records leaves
/// a `read_committed` consumer blocked on a transaction that finished,
/// and naming a committed producer in the aborted list has every client
/// throw the records away on purpose.
async fn txn_commit_is_visible_to_readers(ctx: &ServerCtx) -> Verdict {
    let versions = match txn_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "txncommit", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut leader = produced.conn;
    let id = unique_transactional_id("commit");

    let (mut txn, identity) = match init_producer_id(ctx, versions.init, &id, 980).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId").into_verdict(),
    };
    let actor = TxnActor::new(&id, &identity);
    let conns = TxnConns {
        txn: &mut txn,
        leader: &mut leader,
    };
    let first_offset = match open_transaction(ctx, conns, &versions, actor, &topic, 0, 1_000).await
    {
        Ok(offset) => offset,
        Err(verdict) => return verdict,
    };
    match end_txn(&mut txn, versions.end, actor, true, 1_020).await {
        Ok(code) if code.is_ok() => {}
        Ok(code) => {
            return Verdict::Fail {
                details: format!("committing the transaction answered {code}"),
            };
        }
        Err(e) => return e.context("EndTxn").into_verdict(),
    }

    // The marker lands after EndTxn answers, so the stable offset moves
    // a moment later; reading immediately would prove nothing. On the
    // recovery budget, as in `await_abort_marker` and for the same
    // reason: the wait is on replication, not on the suite.
    let mut data = None;
    for attempt in 0..ctx.config.recovery_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        let at = FetchAt {
            topic: &topic,
            partition: 0,
            offset: first_offset,
            isolation_level: READ_COMMITTED,
        };
        let got = match fetch_partition(
            &mut leader,
            versions.fetch,
            at,
            1_040 + i32::try_from(attempt).unwrap_or(0),
        )
        .await
        {
            Ok(got) => got,
            Err(CheckError::Violation(details)) if still_resolving(&details) => continue,
            Err(e) => return e.context("Fetch (read_committed)").into_verdict(),
        };
        let settled = got.last_stable_offset > first_offset;
        data = Some(got);
        if settled {
            break;
        }
    }
    let data = match data {
        Some(data) => data,
        None => {
            return Verdict::Fail {
                details: format!(
                    "the commit was acknowledged but nothing was readable at {first_offset} \
                     within {:?}: every read_committed fetch was refused, so the commit \
                     marker never landed",
                    ctx.config.recovery_budget
                ),
            };
        }
    };
    if data.last_stable_offset <= first_offset {
        return Verdict::Fail {
            details: format!(
                "the commit was acknowledged but the stable offset never moved past \
                 {first_offset} (still {}); a read_committed consumer stays blocked on a \
                 transaction that finished",
                data.last_stable_offset
            ),
        };
    }
    let aborted = data.aborted_transactions.unwrap_or_default();
    if aborted
        .iter()
        .any(|entry| entry.producer_id == identity.producer_id)
    {
        return Verdict::Fail {
            details: format!(
                "producer {} committed and is still named in the aborted list; every client \
                 reading this partition throws those records away on purpose",
                identity.producer_id
            ),
        };
    }
    if data.records.as_ref().is_none_or(bytes::Bytes::is_empty) {
        return Verdict::Fail {
            details: format!(
                "the transaction committed and the stable offset moved past {first_offset}, \
                 but a read_committed fetch from there returns nothing"
            ),
        };
    }
    Verdict::Pass
}

/// Offsets committed inside a transaction must be held back with it.
///
/// This is exactly-once consume-transform-produce from the broker's
/// side: the offsets go into the same transaction as the output, so
/// they become visible when it commits and never if it aborts. A broker
/// that publishes them immediately has the input marked processed while
/// the output may still be thrown away, which is the duplicate-work
/// window transactions exist to close — and it closes silently, because
/// every request succeeds.
async fn txn_offsets_wait_for_the_commit(ctx: &ServerCtx) -> Verdict {
    let versions = match txn_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let offsets_version = match negotiate(
        "AddOffsetsToTxn",
        match ctx.range(AddOffsetsToTxnRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        AddOffsetsToTxnRequest::MIN_VERSION,
        ADD_OFFSETS_CLIENT_MAX,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let commit_version = match negotiate(
        "TxnOffsetCommit",
        match ctx.range(TxnOffsetCommitRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        TxnOffsetCommitRequest::MIN_VERSION,
        TXN_OFFSET_COMMIT_CLIENT_MAX,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let (_, fetch_offsets_version) = match offsets_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "txnoffsets", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let topic_id = produced.topic_id;
    let id = unique_transactional_id("txnoffsets");
    let group = check_group("txnoffsets");
    // This check is the one that needs all three brokers at once: the
    // transaction coordinator to open and end it, the group coordinator
    // to hold the offsets, and — via `produced` — the partition leader
    // the records went to. On one broker they are one connection, which
    // is why sending all of it down `produced.conn` worked for as long
    // as it did.
    let mut offsets = match coordinator_conn(ctx, &group).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    if let Err(e) = await_topic_known(ctx, &mut offsets, &topic, 1_055).await {
        return e.into_verdict();
    }

    let (mut conn, identity) = match init_producer_id(ctx, versions.init, &id, 1_060).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId").into_verdict(),
    };
    let actor = TxnActor::new(&id, &identity);
    // Tell the coordinator the group's offsets belong to this
    // transaction, then commit one inside it.
    match add_offsets_to_txn(&mut conn, offsets_version, actor, &group, 1_080).await {
        Ok(code) if code.is_ok() => {}
        Ok(code) => {
            return Verdict::Fail {
                details: format!("AddOffsetsToTxn answered {code}"),
            };
        }
        Err(e) => return e.context("AddOffsetsToTxn").into_verdict(),
    }
    let committed = 31;
    match txn_offset_commit(
        &mut offsets,
        commit_version,
        actor,
        &group,
        &topic,
        committed,
        1_100,
    )
    .await
    {
        Ok(code) if code.is_ok() => {}
        Ok(code) => {
            return Verdict::Fail {
                details: format!("TxnOffsetCommit answered {code}"),
            };
        }
        Err(e) => return e.context("TxnOffsetCommit").into_verdict(),
    }

    // Still open: the offset must not be readable yet.
    match fetch_committed(
        &mut offsets,
        fetch_offsets_version,
        &group,
        &topic,
        topic_id,
        1_120,
    )
    .await
    {
        Ok(got) if got == committed => {
            // Leave nothing open behind a failure.
            let _ = end_txn(&mut conn, versions.end, actor, false, 1_139).await;
            return Verdict::Fail {
                details: format!(
                    "offset {committed} was committed inside an open transaction and is \
                     already readable; the input reads as processed while the output may \
                     still be thrown away"
                ),
            };
        }
        Ok(_) => {}
        Err(e) => return e.context("OffsetFetch (mid-transaction)").into_verdict(),
    }

    match end_txn(&mut conn, versions.end, actor, true, 1_140).await {
        Ok(code) if code.is_ok() => {}
        Ok(code) => {
            return Verdict::Fail {
                details: format!("committing the transaction answered {code}"),
            };
        }
        Err(e) => return e.context("EndTxn").into_verdict(),
    }

    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        match fetch_committed(
            &mut offsets,
            fetch_offsets_version,
            &group,
            &topic,
            topic_id,
            1_160 + i32::try_from(attempt).unwrap_or(0),
        )
        .await
        {
            Ok(got) if got == committed => return Verdict::Pass,
            Ok(_) => {}
            Err(e) => return e.context("OffsetFetch (after commit)").into_verdict(),
        }
    }
    Verdict::Fail {
        details: format!(
            "the transaction committed and offset {committed} never became readable; the \
             output exists and the input still reads as unprocessed, so it will be done again"
        ),
    }
}

/// AddOffsetsToTxn versions a client may speak.
const ADD_OFFSETS_CLIENT_MAX: i16 = 4;
/// TxnOffsetCommit below v5, which needs KIP-890 transactions V2.
const TXN_OFFSET_COMMIT_CLIENT_MAX: i16 = 4;

/// Tell the coordinator this transaction will also commit `group`'s
/// offsets.
async fn add_offsets_to_txn(
    conn: &mut RawConnection,
    version: i16,
    actor: TxnActor<'_>,
    group: &str,
    correlation: i32,
) -> Result<ErrorCode, CheckError> {
    let mut request = AddOffsetsToTxnRequest::default();
    request.transactional_id = actor.transactional_id.to_owned();
    request.producer_id = actor.producer_id;
    request.producer_epoch = actor.producer_epoch;
    request.group_id = group.to_owned();
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding AddOffsetsToTxn: {e}")))?;
    let resp: AddOffsetsToTxnResponse = api_call(
        conn,
        AddOffsetsToTxnRequest::API_KEY,
        version,
        correlation,
        &body,
    )
    .await?;
    Ok(ErrorCode(resp.error_code))
}

/// Commit one partition's offset inside the transaction.
#[allow(clippy::too_many_arguments)]
async fn txn_offset_commit(
    conn: &mut RawConnection,
    version: i16,
    actor: TxnActor<'_>,
    group: &str,
    topic: &str,
    offset: i64,
    correlation: i32,
) -> Result<ErrorCode, CheckError> {
    let mut partition = TxnOffsetCommitRequestPartition::default();
    partition.partition_index = 0;
    partition.committed_offset = offset;
    partition.committed_leader_epoch = -1;
    let mut req_topic = TxnOffsetCommitRequestTopic::default();
    req_topic.name = topic.to_owned();
    req_topic.partitions = vec![partition];
    let mut request = TxnOffsetCommitRequest::default();
    request.transactional_id = actor.transactional_id.to_owned();
    request.group_id = group.to_owned();
    request.producer_id = actor.producer_id;
    request.producer_epoch = actor.producer_epoch;
    // A simple (non-member) transactional commit, like the offsets path
    // the plain commit check uses.
    request.generation_id = -1;
    request.member_id = String::new();
    request.group_instance_id = None;
    request.topics = vec![req_topic];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding TxnOffsetCommit: {e}")))?;
    let resp: TxnOffsetCommitResponse = api_call(
        conn,
        TxnOffsetCommitRequest::API_KEY,
        version,
        correlation,
        &body,
    )
    .await?;
    Ok(resp
        .topics
        .first()
        .and_then(|t| t.partitions.first())
        .map_or(ErrorCode::NONE, |p| ErrorCode(p.error_code)))
}

/// A fenced producer must not be able to write records.
///
/// `txn/stale-epoch-is-fenced` asks the *coordinator* to refuse a
/// superseded epoch. This asks the partition leader, which is different
/// code and the one that matters more: the coordinator refusing a
/// bookkeeping call is inconvenient for a zombie, but a leader that
/// accepts its records puts them inside a transaction the live producer
/// is about to commit. The successor then commits work it never did,
/// which is the exact failure fencing exists to prevent, and it leaves
/// no trace anywhere.
async fn txn_fenced_producer_cannot_produce(ctx: &ServerCtx) -> Verdict {
    let versions = match txn_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "txnzombie", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut leader = produced.conn;
    let id = unique_transactional_id("zombie");

    let (mut txn, first) = match init_producer_id(ctx, versions.init, &id, 1_200).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId (first)").into_verdict(),
    };
    let zombie = TxnActor::new(&id, &first);
    // The doomed producer announces its partition while it still can,
    // so what the leader refuses later is the write itself rather than
    // an unannounced partition.
    let conns = TxnConns {
        txn: &mut txn,
        leader: &mut leader,
    };
    if let Err(verdict) = open_transaction(ctx, conns, &versions, zombie, &topic, 0, 1_220).await {
        return verdict;
    }

    let (mut txn, second) = match init_producer_id(ctx, versions.init, &id, 1_260).await {
        Ok(r) => r,
        Err(e) => return e.context("InitProducerId (second)").into_verdict(),
    };
    if second.producer_epoch <= first.producer_epoch {
        return Verdict::Skipped {
            reason: "the epoch never advanced, so there is no fenced producer to refuse \
                     (txn/init-bumps-the-epoch reports that)"
                .into(),
        };
    }

    // The zombie writes on, at the epoch it still believes it holds.
    let outcome = produce_stamped(
        &mut leader,
        versions.produce,
        ProduceStamp::transactional(zombie, 1),
        &topic,
        0,
        1_280,
    )
    .await;
    // Whatever happens, do not leave the successor's transaction open
    // behind this check.
    let live = TxnActor::new(&id, &second);
    let _ = end_txn(&mut txn, versions.end, live, false, 1_299).await;

    match outcome {
        Ok((code, _)) if is_fenced(code) => Verdict::Pass,
        // Refusing for any reason keeps the records out, which is the
        // thing that matters; the code is the secondary question.
        Ok((code, _)) if !code.is_ok() => Verdict::Fail {
            details: format!(
                "a produce at the superseded epoch {} was refused with {code} rather than a \
                 fencing code, so a zombie producer is told its stamp is wrong without being \
                 told it has been replaced",
                first.producer_epoch
            ),
        },
        Ok((_, offset)) => Verdict::Fail {
            details: format!(
                "a produce at the superseded epoch {} was accepted at offset {offset} while \
                 epoch {} holds the id; those records sit inside a transaction the live \
                 producer is about to commit",
                first.producer_epoch, second.producer_epoch
            ),
        },
        Err(e) => e.context("Produce (fenced)").into_verdict(),
    }
}

/// `acks=0` means no response at all.
///
/// Not "an empty response" and not "a response the client can ignore":
/// the broker sends nothing, and a client that speaks fire-and-forget
/// has no correlation id outstanding for it. A broker that answers
/// anyway puts a frame on the wire nobody is waiting for, and the next
/// response the client reads is the wrong one — every reply after that
/// is matched to the wrong request. It is the worst kind of wire bug:
/// silent, and it corrupts everything downstream rather than failing.
async fn produce_acks_zero_is_silent(ctx: &ServerCtx) -> Verdict {
    let produce_version = match negotiate(
        "Produce",
        match ctx.range(ProduceRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        ProduceRequest::MIN_VERSION,
        PRODUCE_NAME_MAX,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "acks0", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut conn = produced.conn;

    let mut partition_data = PartitionProduceData::default();
    partition_data.index = 0;
    partition_data.records = Some(produced.sent.clone());
    let mut topic_data = TopicProduceData::default();
    topic_data.name = topic.clone();
    topic_data.partition_data = vec![partition_data];
    let mut request = ProduceRequest::default();
    request.acks = 0;
    request.timeout_ms = 10_000;
    request.topic_data = vec![topic_data];
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, produce_version) {
        return Verdict::Error {
            details: format!("encoding Produce: {e}"),
        };
    }
    let header_version =
        match header::request_header_version(ProduceRequest::API_KEY, produce_version) {
            Some(v) => v,
            None => {
                return Verdict::Error {
                    details: format!("no header version for Produce v{produce_version}"),
                };
            }
        };
    let silent_correlation = 1_300;
    let frame = match frame_request(
        ProduceRequest::API_KEY,
        produce_version,
        header_version,
        silent_correlation,
        &body,
    ) {
        Ok(frame) => frame,
        Err(details) => return Verdict::Error { details },
    };
    if let Err(e) = conn.send_frame(&frame).await {
        return Verdict::Error {
            details: format!("sending the acks=0 produce: {e}"),
        };
    }

    // A request the broker *must* answer, sent straight after. The next
    // frame on the wire has to be its reply; anything else is a frame
    // nobody asked for.
    let probe_correlation = 1_301;
    let probe = match frame_request(
        ApiVersionsRequest::API_KEY,
        0,
        header::request_header_version(ApiVersionsRequest::API_KEY, 0).unwrap_or(1),
        probe_correlation,
        &[],
    ) {
        Ok(frame) => frame,
        Err(details) => return Verdict::Error { details },
    };
    if let Err(e) = conn.send_frame(&probe).await {
        return Verdict::Error {
            details: format!("sending the follow-up ApiVersions: {e}"),
        };
    }
    let answer = match conn.read_frame().await {
        Ok(frame) => frame,
        Err(e) => {
            return Verdict::Error {
                details: format!("reading the follow-up answer: {e}"),
            };
        }
    };
    if answer.len() < 4 {
        return Verdict::Fail {
            details: format!(
                "the answer after an acks=0 produce is {} byte(s)",
                answer.len()
            ),
        };
    }
    let correlation = i32::from_be_bytes([answer[0], answer[1], answer[2], answer[3]]);
    if correlation == probe_correlation {
        return Verdict::Pass;
    }
    if correlation == silent_correlation {
        return Verdict::Fail {
            details: format!(
                "answered an acks=0 produce (correlation {silent_correlation}); the client \
                 has no id outstanding for it, so it reads that frame as the answer to its \
                 next request and every reply after is matched to the wrong one"
            ),
        };
    }
    Verdict::Fail {
        details: format!(
            "after an acks=0 produce and an ApiVersions (correlation {probe_correlation}), \
             the next frame carried correlation {correlation}"
        ),
    }
}

/// A topic's id must not change under it.
///
/// Ids exist so a client can address a topic that was deleted and
/// recreated without silently writing to the new one. A broker that
/// mints a fresh id for a topic that never went away breaks every
/// id-addressed request a client already had in flight — with
/// UNKNOWN_TOPIC_ID, which reads as "that topic is gone" and sends the
/// client looking for a problem that is not there.
async fn metadata_topic_id_is_stable(ctx: &ServerCtx) -> Verdict {
    let version = match negotiate(
        "Metadata",
        match ctx.range(MetadataRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        // v10 is where ids reach the topics array.
        10,
        MetadataRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "stableid", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let mut conn = produced.conn;

    let mut seen: Option<[u8; 16]> = None;
    for attempt in 0..3 {
        let resp = match metadata_of_topic(&mut conn, version, &topic, 1_320 + attempt).await {
            Ok(resp) => resp,
            Err(e) => return e.context("Metadata").into_verdict(),
        };
        let Some(entry) = resp
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic.as_str()))
        else {
            return Verdict::Fail {
                details: format!("Metadata stopped naming {topic} between asks"),
            };
        };
        if entry.topic_id == [0u8; 16] {
            return Verdict::Skipped {
                reason: format!("Metadata v{version} returned no topic id for {topic}"),
            };
        }
        match seen {
            None => seen = Some(entry.topic_id),
            Some(first) if first == entry.topic_id => {}
            Some(first) => {
                return Verdict::Fail {
                    details: format!(
                        "{topic} was {} and is now {}; every id-addressed request already in \
                         flight fails with UNKNOWN_TOPIC_ID, which reads as the topic being \
                         gone",
                        hex16(&first),
                        hex16(&entry.topic_id)
                    ),
                };
            }
        }
    }
    Verdict::Pass
}

/// A topic id as hex, for a message a human has to compare two of.
fn hex16(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// One request frame, header and body, for the checks that send
/// without waiting for an answer.
fn frame_request(
    api_key: i16,
    api_version: i16,
    header_version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Result<Vec<u8>, String> {
    let mut req_header = RequestHeader::default();
    req_header.request_api_key = api_key;
    req_header.request_api_version = api_version;
    req_header.correlation_id = correlation_id;
    req_header.client_id = Some(CLIENT_ID.into());
    let mut out = BytesMut::new();
    req_header
        .encode(&mut out, header_version)
        .map_err(|e| format!("encoding a request header: {e}"))?;
    out.extend_from_slice(body);
    Ok(out.to_vec())
}

// ---------------------------------------------------------------------
// Cluster checks
//
// Everything above this line can be asked of a single broker. Nothing
// below it can. On one node every partition's leader and every group's
// coordinator is the broker you are already connected to, so the
// questions these ask — does *every* broker agree, and does the wrong
// broker refuse — have no content there. They skip, and say so.
//
// They are not about failure or recovery. Nothing here kills a broker.
// They are about a cluster's answers being consistent while everything
// is working, which is the precondition for anything about failure
// meaning something.
// ---------------------------------------------------------------------

/// One broker of the subject cluster, as Metadata describes it.
struct ClusterNode {
    node_id: i32,
    addr: String,
}

/// Every broker the subject reports, or a skip when there is only one.
///
/// Discovered through Metadata rather than configured, because that is
/// the only way a client could discover them either — and a suite that
/// was told the topology out of band could not notice a subject whose
/// Metadata failed to describe it.
async fn cluster_nodes(ctx: &ServerCtx) -> Result<Vec<ClusterNode>, Verdict> {
    let version = negotiate(
        "Metadata",
        ctx.range(MetadataRequest::API_KEY)?,
        1,
        MetadataRequest::MAX_VERSION,
    )?;
    let mut conn = connect(&ctx.addr).await.map_err(CheckError::into_verdict)?;
    let mut request = MetadataRequest::default();
    request.topics = Some(Vec::new());
    request.allow_auto_topic_creation = false;
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| Verdict::Error {
            details: format!("encoding Metadata: {e}"),
        })?;
    let resp: MetadataResponse =
        api_call(&mut conn, MetadataRequest::API_KEY, version, 1_400, &body)
            .await
            .map_err(|e| e.context("Metadata").into_verdict())?;

    let nodes: Vec<ClusterNode> = resp
        .brokers
        .iter()
        .filter(|b| !b.host.is_empty() && (1..=65535).contains(&b.port))
        .map(|b| ClusterNode {
            node_id: b.node_id,
            addr: format!("{}:{}", b.host, b.port),
        })
        .collect();
    if nodes.len() < 2 {
        return Err(Verdict::Skipped {
            reason: format!(
                "the subject reports {} broker(s); this check is about what \
                 brokers must agree on, which a cluster of one cannot be asked",
                nodes.len()
            ),
        });
    }
    Ok(nodes)
}

/// Where Metadata at `conn` says partition 0 of `topic` is led, and by
/// which replicas.
async fn leadership_at(
    conn: &mut RawConnection,
    version: i16,
    topic: &str,
    correlation: i32,
) -> Result<Option<(i32, Vec<i32>, Vec<i32>)>, CheckError> {
    let resp = metadata_of_topic(conn, version, topic, correlation).await?;
    let Some(entry) = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
    else {
        return Ok(None);
    };
    if !ErrorCode(entry.error_code).is_ok() {
        return Ok(None);
    }
    Ok(entry
        .partitions
        .iter()
        .find(|p| p.partition_index == 0)
        .filter(|p| ErrorCode(p.error_code).is_ok())
        .map(|p| (p.leader_id, p.replica_nodes.clone(), p.isr_nodes.clone())))
}

/// Create a single-partition topic with `replicas` replicas.
async fn create_replicated_topic(
    ctx: &ServerCtx,
    conn: &mut RawConnection,
    topic: &str,
    replicas: i16,
    correlation: i32,
) -> Result<(), Verdict> {
    let version = negotiate(
        "CreateTopics",
        ctx.range(CreateTopicsRequest::API_KEY)?,
        CreateTopicsRequest::MIN_VERSION,
        CreateTopicsRequest::MAX_VERSION,
    )?;
    let mut creatable = CreatableTopic::default();
    creatable.name = topic.to_owned();
    creatable.num_partitions = 1;
    creatable.replication_factor = replicas;
    let mut create = CreateTopicsRequest::default();
    create.topics = vec![creatable];
    create.timeout_ms = 30_000;
    create.validate_only = false;
    let mut body = BytesMut::new();
    create
        .encode(&mut body, version)
        .map_err(|e| Verdict::Error {
            details: format!("encoding CreateTopics: {e}"),
        })?;
    let mut controller = admin_conn(ctx, conn, correlation.wrapping_sub(1))
        .await
        .map_err(|e| e.context("locating the controller").into_verdict())?;
    let resp: CreateTopicsResponse = api_call(
        &mut controller,
        CreateTopicsRequest::API_KEY,
        version,
        correlation,
        &body,
    )
    .await
    .map_err(|e| e.context("CreateTopics").into_verdict())?;
    let code = resp
        .topics
        .first()
        .map_or(ErrorCode::NONE, |t| ErrorCode(t.error_code));
    if !code.is_ok() {
        return Err(Verdict::Skipped {
            reason: format!("the subject would not create a {replicas}-replica topic: {code}"),
        });
    }
    Ok(())
}

/// Wait for a freshly created topic to have a leader, and report it
/// with the replica set the cluster placed it on.
async fn settle_leadership(
    ctx: &ServerCtx,
    conn: &mut RawConnection,
    version: i16,
    topic: &str,
    correlation: i32,
) -> Result<(i32, Vec<i32>), Verdict> {
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        match leadership_at(
            conn,
            version,
            topic,
            correlation + i32::try_from(attempt).unwrap_or(0),
        )
        .await
        {
            Ok(Some((leader, replicas, _))) if leader >= 0 => return Ok((leader, replicas)),
            Ok(_) => {}
            Err(e) => return Err(e.context("Metadata").into_verdict()),
        }
    }
    Err(Verdict::Error {
        details: format!("{topic} was created but never given a leader within the settle budget"),
    })
}

/// Every broker must name the same leader for the same partition.
///
/// Leadership is a fact about the partition, not about who you ask. Two
/// brokers that answer differently give two producers two different
/// places to write, and a partition with two writers has two histories
/// — which is the single thing leadership exists to prevent. It is also
/// invisible to each client individually: both are told something
/// plausible by a broker that sounds sure.
async fn cluster_brokers_agree_on_the_leader(ctx: &ServerCtx) -> Verdict {
    let nodes = match cluster_nodes(ctx).await {
        Ok(nodes) => nodes,
        Err(verdict) => return verdict,
    };
    let version = match negotiate(
        "Metadata",
        match ctx.range(MetadataRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        1,
        MetadataRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "agreeleader", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };

    let mut answers: Vec<(i32, i32)> = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        let mut conn = match connect(&node.addr).await {
            Ok(c) => c,
            Err(e) => {
                return e
                    .context(&format!("connecting to node {}", node.node_id))
                    .into_verdict();
            }
        };
        let correlation = 1_410 + i32::try_from(i).unwrap_or(0);
        // A broker that has not yet heard of the topic has not
        // disagreed about it; it has not answered.
        if let Err(e) = await_topic_known(ctx, &mut conn, &produced.topic, correlation).await {
            return e.into_verdict();
        }
        match leadership_at(&mut conn, version, &produced.topic, correlation + 100).await {
            Ok(Some((leader, _, _))) => answers.push((node.node_id, leader)),
            Ok(None) => {}
            Err(e) => {
                return e
                    .context(&format!("Metadata at node {}", node.node_id))
                    .into_verdict();
            }
        }
    }
    if answers.len() < 2 {
        return Verdict::Skipped {
            reason: format!(
                "only {} broker(s) could describe {} within the settle budget, so \
                 there is no disagreement to detect",
                answers.len(),
                produced.topic
            ),
        };
    }
    let (_, first) = answers[0];
    if let Some((node_id, leader)) = answers.iter().find(|(_, leader)| *leader != first) {
        return Verdict::Fail {
            details: format!(
                "brokers disagree about who leads {}[0]: node {} says {leader}, node {} \
                 says {first}. Two clients refreshing metadata against different \
                 brokers would write to different leaders",
                produced.topic, node_id, answers[0].0
            ),
        };
    }
    Verdict::Pass
}

/// Every broker must name the same coordinator for the same group.
///
/// The same argument as leadership, for the other half of the protocol.
/// FindCoordinator exists so a client can be told where to go; a cluster
/// whose brokers each name themselves has told every client something
/// different, and the group's committed offsets are then spread over
/// however many brokers its members happened to bootstrap against.
async fn cluster_brokers_agree_on_the_coordinator(ctx: &ServerCtx) -> Verdict {
    let nodes = match cluster_nodes(ctx).await {
        Ok(nodes) => nodes,
        Err(verdict) => return verdict,
    };
    let version = match negotiate(
        "FindCoordinator",
        match ctx.range(FindCoordinatorRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        FindCoordinatorRequest::MIN_VERSION,
        FindCoordinatorRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let group = check_group("agreecoord");
    // Warm it first: a group whose coordinator is still being elected
    // is a wait, and the checks that report that are elsewhere.
    if let Err(e) = await_coordinator(ctx, &group).await {
        return e.into_verdict();
    }

    let mut request = FindCoordinatorRequest::default();
    request.key_type = 0;
    if version >= FIND_COORDINATOR_BATCHED {
        request.coordinator_keys = vec![group.clone()];
    } else {
        request.key = group.clone();
    }
    let mut body = BytesMut::new();
    if let Err(e) = request.encode(&mut body, version) {
        return Verdict::Error {
            details: format!("encoding FindCoordinator: {e}"),
        };
    }

    let mut answers: Vec<(i32, String)> = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        let mut conn = match connect(&node.addr).await {
            Ok(c) => c,
            Err(e) => {
                return e
                    .context(&format!("connecting to node {}", node.node_id))
                    .into_verdict();
            }
        };
        let resp: FindCoordinatorResponse = match api_call(
            &mut conn,
            FindCoordinatorRequest::API_KEY,
            version,
            1_430 + i32::try_from(i).unwrap_or(0),
            &body,
        )
        .await
        {
            Ok(resp) => resp,
            Err(e) => {
                return e
                    .context(&format!("FindCoordinator at node {}", node.node_id))
                    .into_verdict();
            }
        };
        if is_coordinator_settling(coordinator_error(&resp, version)) {
            continue;
        }
        if let Some(endpoint) = coordinator_endpoint(&resp, version) {
            answers.push((node.node_id, endpoint));
        }
    }
    if answers.len() < 2 {
        return Verdict::Skipped {
            reason: format!(
                "only {} broker(s) named a coordinator for {group}, so there is no \
                 disagreement to detect",
                answers.len()
            ),
        };
    }
    let first = answers[0].1.clone();
    if let Some((node_id, endpoint)) = answers.iter().find(|(_, e)| *e != first) {
        return Verdict::Fail {
            details: format!(
                "brokers disagree about who coordinates {group}: node {node_id} says \
                 {endpoint}, node {} says {first}. The group's offsets would be split \
                 across brokers by which one each member asked",
                answers[0].0
            ),
        };
    }
    Verdict::Pass
}

/// A topic asked for n replicas must get n distinct brokers.
///
/// Replication that lands twice on one broker is not replication: the
/// copies share a disk and a process, and the cluster has quietly
/// promised a durability it cannot deliver. The leader must also be one
/// of the replicas, and the in-sync set drawn from them, or the numbers
/// describe a partition that does not exist.
async fn cluster_replicas_span_brokers(ctx: &ServerCtx) -> Verdict {
    let nodes = match cluster_nodes(ctx).await {
        Ok(nodes) => nodes,
        Err(verdict) => return verdict,
    };
    let create_version = match negotiate(
        "CreateTopics",
        match ctx.range(CreateTopicsRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        CreateTopicsRequest::MIN_VERSION,
        CreateTopicsRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let metadata_version = match negotiate(
        "Metadata",
        match ctx.range(MetadataRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        1,
        MetadataRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let known: Vec<i32> = nodes.iter().map(|n| n.node_id).collect();
    // Every broker there is, so the answer is unambiguous: with fewer
    // replicas than brokers, a subject could satisfy this by accident.
    let wanted = i16::try_from(nodes.len()).unwrap_or(i16::MAX);

    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let topic = unique_topic("spanned");
    let mut creatable = CreatableTopic::default();
    creatable.name = topic.clone();
    creatable.num_partitions = 1;
    creatable.replication_factor = wanted;
    let mut create = CreateTopicsRequest::default();
    create.topics = vec![creatable];
    create.timeout_ms = 30_000;
    create.validate_only = false;
    let mut body = BytesMut::new();
    if let Err(e) = create.encode(&mut body, create_version) {
        return Verdict::Error {
            details: format!("encoding CreateTopics: {e}"),
        };
    }
    let resp: CreateTopicsResponse = match api_call(
        &mut conn,
        CreateTopicsRequest::API_KEY,
        create_version,
        1_450,
        &body,
    )
    .await
    {
        Ok(resp) => resp,
        Err(e) => return e.context("CreateTopics").into_verdict(),
    };
    let code = resp
        .topics
        .first()
        .map_or(ErrorCode::NONE, |t| ErrorCode(t.error_code));
    if !code.is_ok() {
        return Verdict::Skipped {
            reason: format!(
                "the subject would not create a {wanted}-replica topic on its \
                 {} broker(s): {code}",
                nodes.len()
            ),
        };
    }

    let mut seen = None;
    for attempt in 0..ctx.config.settle_attempts() {
        if attempt > 0 {
            tokio::time::sleep(ctx.config.settle_delay).await;
        }
        match leadership_at(
            &mut conn,
            metadata_version,
            &topic,
            1_460 + i32::try_from(attempt).unwrap_or(0),
        )
        .await
        {
            Ok(Some(found)) => {
                seen = Some(found);
                break;
            }
            Ok(None) => {}
            Err(e) => return e.context("Metadata").into_verdict(),
        }
    }
    let Some((leader, replicas, isr)) = seen else {
        return Verdict::Error {
            details: format!("{topic} was created but never described within the settle budget"),
        };
    };

    let mut distinct = replicas.clone();
    distinct.sort_unstable();
    distinct.dedup();
    if distinct.len() != replicas.len() {
        return Verdict::Fail {
            details: format!(
                "{topic}[0] asked for {wanted} replicas and was given {replicas:?} — the \
                 same broker more than once, so the copies share a disk"
            ),
        };
    }
    if replicas.len() != usize::from(u16::try_from(wanted).unwrap_or(0)) {
        return Verdict::Fail {
            details: format!(
                "{topic}[0] was created asking for {wanted} replicas and is reported on \
                 {replicas:?}; the cluster has brokers {known:?}"
            ),
        };
    }
    if let Some(stranger) = replicas.iter().find(|r| !known.contains(r)) {
        return Verdict::Fail {
            details: format!(
                "{topic}[0] names replica {stranger}, which is not one of the brokers \
                 the same cluster reports ({known:?})"
            ),
        };
    }
    if !replicas.contains(&leader) {
        return Verdict::Fail {
            details: format!(
                "{topic}[0] is led by {leader}, which is not among its replicas \
                 {replicas:?}; the leader holds a copy by definition"
            ),
        };
    }
    if let Some(stranger) = isr.iter().find(|r| !replicas.contains(r)) {
        return Verdict::Fail {
            details: format!(
                "{topic}[0] reports {stranger} in sync, which is not one of its replicas \
                 {replicas:?}"
            ),
        };
    }
    Verdict::Pass
}

/// A broker that does not lead the partition must refuse the write.
///
/// Accepting it is the worst available outcome: the records land in a
/// log the leader knows nothing about, the partition has two histories,
/// and the producer is told everything is fine. The refusal is also the
/// client's only cue to re-read Metadata, so a broker that silently
/// accepted would leave the client pointed at the wrong broker forever.
async fn cluster_writes_go_to_the_leader(ctx: &ServerCtx) -> Verdict {
    let nodes = match cluster_nodes(ctx).await {
        Ok(nodes) => nodes,
        Err(verdict) => return verdict,
    };
    let produce_version = match negotiate(
        "Produce",
        match ctx.range(ProduceRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        ProduceRequest::MIN_VERSION,
        PRODUCE_NAME_MAX,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let metadata_version = match negotiate(
        "Metadata",
        match ctx.range(MetadataRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        1,
        MetadataRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "notleader", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };

    let mut conn = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let leader = match leadership_at(&mut conn, metadata_version, &produced.topic, 1_470).await {
        Ok(Some((leader, _, _))) => leader,
        Ok(None) => {
            return Verdict::Error {
                details: format!("{} has no described leader", produced.topic),
            };
        }
        Err(e) => return e.context("Metadata").into_verdict(),
    };
    let Some(other) = nodes.iter().find(|n| n.node_id != leader) else {
        return Verdict::Skipped {
            reason: format!(
                "every broker the subject reports is node {leader}, so there is no \
                 non-leader to send to"
            ),
        };
    };

    let mut conn = match connect(&other.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    let (code, _) = match produce_stamped(
        &mut conn,
        produce_version,
        ProduceStamp::plain(),
        &produced.topic,
        0,
        1_480,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => return e.context("Produce (to a non-leader)").into_verdict(),
    };
    if code == ErrorCode::NOT_LEADER_OR_FOLLOWER {
        return Verdict::Pass;
    }
    if code.is_ok() {
        return Verdict::Fail {
            details: format!(
                "node {} accepted a write to {}[0], which node {leader} leads; those \
                 records are in a log the leader does not have, and the producer was \
                 told nothing",
                other.node_id, produced.topic
            ),
        };
    }
    // Some other refusal still keeps the records out, which is the part
    // that matters; the code is how the client learns to look again.
    Verdict::Fail {
        details: format!(
            "node {} refused a write to {}[0] with {code} rather than \
             NOT_LEADER_OR_FOLLOWER, so the producer is not told to re-read \
             metadata and find node {leader}",
            other.node_id, produced.topic
        ),
    }
}

/// A broker that does not coordinate the group must refuse the commit.
///
/// Offsets stored anywhere else are stored where nothing will read
/// them: a consumer that resumes goes to the coordinator, sees an older
/// position or none, and reprocesses everything since. Exactly-once
/// becomes at-least-twice, silently, and only on restart.
async fn cluster_group_offsets_need_the_coordinator(ctx: &ServerCtx) -> Verdict {
    let nodes = match cluster_nodes(ctx).await {
        Ok(nodes) => nodes,
        Err(verdict) => return verdict,
    };
    let (commit_version, _) = match offsets_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "notcoord", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if let Some(skip) = skip_without_topic_id(&produced, commit_version) {
        return skip;
    }
    let group = check_group("notcoord");
    let Some(coordinator) = (match await_coordinator(ctx, &group).await {
        Ok(addr) => addr,
        Err(e) => return e.into_verdict(),
    }) else {
        return Verdict::Skipped {
            reason: format!("the subject named no coordinator for {group}"),
        };
    };
    let Some(other) = nodes.iter().find(|n| n.addr != coordinator) else {
        return Verdict::Skipped {
            reason: format!(
                "every broker the subject reports is at {coordinator}, so there is no \
                 non-coordinator to commit against"
            ),
        };
    };

    let mut conn = match connect(&other.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    // The topic has to be known there, or the refusal could be about
    // the topic rather than about the group.
    if let Err(e) = await_topic_known(ctx, &mut conn, &produced.topic, 1_490).await {
        return e.into_verdict();
    }
    match commit_offset(
        &mut conn,
        commit_version,
        &group,
        &produced.topic,
        produced.topic_id,
        13,
        1_495,
    )
    .await
    {
        Ok(()) => Verdict::Fail {
            details: format!(
                "node {} accepted a commit for {group}, which {coordinator} coordinates; \
                 a consumer resuming through the coordinator would never see that offset \
                 and would reprocess everything after it",
                other.node_id
            ),
        },
        // `commit_offset` reports a non-zero code as a violation, which
        // here is the passing answer — so read the code back out of it.
        Err(CheckError::Violation(details))
            if details.contains(&format!("{}", ErrorCode::NOT_COORDINATOR)) =>
        {
            Verdict::Pass
        }
        Err(CheckError::Violation(details)) => Verdict::Fail {
            details: format!(
                "node {} refused the commit, but not with NOT_COORDINATOR, so the client \
                 is not told where to go instead: {details}",
                other.node_id
            ),
        },
        Err(e) => e
            .context("OffsetCommit (to a non-coordinator)")
            .into_verdict(),
    }
}

// ---------------------------------------------------------------------
// Recovery checks
//
// The `cluster/*` checks above ask whether a working cluster's answers
// are consistent. These ask what happens when it stops working — which
// is the question every deployment eventually asks, and the one a suite
// that only ever sees healthy brokers can never answer.
//
// They are the only checks that change the subject rather than
// observing it, so two rules govern them. They need a
// [`ClusterControl`] and skip without one, because a suite is not
// entitled to assume it may stop somebody's broker. And each restores
// the cluster before returning, on every path out — a check that left a
// broker down would hand its failure to whatever ran next, and the
// report would blame the wrong thing.
// ---------------------------------------------------------------------

/// Stop `node_id` for the duration of `body`, and start it again
/// whatever happens.
///
/// Rust has no async drop, so restoration cannot be left to a guard:
/// it is done here, once, around a closure that cannot return early
/// past it. A failure to restore outranks whatever `body` concluded,
/// because from that point on the report describes a cluster the
/// subject's operator did not agree to.
async fn while_stopped(
    ctx: &ServerCtx,
    broker: Broker<'_>,
    body: BoxFuture<'_, Verdict>,
) -> Verdict {
    let Some(control) = ctx.config.control.clone() else {
        return Verdict::Skipped {
            reason: "no --cluster-control was given, so the suite has no way to stop \
                     a broker and no business assuming one"
                .into(),
        };
    };
    if let Err(e) = control.stop(broker).await {
        return Verdict::Error {
            details: format!(
                "could not stop node {} at {}: {e}",
                broker.node_id, broker.addr
            ),
        };
    }
    let verdict = body.await;
    match control.start(broker).await {
        Ok(()) => verdict,
        Err(e) => Verdict::Error {
            details: format!(
                "node {} could not be restarted after the check ({e}); the cluster is \
                 short a broker and every later check is suspect. The check itself had \
                 reached: {verdict:?}",
                broker.node_id
            ),
        },
    }
}

/// Leadership must move off a broker that has stopped.
///
/// A partition whose leader is gone is a partition nothing can write
/// to. The replicas hold the data and one of them has to take over, or
/// the copies were decoration: a cluster that kept naming the stopped
/// broker would leave the partition unwritable for as long as the
/// broker stayed down, which is exactly the outage replication is sold
/// as preventing.
///
/// The check is not that the new leader is any particular broker — that
/// is the cluster's business — but that a leader exists, that it is one
/// of the replicas the cluster named while healthy, and that it accepts
/// a write.
async fn cluster_leadership_moves_when_a_broker_stops(ctx: &ServerCtx) -> Verdict {
    let nodes = match cluster_nodes(ctx).await {
        Ok(nodes) => nodes,
        Err(verdict) => return verdict,
    };
    if ctx.config.control.is_none() {
        return Verdict::Skipped {
            reason: "no --cluster-control was given, so the suite has no way to stop \
                     a broker and no business assuming one"
                .into(),
        };
    }
    let metadata_version = match negotiate(
        "Metadata",
        match ctx.range(MetadataRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        1,
        MetadataRequest::MAX_VERSION,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produce_version = match negotiate(
        "Produce",
        match ctx.range(ProduceRequest::API_KEY) {
            Ok(range) => range,
            Err(skip) => return skip,
        },
        ProduceRequest::MIN_VERSION,
        PRODUCE_NAME_MAX,
    ) {
        Ok(v) => v,
        Err(skip) => return skip,
    };

    // Replicated across every broker, so that stopping the leader
    // leaves someone able to take over. A single-replica partition has
    // no story here and the check would be about nothing.
    let topic = unique_topic("failover");
    let wanted = i16::try_from(nodes.len()).unwrap_or(1);
    let mut bootstrap = match connect(&ctx.addr).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    match create_replicated_topic(ctx, &mut bootstrap, &topic, wanted, 1_500).await {
        Ok(()) => {}
        Err(verdict) => return verdict,
    }

    let (leader, replicas) =
        match settle_leadership(ctx, &mut bootstrap, metadata_version, &topic, 1_510).await {
            Ok(found) => found,
            Err(verdict) => return verdict,
        };
    let Some(leader_addr) = nodes
        .iter()
        .find(|n| n.node_id == leader)
        .map(|n| n.addr.clone())
    else {
        return Verdict::Fail {
            details: format!(
                "{topic}[0] is led by node {leader}, which is not one of the brokers the \
                 same cluster reports"
            ),
        };
    };
    // Ask somewhere that will still be answering afterwards.
    let Some(survivor) = nodes.iter().find(|n| n.node_id != leader) else {
        return Verdict::Skipped {
            reason: format!("every broker is node {leader}, so stopping it leaves nobody to ask"),
        };
    };
    let survivor_addr = survivor.addr.clone();

    let stopping = Broker {
        node_id: leader,
        addr: &leader_addr,
    };
    while_stopped(
        ctx,
        stopping,
        Box::pin(async {
            let mut conn = match connect(&survivor_addr).await {
                Ok(c) => c,
                Err(e) => return e.context("connecting to a surviving broker").into_verdict(),
            };
            let mut moved = None;
            for attempt in 0..ctx.config.recovery_attempts() {
                if attempt > 0 {
                    tokio::time::sleep(ctx.config.settle_delay).await;
                }
                // No let-chain: this crate builds on the declared MSRV,
                // which predates them.
                if let Ok(Some((new_leader, _, _))) =
                    leadership_at(&mut conn, metadata_version, &topic, 1_520).await
                {
                    if new_leader != leader && new_leader >= 0 {
                        moved = Some(new_leader);
                        break;
                    }
                }
            }
            let Some(new_leader) = moved else {
                return Verdict::Fail {
                    details: format!(
                        "node {leader} was stopped and {topic}[0] still has no other leader \
                     after {:?}; its replicas were {replicas:?}, so there was somewhere \
                     for leadership to go and it did not",
                        ctx.config.recovery_budget
                    ),
                };
            };
            if !replicas.contains(&new_leader) {
                return Verdict::Fail {
                    details: format!(
                        "{topic}[0] failed over to node {new_leader}, which was not among the \
                     replicas the cluster named while healthy ({replicas:?}); that broker \
                     cannot have had the data"
                    ),
                };
            }
            // A leader that cannot be written to has not taken over.
            let Some(node) = nodes.iter().find(|n| n.node_id == new_leader) else {
                return Verdict::Fail {
                    details: format!(
                        "{topic}[0] failed over to node {new_leader}, which is not one of the \
                     brokers the cluster reports"
                    ),
                };
            };
            let mut leader_conn = match connect(&node.addr).await {
                Ok(c) => c,
                Err(e) => return e.context("connecting to the new leader").into_verdict(),
            };
            let mut last = ErrorCode::NONE;
            let mut accepted = false;
            for attempt in 0..ctx.config.recovery_attempts() {
                if attempt > 0 {
                    tokio::time::sleep(ctx.config.settle_delay).await;
                }
                match produce_stamped(
                    &mut leader_conn,
                    produce_version,
                    ProduceStamp::plain(),
                    &topic,
                    0,
                    1_530,
                )
                .await
                {
                    Ok((code, _)) if code.is_ok() => {
                        accepted = true;
                        break;
                    }
                    Ok((code, _)) => last = code,
                    Err(_) => {}
                }
            }
            if accepted {
                Verdict::Pass
            } else {
                Verdict::Fail {
                    details: format!(
                        "{topic}[0] reports node {new_leader} as its leader with node {leader} \
                     stopped, but a write there answers {last}; the partition has a leader \
                     in name only"
                    ),
                }
            }
        }),
    )
    .await
}

/// Committed offsets must outlive the broker that coordinated them.
///
/// A group's offsets are the only record of what it has processed. If
/// they live on one broker and die with it, a consumer that resumes
/// after that broker fails rewinds to whatever survived — reprocessing
/// everything since, silently, and only on the day something went
/// wrong. The commit was acknowledged; the acknowledgement has to mean
/// the offset is as durable as the cluster is.
async fn cluster_committed_offsets_outlive_the_coordinator(ctx: &ServerCtx) -> Verdict {
    let nodes = match cluster_nodes(ctx).await {
        Ok(nodes) => nodes,
        Err(verdict) => return verdict,
    };
    if ctx.config.control.is_none() {
        return Verdict::Skipped {
            reason: "no --cluster-control was given, so the suite has no way to stop \
                     a broker and no business assuming one"
                .into(),
        };
    }
    let (commit_version, fetch_version) = match offsets_versions(ctx) {
        Ok(v) => v,
        Err(skip) => return skip,
    };
    let produced = match produce_flow(ctx, "coordloss", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    if let Some(skip) = skip_without_topic_id(&produced, commit_version) {
        return skip;
    }
    let group = check_group("coordloss");
    let Some(coordinator) = (match await_coordinator(ctx, &group).await {
        Ok(addr) => addr,
        Err(e) => return e.into_verdict(),
    }) else {
        return Verdict::Skipped {
            reason: format!("the subject named no coordinator for {group}"),
        };
    };
    let Some(owner) = nodes.iter().find(|n| n.addr == coordinator) else {
        return Verdict::Skipped {
            reason: format!(
                "the coordinator for {group} is at {coordinator}, which is not one of the \
                 brokers Metadata reports, so there is no node to stop"
            ),
        };
    };
    let owner_id = owner.node_id;
    let Some(survivor) = nodes.iter().find(|n| n.node_id != owner_id) else {
        return Verdict::Skipped {
            reason: "the coordinator is the only broker, so stopping it leaves nobody to ask"
                .into(),
        };
    };
    let survivor_addr = survivor.addr.clone();

    // A number nothing else would produce by accident.
    let committed = 23;
    let mut conn = match connect(&coordinator).await {
        Ok(c) => c,
        Err(e) => return e.into_verdict(),
    };
    if let Err(e) = await_topic_known(ctx, &mut conn, &produced.topic, 1_540).await {
        return e.into_verdict();
    }
    if let Err(e) = commit_offset(
        &mut conn,
        commit_version,
        &group,
        &produced.topic,
        produced.topic_id,
        committed,
        1_545,
    )
    .await
    {
        return e.context("OffsetCommit").into_verdict();
    }
    drop(conn);

    let topic = produced.topic.clone();
    let topic_id = produced.topic_id;
    let stopping = Broker {
        node_id: owner_id,
        addr: &coordinator,
    };
    while_stopped(
        ctx,
        stopping,
        Box::pin(async {
            let mut conn = match connect(&survivor_addr).await {
                Ok(c) => c,
                Err(e) => return e.context("connecting to a surviving broker").into_verdict(),
            };
            // The group needs a coordinator again before it can be asked
            // anything; that it gets one is the other half of this.
            let mut relocated = None;
            for attempt in 0..ctx.config.recovery_attempts() {
                if attempt > 0 {
                    tokio::time::sleep(ctx.config.settle_delay).await;
                }
                // Asked of a survivor, not of the bootstrap: the
                // broker that was stopped may well be the bootstrap,
                // and asking it would fail for the obvious reason
                // rather than tell us anything about failing over.
                if let Ok(Some(addr)) = await_coordinator_at(ctx, &survivor_addr, &group).await {
                    if addr != coordinator {
                        relocated = Some(addr);
                        break;
                    }
                }
            }
            let Some(addr) = relocated else {
                return Verdict::Fail {
                    details: format!(
                        "node {owner_id} coordinated {group} and was stopped; after {:?} the \
                     cluster still names no other coordinator, so the group cannot commit \
                     or resume at all",
                        ctx.config.recovery_budget
                    ),
                };
            };
            if let Ok(reconnected) = connect(&addr).await {
                conn = reconnected;
            }
            // A coordinator that has just taken the group over has to
            // load its state before it can answer about it, and until
            // it has, the group's partitions are not in its reply at
            // all. That is a wait, not a lost offset — the two are
            // distinguished by whether it is still true at the end of
            // the recovery budget.
            let mut last = None;
            for attempt in 0..ctx.config.recovery_attempts() {
                if attempt > 0 {
                    tokio::time::sleep(ctx.config.settle_delay).await;
                }
                match fetch_committed(&mut conn, fetch_version, &group, &topic, topic_id, 1_550)
                    .await
                {
                    Ok(got) if got == committed => return Verdict::Pass,
                    Ok(got) => last = Some(format!("reports {got}")),
                    Err(CheckError::Violation(details)) => last = Some(details),
                    Err(e) => return e.context("OffsetFetch (after failover)").into_verdict(),
                }
            }
            Verdict::Fail {
                details: format!(
                    "{group} committed offset {committed}, node {owner_id} (its coordinator) \
                     was stopped, and the new coordinator at {addr} still {} after {:?}. A \
                     consumer resuming here reprocesses everything since",
                    last.unwrap_or_else(|| "says nothing about it".into()),
                    ctx.config.recovery_budget
                ),
            }
        }),
    )
    .await
}

// ---------------------------------------------------------------------
// Version sweep
// ---------------------------------------------------------------------

/// Every version a server advertises must be one it can actually speak.
///
/// ApiVersions is a promise: a client reads the advertised range and
/// picks from it, usually the highest it also understands. A server that
/// advertises a version it cannot serve has told every client to go
/// somewhere that does not work, and the failure lands at whatever
/// moment that client happens to negotiate — which is to say, on
/// upgrade, in production, for the newest clients first.
///
/// The suite mostly negotiates the *highest* version on offer, which is
/// the interesting one for behaviour and the least interesting one for
/// coverage: it leaves everything below it unspoken. Five apis are
/// already swept across their whole range by the checks that own them
/// (Metadata, Fetch by name, ListOffsets, FindCoordinator,
/// OffsetFetch). This sweeps ones that were pinned to a single version,
/// plus the id-addressed half of Fetch, where only the topmost version
/// was ever sent.
///
/// **It judges shape, and nothing else.** An error code is a fine
/// answer here; what is not fine is a version that cannot be spoken at
/// all — a connection closed, a body that will not decode at the
/// version it was requested at, bytes left over afterwards. That
/// restraint is not modesty, it is what keeps the suite calibrated: a
/// check that sweeps many apis overlaps every check that owns one of
/// them, and if it asserted their semantics too, every fault would trip
/// two checks and neither would be evidence of anything in particular.
/// For the same reason ApiVersions itself is not swept here — it has
/// four checks of its own, including both header quirks.
///
/// What it exercises is as much this crate as the subject. Each
/// exchange is encoded at version *n* by the generated codec and the
/// response decoded at version *n* and required to consume the frame
/// exactly, so a schema this crate models wrongly at some version fails
/// here and reads as the server's fault. The matrix is what tells them
/// apart: a version that fails against every implementation is ours,
/// one that fails against a single implementation is theirs. That is
/// why the failure lists every (api, version) pair rather than stopping
/// at the first.
///
/// Not swept, and honestly: the group lifecycle (JoinGroup, SyncGroup,
/// Heartbeat, LeaveGroup, ConsumerGroupHeartbeat), the transaction
/// apis, and the SASL exchange. Each is a *sequence* whose steps must
/// agree on a version and which leaves state behind, so sweeping them
/// means standing up a fresh member or producer per version rather than
/// re-sending one request. Worth doing; not done here.
async fn versions_advertised_are_speakable(ctx: &ServerCtx) -> Verdict {
    let produced = match produce_flow(ctx, "sweep", Addressing::Name).await {
        Ok(p) => p,
        Err(verdict) => return verdict,
    };
    let topic = produced.topic.clone();
    let topic_id = produced.topic_id;

    let mut failures: Vec<String> = Vec::new();
    let mut swept: Vec<String> = Vec::new();

    // Produce, by name. The id-addressed version is the newest one and
    // `produce/topic-id` already sends it.
    if let Ok(versions) = versions_of(
        ctx,
        ProduceRequest::API_KEY,
        "Produce",
        PRODUCE_RECORD_BATCH_MIN,
        PRODUCE_NAME_MAX,
    ) {
        for version in &versions {
            let correlation = 1_600 + i32::from(*version);
            // A fresh connection per version. A server that refuses a
            // version by closing the connection would otherwise condemn
            // every version after it, and the report would say thirteen
            // versions are unspeakable when one is.
            let mut conn = match connect(&produced.addr).await {
                Ok(c) => c,
                Err(e) => return e.into_verdict(),
            };
            // The code is not this check's business; that it answered in
            // the shape v{version} specifies is.
            if let Err(e) = produce_stamped(
                &mut conn,
                *version,
                ProduceStamp::plain(),
                &topic,
                0,
                correlation,
            )
            .await
            {
                failures.push(format!("Produce v{version}: {}", e.details()));
            }
        }
        swept.push(format!("Produce {}", span(&versions)));
    }

    // Fetch, by topic id — the half `fetch/batch-integrity` does not
    // reach, since it sweeps only the name-addressed versions.
    if topic_id != [0u8; 16] {
        if let Ok(versions) = versions_of(
            ctx,
            FetchRequest::API_KEY,
            "Fetch",
            FETCH_ID_MIN,
            FetchRequest::MAX_VERSION,
        ) {
            for version in &versions {
                let correlation = 1_640 + i32::from(*version);
                let mut conn = match connect(&produced.addr).await {
                    Ok(c) => c,
                    Err(e) => return e.into_verdict(),
                };
                if let Err(e) = sweep_fetch_by_id(&mut conn, *version, topic_id, correlation).await
                {
                    failures.push(format!("Fetch v{version} (by id): {}", e.details()));
                }
            }
            swept.push(format!("Fetch-by-id {}", span(&versions)));
        }
    }

    // OffsetCommit, to the group's coordinator.
    let group = check_group("sweep");
    let commit_max = if topic_id == [0u8; 16] {
        OFFSETS_BY_TOPIC_ID - 1
    } else {
        OffsetCommitRequest::MAX_VERSION
    };
    if let Ok(versions) = versions_of(
        ctx,
        OffsetCommitRequest::API_KEY,
        "OffsetCommit",
        OffsetCommitRequest::MIN_VERSION,
        commit_max,
    ) {
        for version in &versions {
            let correlation = 1_680 + i32::from(*version);
            let mut conn = match coordinator_conn(ctx, &group).await {
                Ok(c) => c,
                Err(e) => return e.context("locating the group coordinator").into_verdict(),
            };
            if let Err(e) =
                sweep_offset_commit(&mut conn, *version, &group, &topic, topic_id, correlation)
                    .await
            {
                failures.push(format!("OffsetCommit v{version}: {}", e.details()));
            }
        }
        swept.push(format!("OffsetCommit {}", span(&versions)));
    }

    // CreateTopics, asked not to create. v0 has no `validate_only`
    // field, so there it really does create — hence a fresh name per
    // version rather than one shared throughout.
    if let Ok(versions) = versions_of(
        ctx,
        CreateTopicsRequest::API_KEY,
        "CreateTopics",
        CreateTopicsRequest::MIN_VERSION,
        CreateTopicsRequest::MAX_VERSION,
    ) {
        for version in &versions {
            let mut conn = match connect(&ctx.addr).await {
                Ok(c) => c,
                Err(e) => return e.into_verdict(),
            };
            let name = unique_topic(&format!("sweepct{version}"));
            let correlation = 1_720 + i32::from(*version) * 4;
            if let Err(e) =
                create_topic_call(ctx, &mut conn, *version, &name, true, correlation).await
            {
                failures.push(format!("CreateTopics v{version}: {}", e.details()));
            }
        }
        swept.push(format!("CreateTopics {}", span(&versions)));
    }

    if swept.is_empty() {
        return Verdict::Skipped {
            reason: "the subject advertises none of the apis this sweeps".into(),
        };
    }
    if failures.is_empty() {
        return Verdict::Pass;
    }
    Verdict::Fail {
        details: format!(
            "{} advertised version(s) could not be spoken. Swept {}. {}",
            failures.len(),
            swept.join(", "),
            failures.join("; ")
        ),
    }
}

/// A topic-id-addressed fetch, decoded and thrown away.
///
/// Deliberately not [`run_fetch`], which also demands the response echo
/// the requested topic id — that is `fetch/topic-id`'s requirement, and
/// borrowing it here would make its fault trip two checks.
async fn sweep_fetch_by_id(
    conn: &mut RawConnection,
    version: i16,
    topic_id: [u8; 16],
    correlation: i32,
) -> Result<(), CheckError> {
    let mut partition = FetchPartition::default();
    partition.partition = 0;
    partition.current_leader_epoch = -1;
    partition.fetch_offset = 0;
    partition.last_fetched_epoch = -1;
    partition.log_start_offset = -1;
    partition.partition_max_bytes = 1 << 20;
    let mut fetch_topic = FetchTopic::default();
    fetch_topic.topic_id = topic_id;
    fetch_topic.partitions = vec![partition];
    let mut request = FetchRequest::default();
    request.max_wait_ms = 500;
    request.min_bytes = 0;
    request.max_bytes = 1 << 22;
    request.session_id = 0;
    request.session_epoch = -1;
    request.topics = vec![fetch_topic];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding Fetch: {e}")))?;
    let _: FetchResponse =
        api_call(conn, FetchRequest::API_KEY, version, correlation, &body).await?;
    Ok(())
}

/// An offset commit, decoded and thrown away.
///
/// Not [`commit_offset`], which reports a non-zero code as a violation.
/// Here the code is the coordinator's business: only the shape is this
/// check's.
async fn sweep_offset_commit(
    conn: &mut RawConnection,
    version: i16,
    group: &str,
    topic: &str,
    topic_id: [u8; 16],
    correlation: i32,
) -> Result<(), CheckError> {
    let mut partition = OffsetCommitRequestPartition::default();
    partition.partition_index = 0;
    partition.committed_offset = 1;
    partition.committed_leader_epoch = -1;
    let mut req_topic = OffsetCommitRequestTopic::default();
    if version >= OFFSETS_BY_TOPIC_ID {
        req_topic.topic_id = topic_id;
    } else {
        req_topic.name = topic.to_owned();
    }
    req_topic.partitions = vec![partition];
    let mut request = OffsetCommitRequest::default();
    request.group_id = group.to_owned();
    request.generation_id_or_member_epoch = -1;
    request.member_id = String::new();
    request.retention_time_ms = -1;
    request.topics = vec![req_topic];
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| CheckError::Infra(format!("encoding OffsetCommit: {e}")))?;
    let _: OffsetCommitResponse = api_call(
        conn,
        OffsetCommitRequest::API_KEY,
        version,
        correlation,
        &body,
    )
    .await?;
    Ok(())
}

/// The versions of `api_key` that both the subject advertises and this
/// sweep can send, or `Err` when there are none.
fn versions_of(
    ctx: &ServerCtx,
    api_key: i16,
    api: &'static str,
    min: i16,
    max: i16,
) -> Result<Vec<i16>, Verdict> {
    negotiate_all(api, ctx.range(api_key)?, min, max)
}

/// `v0-12`, or `v7` when there is only one.
fn span(versions: &[i16]) -> String {
    match (versions.first(), versions.last()) {
        (Some(first), Some(last)) if first != last => format!("v{first}-{last}"),
        (Some(only), _) => format!("v{only}"),
        _ => "none".into(),
    }
}
