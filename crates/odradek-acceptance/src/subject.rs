//! A reference subject server with fault injection.
//!
//! This is the suite's calibration instrument. Run with no faults it is a
//! minimal conformant ApiVersions responder; each [`Fault`] makes it commit
//! exactly one protocol violation. The sensitivity tests assert a 1:1
//! mapping between faults and the checks that claim to detect them — a
//! check that cannot catch its own targeted fault is vacuous, and a check
//! that fails against the compliant subject is wrong.

use std::collections::{HashMap, HashSet};
use std::io;

use crate::checks::BoxFuture;
use crate::checks::server::{Broker, ClusterControl};
use bytes::{Bytes, BytesMut};
use odradek_protocol::messages::add_offsets_to_txn_request::AddOffsetsToTxnRequest;
use odradek_protocol::messages::add_offsets_to_txn_response::AddOffsetsToTxnResponse;
use odradek_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnRequest;
use odradek_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResponse, AddPartitionsToTxnTopicResult,
};
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use odradek_protocol::messages::consumer_group_heartbeat_response::{
    Assignment, ConsumerGroupHeartbeatResponse, TopicPartitions,
};
use odradek_protocol::messages::create_topics_request::CreateTopicsRequest;
use odradek_protocol::messages::create_topics_response::{
    CreatableTopicResult, CreateTopicsResponse,
};
use odradek_protocol::messages::delete_topics_request::DeleteTopicsRequest;
use odradek_protocol::messages::delete_topics_response::{
    DeletableTopicResult, DeleteTopicsResponse,
};
use odradek_protocol::messages::describe_groups_request::DescribeGroupsRequest;
use odradek_protocol::messages::describe_groups_response::{
    DescribeGroupsResponse, DescribedGroup, DescribedGroupMember,
};
use odradek_protocol::messages::end_txn_request::EndTxnRequest;
use odradek_protocol::messages::end_txn_response::EndTxnResponse;
use odradek_protocol::messages::fetch_request::FetchRequest;
use odradek_protocol::messages::fetch_response::{
    AbortedTransaction, FetchResponse, FetchableTopicResponse, PartitionData,
};
use odradek_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use odradek_protocol::messages::find_coordinator_response::{Coordinator, FindCoordinatorResponse};
use odradek_protocol::messages::heartbeat_request::HeartbeatRequest;
use odradek_protocol::messages::heartbeat_response::HeartbeatResponse;
use odradek_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use odradek_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use odradek_protocol::messages::join_group_request::JoinGroupRequest;
use odradek_protocol::messages::join_group_response::{JoinGroupResponse, JoinGroupResponseMember};
use odradek_protocol::messages::leave_group_request::LeaveGroupRequest;
use odradek_protocol::messages::leave_group_response::LeaveGroupResponse;
use odradek_protocol::messages::list_offsets_request::ListOffsetsRequest;
use odradek_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
use odradek_protocol::messages::metadata_request::{self, MetadataRequest};
use odradek_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use odradek_protocol::messages::offset_commit_request::OffsetCommitRequest;
use odradek_protocol::messages::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use odradek_protocol::messages::offset_fetch_request::OffsetFetchRequest;
use odradek_protocol::messages::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
    OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use odradek_protocol::messages::produce_request::ProduceRequest;
use odradek_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use odradek_protocol::messages::sasl_authenticate_request::SaslAuthenticateRequest;
use odradek_protocol::messages::sasl_authenticate_response::SaslAuthenticateResponse;
use odradek_protocol::messages::sasl_handshake_request::SaslHandshakeRequest;
use odradek_protocol::messages::sasl_handshake_response::SaslHandshakeResponse;
use odradek_protocol::messages::sync_group_request::SyncGroupRequest;
use odradek_protocol::messages::sync_group_response::SyncGroupResponse;
use odradek_protocol::messages::txn_offset_commit_request::TxnOffsetCommitRequest;
use odradek_protocol::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use odradek_protocol::records;
use odradek_protocol::{ErrorCode, frame, header};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// The newest ApiVersions version the subject supports.
pub const MAX_SUPPORTED_API_VERSIONS: i16 = ApiVersionsRequest::MAX_VERSION;

/// The single broker this subject presents itself as.
/// The node id of the broker a client bootstraps against.
const BROKER_NODE_ID: i32 = 1;

/// How many brokers the reference subject presents.
///
/// Three, and not one, because the questions that matter most about a
/// Kafka cluster cannot be asked of a cluster of one. On a single
/// broker every partition's leader and every group's and transaction's
/// coordinator is the broker you are already connected to, so a client
/// — or a suite — that never consults Metadata is indistinguishable
/// from one that does. Three is also what the docker cluster subject
/// runs, which keeps the model and the thing it models the same shape.
///
/// Not two: with two nodes, "some other broker" is always the same
/// broker, and a check that meant to find a non-leader could find one by
/// accident rather than by looking.
const NODES: usize = 3;
/// ListOffsets sentinel timestamps: the log start and the log end.
const EARLIEST_TIMESTAMP: i64 = -2;
const LATEST_TIMESTAMP: i64 = -1;
/// "This group has committed nothing for this partition" — not an error.
const UNSET_OFFSET: i64 = -1;
/// The version at which FindCoordinator began batching keys.
const FIND_COORDINATOR_BATCHED: i16 = 4;
/// The version at which OffsetFetch began batching groups.
const OFFSET_FETCH_BATCHED: i16 = 8;

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
    /// Append junk bytes inside the ApiVersions response frame after the
    /// body.
    TrailingGarbage,
    /// Append junk bytes inside Fetch response frames after the body —
    /// proof the consolidated exchange path polices trailing bytes on the
    /// produce/fetch flows, not just ApiVersions and Metadata.
    FetchTrailingGarbage,
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
    /// Answer a topic-id-addressed produce with UNKNOWN_TOPIC_ID even
    /// though the id was minted by this subject's CreateTopics.
    ProduceTopicIdUnknown,
    /// Report a log start that is not 0, though every log in this
    /// subject starts at 0.
    ListOffsetsWrongEarliest,
    /// Echo a coordinator key the request did not ask about.
    FindCoordinatorWrongKey,
    /// Report a committed offset as never-committed.
    OffsetFetchLosesCommit,
    /// Answer a never-committed partition with 0 rather than the -1
    /// sentinel — a plausible offset where "nothing here" was meant.
    OffsetFetchUnsetIsZero,
    /// Answer with a server nonce that replaces the client's instead of
    /// extending it.
    ScramNonceReplacesClients,
    /// Name an iteration count far below the RFC 7677 floor, weakening
    /// the key derivation every client is forced to spend.
    ScramWeakIterations,
    /// Complete the exchange without the `v=` signature, so the client
    /// has no way to know whether the server holds the account at all.
    ScramSkipsServerSignature,
    /// Every broker names itself the leader of every partition, so a
    /// client is routed to a different broker depending on which one it
    /// last refreshed metadata against.
    BrokersDisagreeOnLeader,
    /// Report a replicated partition as living on one broker, so a
    /// client has no idea how much of the cluster it would take to lose
    /// the data.
    ReplicasCollapseToTheLeader,
    /// Every broker names itself the coordinator, so a group has as
    /// many coordinators as it has bootstrap addresses.
    CoordinatorIsWhoeverAsked,
    /// Take a write on a broker that does not lead the partition,
    /// giving it two histories.
    AnyBrokerAcceptsWrites,
    /// Store a group's offsets on whichever broker was asked, where its
    /// coordinator will never see them.
    AnyBrokerServesGroups,
    /// Keep naming a stopped broker as the leader, leaving the
    /// partition unwritable for as long as it stays down.
    LeadershipStaysWithTheStoppedBroker,
    /// Lose a group's committed offsets with the broker that
    /// coordinated them, so a consumer that resumes after a failure
    /// reprocesses everything since.
    OffsetsDieWithTheirCoordinator,
    /// Answer one Produce version in the shape of another, while
    /// continuing to advertise it. The range stays well-formed and
    /// every other version works, which is what makes this the
    /// plausible bug rather than an obvious one — a schema bumped
    /// without its handler, found only by a client that happens to
    /// negotiate that version.
    ///
    /// v5 because nothing else in the suite sends it: the produce
    /// checks negotiate the highest version on offer, so a fault on
    /// one of those would trip them too, and a fault that trips two
    /// checks is evidence about neither.
    MisshapesOneProduceVersion,
    /// Refuse an unsupported mechanism without naming any supported
    /// one, leaving the client nothing to fall back to.
    SaslHandshakeHidesMechanisms,
    /// Accept a SASL token on a connection that never negotiated a
    /// mechanism.
    SaslAuthenticateWithoutHandshake,
    /// Never advance a KIP-848 member past epoch 0.
    ConsumerGroupEpochStuck,
    /// Treat an omitted `subscribed_topic_names` as "subscribed to
    /// nothing" rather than "unchanged", revoking a steady-state
    /// member's assignment.
    ConsumerGroupNullSubscriptionRevokes,
    /// Assign nothing, however the member subscribed.
    ConsumerGroupAssignsNothing,
    /// Accept any member epoch.
    ConsumerGroupIgnoresEpoch,
    /// Admit a join with no member id instead of answering
    /// MEMBER_ID_REQUIRED with one to retry with.
    JoinGroupAcceptsEmptyMemberId,
    /// Alter the assignment bytes the leader supplied before handing
    /// them to their member.
    SyncGroupRewritesAssignment,
    /// Accept any generation on a post-join request.
    GroupIgnoresGeneration,
    /// Answer a fetch past the high watermark with an empty batch set
    /// instead of OFFSET_OUT_OF_RANGE.
    FetchPastEndSucceeds,
    /// Omit an unknown topic from a Metadata response instead of naming
    /// it with UNKNOWN_TOPIC_OR_PARTITION — the client cannot tell
    /// "absent" from "never mentioned".
    MetadataUnknownTopicOmitted,
    /// Answer a second CreateTopics for an existing topic with NONE.
    CreateTopicsDuplicateSucceeds,
    /// Create the topic even though the request said validate_only.
    CreateTopicsValidateOnlyCreates,
    /// Corrupt stored batches only when the fetch was made at the
    /// *lowest* version this subject advertises.
    ///
    /// This one exists to test the suite rather than a subject. The
    /// lowest version specifically, rather than "anything below the
    /// maximum": the fetch check caps itself below the advertised
    /// maximum for name addressing, so a fault keyed on that cap would
    /// fire at the check's own top version and prove nothing. Keyed
    /// here, it is invisible to any check that negotiates one version
    /// and stops — and calibration fails the moment the fetch check
    /// stops sweeping the range.
    FetchCorruptOnOldVersions,
    /// Echo a different topic id than the fetch requested.
    FetchWrongTopicId,

    /// Answer an acks=0 produce, putting a frame on the wire the client
    /// has no correlation id outstanding for.
    ProduceAnswersAcksZero,
    /// Report a different topic id each time for a topic that never
    /// went away.
    MetadataRemintsTopicId,
    /// Accept a transactional write carrying a superseded epoch at the
    /// partition leader, so a zombie's records land inside the
    /// transaction its successor commits.
    ProduceAcceptsFencedEpoch,
    /// Report a committed transaction as aborted, so every reader
    /// throws its records away on purpose.
    TxnCommitMarksAborted,
    /// Publish an offset committed inside a transaction straight away,
    /// so the input reads as processed while the output can still be
    /// thrown away.
    TxnOffsetsPublishImmediately,
    /// Re-stamp a gzip batch before storing it, rewriting bytes the
    /// producer's crc covered. The records still decode, which is what
    /// makes it quiet.
    ///
    /// One per codec, rather than one for all of them, because each
    /// codec has a check of its own: a single fault that rewrote every
    /// compressed batch would trip four checks at once, and a fault
    /// that trips four checks is evidence about none of them.
    ProduceRewritesGzipBatches,
    /// The same, for snappy.
    ProduceRewritesSnappyBatches,
    /// The same, for lz4.
    ProduceRewritesLz4Batches,
    /// The same, for zstd.
    ProduceRewritesZstdBatches,
    /// Answer a fetch that cannot be satisfied immediately instead of
    /// waiting out `max_wait_ms`, turning every caught-up consumer into
    /// a busy loop.
    FetchIgnoresMaxWait,
    /// Name a partition leader that is not in the response's own broker
    /// list, leaving a client with nowhere to route to and no error to
    /// explain it.
    MetadataLeaderIsUnknown,
    /// Describe a live group as having no members, which reads as idle.
    DescribeGroupsHidesMembers,
    /// Store a commit without the metadata string it carried, so the
    /// offset reads back fine and the client's own state is gone.
    OffsetCommitDropsMetadata,
    /// Acknowledge a LeaveGroup and keep the member, so the coordinator
    /// goes on believing a departed member owns its partitions.
    LeaveGroupKeepsTheMember,
    /// Create a topic at whatever replication factor is available
    /// instead of refusing one the cluster cannot satisfy.
    CreateTopicsIgnoresReplicationFactor,
    /// Answer a timestamp lookup with the log end instead of searching,
    /// so a consumer seeking past the last record is told it is caught
    /// up rather than that there is nothing there.
    ListOffsetsTimestampReturnsLogEnd,
    /// Report a topic deleted and keep it, so the caller has no way to
    /// tell the deletion happened.
    DeleteTopicsKeepsTheTopic,
    /// Append a batch whose (producer id, sequence) the partition has
    /// already stored, so a producer retrying a lost acknowledgement
    /// writes its records twice.
    ProduceAppendsIdempotentRetries,
    /// Accept a stamped batch whose sequence skips past what the
    /// partition last took, abandoning the ordering the producer was
    /// promised without saying so.
    ProduceAcceptsSequenceGaps,
    /// Hand a producer re-taking a transactional id the same epoch its
    /// predecessor had, so the two are indistinguishable and neither is
    /// fenced.
    TxnInitReusesEpoch,
    /// Honour a transaction request carrying a superseded epoch, letting
    /// a fenced producer write into its successor's transaction.
    TxnIgnoresProducerEpoch,
    /// Accept a transactional write to a partition the transaction never
    /// announced *and* leave it out of the transaction, so no marker
    /// covers it and aborting does not disown it.
    TxnUnannouncedWriteEscapes,
    /// Report the last stable offset as the end of the log even with a
    /// transaction open, offering read_committed consumers records that
    /// may yet be aborted.
    TxnStableOffsetIgnoresOpenTxn,
    /// Finish an aborted transaction without telling readers it was
    /// aborted, so its records read as data.
    TxnAbortListOmitted,
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
        Fault::FetchTrailingGarbage,
        Fault::FlexibleHeaderOnV3,
        Fault::MetadataEmptyBrokers,
        Fault::MetadataUnrequestedTopic,
        Fault::MetadataNonFlexibleHeader,
        Fault::ProduceWrongBaseOffset,
        Fault::FetchCorruptBatch,
        Fault::ProduceTopicIdUnknown,
        Fault::FetchWrongTopicId,
        Fault::ProduceAnswersAcksZero,
        Fault::MetadataRemintsTopicId,
        Fault::ProduceAcceptsFencedEpoch,
        Fault::TxnCommitMarksAborted,
        Fault::TxnOffsetsPublishImmediately,
        Fault::ProduceRewritesGzipBatches,
        Fault::ProduceRewritesSnappyBatches,
        Fault::ProduceRewritesLz4Batches,
        Fault::ProduceRewritesZstdBatches,
        Fault::FetchIgnoresMaxWait,
        Fault::MetadataLeaderIsUnknown,
        Fault::DescribeGroupsHidesMembers,
        Fault::OffsetCommitDropsMetadata,
        Fault::LeaveGroupKeepsTheMember,
        Fault::CreateTopicsIgnoresReplicationFactor,
        Fault::ListOffsetsTimestampReturnsLogEnd,
        Fault::DeleteTopicsKeepsTheTopic,
        Fault::ProduceAppendsIdempotentRetries,
        Fault::ProduceAcceptsSequenceGaps,
        Fault::TxnInitReusesEpoch,
        Fault::TxnIgnoresProducerEpoch,
        Fault::TxnUnannouncedWriteEscapes,
        Fault::TxnStableOffsetIgnoresOpenTxn,
        Fault::TxnAbortListOmitted,
        Fault::ListOffsetsWrongEarliest,
        Fault::FindCoordinatorWrongKey,
        Fault::OffsetFetchLosesCommit,
        Fault::OffsetFetchUnsetIsZero,
        Fault::FetchCorruptOnOldVersions,
        Fault::FetchPastEndSucceeds,
        Fault::MetadataUnknownTopicOmitted,
        Fault::CreateTopicsDuplicateSucceeds,
        Fault::CreateTopicsValidateOnlyCreates,
        Fault::JoinGroupAcceptsEmptyMemberId,
        Fault::SyncGroupRewritesAssignment,
        Fault::GroupIgnoresGeneration,
        Fault::ConsumerGroupEpochStuck,
        Fault::ConsumerGroupNullSubscriptionRevokes,
        Fault::ConsumerGroupAssignsNothing,
        Fault::ConsumerGroupIgnoresEpoch,
        Fault::SaslHandshakeHidesMechanisms,
        Fault::SaslAuthenticateWithoutHandshake,
        Fault::ScramNonceReplacesClients,
        Fault::ScramWeakIterations,
        Fault::ScramSkipsServerSignature,
        Fault::BrokersDisagreeOnLeader,
        Fault::ReplicasCollapseToTheLeader,
        Fault::CoordinatorIsWhoeverAsked,
        Fault::AnyBrokerAcceptsWrites,
        Fault::AnyBrokerServesGroups,
        Fault::LeadershipStaysWithTheStoppedBroker,
        Fault::OffsetsDieWithTheirCoordinator,
        Fault::MisshapesOneProduceVersion,
    ];
}

/// A running subject server bound to an ephemeral local port.
#[derive(Debug)]
pub struct SubjectServer {
    addr: String,
    handles: Vec<JoinHandle<()>>,
    cluster: Cluster,
}

impl SubjectServer {
    /// Spawn a subject exhibiting `faults` (none = conformant).
    ///
    /// Three listeners over one shared cluster state, which between
    /// them are a cluster: each leads the partitions and coordinates the
    /// groups that hash to it, and refers a client that asks the wrong
    /// one onwards. [`addr`](Self::addr) is the bootstrap; the rest are
    /// found through Metadata, as on any cluster.
    pub async fn spawn(faults: Vec<Fault>) -> io::Result<SubjectServer> {
        let mut listeners = Vec::with_capacity(NODES);
        for _ in 0..NODES {
            listeners.push(TcpListener::bind("127.0.0.1:0").await?);
        }
        let addr = listeners[0].local_addr()?.to_string();
        let cluster: Cluster = Cluster::default();
        // The ports have to be in the state before the first request is
        // served: a Metadata answer names every broker, including the
        // ones nothing has connected to yet.
        {
            let mut state = cluster.lock().unwrap();
            state.faults.clone_from(&faults);
            for listener in &listeners {
                state.ports.push(i32::from(listener.local_addr()?.port()));
            }
        }

        let mut handles = Vec::with_capacity(NODES);
        for (index, listener) in listeners.into_iter().enumerate() {
            let faults = faults.clone();
            let cluster = std::sync::Arc::clone(&cluster);
            let node_id = i32::try_from(index + 1).unwrap_or(BROKER_NODE_ID);
            handles.push(tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    // A stopped broker is still bound — rebinding the
                    // same port later is a race worth not running — but
                    // it serves nobody. Dropping the stream here is what
                    // a client sees as the broker being gone.
                    if cluster.lock().unwrap().is_down(node_id) {
                        continue;
                    }
                    tokio::spawn(handle_connection(
                        stream,
                        faults.clone(),
                        std::sync::Arc::clone(&cluster),
                        node_id,
                    ));
                }
            }));
        }
        Ok(SubjectServer {
            addr,
            handles,
            cluster,
        })
    }

    /// The broker a client bootstraps against.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// A handle that stops and starts this subject's brokers, for the
    /// checks that need one.
    ///
    /// In-process rather than out: the calibration subject's brokers are
    /// tasks, not containers, so there is nothing to shell out to. The
    /// point is that the checks cannot tell — they are handed a
    /// [`ClusterControl`] either way, and the one the CLI builds runs
    /// somebody's `docker stop`.
    pub fn control(&self) -> std::sync::Arc<dyn ClusterControl> {
        std::sync::Arc::new(SubjectControl {
            cluster: std::sync::Arc::clone(&self.cluster),
        })
    }
}

/// [`ClusterControl`] over a [`SubjectServer`]'s own brokers.
#[derive(Debug)]
struct SubjectControl {
    cluster: Cluster,
}

impl ClusterControl for SubjectControl {
    fn stop(&self, broker: Broker<'_>) -> BoxFuture<'_, Result<(), String>> {
        let node_id = broker.node_id;
        Box::pin(async move {
            let mut state = self.cluster.lock().unwrap();
            // Committed offsets are replicated across the cluster, so
            // the broker that served them going away loses nothing.
            // The fault models the cluster that kept each group's
            // offsets on its coordinator alone: they go when it does.
            if state
                .faults
                .contains(&Fault::OffsetsDieWithTheirCoordinator)
            {
                let all: Vec<i32> = state.nodes().map(|(id, _)| id).collect();
                let orphaned: Vec<(String, String, i32)> = state
                    .committed
                    .keys()
                    .filter(|(group, _, _)| state.placement_over(group, &all) == node_id)
                    .cloned()
                    .collect();
                for key in orphaned {
                    state.committed.remove(&key);
                }
            }
            state.down.insert(node_id);
            Ok(())
        })
    }

    fn start(&self, broker: Broker<'_>) -> BoxFuture<'_, Result<(), String>> {
        let node_id = broker.node_id;
        Box::pin(async move {
            self.cluster.lock().unwrap().down.remove(&node_id);
            Ok(())
        })
    }
}

impl Drop for SubjectServer {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

fn advertised_keys() -> Vec<ApiVersion> {
    // Only versions the subject actually implements — the full schema
    // ranges: its produce/fetch handlers resolve topic ids, so even the
    // id-addressed (v13+) versions are served.
    [
        (
            ApiVersionsRequest::API_KEY,
            ApiVersionsRequest::MIN_VERSION,
            MAX_SUPPORTED_API_VERSIONS,
        ),
        (
            ProduceRequest::API_KEY,
            ProduceRequest::MIN_VERSION,
            ProduceRequest::MAX_VERSION,
        ),
        (
            FetchRequest::API_KEY,
            FetchRequest::MIN_VERSION,
            FetchRequest::MAX_VERSION,
        ),
        (
            MetadataRequest::API_KEY,
            MetadataRequest::MIN_VERSION,
            MetadataRequest::MAX_VERSION,
        ),
        (
            CreateTopicsRequest::API_KEY,
            CreateTopicsRequest::MIN_VERSION,
            CreateTopicsRequest::MAX_VERSION,
        ),
        (
            ListOffsetsRequest::API_KEY,
            ListOffsetsRequest::MIN_VERSION,
            ListOffsetsRequest::MAX_VERSION,
        ),
        (
            FindCoordinatorRequest::API_KEY,
            FindCoordinatorRequest::MIN_VERSION,
            FindCoordinatorRequest::MAX_VERSION,
        ),
        (
            DescribeGroupsRequest::API_KEY,
            DescribeGroupsRequest::MIN_VERSION,
            DescribeGroupsRequest::MAX_VERSION,
        ),
        (
            DeleteTopicsRequest::API_KEY,
            DeleteTopicsRequest::MIN_VERSION,
            DeleteTopicsRequest::MAX_VERSION,
        ),
        (
            InitProducerIdRequest::API_KEY,
            InitProducerIdRequest::MIN_VERSION,
            InitProducerIdRequest::MAX_VERSION,
        ),
        (
            AddPartitionsToTxnRequest::API_KEY,
            AddPartitionsToTxnRequest::MIN_VERSION,
            AddPartitionsToTxnRequest::MAX_VERSION,
        ),
        (
            EndTxnRequest::API_KEY,
            EndTxnRequest::MIN_VERSION,
            EndTxnRequest::MAX_VERSION,
        ),
        (
            AddOffsetsToTxnRequest::API_KEY,
            AddOffsetsToTxnRequest::MIN_VERSION,
            AddOffsetsToTxnRequest::MAX_VERSION,
        ),
        (
            TxnOffsetCommitRequest::API_KEY,
            TxnOffsetCommitRequest::MIN_VERSION,
            TxnOffsetCommitRequest::MAX_VERSION,
        ),
        (
            OffsetCommitRequest::API_KEY,
            OffsetCommitRequest::MIN_VERSION,
            OffsetCommitRequest::MAX_VERSION,
        ),
        (
            OffsetFetchRequest::API_KEY,
            OffsetFetchRequest::MIN_VERSION,
            OffsetFetchRequest::MAX_VERSION,
        ),
        (
            JoinGroupRequest::API_KEY,
            JoinGroupRequest::MIN_VERSION,
            JoinGroupRequest::MAX_VERSION,
        ),
        (
            SyncGroupRequest::API_KEY,
            SyncGroupRequest::MIN_VERSION,
            SyncGroupRequest::MAX_VERSION,
        ),
        (
            HeartbeatRequest::API_KEY,
            HeartbeatRequest::MIN_VERSION,
            HeartbeatRequest::MAX_VERSION,
        ),
        (
            LeaveGroupRequest::API_KEY,
            LeaveGroupRequest::MIN_VERSION,
            LeaveGroupRequest::MAX_VERSION,
        ),
        (
            ConsumerGroupHeartbeatRequest::API_KEY,
            ConsumerGroupHeartbeatRequest::MIN_VERSION,
            ConsumerGroupHeartbeatRequest::MAX_VERSION,
        ),
        (
            SaslHandshakeRequest::API_KEY,
            SaslHandshakeRequest::MIN_VERSION,
            SaslHandshakeRequest::MAX_VERSION,
        ),
        (
            SaslAuthenticateRequest::API_KEY,
            SaslAuthenticateRequest::MIN_VERSION,
            SaslAuthenticateRequest::MAX_VERSION,
        ),
    ]
    .into_iter()
    .map(|(api_key, min_version, max_version)| {
        let mut v = ApiVersion::default();
        v.api_key = api_key;
        v.min_version = min_version;
        v.max_version = max_version;
        v
    })
    .collect()
}

/// One partition's log: appended record sets, the next offset to
/// assign, and what transactions have done to it.
#[derive(Debug, Default)]
struct PartitionLog {
    bytes: BytesMut,
    next_offset: i64,
    /// Producers with a transaction open here, and the offset their
    /// first record landed at. The lowest of these is the last stable
    /// offset: nothing at or past it is decided yet.
    open: HashMap<i64, i64>,
    /// Finished transactions that were aborted: (producer id, first
    /// offset). Reported to `read_committed` fetches, which is the only
    /// way a reader learns those records were thrown away.
    aborted: Vec<(i64, i64)>,
    /// What each producer last wrote here: the sequence range it
    /// claimed and the offset that batch landed at.
    ///
    /// This is the whole of idempotence on the broker side. Keeping the
    /// offset as well as the sequence is what lets a recognized retry be
    /// answered with the original position rather than a new one.
    last_batch: HashMap<i64, LastBatch>,
}

/// The last batch one producer wrote to a partition.
#[derive(Debug, Clone, Copy)]
struct LastBatch {
    base_sequence: i32,
    /// One past the last sequence the batch claimed — the sequence the
    /// next batch from this producer must start at.
    next_sequence: i32,
    base_offset: i64,
}

impl PartitionLog {
    /// The last stable offset: the first offset belonging to a
    /// transaction that has not finished, or the end of the log when
    /// none has started.
    fn last_stable_offset(&self) -> i64 {
        self.open
            .values()
            .copied()
            .min()
            .unwrap_or(self.next_offset)
    }
}

/// One transactional id's state, as the coordinator sees it.
#[derive(Debug, Default)]
struct TxnState {
    producer_id: i64,
    epoch: i16,
    /// Partitions announced for the transaction currently open.
    partitions: HashSet<(String, i32)>,
    /// Offsets committed inside the open transaction, held back until
    /// it commits. This is the exactly-once half: the input is not
    /// marked processed while the output can still be thrown away.
    pending_offsets: HashMap<(String, String, i32), (i64, Option<String>)>,
}

/// Broker state as the connections share it.
///
/// A plain `std` mutex, not an async one: every handler but the fetch is
/// synchronous and holds the lock only for the length of one exchange.
/// The fetch takes it twice instead of holding it across its long poll —
/// see [`fetch_exchange`].
type Cluster = std::sync::Arc<std::sync::Mutex<ClusterState>>;

/// Broker state, shared by every connection to this subject.
///
/// It was per-connection once, on the reasoning that concurrent checks
/// would otherwise leak into each other. That reasoning was wrong twice
/// over. Checks already name their topics and groups per run and per
/// check precisely so that sharing is safe, and a broker whose topics
/// exist only on the connection that created them is not modelling a
/// broker at all — it is modelling the suite's old habit of doing
/// everything down one socket. The moment checks began routing to
/// leaders and coordinators, a topic created on one connection had to be
/// visible from another, exactly as on a real cluster.
///
/// Only the SASL exchange is genuinely per-connection, and it stays
/// there.
#[derive(Debug, Default)]
struct ClusterState {
    logs: HashMap<(String, i32), PartitionLog>,
    /// Topic ids minted by CreateTopics, keyed by id.
    topic_names: HashMap<[u8; 16], String>,
    /// Committed offsets, keyed by (group, topic, partition).
    committed: HashMap<(String, String, i32), (i64, Option<String>)>,
    /// Consumer groups the cluster holds (classic protocol).
    groups: HashMap<String, GroupState>,
    /// KIP-848 members, keyed by (group, member id).
    members_848: HashMap<(String, String), Member848>,
    /// Transactions by transactional id.
    txns: HashMap<String, TxnState>,
    /// The next producer id to hand out.
    next_producer_id: i64,
    /// Topics CreateTopics actually created. Distinct from `logs`, which
    /// only gains an entry once something is produced, and from
    /// `topic_names`, which maps ids: a topic can exist and be empty.
    created: HashMap<String, [u8; 16]>,
    /// The port each broker listens on, indexed by node id minus one.
    /// Filled in at spawn, once the listeners have their ports.
    ports: Vec<i32>,
    /// The faults this subject exhibits. Fixed for its lifetime, and
    /// kept here as well as per connection because stopping a broker is
    /// a cluster-wide event with no connection to hang off.
    faults: Vec<Fault>,
    /// Nodes currently stopped.
    ///
    /// A stopped node keeps its listener bound — rebinding the same port
    /// later is a race this has no need to run — but refuses every
    /// connection the moment it is accepted, and drops the ones it
    /// already had. To a client that is a broker that has gone away,
    /// which is all the checks need it to be.
    down: HashSet<i32>,
}

impl ClusterState {
    /// Which node leads every partition of `topic`, or coordinates the
    /// group or transactional id `key`.
    ///
    /// A hash of the name rather than anything stored, so it is the same
    /// answer from every broker without any agreement protocol between
    /// them — and different names land on different nodes, which is what
    /// makes "go and ask the right one" a thing a check can observe.
    fn placement(&self, key: &str) -> i32 {
        self.placement_over(key, &self.live())
    }

    /// The same, restricted to a given set of candidates.
    ///
    /// Leadership and coordination are only ever assigned to brokers
    /// that are up: a cluster that kept naming a stopped broker would
    /// leave its partitions unwritable and its groups uncommittable for
    /// as long as it stayed down, which is the whole of what failing
    /// over means.
    fn placement_over(&self, key: &str, candidates: &[i32]) -> i32 {
        if candidates.is_empty() {
            return BROKER_NODE_ID;
        }
        let mut acc: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in key.bytes() {
            acc ^= u64::from(byte);
            acc = acc.wrapping_mul(0x0000_0100_0000_01b3);
        }
        let index = usize::try_from(acc % candidates.len() as u64).unwrap_or(0);
        candidates.get(index).copied().unwrap_or(BROKER_NODE_ID)
    }

    /// Node ids that are up, in order.
    fn live(&self) -> Vec<i32> {
        self.nodes()
            .map(|(id, _)| id)
            .filter(|id| !self.down.contains(id))
            .collect()
    }

    fn is_down(&self, node_id: i32) -> bool {
        self.down.contains(&node_id)
    }

    /// The port a node id listens on.
    fn port_of(&self, node_id: i32) -> i32 {
        usize::try_from(node_id - 1)
            .ok()
            .and_then(|i| self.ports.get(i))
            .copied()
            .unwrap_or_default()
    }

    /// Every broker, as node id and port.
    fn nodes(&self) -> impl Iterator<Item = (i32, i32)> + '_ {
        self.ports
            .iter()
            .enumerate()
            .map(|(i, port)| (i32::try_from(i + 1).unwrap_or(BROKER_NODE_ID), *port))
    }
}

/// One consumer group, as much of it as the classic protocol needs.
///
/// Writing this down is what surfaced the checks that go with it. Three
/// things the request/response schemas do not say, and an implementer
/// has to decide:
///
/// 1. A join with no member id cannot simply be given one and waved
///    through — from JoinGroup v4 the coordinator answers
///    MEMBER_ID_REQUIRED *and* hands back the id to retry with, so a
///    client always rejoins with an id the coordinator minted. Without
///    that round trip a client that dies mid-join leaves a member
///    nobody can name.
/// 2. The assignment bytes are the group leader's to decide and the
///    coordinator's only to deliver. They are opaque: the coordinator
///    that parses or rewrites them has broken the same guarantee a
///    proxy breaks by re-encoding a record batch.
/// 3. The generation is the fence. Every later request carries it, and
///    one that carries a stale one has to be refused rather than
///    applied, or a member evicted during a rebalance quietly keeps
///    acting on an assignment it no longer owns.
#[derive(Debug, Default)]
struct GroupState {
    generation_id: i32,
    /// Members in join order; the first is the leader.
    members: Vec<String>,
    protocol_type: String,
    protocol_name: String,
    /// Set by the leader at SyncGroup, handed back verbatim.
    assignments: HashMap<String, Bytes>,
    /// Member ids minted for a join that had none, awaiting the rejoin.
    minted: Vec<String>,
}

/// How far along a connection's SASL exchange is.
///
/// The exchange is a sequence, and writing it as one is what surfaces
/// the requirements. A token that arrives before the handshake has
/// chosen a mechanism cannot be interpreted at all — there is no
/// mechanism to interpret it under — so it is refused as a *state*
/// error rather than as bad credentials. The two answers are different
/// on purpose: one tells a client its code is wrong, the other tells it
/// its password is.
#[derive(Debug, Default)]
enum SaslState {
    /// Nothing has been negotiated on this connection.
    #[default]
    Unstarted,
    /// A handshake named this mechanism; tokens are now interpretable.
    Negotiated(String),
    /// A SCRAM exchange is mid-flight: the client's first bare message
    /// and our first message, both needed to rebuild the auth message
    /// the proof is computed over.
    ScramPending(Box<odradek_sasl::ScramServer>),
    /// Authentication completed.
    Authenticated,
}

/// The mechanisms this subject claims. The list is what a client falls
/// back on, so a refusal that omits it leaves the client with nothing to
/// try next.
const SASL_MECHANISMS: &[&str] = &["SCRAM-SHA-256", "PLAIN"];

/// The one account this subject knows. Published, because the checks
/// that exercise a full SCRAM exchange need both halves of it and there
/// is nothing here worth protecting.
pub const SCRAM_USER: &str = "conformance";
pub const SCRAM_PASSWORD: &str = "conformance";
/// RFC 7677 makes 4096 the floor for SCRAM-SHA-256.
const SCRAM_ITERATIONS: u32 = 4096;
/// A fixed salt. Real servers vary it per account; this one has one
/// account and the checks need the exchange to be reproducible.
const SCRAM_SALT: &[u8] = b"odradek-acceptance-salt";

/// One KIP-848 member.
///
/// The decisions this shape forced, none of which the schema states:
///
/// 1. **The member names itself.** Classic JoinGroup has the coordinator
///    mint the id; here the client generates one and the first heartbeat
///    arrives carrying it at epoch 0. The coordinator accepts it and
///    answers with the epoch it has been admitted at, which is never 0 —
///    a member at epoch 0 has not been admitted yet, so "still 0" is how
///    a client learns nothing happened.
/// 2. **An omitted subscription means unchanged, not empty.** Every
///    field a heartbeat can omit is one the client is saying nothing
///    about. A coordinator that reads `subscribed_topic_names: null` as
///    "subscribed to nothing" revokes the assignment of every member
///    that is simply idling correctly — and the steady-state heartbeat
///    is exactly the one that omits everything.
/// 3. **Assignments are addressed by topic id.** The coordinator has to
///    resolve the names a member subscribed by into ids, which means a
///    subscription naming a topic that does not exist is not an error,
///    it is an assignment that does not mention it.
#[derive(Debug, Default)]
struct Member848 {
    epoch: i32,
    /// Last subscription the member actually stated.
    subscribed: Vec<String>,
    /// The assignment this member was last *told*. The response carries
    /// one only when it differs — the same rule the request follows in
    /// the other direction, and the reason a steady-state heartbeat is
    /// nearly empty in both directions.
    told: Option<Vec<([u8; 16], Vec<i32>)>>,
}

/// A deterministic per-name topic id; never the zero uuid.
fn mint_topic_id(name: &str) -> [u8; 16] {
    let mut id = [0u8; 16];
    let mut acc: u8 = 0x9e;
    for (i, byte) in name.bytes().enumerate() {
        acc = acc.wrapping_mul(31).wrapping_add(byte);
        id[i % 16] ^= acc.rotate_left(u32::try_from(i % 7).unwrap_or(0));
    }
    id[0] |= 1;
    id
}

async fn handle_connection(
    mut stream: TcpStream,
    faults: Vec<Fault>,
    cluster: Cluster,
    node_id: i32,
) {
    // The SASL exchange is this connection's; everything else belongs to
    // the cluster and is locked per request.
    let mut sasl = SaslState::default();
    loop {
        let mut len_bytes = [0u8; 4];
        if stream.read_exact(&mut len_bytes).await.is_err() {
            return;
        }
        // A negative or implausibly large length prefix is a wire
        // violation: close the connection, never clamp.
        let Ok(len) = frame::check_len(len_bytes, frame::DEFAULT_MAX_FRAME) else {
            return;
        };
        let mut frame = vec![0u8; len];
        if stream.read_exact(&mut frame).await.is_err() {
            return;
        }
        let frame = Bytes::from(frame);
        if frame.len() < 4 {
            return;
        }
        // Stopped part way through a connection: the broker is gone,
        // so the connection goes with it. A client that kept getting
        // answers from a stopped broker would never look elsewhere.
        if cluster.lock().unwrap().is_down(node_id) {
            return;
        }
        let api_key = i16::from_be_bytes([frame[0], frame[1]]);
        let api_version = i16::from_be_bytes([frame[2], frame[3]]);
        let out = match api_key {
            ApiVersionsRequest::API_KEY => api_versions_exchange(frame, api_version, &faults),
            MetadataRequest::API_KEY => metadata_exchange(
                frame,
                api_version,
                node_id,
                &faults,
                &cluster.lock().unwrap(),
            ),
            CreateTopicsRequest::API_KEY => {
                create_topics_exchange(frame, api_version, &faults, &mut cluster.lock().unwrap())
            }
            ProduceRequest::API_KEY => produce_exchange(
                frame,
                api_version,
                node_id,
                &faults,
                &mut cluster.lock().unwrap(),
            ),
            FetchRequest::API_KEY => fetch_exchange(frame, api_version, &faults, &cluster).await,
            ListOffsetsRequest::API_KEY => {
                list_offsets_exchange(frame, api_version, &faults, &cluster.lock().unwrap())
            }
            FindCoordinatorRequest::API_KEY => find_coordinator_exchange(
                frame,
                api_version,
                node_id,
                &faults,
                &cluster.lock().unwrap(),
            ),
            OffsetCommitRequest::API_KEY => offset_commit_exchange(
                frame,
                api_version,
                node_id,
                &faults,
                &mut cluster.lock().unwrap(),
            ),
            OffsetFetchRequest::API_KEY => {
                offset_fetch_exchange(frame, api_version, &faults, &cluster.lock().unwrap())
            }
            JoinGroupRequest::API_KEY => {
                join_group_exchange(frame, api_version, &faults, &mut cluster.lock().unwrap())
            }
            SyncGroupRequest::API_KEY => {
                sync_group_exchange(frame, api_version, &faults, &mut cluster.lock().unwrap())
            }
            HeartbeatRequest::API_KEY => {
                heartbeat_exchange(frame, api_version, &faults, &mut cluster.lock().unwrap())
            }
            LeaveGroupRequest::API_KEY => {
                leave_group_exchange(frame, api_version, &faults, &mut cluster.lock().unwrap())
            }
            ConsumerGroupHeartbeatRequest::API_KEY => consumer_group_heartbeat_exchange(
                frame,
                api_version,
                &faults,
                &mut cluster.lock().unwrap(),
            ),
            DescribeGroupsRequest::API_KEY => {
                describe_groups_exchange(frame, api_version, &faults, &cluster.lock().unwrap())
            }
            DeleteTopicsRequest::API_KEY => {
                delete_topics_exchange(frame, api_version, &faults, &mut cluster.lock().unwrap())
            }
            InitProducerIdRequest::API_KEY => {
                init_producer_id_exchange(frame, api_version, &faults, &mut cluster.lock().unwrap())
            }
            AddPartitionsToTxnRequest::API_KEY => add_partitions_to_txn_exchange(
                frame,
                api_version,
                &faults,
                &mut cluster.lock().unwrap(),
            ),
            EndTxnRequest::API_KEY => {
                end_txn_exchange(frame, api_version, &faults, &mut cluster.lock().unwrap())
            }
            AddOffsetsToTxnRequest::API_KEY => add_offsets_to_txn_exchange(
                frame,
                api_version,
                &faults,
                &mut cluster.lock().unwrap(),
            ),
            TxnOffsetCommitRequest::API_KEY => txn_offset_commit_exchange(
                frame,
                api_version,
                &faults,
                &mut cluster.lock().unwrap(),
            ),
            SaslHandshakeRequest::API_KEY => {
                sasl_handshake_exchange(frame, api_version, &faults, &mut sasl)
            }
            SaslAuthenticateRequest::API_KEY => {
                sasl_authenticate_exchange(frame, api_version, &faults, &mut sasl)
            }
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
/// A SCRAM server for this subject's one account.
///
/// The exchange itself is [`odradek_sasl`]'s, which speaks both roles
/// and is checked against the RFC 7677 vectors. What is left here is the
/// fault injection: a subject whose whole job is to be wrong in one
/// named way at a time cannot share an implementation with the thing
/// under test, so the faults reach in through the seams the crate
/// leaves — a fixed nonce, a stated iteration count, a dropped
/// signature.
fn scram_server(faults: &[Fault]) -> Result<odradek_sasl::ScramServer, String> {
    let iterations = if faults.contains(&Fault::ScramWeakIterations) {
        1
    } else {
        SCRAM_ITERATIONS
    };
    let credential = odradek_sasl::ScramCredential::derive(
        odradek_sasl::Mechanism::ScramSha256,
        SCRAM_PASSWORD,
        SCRAM_SALT.to_vec(),
        iterations,
        // A floor of 1, because this subject has to be *allowed* to
        // misbehave: the crate's default floor would refuse to build a
        // credential weak enough to test a client against.
        odradek_sasl::Limits::new(1, 1_000_000),
    )
    .map_err(|e| e.to_string())?;
    let server =
        odradek_sasl::ScramServer::new(SCRAM_USER, credential).map_err(|e| e.to_string())?;
    Ok(server)
}

/// Make a conformant `server-first` wrong, one named way at a time.
///
/// The faults live here rather than in the SCRAM implementation on
/// purpose: `odradek-sasl`'s server always extends the client's nonce,
/// because that is the invariant it exists to hold. A subject that has
/// to violate it therefore mangles the message on the way out, which is
/// also what a broken server actually does — the wrongness is in what
/// went on the wire, not in some setting it was given.
fn mangle_server_first(server_first: String, faults: &[Fault]) -> String {
    if !faults.contains(&Fault::ScramNonceReplacesClients) {
        return server_first;
    }
    server_first
        .split(',')
        .map(|part| {
            if part.starts_with("r=") {
                // Our nonce alone: the client's contribution is gone, so
                // this answer could be a recording of any exchange.
                "r=odradek-server-nonce".to_owned()
            } else {
                part.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// SaslHandshake: agree a mechanism, or say what is on offer.
fn sasl_handshake_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    sasl: &mut SaslState,
) -> Option<BytesMut> {
    if !(SaslHandshakeRequest::MIN_VERSION..=SaslHandshakeRequest::MAX_VERSION)
        .contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(SaslHandshakeRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = SaslHandshakeRequest::decode(&mut frame, api_version).ok()?;

    let supported = SASL_MECHANISMS.contains(&request.mechanism.as_str());
    let mut resp = SaslHandshakeResponse::default();
    if supported {
        *sasl = SaslState::Negotiated(request.mechanism.clone());
        resp.error_code = 0;
    } else {
        resp.error_code = ErrorCode::UNSUPPORTED_SASL_MECHANISM.0;
    }
    // The list goes out either way, and it matters most on the refusal:
    // a client told only "no" has nothing to try next, and a client that
    // has to guess will guess PLAIN.
    resp.mechanisms = if faults.contains(&Fault::SaslHandshakeHidesMechanisms) {
        Vec::new()
    } else {
        SASL_MECHANISMS.iter().map(|m| (*m).to_owned()).collect()
    };
    frame_response(
        req_header.correlation_id,
        response_header_version(SaslHandshakeRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// SaslAuthenticate: carry one mechanism token, once a mechanism exists.
///
/// This subject authenticates nobody — it has no credential store and
/// the checks that use it are about sequencing, not secrets. What it
/// does model is the part a reimplementer has to get right regardless of
/// mechanism: a token arriving with no negotiated mechanism is a state
/// error, not an authentication failure.
fn sasl_authenticate_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    sasl: &mut SaslState,
) -> Option<BytesMut> {
    if !(SaslAuthenticateRequest::MIN_VERSION..=SaslAuthenticateRequest::MAX_VERSION)
        .contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(SaslAuthenticateRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let _request = SaslAuthenticateRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = SaslAuthenticateResponse::default();
    let token = String::from_utf8_lossy(&_request.auth_bytes).into_owned();
    match sasl {
        SaslState::Unstarted if !faults.contains(&Fault::SaslAuthenticateWithoutHandshake) => {
            resp.error_code = ErrorCode::ILLEGAL_SASL_STATE.0;
            resp.error_message = Some("no mechanism negotiated".into());
        }
        SaslState::Negotiated(mechanism) if mechanism == "SCRAM-SHA-256" => {
            match scram_server(faults).and_then(|mut server| {
                server
                    .server_first(&token)
                    .map(|first| (server, first))
                    .map_err(|e| e.to_string())
            }) {
                Ok((server, server_first)) => {
                    resp.error_code = 0;
                    let server_first = mangle_server_first(server_first, faults);
                    resp.auth_bytes = Bytes::from(server_first.into_bytes());
                    *sasl = SaslState::ScramPending(Box::new(server));
                }
                Err(message) => {
                    resp.error_code = ErrorCode::SASL_AUTHENTICATION_FAILED.0;
                    resp.error_message = Some(message);
                }
            }
        }
        SaslState::ScramPending(server) => match server.server_final(&token) {
            Ok(server_final) => {
                resp.error_code = 0;
                // A subject that completes the exchange without signing
                // it: the client has authenticated itself to whatever
                // this is, and has no way to notice.
                resp.auth_bytes = if faults.contains(&Fault::ScramSkipsServerSignature) {
                    Bytes::new()
                } else {
                    Bytes::from(server_final.into_bytes())
                };
                *sasl = SaslState::Authenticated;
            }
            Err(e) => {
                resp.error_code = ErrorCode::SASL_AUTHENTICATION_FAILED.0;
                resp.error_message = Some(e.to_string());
            }
        },
        _ => {
            // Any other mechanism, or a token after the exchange ended:
            // this subject has no credentials to check beyond SCRAM.
            *sasl = SaslState::Authenticated;
            resp.error_code = 0;
            resp.auth_bytes = Bytes::new();
        }
    }
    frame_response(
        req_header.correlation_id,
        response_header_version(SaslAuthenticateRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// The interval this subject tells members to heartbeat at.
const HEARTBEAT_INTERVAL_MS: i32 = 5_000;
/// A heartbeat carrying this epoch is the member leaving.
const LEAVE_EPOCH: i32 = -1;

/// ConsumerGroupHeartbeat: the whole KIP-848 membership in one call.
fn consumer_group_heartbeat_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(ConsumerGroupHeartbeatRequest::MIN_VERSION..=ConsumerGroupHeartbeatRequest::MAX_VERSION)
        .contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(ConsumerGroupHeartbeatRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = ConsumerGroupHeartbeatRequest::decode(&mut frame, api_version).ok()?;

    let key = (request.group_id.clone(), request.member_id.clone());
    let mut resp = ConsumerGroupHeartbeatResponse::default();
    resp.member_id = Some(request.member_id.clone());
    resp.heartbeat_interval_ms = HEARTBEAT_INTERVAL_MS;

    // Leaving: acknowledged by echoing the epoch back, and the member is
    // gone. Nothing to assign and nothing to fence against.
    if request.member_epoch == LEAVE_EPOCH {
        state.members_848.remove(&key);
        resp.error_code = 0;
        resp.member_epoch = LEAVE_EPOCH;
        return frame_response(
            req_header.correlation_id,
            response_header_version(ConsumerGroupHeartbeatRequest::API_KEY, api_version),
            |out| resp.encode(out, api_version),
            false,
        );
    }

    let known = state.members_848.contains_key(&key);
    if known {
        let current = state.members_848[&key].epoch;
        if request.member_epoch != current && !faults.contains(&Fault::ConsumerGroupIgnoresEpoch) {
            resp.error_code = ErrorCode::FENCED_MEMBER_EPOCH.0;
            resp.member_epoch = 0;
            return frame_response(
                req_header.correlation_id,
                response_header_version(ConsumerGroupHeartbeatRequest::API_KEY, api_version),
                |out| resp.encode(out, api_version),
                false,
            );
        }
    } else if request.member_epoch != 0 {
        // An unknown member can only be introducing itself, which is
        // epoch 0. Anything else is a member the coordinator forgot.
        resp.error_code = ErrorCode::UNKNOWN_MEMBER_ID.0;
        resp.member_epoch = 0;
        return frame_response(
            req_header.correlation_id,
            response_header_version(ConsumerGroupHeartbeatRequest::API_KEY, api_version),
            |out| resp.encode(out, api_version),
            false,
        );
    }

    let stuck = faults.contains(&Fault::ConsumerGroupEpochStuck);
    let member = state.members_848.entry(key).or_default();
    if !known {
        // Admission is what the epoch records. Leaving it at 0 tells the
        // member it was never admitted, however cheerful the error code.
        member.epoch = if stuck { 0 } else { 1 };
    }
    // Omitted means unchanged. Only a stated subscription replaces the
    // one on file — including a stated empty one, which really is
    // "nothing", unlike an absent one.
    match &request.subscribed_topic_names {
        Some(names) => member.subscribed = names.clone(),
        None if faults.contains(&Fault::ConsumerGroupNullSubscriptionRevokes) => {
            member.subscribed.clear();
        }
        None => {}
    }
    let epoch = member.epoch;
    let subscribed = member.subscribed.clone();

    // Server-side assignment: every partition of every subscribed topic
    // that exists, addressed by id because that is what the wire carries.
    let assigned: Vec<TopicPartitions> = if faults.contains(&Fault::ConsumerGroupAssignsNothing) {
        Vec::new()
    } else {
        subscribed
            .iter()
            .filter_map(|name| state.created.get(name).map(|id| (name, *id)))
            .map(|(_, topic_id)| {
                let mut tp = TopicPartitions::default();
                tp.topic_id = topic_id;
                tp.partitions = vec![0];
                tp
            })
            .collect()
    };
    // Send the assignment only when it is news. An unchanged assignment
    // is reported by saying nothing about it, so a member that hears
    // nothing keeps what it has — and, crucially, a member whose
    // assignment was *revoked* hears an empty one, which is how the two
    // are told apart on the wire.
    let current: Vec<([u8; 16], Vec<i32>)> = assigned
        .iter()
        .map(|tp| (tp.topic_id, tp.partitions.clone()))
        .collect();
    let member = state
        .members_848
        .get_mut(&(request.group_id.clone(), request.member_id.clone()))
        .expect("member was just inserted");
    let changed = member.told.as_ref() != Some(&current);
    member.told = Some(current);

    resp.error_code = 0;
    resp.member_epoch = epoch;
    if changed {
        let mut assignment = Assignment::default();
        assignment.topic_partitions = assigned;
        resp.assignment = Some(assignment);
    }
    frame_response(
        req_header.correlation_id,
        response_header_version(ConsumerGroupHeartbeatRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// The version from which a join with no member id must be refused and
/// given one to retry with.
const JOIN_GROUP_MEMBER_ID_REQUIRED: i16 = 4;

/// JoinGroup: mint a member id, or admit the member and name a leader.
fn join_group_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(JoinGroupRequest::MIN_VERSION..=JoinGroupRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(JoinGroupRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = JoinGroupRequest::decode(&mut frame, api_version).ok()?;

    let group = state.groups.entry(request.group_id.clone()).or_default();
    let mut resp = JoinGroupResponse::default();

    let needs_id = request.member_id.is_empty()
        && api_version >= JOIN_GROUP_MEMBER_ID_REQUIRED
        && !faults.contains(&Fault::JoinGroupAcceptsEmptyMemberId);
    if needs_id {
        // The id is minted here and the join refused, so the member that
        // comes back is one the coordinator named.
        let minted = format!("odradek-member-{}", group.minted.len() + 1);
        group.minted.push(minted.clone());
        resp.error_code = ErrorCode::MEMBER_ID_REQUIRED.0;
        resp.member_id = minted;
        resp.generation_id = -1;
        resp.leader = String::new();
        resp.protocol_type = Some(request.protocol_type.clone());
        // `ProtocolName` is nullable only from v7. Below that a null
        // cannot be encoded at all, and the empty string is what a
        // broker sends with MEMBER_ID_REQUIRED. Sending `None` there
        // made the encode fail, and the encode failure used to be an
        // `unwrap` -- so one unanswerable JoinGroup took the whole
        // subject down, poisoned the cluster mutex, and reported every
        // check after it as the subject being unreachable.
        resp.protocol_name = (api_version < JOIN_GROUP_NULLABLE_PROTOCOL).then(String::new);
        return frame_response(
            req_header.correlation_id,
            response_header_version(JoinGroupRequest::API_KEY, api_version),
            |out| resp.encode(out, api_version),
            false,
        );
    }

    let member_id = if request.member_id.is_empty() {
        let minted = format!("odradek-member-{}", group.minted.len() + 1);
        group.minted.push(minted.clone());
        minted
    } else {
        request.member_id.clone()
    };
    if !group.members.contains(&member_id) {
        group.members.push(member_id.clone());
        group.generation_id += 1;
        group.assignments.clear();
    }
    group.protocol_type = request.protocol_type.clone();
    group.protocol_name = request
        .protocols
        .first()
        .map(|p| p.name.clone())
        .unwrap_or_default();

    let leader = group.members.first().cloned().unwrap_or_default();
    resp.error_code = 0;
    resp.generation_id = group.generation_id;
    resp.protocol_type = Some(group.protocol_type.clone());
    resp.protocol_name = Some(group.protocol_name.clone());
    resp.leader = leader.clone();
    resp.member_id = member_id.clone();
    // Only the leader is told who else is in the group: it is the one
    // that has to compute an assignment for them.
    resp.members = if member_id == leader {
        group
            .members
            .iter()
            .map(|id| {
                let mut m = JoinGroupResponseMember::default();
                m.member_id = id.clone();
                m.metadata = request
                    .protocols
                    .first()
                    .map(|p| p.metadata.clone())
                    .unwrap_or_default();
                m
            })
            .collect()
    } else {
        Vec::new()
    };
    frame_response(
        req_header.correlation_id,
        response_header_version(JoinGroupRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// SyncGroup: take the leader's assignments, hand each member its own.
fn sync_group_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(SyncGroupRequest::MIN_VERSION..=SyncGroupRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(SyncGroupRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = SyncGroupRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = SyncGroupResponse::default();
    let group = state.groups.entry(request.group_id.clone()).or_default();
    if let Some(code) = fence(group, request.generation_id, &request.member_id, faults) {
        resp.error_code = code.0;
    } else {
        // The leader's assignments land here; everyone else is told what
        // the leader decided for them.
        if group.members.first() == Some(&request.member_id) {
            for a in &request.assignments {
                let bytes = if faults.contains(&Fault::SyncGroupRewritesAssignment) {
                    let mut mangled = BytesMut::from(&a.assignment[..]);
                    if mangled.is_empty() {
                        mangled.extend_from_slice(b"x");
                    } else {
                        let last = mangled.len() - 1;
                        mangled[last] ^= 0x01;
                    }
                    mangled.freeze()
                } else {
                    a.assignment.clone()
                };
                group.assignments.insert(a.member_id.clone(), bytes);
            }
        }
        resp.error_code = 0;
        resp.protocol_type = Some(group.protocol_type.clone());
        resp.protocol_name = Some(group.protocol_name.clone());
        resp.assignment = group
            .assignments
            .get(&request.member_id)
            .cloned()
            .unwrap_or_default();
    }
    frame_response(
        req_header.correlation_id,
        response_header_version(SyncGroupRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// Heartbeat: alive, and still of this generation.
fn heartbeat_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(HeartbeatRequest::MIN_VERSION..=HeartbeatRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(HeartbeatRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = HeartbeatRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = HeartbeatResponse::default();
    let group = state.groups.entry(request.group_id.clone()).or_default();
    resp.error_code =
        fence(group, request.generation_id, &request.member_id, faults).map_or(0, |code| code.0);
    frame_response(
        req_header.correlation_id,
        response_header_version(HeartbeatRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// LeaveGroup: forget the member.
fn leave_group_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(LeaveGroupRequest::MIN_VERSION..=LeaveGroupRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(LeaveGroupRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = LeaveGroupRequest::decode(&mut frame, api_version).ok()?;

    // v3 moved the departing member from a top-level id to a `members`
    // array, and the old field stops being on the wire — so a subject
    // that reads only one of them ignores half the leaves it is sent.
    let mut leaving: Vec<String> = request
        .members
        .iter()
        .map(|m| m.member_id.clone())
        .collect();
    if !request.member_id.is_empty() {
        leaving.push(request.member_id.clone());
    }
    if !faults.contains(&Fault::LeaveGroupKeepsTheMember) {
        if let Some(group) = state.groups.get_mut(&request.group_id) {
            group.members.retain(|id| !leaving.contains(id));
            for id in &leaving {
                group.assignments.remove(id);
            }
        }
    }
    let mut resp = LeaveGroupResponse::default();
    resp.error_code = 0;
    frame_response(
        req_header.correlation_id,
        response_header_version(LeaveGroupRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// The generation fence every post-join request passes through.
///
/// `None` means the request may proceed. A member the group does not
/// know is UNKNOWN_MEMBER_ID; a known member carrying the wrong
/// generation is ILLEGAL_GENERATION — the distinction matters because a
/// client answers them differently, rejoining from scratch versus
/// rejoining as itself.
fn fence(
    group: &GroupState,
    generation_id: i32,
    member_id: &str,
    faults: &[Fault],
) -> Option<ErrorCode> {
    // Membership first, and outside the generation fault: whether the
    // coordinator knows this member at all is a different question from
    // whether it checks generations, and one fault should not answer
    // both.
    if !group.members.iter().any(|id| id == member_id) {
        return Some(ErrorCode::UNKNOWN_MEMBER_ID);
    }
    if faults.contains(&Fault::GroupIgnoresGeneration) {
        return None;
    }
    if generation_id != group.generation_id {
        return Some(ErrorCode::ILLEGAL_GENERATION);
    }
    None
}

/// ListOffsets: `-2` is the log start, `-1` the log end, and any other
/// ListOffsets: `-2` is the log start, `-1` the log end, and any other
/// timestamp is a lookup this subject answers "no such message" to.
fn list_offsets_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &ClusterState,
) -> Option<BytesMut> {
    if !(ListOffsetsRequest::MIN_VERSION..=ListOffsetsRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(ListOffsetsRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = ListOffsetsRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = ListOffsetsResponse::default();
    resp.topics = request
        .topics
        .iter()
        .map(|topic| {
            let mut out = ListOffsetsTopicResponse::default();
            out.name = topic.name.clone();
            out.partitions = topic
                .partitions
                .iter()
                .map(|p| {
                    let log = state.logs.get(&(topic.name.clone(), p.partition_index));
                    let end = log.map_or(0, |log| log.next_offset);
                    let (timestamp, offset) = match p.timestamp {
                        EARLIEST_TIMESTAMP => {
                            let start = if faults.contains(&Fault::ListOffsetsWrongEarliest) {
                                // The log start is always 0 here, so any
                                // other answer is wrong by construction.
                                end.max(1)
                            } else {
                                0
                            };
                            (-1, start)
                        }
                        LATEST_TIMESTAMP => (-1, end),
                        // A real timestamp is a search: the first record
                        // at or after it, or no offset at all when every
                        // record predates it. Answering the log end
                        // instead reads to a time-seeking consumer as
                        // "you are caught up".
                        wanted if faults.contains(&Fault::ListOffsetsTimestampReturnsLogEnd) => {
                            let _ = wanted;
                            (-1, end)
                        }
                        wanted => log.map_or((-1, -1), |log| timestamp_search(log, wanted)),
                    };
                    let mut out = ListOffsetsPartitionResponse::default();
                    out.partition_index = p.partition_index;
                    out.error_code = 0;
                    out.timestamp = timestamp;
                    out.offset = offset;
                    out.leader_epoch = -1;
                    out
                })
                .collect();
            out
        })
        .collect();
    frame_response(
        req_header.correlation_id,
        response_header_version(ListOffsetsRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// FindCoordinator: this subject is its own coordinator for every group.
///
/// v4 replaced the single `key` with `coordinator_keys`, and the flat
/// node/host/port with a `coordinators` array — so the two shapes are
/// answered separately, which is the whole point of checking it.
fn find_coordinator_exchange(
    mut frame: Bytes,
    api_version: i16,
    node_id: i32,
    faults: &[Fault],
    state: &ClusterState,
) -> Option<BytesMut> {
    if !(FindCoordinatorRequest::MIN_VERSION..=FindCoordinatorRequest::MAX_VERSION)
        .contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(FindCoordinatorRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = FindCoordinatorRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = FindCoordinatorResponse::default();
    if api_version >= FIND_COORDINATOR_BATCHED {
        resp.coordinators = request
            .coordinator_keys
            .iter()
            .map(|key| {
                let mut c = Coordinator::default();
                c.key = if faults.contains(&Fault::FindCoordinatorWrongKey) {
                    format!("{key}-not-yours")
                } else {
                    key.clone()
                };
                // The coordinator for this key, which any broker can
                // be asked about and every broker answers the same.
                // As with leadership: the answer is a property of the
                // key, not of who was asked. A broker that named itself
                // would send every client to whichever broker it
                // happened to ask, and the group would have as many
                // coordinators as it had bootstrap addresses.
                let owner = if faults.contains(&Fault::CoordinatorIsWhoeverAsked) {
                    node_id
                } else {
                    state.placement(key)
                };
                c.node_id = owner;
                c.host = "127.0.0.1".to_owned();
                c.port = state.port_of(owner);
                c.error_code = 0;
                c
            })
            .collect();
    } else {
        let owner = if faults.contains(&Fault::CoordinatorIsWhoeverAsked) {
            node_id
        } else {
            state.placement(&request.key)
        };
        resp.error_code = 0;
        resp.node_id = owner;
        resp.host = "127.0.0.1".to_owned();
        resp.port = state.port_of(owner);
    }
    frame_response(
        req_header.correlation_id,
        response_header_version(FindCoordinatorRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// OffsetCommit: store what was committed, per (group, topic, partition).
fn offset_commit_exchange(
    mut frame: Bytes,
    api_version: i16,
    node_id: i32,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(OffsetCommitRequest::MIN_VERSION..=OffsetCommitRequest::MAX_VERSION).contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(OffsetCommitRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = OffsetCommitRequest::decode(&mut frame, api_version).ok()?;

    // A group's offsets live with its coordinator and nowhere else. A
    // broker that stored them anyway would hold a commit the
    // coordinator cannot see, so a consumer that resumed through the
    // coordinator would rewind to before it — reprocessing everything
    // between, exactly once having become at least twice. NOT_COORDINATOR
    // tells the client to go and ask the broker that owns the group,
    // which it can find with FindCoordinator.
    let owner = state.placement(&request.group_id);
    if owner != node_id && !faults.contains(&Fault::AnyBrokerServesGroups) {
        let mut resp = OffsetCommitResponse::default();
        resp.topics = request
            .topics
            .iter()
            .map(|topic| {
                let mut out = OffsetCommitResponseTopic::default();
                out.name = topic.name.clone();
                out.topic_id = topic.topic_id;
                out.partitions = topic
                    .partitions
                    .iter()
                    .map(|p| {
                        let mut entry = OffsetCommitResponsePartition::default();
                        entry.partition_index = p.partition_index;
                        entry.error_code = ErrorCode::NOT_COORDINATOR.0;
                        entry
                    })
                    .collect();
                out
            })
            .collect();
        return frame_response(
            req_header.correlation_id,
            response_header_version(OffsetCommitRequest::API_KEY, api_version),
            |out| resp.encode(out, api_version),
            false,
        );
    }

    let mut resp = OffsetCommitResponse::default();
    resp.topics = request
        .topics
        .iter()
        .map(|topic| {
            let name = if topic.name.is_empty() {
                state
                    .topic_names
                    .get(&topic.topic_id)
                    .cloned()
                    .unwrap_or_default()
            } else {
                topic.name.clone()
            };
            let mut out = OffsetCommitResponseTopic::default();
            out.name = name.clone();
            out.topic_id = topic.topic_id;
            out.partitions = topic
                .partitions
                .iter()
                .map(|p| {
                    // The metadata is the client's string and stored
                    // beside the offset, not instead of it: dropping it
                    // leaves a commit that reads back looking fine.
                    let metadata = if faults.contains(&Fault::OffsetCommitDropsMetadata) {
                        None
                    } else {
                        p.committed_metadata.clone()
                    };
                    state.committed.insert(
                        (request.group_id.clone(), name.clone(), p.partition_index),
                        (p.committed_offset, metadata),
                    );
                    let mut out = OffsetCommitResponsePartition::default();
                    out.partition_index = p.partition_index;
                    out.error_code = 0;
                    out
                })
                .collect();
            out
        })
        .collect();
    frame_response(
        req_header.correlation_id,
        response_header_version(OffsetCommitRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// OffsetFetch: report what was committed, or `-1` where nothing was.
///
/// The sentinel is the subtle part and the reason this is checked: a
/// group that never committed is not an error, it is offset `-1` with
/// `error_code` 0. v8 moved the whole exchange into a `groups` array.
fn offset_fetch_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &ClusterState,
) -> Option<BytesMut> {
    if !(OffsetFetchRequest::MIN_VERSION..=OffsetFetchRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(OffsetFetchRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = OffsetFetchRequest::decode(&mut frame, api_version).ok()?;

    // From v10 the request names topics only by id, exactly as
    // OffsetCommit does, so both sides have to resolve to the same key or
    // a commit and its read-back silently miss each other.
    let resolve = |name: &str, topic_id: &[u8; 16]| -> String {
        if name.is_empty() {
            state.topic_names.get(topic_id).cloned().unwrap_or_default()
        } else {
            name.to_owned()
        }
    };
    let lookup = |group: &str, topic: &str, partition: i32| -> (i64, Option<String>) {
        let stored = state
            .committed
            .get(&(group.to_owned(), topic.to_owned(), partition))
            .cloned();
        match stored {
            Some(_) if faults.contains(&Fault::OffsetFetchLosesCommit) => {
                // Answer as though nothing was ever committed.
                (UNSET_OFFSET, None)
            }
            Some(entry) => entry,
            // The sentinel, unless told to report a plausible-looking 0.
            None if faults.contains(&Fault::OffsetFetchUnsetIsZero) => (0, None),
            None => (UNSET_OFFSET, None),
        }
    };

    let mut resp = OffsetFetchResponse::default();
    if api_version >= OFFSET_FETCH_BATCHED {
        resp.groups = request
            .groups
            .iter()
            .map(|group| {
                let mut out = OffsetFetchResponseGroup::default();
                out.group_id = group.group_id.clone();
                out.error_code = 0;
                out.topics = group
                    .topics
                    .iter()
                    .flatten()
                    .map(|topic| {
                        let mut t = OffsetFetchResponseTopics::default();
                        t.name = topic.name.clone();
                        t.topic_id = topic.topic_id;
                        t.partitions = topic
                            .partition_indexes
                            .iter()
                            .map(|index| {
                                let mut p = OffsetFetchResponsePartitions::default();
                                p.partition_index = *index;
                                let (offset, metadata) = lookup(
                                    &group.group_id,
                                    &resolve(&topic.name, &topic.topic_id),
                                    *index,
                                );
                                p.committed_offset = offset;
                                p.metadata = metadata;
                                p.committed_leader_epoch = -1;
                                p.error_code = 0;
                                p
                            })
                            .collect();
                        t
                    })
                    .collect();
                out
            })
            .collect();
    } else {
        resp.topics = request
            .topics
            .iter()
            .flatten()
            .map(|topic| {
                let mut t = OffsetFetchResponseTopic::default();
                t.name = topic.name.clone();
                t.partitions = topic
                    .partition_indexes
                    .iter()
                    .map(|index| {
                        let mut p = OffsetFetchResponsePartition::default();
                        p.partition_index = *index;
                        let (offset, metadata) = lookup(&request.group_id, &topic.name, *index);
                        p.committed_offset = offset;
                        p.metadata = metadata;
                        p.committed_leader_epoch = -1;
                        p.error_code = 0;
                        p
                    })
                    .collect();
                t
            })
            .collect();
    }
    frame_response(
        req_header.correlation_id,
        response_header_version(OffsetFetchRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// The first JoinGroup response version whose `ProtocolName` may be
/// null.
const JOIN_GROUP_NULLABLE_PROTOCOL: i16 = 7;

fn frame_response(
    correlation_id: i32,
    header_version: i16,
    body: impl FnOnce(&mut BytesMut) -> Result<(), odradek_protocol::EncodeError>,
    trailing_garbage: bool,
) -> Option<BytesMut> {
    let mut resp_header = ResponseHeader::default();
    resp_header.correlation_id = correlation_id;
    let mut out = BytesMut::new();
    frame::frame(&mut out, |out| {
        resp_header.encode(out, header_version)?;
        body(out)?;
        if trailing_garbage {
            out.extend_from_slice(&[0xde, 0xad, 0xbe]);
        }
        Ok(())
    })
    .ok()?;
    Some(out)
}

fn api_versions_exchange(mut frame: Bytes, api_version: i16, faults: &[Fault]) -> Option<BytesMut> {
    let has = |f: Fault| faults.contains(&f);
    let supported = api_version <= MAX_SUPPORTED_API_VERSIONS;

    // For unknown future versions this parses the header at our newest
    // known header version — the same rule real brokers apply, and the
    // shared table's answer for any v3+ request.
    let header_version = header::request_header_version(ApiVersionsRequest::API_KEY, api_version)?;
    let header = RequestHeader::decode(&mut frame, header_version).ok()?;

    let mut keys = advertised_keys();
    if has(Fault::OmitApiVersionsKey) {
        keys.retain(|k| k.api_key != ApiVersionsRequest::API_KEY);
    }
    if has(Fault::InvertedVersionRange) {
        if let Some(k) = keys
            .iter_mut()
            .find(|k| k.api_key == ProduceRequest::API_KEY)
        {
            (k.min_version, k.max_version) = (12, 3);
        }
    }

    let (error_code, encode_at) = if supported {
        (ErrorCode::NONE.0, api_version)
    } else {
        if has(Fault::AdvertiseWrongMaxInError) {
            if let Some(k) = keys
                .iter_mut()
                .find(|k| k.api_key == ApiVersionsRequest::API_KEY)
            {
                k.max_version += 1;
            }
        }
        let error_code = if has(Fault::WrongErrorOnUnsupportedVersion) {
            ErrorCode::NONE.0
        } else {
            ErrorCode::UNSUPPORTED_VERSION.0
        };
        let encode_at = if has(Fault::ErrorBodyNotV0) { 3 } else { 0 };
        (error_code, encode_at)
    };

    let mut resp = ApiVersionsResponse::default();
    resp.error_code = error_code;
    resp.api_keys = keys;
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
        |out| resp.encode(out, encode_at),
        has(Fault::TrailingGarbage),
    )
}

fn metadata_exchange(
    mut frame: Bytes,
    api_version: i16,
    node_id: i32,
    faults: &[Fault],
    state: &ClusterState,
) -> Option<BytesMut> {
    let has = |f: Fault| faults.contains(&f);
    if !(MetadataRequest::MIN_VERSION..=MetadataRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let flexible = metadata_request::is_flexible(api_version);
    let header_version = header::request_header_version(MetadataRequest::API_KEY, api_version)?;
    let header = RequestHeader::decode(&mut frame, header_version).ok()?;
    let request = MetadataRequest::decode(&mut frame, api_version).ok()?;

    let brokers = if has(Fault::MetadataEmptyBrokers) {
        Vec::new()
    } else {
        // Every broker in the cluster, not just this one. A client that
        // was told only about the broker it is already connected to
        // could never reach a partition this one does not lead.
        state
            .nodes()
            .map(|(node_id, port)| {
                let mut broker = MetadataResponseBroker::default();
                broker.node_id = node_id;
                broker.host = "127.0.0.1".into();
                broker.port = port;
                broker
            })
            .collect()
    };
    // A response names exactly the topics the request named: the ones
    // that exist, and the ones that do not with a code saying so. A
    // client cannot distinguish "this topic is absent" from "the server
    // ignored my question" unless the absent one is named.
    let requested: Vec<String> = request
        .topics
        .iter()
        .flatten()
        .filter_map(|t| t.name.clone())
        .collect();
    let topics = if has(Fault::MetadataUnrequestedTopic) && requested.is_empty() {
        let mut topic = MetadataResponseTopic::default();
        topic.name = Some("phantom".into());
        vec![topic]
    } else if has(Fault::MetadataUnknownTopicOmitted) {
        Vec::new()
    } else {
        requested
            .iter()
            .map(|name| {
                let mut topic = MetadataResponseTopic::default();
                topic.name = Some(name.clone());
                match state.created.get(name) {
                    Some(id) => {
                        // Ids exist so a client can tell a recreated
                        // topic from the one it meant; reminting one
                        // that never went away fails every id-addressed
                        // request already in flight.
                        topic.topic_id = if has(Fault::MetadataRemintsTopicId) {
                            // A fresh id per ask, which is what makes
                            // the failure visible at all: one that
                            // changed once and then held still would
                            // simply look like a different topic.
                            let mut minted = *id;
                            let salt = header.correlation_id.to_be_bytes();
                            minted[12..16].copy_from_slice(&salt);
                            minted
                        } else {
                            *id
                        };
                        topic.error_code = 0;
                        let mut p = MetadataResponsePartition::default();
                        p.partition_index = 0;
                        p.error_code = 0;
                        // The leader has to be a node the same response
                        // names, or a client is handed a healthy topic
                        // with nowhere to send to.
                        // Every broker computes this the same way, so
                        // every broker gives the same answer. The fault
                        // makes each claim leadership for itself, which
                        // is the shape of the real bug: a client that
                        // refreshes metadata against a different broker
                        // than last time is sent somewhere else, and
                        // two producers can believe in two leaders.
                        let leader = if has(Fault::BrokersDisagreeOnLeader) {
                            node_id
                        } else if has(Fault::LeadershipStaysWithTheStoppedBroker) {
                            // Placed over every broker rather than the
                            // live ones, so a stopped leader keeps the
                            // partition.
                            let all: Vec<i32> = state.nodes().map(|(id, _)| id).collect();
                            state.placement_over(name, &all)
                        } else {
                            state.placement(name)
                        };
                        p.leader_id = if has(Fault::MetadataLeaderIsUnknown) {
                            leader + 999
                        } else {
                            leader
                        };
                        p.leader_epoch = 0;
                        // Replicated across every node, the leader
                        // first, and all of them in sync — this subject
                        // has no failure to model, only placement.
                        let replicas: Vec<i32> = if has(Fault::ReplicasCollapseToTheLeader) {
                            vec![leader]
                        } else {
                            std::iter::once(leader)
                                .chain(state.nodes().map(|(id, _)| id).filter(|id| *id != leader))
                                .collect()
                        };
                        p.isr_nodes.clone_from(&replicas);
                        p.replica_nodes = replicas;
                        topic.partitions = vec![p];
                    }
                    None => topic.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.0,
                }
                topic
            })
            .collect()
    };
    let mut resp = MetadataResponse::default();
    resp.brokers = brokers;
    resp.cluster_id = Some("odradek-reference".into());
    resp.controller_id = 1;
    resp.topics = topics;
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
        |out| resp.encode(out, api_version),
        false,
    )
}

fn create_topics_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(CreateTopicsRequest::MIN_VERSION..=CreateTopicsRequest::MAX_VERSION).contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(CreateTopicsRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = CreateTopicsRequest::decode(&mut frame, api_version).ok()?;

    // A topic creates once. A second attempt is TOPIC_ALREADY_EXISTS, and
    // `validate_only` answers the same as a real creation would without
    // performing one — both are the kind of thing that looks like a
    // detail until a client's create-if-absent path depends on it.
    let validate_only =
        request.validate_only && !faults.contains(&Fault::CreateTopicsValidateOnlyCreates);
    let mut resp = CreateTopicsResponse::default();
    resp.topics = request
        .topics
        .iter()
        .map(|t| {
            let topic_id = mint_topic_id(&t.name);
            let exists = state.created.contains_key(&t.name);
            let mut result = CreatableTopicResult::default();
            result.name = t.name.clone();
            result.topic_id = topic_id;
            result.num_partitions = t.num_partitions.max(1);
            result.replication_factor = t.replication_factor.max(1);
            // More replicas than there are brokers is a durability
            // level this cluster cannot give — and saying yes to it
            // anyway is the failure the check looks for: the caller
            // would be told it has replication it does not have.
            let brokers = i16::try_from(state.ports.len().max(1)).unwrap_or(i16::MAX);
            let overreplicated = t.replication_factor > brokers
                && !faults.contains(&Fault::CreateTopicsIgnoresReplicationFactor);
            result.error_code = if exists && !faults.contains(&Fault::CreateTopicsDuplicateSucceeds)
            {
                ErrorCode::TOPIC_ALREADY_EXISTS.0
            } else if overreplicated {
                ErrorCode::INVALID_REPLICATION_FACTOR.0
            } else {
                0
            };
            if !exists && result.error_code == 0 && !validate_only {
                state.created.insert(t.name.clone(), topic_id);
                state.topic_names.insert(topic_id, t.name.clone());
            }
            result
        })
        .collect();
    frame_response(
        req_header.correlation_id,
        response_header_version(CreateTopicsRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

fn produce_exchange(
    mut frame: Bytes,
    api_version: i16,
    node_id: i32,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(ProduceRequest::MIN_VERSION..=ProduceRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(ProduceRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = ProduceRequest::decode(&mut frame, api_version).ok()?;

    // Taken before the logs are borrowed mutably; a produce never
    // changes which partitions were announced.
    let txns_snapshot: TxnSnapshot = state
        .txns
        .iter()
        .map(|(id, txn)| (id.clone(), (txn.epoch, txn.partitions.clone())))
        .collect();

    let mut responses = Vec::new();
    for topic in &request.topic_data {
        // v13+ addresses by id; earlier versions by name.
        let by_id = topic.name.is_empty();
        let resolved = if by_id {
            state.topic_names.get(&topic.topic_id).cloned()
        } else {
            Some(topic.name.clone())
        };
        let refuse_id = by_id && faults.contains(&Fault::ProduceTopicIdUnknown);

        let mut partition_responses = Vec::new();
        for partition in &topic.partition_data {
            let (Some(name), false) = (&resolved, refuse_id) else {
                let mut entry = PartitionProduceResponse::default();
                entry.index = partition.index;
                entry.error_code = ErrorCode::UNKNOWN_TOPIC_ID.0;
                entry.base_offset = -1;
                entry.log_append_time_ms = -1;
                partition_responses.push(entry);
                continue;
            };
            // Only the leader may append. A follower that took the write
            // instead of refusing it would put records in a log the
            // leader knows nothing about, and the partition would have
            // two histories — the divergence that leadership exists to
            // prevent. The refusal is the client's cue to re-read
            // Metadata, which is why it must not be silent.
            if state.placement(name) != node_id && !faults.contains(&Fault::AnyBrokerAcceptsWrites)
            {
                let mut entry = PartitionProduceResponse::default();
                entry.index = partition.index;
                entry.error_code = ErrorCode::NOT_LEADER_OR_FOLLOWER.0;
                entry.base_offset = -1;
                entry.log_append_time_ms = -1;
                partition_responses.push(entry);
                continue;
            }
            let log = state
                .logs
                .entry((name.clone(), partition.index))
                .or_default();
            let set = partition.records.clone().unwrap_or_default();
            let batches = records::decode_set(&mut set.clone()).ok()?;
            // A stamped batch is the broker's to recognize: the same
            // producer, epoch and sequence twice is one write, and a
            // sequence that skips ahead is a gap it cannot fill.
            // Fencing first: a producer that has been replaced is not
            // owed an opinion about its sequence numbers, and telling it
            // the sequence is wrong sends it to rebuild a stamp it is
            // no longer entitled to use at all.
            if let Some(code) = fenced_at_the_leader(
                &txns_snapshot,
                request.transactional_id.as_deref(),
                batches.first(),
                faults,
            ) {
                let mut entry = PartitionProduceResponse::default();
                entry.index = partition.index;
                entry.error_code = code.0;
                entry.base_offset = -1;
                entry.log_append_time_ms = -1;
                partition_responses.push(entry);
                continue;
            }
            if let Some(code) = stamped_verdict(log, batches.first(), faults) {
                let mut entry = PartitionProduceResponse::default();
                entry.index = partition.index;
                entry.error_code = code.0;
                entry.base_offset = -1;
                entry.log_append_time_ms = -1;
                partition_responses.push(entry);
                continue;
            }
            if let Some(known) = duplicate_of(log, batches.first(), faults) {
                // Recognized: answer with where it landed the first
                // time and append nothing.
                let mut entry = PartitionProduceResponse::default();
                entry.index = partition.index;
                entry.error_code = 0;
                entry.base_offset = known;
                entry.log_append_time_ms = -1;
                entry.log_start_offset = 0;
                partition_responses.push(entry);
                continue;
            }
            // Advance the offset by the records just appended.
            let appended: i64 = batches
                .iter()
                .map(|b| i64::from(b.last_offset_delta) + 1)
                .sum();
            let mut base_offset = log.next_offset;
            // A transactional write joins the transaction, announced or
            // not: this subject adopts the partition the way Kafka's
            // own leader does rather than refusing, so the records are
            // covered by whatever marker ends the transaction. Under
            // TxnUnannouncedWriteEscapes it does neither, and they are
            // covered by nothing.
            if request.transactional_id.is_some() {
                let producer_id = batches.first().map_or(-1, |batch| batch.producer_id);
                let announced = state_txn_has_partition(
                    &txns_snapshot,
                    request.transactional_id.as_deref(),
                    name,
                    partition.index,
                );
                let escapes = !announced && faults.contains(&Fault::TxnUnannouncedWriteEscapes);
                if producer_id >= 0 && !escapes {
                    log.open.entry(producer_id).or_insert(base_offset);
                }
            }
            // A broker that re-stamps what it stores rewrites bytes the
            // producer's crc covered: the batch still decodes, it is
            // simply not the one anybody wrote. Confined to compressed
            // batches so the uncompressed integrity check is untouched.
            let stored = match rewrite_compressed(&set, &batches, faults) {
                Some(rewritten) => rewritten,
                None => set.clone(),
            };
            log.bytes.extend_from_slice(&stored);
            log.next_offset += appended;
            if let Some(batch) = batches.first() {
                if batch.producer_id >= 0 && batch.base_sequence >= 0 {
                    let count = i32::try_from(appended).unwrap_or(i32::MAX);
                    log.last_batch.insert(
                        batch.producer_id,
                        LastBatch {
                            base_sequence: batch.base_sequence,
                            next_sequence: batch.base_sequence.wrapping_add(count),
                            base_offset,
                        },
                    );
                }
            }
            if faults.contains(&Fault::ProduceWrongBaseOffset) {
                base_offset += 1;
            }
            let mut entry = PartitionProduceResponse::default();
            entry.index = partition.index;
            entry.error_code = 0;
            entry.base_offset = base_offset;
            entry.log_append_time_ms = -1;
            entry.log_start_offset = 0;
            partition_responses.push(entry);
        }
        let mut topic_resp = TopicProduceResponse::default();
        topic_resp.name = topic.name.clone();
        topic_resp.topic_id = topic.topic_id;
        topic_resp.partition_responses = partition_responses;
        responses.push(topic_resp);
    }
    // acks=0 is fire and forget, and the broker's half of that is
    // silence. A frame here is one the client has no correlation id
    // outstanding for, so it reads it as the answer to whatever it asks
    // next and every reply after is matched to the wrong request.
    if request.acks == 0 && !faults.contains(&Fault::ProduceAnswersAcksZero) {
        // An empty buffer, not `None`: `None` means the frame was
        // unparseable and the connection should go, whereas here the
        // request was understood perfectly and the right answer is
        // nothing at all.
        return Some(BytesMut::new());
    }
    let mut resp = ProduceResponse::default();
    resp.responses = responses;
    // The advertised range still claims this version; only the answer
    // is in the wrong shape. A v0 produce response carries neither
    // throttle time nor log-start offset, so read as v5 it runs out of
    // body — which is exactly how a version nobody tests fails.
    let encode_at = if faults.contains(&Fault::MisshapesOneProduceVersion) && api_version == 5 {
        0
    } else {
        api_version
    };
    frame_response(
        req_header.correlation_id,
        response_header_version(ProduceRequest::API_KEY, api_version),
        |out| resp.encode(out, encode_at),
        false,
    )
}

/// The topic a fetch names, however it names it: v13+ addresses by id
/// and leaves the name empty, earlier versions do the reverse.
///
/// One place, because the satisfiability question and the response have
/// to agree about which log is being asked for — disagree and the poll
/// waits out records it is about to serve.
fn resolve_topic(state: &ClusterState, name: String, topic_id: &[u8; 16]) -> Option<String> {
    if name.is_empty() {
        state.topic_names.get(topic_id).cloned()
    } else {
        Some(name)
    }
}

async fn fetch_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    cluster: &Cluster,
) -> Option<BytesMut> {
    if !(FetchRequest::MIN_VERSION..=FetchRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(FetchRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = FetchRequest::decode(&mut frame, api_version).ok()?;

    // The long-poll half of a fetch: when the request asks for bytes
    // that are not there yet, the wait is the answer. Returning at once
    // is correct data and a busy loop.
    //
    // The lock is taken for the question and dropped before the wait.
    // Holding it across the sleep would stop the very produce the wait
    // is waiting for — on a shared cluster that is not slowness, it is
    // deadlock — and dropping it is also what makes the wait mean
    // something: the response is built from the log as it stands
    // *after* the poll, so a record that arrives during it is served.
    //
    // Addressed by id or by name, and resolved the same way here as in
    // the response below: a v13+ request carries an empty name, so
    // asking the log by name alone answers "nothing there" for every
    // id-addressed fetch and waits out a poll over records already
    // written.
    let satisfiable = {
        let state = cluster.lock().unwrap();
        request.min_bytes <= 0
            || request.topics.iter().any(|topic| {
                let Some(name) = resolve_topic(&state, topic.topic.clone(), &topic.topic_id) else {
                    return false;
                };
                topic.partitions.iter().any(|p| {
                    state
                        .logs
                        .get(&(name.clone(), p.partition))
                        .is_some_and(|log| p.fetch_offset < log.next_offset)
                })
            })
    };
    if !satisfiable && request.max_wait_ms > 0 && !faults.contains(&Fault::FetchIgnoresMaxWait) {
        let wait = u64::try_from(request.max_wait_ms).unwrap_or(0);
        tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
    }
    let state = &*cluster.lock().unwrap();

    let responses = request
        .topics
        .iter()
        .map(|topic| {
            let resolved = resolve_topic(state, topic.topic.clone(), &topic.topic_id);
            let mut echoed_id = topic.topic_id;
            if faults.contains(&Fault::FetchWrongTopicId) {
                echoed_id[0] ^= 0x80;
            }
            let mut topic_resp = FetchableTopicResponse::default();
            topic_resp.topic = topic.topic.clone();
            topic_resp.topic_id = echoed_id;
            topic_resp.partitions = topic
                .partitions
                .iter()
                .map(|p| {
                    let log = resolved
                        .as_ref()
                        .and_then(|name| state.logs.get(&(name.clone(), p.partition)));
                    match log {
                        // Reading past the end of the log is a client
                        // mistake the protocol has a code for. Answering
                        // it with an empty batch set instead would look
                        // to a consumer exactly like "caught up".
                        Some(log)
                            if p.fetch_offset > log.next_offset
                                && !faults.contains(&Fault::FetchPastEndSucceeds) =>
                        {
                            let mut data = PartitionData::default();
                            data.partition_index = p.partition;
                            data.error_code = ErrorCode::OFFSET_OUT_OF_RANGE.0;
                            data.high_watermark = log.next_offset;
                            data.last_stable_offset = stable_offset(log, faults);
                            data.log_start_offset = 0;
                            data
                        }
                        Some(log) => {
                            let mut bytes = log.bytes.clone();
                            let corrupt = faults.contains(&Fault::FetchCorruptBatch)
                                || (faults.contains(&Fault::FetchCorruptOnOldVersions)
                                    && api_version == FetchRequest::MIN_VERSION);
                            if corrupt && !bytes.is_empty() {
                                let last = bytes.len() - 1;
                                bytes[last] ^= 0x01;
                            }
                            let mut data = PartitionData::default();
                            data.partition_index = p.partition;
                            data.error_code = 0;
                            data.high_watermark = log.next_offset;
                            data.last_stable_offset = stable_offset(log, faults);
                            data.log_start_offset = 0;
                            // Aborted records are returned like any
                            // others, with a list of what to disown:
                            // dropping them is the reader's job, and it
                            // cannot do it without being told.
                            if request.isolation_level == READ_COMMITTED {
                                data.aborted_transactions = Some(
                                    log.aborted
                                        .iter()
                                        .map(|(producer_id, first_offset)| {
                                            let mut entry = AbortedTransaction::default();
                                            entry.producer_id = *producer_id;
                                            entry.first_offset = *first_offset;
                                            entry
                                        })
                                        .collect(),
                                );
                            }
                            data.records = Some(bytes.freeze());
                            data
                        }
                        None => {
                            let mut data = PartitionData::default();
                            data.partition_index = p.partition;
                            // Unknown id vs unknown name/partition.
                            data.error_code = if topic.topic.is_empty() {
                                ErrorCode::UNKNOWN_TOPIC_ID.0
                            } else {
                                ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.0
                            };
                            data
                        }
                    }
                })
                .collect();
            topic_resp
        })
        .collect();
    let mut resp = FetchResponse::default();
    resp.error_code = 0;
    resp.session_id = 0;
    resp.responses = responses;
    frame_response(
        req_header.correlation_id,
        response_header_version(FetchRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        faults.contains(&Fault::FetchTrailingGarbage),
    )
}

// ---- transactions ---------------------------------------------------

/// Claim a transactional id: hand out a producer id, and a *higher*
/// epoch every time the id changes hands.
///
/// The epoch bump is the whole mechanism. It is what lets the
/// coordinator tell the producer holding the id now from the one that
/// held it a moment ago, so a half-dead predecessor cannot write into
/// its successor's transaction.
fn init_producer_id_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(InitProducerIdRequest::MIN_VERSION..=InitProducerIdRequest::MAX_VERSION)
        .contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(InitProducerIdRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = InitProducerIdRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = InitProducerIdResponse::default();
    match &request.transactional_id {
        None => {
            // No id to fence: the plain idempotent case, where an id
            // scoped to the session is all anyone asked for.
            state.next_producer_id += 1;
            resp.producer_id = state.next_producer_id;
            resp.producer_epoch = 0;
        }
        Some(id) => {
            let next_id = state.next_producer_id + 1;
            let txn = state.txns.entry(id.clone()).or_insert_with(|| TxnState {
                producer_id: next_id,
                // Pre-first: the first claim bumps it to 0, so every
                // handover including the first one moves the epoch.
                epoch: -1,
                ..Default::default()
            });
            if txn.producer_id == next_id {
                state.next_producer_id = next_id;
            }
            if faults.contains(&Fault::TxnInitReusesEpoch) {
                // Same epoch for the successor: the predecessor is now
                // indistinguishable from it and stays able to write.
                txn.epoch = txn.epoch.max(0);
            } else {
                txn.epoch += 1;
            }
            // Taking over the id ends whatever the predecessor left
            // open; the announced partitions go with it.
            txn.partitions.clear();
            resp.producer_id = txn.producer_id;
            resp.producer_epoch = txn.epoch;
        }
    }
    resp.error_code = ErrorCode::NONE.0;
    frame_response(
        req_header.correlation_id,
        response_header_version(InitProducerIdRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// Announce partitions to an open transaction.
fn add_partitions_to_txn_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(AddPartitionsToTxnRequest::MIN_VERSION..=AddPartitionsToTxnRequest::MAX_VERSION)
        .contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(AddPartitionsToTxnRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = AddPartitionsToTxnRequest::decode(&mut frame, api_version).ok()?;

    let id = request.v3_and_below_transactional_id.clone();
    let epoch = request.v3_and_below_producer_epoch;
    let code = txn_epoch_check(state, &id, epoch, faults);
    if code.is_ok() {
        if let Some(txn) = state.txns.get_mut(&id) {
            for topic in &request.v3_and_below_topics {
                for partition in &topic.partitions {
                    txn.partitions.insert((topic.name.clone(), *partition));
                }
            }
        }
    }

    let mut resp = AddPartitionsToTxnResponse::default();
    resp.results_by_topic_v3_and_below = request
        .v3_and_below_topics
        .iter()
        .map(|topic| {
            let mut result = AddPartitionsToTxnTopicResult::default();
            result.name = topic.name.clone();
            result.results_by_partition = topic
                .partitions
                .iter()
                .map(|partition| {
                    let mut entry = AddPartitionsToTxnPartitionResult::default();
                    entry.partition_index = *partition;
                    entry.partition_error_code = code.0;
                    entry
                })
                .collect();
            result
        })
        .collect();
    frame_response(
        req_header.correlation_id,
        response_header_version(AddPartitionsToTxnRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// Finish a transaction, marking every partition it touched.
fn end_txn_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(EndTxnRequest::MIN_VERSION..=EndTxnRequest::MAX_VERSION).contains(&api_version) {
        return None;
    }
    let hv = header::request_header_version(EndTxnRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = EndTxnRequest::decode(&mut frame, api_version).ok()?;

    let code = txn_epoch_check(
        state,
        &request.transactional_id,
        request.producer_epoch,
        faults,
    );
    if code.is_ok() {
        let producer_id = state
            .txns
            .get(&request.transactional_id)
            .map_or(-1, |txn| txn.producer_id);
        // Every partition the producer wrote to under this id, not only
        // the announced ones: a broker that adopted an unannounced
        // write owes it a marker like any other.
        let touched: Vec<(String, i32)> = state
            .logs
            .iter()
            .filter(|(_, log)| log.open.contains_key(&producer_id))
            .map(|(key, _)| key.clone())
            .collect();
        for key in touched {
            let Some(log) = state.logs.get_mut(&key) else {
                continue;
            };
            let Some(first_offset) = log.open.remove(&producer_id) else {
                continue;
            };
            // The marker is a record of its own, which is why the
            // stable offset ends up past the transaction's records
            // rather than at them.
            log.next_offset += 1;
            let disown = if faults.contains(&Fault::TxnCommitMarksAborted) {
                // Marker-type confusion: a committed transaction
                // reported as aborted, so every reader throws its
                // records away on purpose.
                true
            } else {
                !request.committed
            };
            if disown && !faults.contains(&Fault::TxnAbortListOmitted) {
                log.aborted.push((producer_id, first_offset));
            }
        }
        // The offsets go with the transaction: published on commit,
        // dropped on abort.
        let pending = state
            .txns
            .get_mut(&request.transactional_id)
            .map(|txn| std::mem::take(&mut txn.pending_offsets))
            .unwrap_or_default();
        if request.committed {
            for (key, value) in pending {
                state.committed.insert(key, value);
            }
        }
        if let Some(txn) = state.txns.get_mut(&request.transactional_id) {
            txn.partitions.clear();
        }
    }

    let mut resp = EndTxnResponse::default();
    resp.error_code = code.0;
    frame_response(
        req_header.correlation_id,
        response_header_version(EndTxnRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// Whether the epoch on a transaction request is the current one.
///
/// A superseded epoch means the producer has been fenced: another one
/// took the id. Honouring it anyway would let both write into the same
/// transaction, which is the failure transactions exist to prevent.
fn txn_epoch_check(
    state: &ClusterState,
    transactional_id: &str,
    epoch: i16,
    faults: &[Fault],
) -> ErrorCode {
    let Some(txn) = state.txns.get(transactional_id) else {
        return ErrorCode::INVALID_PRODUCER_ID_MAPPING;
    };
    if faults.contains(&Fault::TxnIgnoresProducerEpoch) || epoch == txn.epoch {
        ErrorCode::NONE
    } else {
        ErrorCode::PRODUCER_FENCED
    }
}

/// The fetch isolation level that filters uncommitted data.
const READ_COMMITTED: i8 = 1;

/// What a produce needs to know about the open transactions, taken
/// before the logs are borrowed mutably: each id's current epoch and
/// the partitions it has announced.
type TxnSnapshot = HashMap<String, (i16, HashSet<(String, i32)>)>;

/// Whether `partition` was announced for the transaction `id` names.
fn state_txn_has_partition(
    snapshot: &TxnSnapshot,
    transactional_id: Option<&str>,
    topic: &str,
    partition: i32,
) -> bool {
    transactional_id
        .and_then(|id| snapshot.get(id))
        .is_some_and(|(_, partitions)| partitions.contains(&(topic.to_owned(), partition)))
}

/// Whether the partition leader should refuse this write because the
/// epoch it carries has been superseded.
fn fenced_at_the_leader(
    snapshot: &TxnSnapshot,
    transactional_id: Option<&str>,
    batch: Option<&records::RecordBatch>,
    faults: &[Fault],
) -> Option<ErrorCode> {
    if faults.contains(&Fault::ProduceAcceptsFencedEpoch) {
        return None;
    }
    let batch = batch?;
    let (epoch, _) = snapshot.get(transactional_id?)?;
    (batch.producer_epoch < *epoch).then_some(ErrorCode::PRODUCER_FENCED)
}

/// The last stable offset this partition reports.
///
/// Under `TxnStableOffsetIgnoresOpenTxn` it is simply the end of the
/// log, which offers every `read_committed` consumer the records of
/// transactions that have not finished — records that may yet be
/// aborted, and that the consumer asked specifically not to see.
fn stable_offset(log: &PartitionLog, faults: &[Fault]) -> i64 {
    if faults.contains(&Fault::TxnStableOffsetIgnoresOpenTxn) {
        log.next_offset
    } else {
        log.last_stable_offset()
    }
}

/// Whether a stamped batch's sequence is one this partition can accept,
/// and the error to answer with when it is not.
///
/// A gap cannot be papered over: the broker cannot tell "the batch you
/// skipped never existed" from "it is still in flight and will arrive
/// out of order", so the only honest answer is to refuse and let the
/// producer learn its stamp has drifted.
fn stamped_verdict(
    log: &PartitionLog,
    batch: Option<&records::RecordBatch>,
    faults: &[Fault],
) -> Option<ErrorCode> {
    if faults.contains(&Fault::ProduceAcceptsSequenceGaps) {
        return None;
    }
    let batch = batch?;
    if batch.producer_id < 0 || batch.base_sequence < 0 {
        return None;
    }
    let known = log.last_batch.get(&batch.producer_id)?;
    // The repeat of the last batch is a retry, not a gap; `duplicate_of`
    // answers that one.
    if batch.base_sequence == known.base_sequence || batch.base_sequence == known.next_sequence {
        return None;
    }
    Some(ErrorCode::OUT_OF_ORDER_SEQUENCE_NUMBER)
}

/// Where this batch landed the first time, when it is a repeat of the
/// last one this producer wrote here.
fn duplicate_of(
    log: &PartitionLog,
    batch: Option<&records::RecordBatch>,
    faults: &[Fault],
) -> Option<i64> {
    if faults.contains(&Fault::ProduceAppendsIdempotentRetries) {
        return None;
    }
    let batch = batch?;
    if batch.producer_id < 0 || batch.base_sequence < 0 {
        return None;
    }
    let known = log.last_batch.get(&batch.producer_id)?;
    (batch.base_sequence == known.base_sequence).then_some(known.base_offset)
}

/// The first offset in `log` whose record timestamp is at or after
/// `wanted`, as the `(timestamp, offset)` pair ListOffsets answers with.
///
/// Absolute offsets are reconstructed by counting rather than read off
/// the batches: this subject stores what it was produced byte for byte
/// (that is what `fetch/batch-integrity` checks), so the base offsets in
/// the log are the producer's zeros, not positions.
fn timestamp_search(log: &PartitionLog, wanted: i64) -> (i64, i64) {
    let Ok(batches) = records::decode_set(&mut log.bytes.clone().freeze()) else {
        return (-1, -1);
    };
    let mut offset = 0i64;
    for batch in &batches {
        match &batch.records {
            records::Records::Plain(plain) => {
                for record in plain {
                    let at = batch.base_timestamp + record.timestamp_delta;
                    if at >= wanted {
                        return (at, offset);
                    }
                    offset += 1;
                }
            }
            // Opaque without decompressing, and nothing this subject
            // produces is compressed; skip the span it covers.
            records::Records::Compressed { .. } => {
                offset += i64::from(batch.last_offset_delta) + 1;
            }
        }
    }
    (-1, -1)
}

/// Delete topics, which means they stop existing — the part a caller
/// cannot verify except by asking again.
fn delete_topics_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(DeleteTopicsRequest::MIN_VERSION..=DeleteTopicsRequest::MAX_VERSION).contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(DeleteTopicsRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = DeleteTopicsRequest::decode(&mut frame, api_version).ok()?;

    // v6+ carries the `topics` array; below that, a list of names.
    let names: Vec<String> = if request.topics.is_empty() {
        request.topic_names.clone()
    } else {
        request
            .topics
            .iter()
            .filter_map(|t| t.name.clone())
            .collect()
    };

    let mut results = Vec::with_capacity(names.len());
    for name in &names {
        let existed = state.created.contains_key(name);
        if existed && !faults.contains(&Fault::DeleteTopicsKeepsTheTopic) {
            if let Some(id) = state.created.remove(name) {
                state.topic_names.remove(&id);
            }
            state.logs.retain(|(topic, _), _| topic != name);
        }
        let mut result = DeletableTopicResult::default();
        result.name = Some(name.clone());
        result.error_code = if existed {
            ErrorCode::NONE.0
        } else {
            ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.0
        };
        results.push(result);
    }

    let mut resp = DeleteTopicsResponse::default();
    resp.responses = results;
    frame_response(
        req_header.correlation_id,
        response_header_version(DeleteTopicsRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// DescribeGroups: who is in the group, which is the only view an
/// operator or an admin client has of who holds what.
fn describe_groups_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &ClusterState,
) -> Option<BytesMut> {
    if !(DescribeGroupsRequest::MIN_VERSION..=DescribeGroupsRequest::MAX_VERSION)
        .contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(DescribeGroupsRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = DescribeGroupsRequest::decode(&mut frame, api_version).ok()?;

    let mut resp = DescribeGroupsResponse::default();
    resp.groups = request
        .groups
        .iter()
        .map(|group_id| {
            let mut described = DescribedGroup::default();
            described.group_id = group_id.clone();
            match state.groups.get(group_id) {
                None => {
                    described.error_code = ErrorCode::NONE.0;
                    described.group_state = "Dead".into();
                }
                Some(group) => {
                    described.error_code = ErrorCode::NONE.0;
                    described.group_state = "Stable".into();
                    described.protocol_type = group.protocol_type.clone();
                    described.protocol_data = group.protocol_name.clone();
                    if !faults.contains(&Fault::DescribeGroupsHidesMembers) {
                        described.members = group
                            .members
                            .iter()
                            .map(|member_id| {
                                let mut member = DescribedGroupMember::default();
                                member.member_id = member_id.clone();
                                member.client_id = CLIENT_ID_LABEL.into();
                                member.client_host = "/127.0.0.1".into();
                                // Handed back exactly as the leader
                                // supplied it, like everywhere else.
                                member.member_assignment = group
                                    .assignments
                                    .get(member_id)
                                    .cloned()
                                    .unwrap_or_default();
                                member
                            })
                            .collect();
                    }
                }
            }
            described
        })
        .collect();
    frame_response(
        req_header.correlation_id,
        response_header_version(DescribeGroupsRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// The client id this subject reports for its members.
const CLIENT_ID_LABEL: &str = "odradek-acceptance";

/// Under the `ProduceRewrites…Batches` faults, re-encode a compressed
/// batch with a different max timestamp — a crc-covered field, so the
/// stored bytes stop being the produced ones while still decoding.
///
/// Which codec is rewritten is read from the batch's own attributes, so
/// each fault reaches exactly the check that produces that codec.
fn rewrite_compressed(
    set: &Bytes,
    batches: &[records::RecordBatch],
    faults: &[Fault],
) -> Option<Bytes> {
    let batch = batches.first()?;
    if !matches!(batch.records, records::Records::Compressed { .. }) {
        return None;
    }
    let wanted = match batch.attributes & 0b111 {
        1 => Fault::ProduceRewritesGzipBatches,
        2 => Fault::ProduceRewritesSnappyBatches,
        3 => Fault::ProduceRewritesLz4Batches,
        4 => Fault::ProduceRewritesZstdBatches,
        _ => return None,
    };
    if !faults.contains(&wanted) {
        return None;
    }
    let _ = set;
    let mut rewritten = batch.clone();
    rewritten.max_timestamp += 1;
    let mut out = BytesMut::new();
    rewritten.encode_to(&mut out).ok()?;
    Some(out.freeze())
}

/// AddOffsetsToTxn: the group's offsets are part of this transaction.
///
/// Nothing to record beyond the epoch check — the offsets themselves
/// arrive with TxnOffsetCommit — but a coordinator that answered
/// without checking would let a fenced producer enrol a group.
fn add_offsets_to_txn_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(AddOffsetsToTxnRequest::MIN_VERSION..=AddOffsetsToTxnRequest::MAX_VERSION)
        .contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(AddOffsetsToTxnRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = AddOffsetsToTxnRequest::decode(&mut frame, api_version).ok()?;

    let code = txn_epoch_check(
        state,
        &request.transactional_id,
        request.producer_epoch,
        faults,
    );
    let mut resp = AddOffsetsToTxnResponse::default();
    resp.error_code = code.0;
    frame_response(
        req_header.correlation_id,
        response_header_version(AddOffsetsToTxnRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}

/// TxnOffsetCommit: hold the offset with the transaction.
fn txn_offset_commit_exchange(
    mut frame: Bytes,
    api_version: i16,
    faults: &[Fault],
    state: &mut ClusterState,
) -> Option<BytesMut> {
    if !(TxnOffsetCommitRequest::MIN_VERSION..=TxnOffsetCommitRequest::MAX_VERSION)
        .contains(&api_version)
    {
        return None;
    }
    let hv = header::request_header_version(TxnOffsetCommitRequest::API_KEY, api_version)?;
    let req_header = RequestHeader::decode(&mut frame, hv).ok()?;
    let request = TxnOffsetCommitRequest::decode(&mut frame, api_version).ok()?;

    let code = txn_epoch_check(
        state,
        &request.transactional_id,
        request.producer_epoch,
        faults,
    );
    if code.is_ok() {
        let immediate = faults.contains(&Fault::TxnOffsetsPublishImmediately);
        for topic in &request.topics {
            for p in &topic.partitions {
                let key = (
                    request.group_id.clone(),
                    topic.name.clone(),
                    p.partition_index,
                );
                let value = (p.committed_offset, p.committed_metadata.clone());
                if immediate {
                    state.committed.insert(key, value);
                } else if let Some(txn) = state.txns.get_mut(&request.transactional_id) {
                    txn.pending_offsets.insert(key, value);
                }
            }
        }
    }

    let mut resp = TxnOffsetCommitResponse::default();
    resp.topics = request
        .topics
        .iter()
        .map(|topic| {
            let mut out = TxnOffsetCommitResponseTopic::default();
            out.name = topic.name.clone();
            out.partitions = topic
                .partitions
                .iter()
                .map(|p| {
                    let mut entry = TxnOffsetCommitResponsePartition::default();
                    entry.partition_index = p.partition_index;
                    entry.error_code = code.0;
                    entry
                })
                .collect();
            out
        })
        .collect();
    frame_response(
        req_header.correlation_id,
        response_header_version(TxnOffsetCommitRequest::API_KEY, api_version),
        |out| resp.encode(out, api_version),
        false,
    )
}
