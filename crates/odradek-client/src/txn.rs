//! The wire half of transactional produce: the five requests that make
//! a set of writes atomic.
//!
//! A Kafka transaction is not a session held open on a connection. It is
//! a running agreement between a producer and one broker — the
//! *transaction coordinator* for its transactional id — recorded in an
//! internal log so it survives both of them. The producer announces
//! every partition it is about to write to; the coordinator remembers;
//! and at the end the coordinator writes a commit or abort marker into
//! each of those partitions. Consumers reading committed data stop at
//! the last stable offset and skip what the markers disown.
//!
//! Five requests, four of them here:
//!
//! - **InitProducerId** with a transactional id, which does more than
//!   hand out an id: it fences every earlier producer using that id and
//!   aborts whatever transaction they left open. That is what makes a
//!   crashed producer recoverable — its successor takes the name and
//!   the coordinator cleans up behind it.
//! - **AddPartitionsToTxn** before the first write to each partition.
//!   The coordinator cannot write a marker into a partition it was
//!   never told about, so a write to an unannounced partition is not
//!   part of the transaction and the broker refuses it.
//! - **AddOffsetsToTxn**, which is the same announcement for the
//!   consumer offsets topic, followed by **TxnOffsetCommit** — the pair
//!   that puts "where I read to" inside the same transaction as "what I
//!   wrote", which is the whole of exactly-once consume-transform-produce.
//! - **EndTxn**, commit or abort.
//!
//! # Where each request goes
//!
//! Everything here addresses the transaction coordinator, discovered
//! with [`Cluster::transaction_coordinator`] — except TxnOffsetCommit,
//! which goes to the **group** coordinator. It is the group's offsets
//! that are being written, and the transaction coordinator already
//! learned about them from AddOffsetsToTxn. Sending either to the other
//! one earns `NOT_COORDINATOR`.
//!
//! # EndTxn returns before the transaction is visible
//!
//! A successful commit means the coordinator has durably decided to
//! commit, not that every marker has been written. A `read_committed`
//! consumer sees the records once the markers land in their partitions,
//! which is shortly after — so a test that commits and immediately
//! fetches is testing a race, not a guarantee.

use std::collections::BTreeMap;

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::add_offsets_to_txn_request::AddOffsetsToTxnRequest;
use odradek_protocol::messages::add_offsets_to_txn_response::AddOffsetsToTxnResponse;
use odradek_protocol::messages::add_partitions_to_txn_request::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnTopic,
};
use odradek_protocol::messages::add_partitions_to_txn_response::AddPartitionsToTxnResponse;
use odradek_protocol::messages::end_txn_request::EndTxnRequest;
use odradek_protocol::messages::end_txn_response::EndTxnResponse;
use odradek_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use odradek_protocol::messages::txn_offset_commit_response::TxnOffsetCommitResponse;

use crate::cluster::Cluster;
use crate::conn;
use crate::error::ClientError;
use crate::producer::{ProducerConfig, ProducerIdentity};
use crate::retry::{or_forget_coordinator, or_forget_txn_coordinator, retry_loop};

/// AddPartitionsToTxn versions a *client* may speak. v4 added batching
/// across transactional ids for the broker-to-broker verification path,
/// and the schema says so outright: "versions 3 and below will be
/// exclusively used by clients".
const ADD_PARTITIONS_SUPPORTED: (i16, i16) = (0, 3);

/// AddOffsetsToTxn versions this client speaks.
const ADD_OFFSETS_SUPPORTED: (i16, i16) = (0, 4);

/// TxnOffsetCommit versions this client speaks.
///
/// Capped below v5 deliberately: at v5 the request doubles as its own
/// AddOffsetsToTxn, but only when the broker has KIP-890 transactions V2
/// enabled, and sending v5 without it is an error. Staying at v4 means
/// one extra round trip and one set of semantics.
const TXN_OFFSET_COMMIT_SUPPORTED: (i16, i16) = (0, 4);

/// EndTxn versions this client speaks.
///
/// Capped below v5 for the same reason: v5 bumps the producer epoch on
/// every transaction and returns the new one, which a client must then
/// adopt. A client that sends v5 and ignores the returned epoch is
/// fenced by its own commit.
const END_TXN_SUPPORTED: (i16, i16) = (0, 4);

/// Who is asking, and about which transaction.
///
/// Bundled because every request here carries the same four things and
/// gets them wrong in the same way if they drift apart: the id names the
/// transaction, the producer id and epoch prove this is the producer
/// that owns it.
pub(crate) struct Txn<'a> {
    pub cluster: &'a Cluster,
    pub config: &'a ProducerConfig,
    pub transactional_id: &'a str,
    pub identity: ProducerIdentity,
}

/// Announce the partitions this transaction is about to write to.
///
/// Must precede the first write to each one. The coordinator's job at
/// the end is to write a marker into every partition it was told about,
/// so a partition it never heard of cannot be committed or aborted —
/// and the partition leader, which learns the transaction's membership
/// from the coordinator, rejects the write outright.
pub(crate) async fn add_partitions(
    txn: &Txn<'_>,
    partitions: &[(String, i32)],
) -> Result<(), ClientError> {
    if partitions.is_empty() {
        return Ok(());
    }
    // Grouped and sorted so a retry is byte-identical to the attempt it
    // repeats, and so a caller that hands over the same partitions in a
    // different order asks the same question.
    let mut by_topic: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
    for (topic, partition) in partitions {
        by_topic.entry(topic).or_default().push(*partition);
    }
    let topics: Vec<AddPartitionsToTxnTopic> = by_topic
        .into_iter()
        .map(|(name, mut partitions)| {
            partitions.sort_unstable();
            partitions.dedup();
            let mut topic = AddPartitionsToTxnTopic::default();
            topic.name = name.to_owned();
            topic.partitions = partitions;
            topic
        })
        .collect();

    retry_loop(
        &mut &*txn.cluster,
        txn.config.max_attempts,
        txn.config.retry_backoff,
        |cluster| {
            let cluster: &Cluster = cluster;
            let topics = topics.clone();
            Box::pin(async move {
                let result = add_partitions_once(cluster, txn, topics).await;
                or_forget_txn_coordinator(cluster, txn.transactional_id, result)
            })
        },
    )
    .await
}

async fn add_partitions_once(
    cluster: &Cluster,
    txn: &Txn<'_>,
    topics: Vec<AddPartitionsToTxnTopic>,
) -> Result<(), ClientError> {
    let broker = cluster
        .transaction_coordinator(txn.transactional_id)
        .await?;
    let version = broker
        .ranges
        .pick(AddPartitionsToTxnRequest::API_KEY, ADD_PARTITIONS_SUPPORTED)?;
    let mut request = AddPartitionsToTxnRequest::default();
    request.v3_and_below_transactional_id = txn.transactional_id.to_owned();
    request.v3_and_below_producer_id = txn.identity.id;
    request.v3_and_below_producer_epoch = txn.identity.epoch;
    request.v3_and_below_topics = topics;
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = broker
        .conn
        .request(AddPartitionsToTxnRequest::API_KEY, version, &body)
        .await?;
    let resp = conn::decode_body::<AddPartitionsToTxnResponse>(&broker.conn, &mut resp, version)?;
    // The error is per partition, and each one matters: a transaction
    // missing one of its partitions is not a transaction, so the first
    // refusal is the answer.
    for topic in &resp.results_by_topic_v3_and_below {
        for partition in &topic.results_by_partition {
            let code = ErrorCode(partition.partition_error_code);
            if !code.is_ok() {
                return Err(ClientError::Broker(code));
            }
        }
    }
    Ok(())
}

/// Tell the coordinator this transaction will also commit offsets for
/// `group_id`.
///
/// The offsets live in `__consumer_offsets`, which is a topic like any
/// other: this is [`add_partitions`] for the partition of it that holds
/// the group, so the coordinator writes a marker there too.
pub(crate) async fn add_offsets(txn: &Txn<'_>, group_id: &str) -> Result<(), ClientError> {
    retry_loop(
        &mut &*txn.cluster,
        txn.config.max_attempts,
        txn.config.retry_backoff,
        |cluster| {
            let cluster: &Cluster = cluster;
            Box::pin(async move {
                let result = add_offsets_once(cluster, txn, group_id).await;
                or_forget_txn_coordinator(cluster, txn.transactional_id, result)
            })
        },
    )
    .await
}

async fn add_offsets_once(
    cluster: &Cluster,
    txn: &Txn<'_>,
    group_id: &str,
) -> Result<(), ClientError> {
    let broker = cluster
        .transaction_coordinator(txn.transactional_id)
        .await?;
    let version = broker
        .ranges
        .pick(AddOffsetsToTxnRequest::API_KEY, ADD_OFFSETS_SUPPORTED)?;
    let mut request = AddOffsetsToTxnRequest::default();
    request.transactional_id = txn.transactional_id.to_owned();
    request.producer_id = txn.identity.id;
    request.producer_epoch = txn.identity.epoch;
    request.group_id = group_id.to_owned();
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = broker
        .conn
        .request(AddOffsetsToTxnRequest::API_KEY, version, &body)
        .await?;
    let resp = conn::decode_body::<AddOffsetsToTxnResponse>(&broker.conn, &mut resp, version)?;
    let code = ErrorCode(resp.error_code);
    if code.is_ok() {
        Ok(())
    } else {
        Err(ClientError::Broker(code))
    }
}

/// One consumed position, committed as part of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransactionalOffset {
    pub topic: String,
    pub partition: i32,
    /// The offset to resume from — one *past* the last record
    /// processed, the same convention as a plain offset commit.
    pub offset: i64,
    pub metadata: Option<String>,
}

impl TransactionalOffset {
    /// A position with no metadata.
    pub fn new(topic: impl Into<String>, partition: i32, offset: i64) -> TransactionalOffset {
        TransactionalOffset {
            topic: topic.into(),
            partition,
            offset,
            metadata: None,
        }
    }
}

/// Commit consumed positions inside the transaction.
///
/// Goes to the **group** coordinator, not the transaction one: these
/// are the group's offsets, and the transaction coordinator was told
/// about them by [`add_offsets`]. The generation and member id are
/// checked, so a member that has been rebalanced out cannot commit —
/// which is the point, since its successor already owns those
/// partitions.
pub(crate) async fn offset_commit(
    txn: &Txn<'_>,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    offsets: &[TransactionalOffset],
) -> Result<(), ClientError> {
    if offsets.is_empty() {
        return Ok(());
    }
    let mut by_topic: BTreeMap<&str, Vec<&TransactionalOffset>> = BTreeMap::new();
    for offset in offsets {
        by_topic.entry(&offset.topic).or_default().push(offset);
    }
    let topics: Vec<TxnOffsetCommitRequestTopic> = by_topic
        .into_iter()
        .map(|(name, offsets)| {
            let mut topic = TxnOffsetCommitRequestTopic::default();
            topic.name = name.to_owned();
            topic.partitions = offsets
                .into_iter()
                .map(|offset| {
                    let mut partition = TxnOffsetCommitRequestPartition::default();
                    partition.partition_index = offset.partition;
                    partition.committed_offset = offset.offset;
                    partition.committed_leader_epoch = -1;
                    partition.committed_metadata = offset.metadata.clone();
                    partition
                })
                .collect();
            topic
        })
        .collect();

    retry_loop(
        &mut &*txn.cluster,
        txn.config.max_attempts,
        txn.config.retry_backoff,
        |cluster| {
            let cluster: &Cluster = cluster;
            let topics = topics.clone();
            Box::pin(async move {
                let result =
                    offset_commit_once(cluster, txn, group_id, generation_id, member_id, topics)
                        .await;
                or_forget_coordinator(cluster, group_id, result)
            })
        },
    )
    .await
}

async fn offset_commit_once(
    cluster: &Cluster,
    txn: &Txn<'_>,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    topics: Vec<TxnOffsetCommitRequestTopic>,
) -> Result<(), ClientError> {
    let broker = cluster.coordinator(group_id).await?;
    let version = broker
        .ranges
        .pick(TxnOffsetCommitRequest::API_KEY, TXN_OFFSET_COMMIT_SUPPORTED)?;
    let mut request = TxnOffsetCommitRequest::default();
    request.transactional_id = txn.transactional_id.to_owned();
    request.group_id = group_id.to_owned();
    request.producer_id = txn.identity.id;
    request.producer_epoch = txn.identity.epoch;
    request.generation_id = generation_id;
    request.member_id = member_id.to_owned();
    request.group_instance_id = None;
    request.topics = topics;
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = broker
        .conn
        .request(TxnOffsetCommitRequest::API_KEY, version, &body)
        .await?;
    let resp = conn::decode_body::<TxnOffsetCommitResponse>(&broker.conn, &mut resp, version)?;
    for topic in &resp.topics {
        for partition in &topic.partitions {
            let code = ErrorCode(partition.error_code);
            if !code.is_ok() {
                return Err(ClientError::Broker(code));
            }
        }
    }
    Ok(())
}

/// Finish the transaction: `committed` decides which marker the
/// coordinator writes into every partition the transaction touched.
///
/// Returning means the coordinator has durably decided, not that the
/// markers are written — see the [module docs](self).
pub(crate) async fn end_txn(txn: &Txn<'_>, committed: bool) -> Result<(), ClientError> {
    retry_loop(
        &mut &*txn.cluster,
        txn.config.max_attempts,
        txn.config.retry_backoff,
        |cluster| {
            let cluster: &Cluster = cluster;
            Box::pin(async move {
                let result = end_txn_once(cluster, txn, committed).await;
                or_forget_txn_coordinator(cluster, txn.transactional_id, result)
            })
        },
    )
    .await
}

async fn end_txn_once(
    cluster: &Cluster,
    txn: &Txn<'_>,
    committed: bool,
) -> Result<(), ClientError> {
    let broker = cluster
        .transaction_coordinator(txn.transactional_id)
        .await?;
    let version = broker
        .ranges
        .pick(EndTxnRequest::API_KEY, END_TXN_SUPPORTED)?;
    let mut request = EndTxnRequest::default();
    request.transactional_id = txn.transactional_id.to_owned();
    request.producer_id = txn.identity.id;
    request.producer_epoch = txn.identity.epoch;
    request.committed = committed;
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = broker
        .conn
        .request(EndTxnRequest::API_KEY, version, &body)
        .await?;
    let resp = conn::decode_body::<EndTxnResponse>(&broker.conn, &mut resp, version)?;
    let code = ErrorCode(resp.error_code);
    if code.is_ok() {
        Ok(())
    } else {
        Err(ClientError::Broker(code))
    }
}
