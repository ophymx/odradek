//! Producer: batch records per partition, optionally compress, route to
//! the partition leader, and retry through leadership changes.
//!
//! Two delivery styles share one path: [`Producer::produce`] sends the
//! given records as one batch immediately, while [`Producer::enqueue`]
//! buffers per partition and delivers when a partition's buffer exceeds
//! [`ProducerConfig::batch_max_bytes`] or on [`Producer::flush`]. What
//! this layer owns is delivery: version selection per broker, leader
//! routing via the [`Cluster`] cache, and retries that invalidate stale
//! leadership rather than hammering the same broker.
//!
//! # Throughput comes from flushing, not from enqueueing
//!
//! [`Producer::flush`] delivers every buffered partition *at once*:
//! brokers match responses to requests by correlation id, so N
//! partitions cost one round trip's latency, not N. With `acks = -1` —
//! where a round trip is a full ISR commit, easily milliseconds — that
//! is the difference between a per-partition ceiling and a per-flush
//! one, and it is why adding partitions speeds this producer up rather
//! than slowing it down.
//!
//! [`Producer::enqueue`]'s size trigger, by contrast, delivers inline:
//! it hands back the [`Delivery`] on the same `.await` the caller made,
//! which means one round trip in the caller's path. That is the price
//! of a caller-driven producer with no background task, and it is a
//! deliberate trade. A throughput-shaped caller buffers with a
//! `batch_max_bytes` large enough that the trigger rarely fires and
//! drives the wire from [`Producer::flush`].

use std::collections::HashMap;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use odradek_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use odradek_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use odradek_protocol::messages::produce_response::ProduceResponse;
use odradek_protocol::records::{Compression, Record, RecordBatch, Records};

use crate::cluster::Cluster;
use crate::compression::compress;
use crate::conn;
use crate::error::ClientError;
use crate::join::join_all;
use crate::retry::{Attempt, or_mark_stale, retry_loop};

/// Produce versions this producer speaks: name-addressed (v13+ switches
/// to topic ids).
const PRODUCE_SUPPORTED: (i16, i16) = (3, 12);

/// Delivery knobs.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProducerConfig {
    /// Acknowledgement level; -1 = full ISR.
    pub acks: i16,
    /// Broker-side timeout per produce request.
    pub request_timeout_ms: i32,
    /// Total delivery attempts per call (first try included).
    pub max_attempts: u32,
    /// Pause between attempts.
    pub retry_backoff: Duration,
    /// Codec for produced batches (gzip, lz4, snappy, zstd).
    pub compression: Compression,
    /// [`Producer::enqueue`] delivers a partition's buffer once its
    /// estimated size passes this (pre-compression bytes).
    pub batch_max_bytes: usize,
    /// Number each batch so the broker can discard a retry it has
    /// already applied (default: on).
    ///
    /// Without this a produce that succeeds and whose *acknowledgement*
    /// is lost gets retried, and the broker has no way to tell the
    /// retry from a second write — the batch is appended twice. With
    /// it, the producer takes an id from the broker and numbers every
    /// batch per partition, and the broker drops a sequence it has
    /// already seen.
    ///
    /// It is not free: it costs one InitProducerId round trip on the
    /// first produce, and it requires `acks = -1`, because a batch the
    /// full ISR has not acknowledged can be lost in a way that breaks
    /// the sequence. A producer configured otherwise refuses to start
    /// rather than silently offering a guarantee it cannot keep.
    pub idempotent: bool,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        ProducerConfig {
            acks: -1,
            request_timeout_ms: 10_000,
            // Fresh topics can take seconds to elect leaders; budget for it.
            max_attempts: 20,
            retry_backoff: Duration::from_millis(250),
            compression: Compression::None,
            batch_max_bytes: 16 * 1024,
            idempotent: true,
        }
    }
}

/// One delivered batch: where it landed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Delivery {
    pub topic: String,
    pub partition: i32,
    pub base_offset: i64,
    pub records: usize,
}

#[derive(Debug, Default)]
struct PendingBatch {
    records: Vec<Record>,
    estimated_bytes: usize,
}

/// A producer over a connected [`Cluster`].
#[derive(Debug)]
pub struct Producer {
    cluster: Cluster,
    config: ProducerConfig,
    pending: HashMap<(String, i32), PendingBatch>,
    /// Round-robin cursor for keyless [`Producer::enqueue_keyed`] records.
    next_round_robin: u64,
    /// The identity the broker issued, once something has been produced.
    identity: Option<ProducerIdentity>,
    /// The next sequence number owed per partition.
    sequences: HashMap<(String, i32), i32>,
}

/// The acknowledgement level idempotence requires: every in-sync
/// replica.
const ACKS_ALL: i16 = -1;

/// Refusing beats pretending.
///
/// Below the full ISR a batch the broker took can still be lost, and the
/// next one then carries a sequence whose predecessor the broker never
/// saw — so the guarantee this configuration asks for cannot be given,
/// and silently not giving it is the worst of the three options.
fn check_idempotent_acks(acks: i16) -> Result<(), ClientError> {
    if acks == ACKS_ALL {
        return Ok(());
    }
    Err(ClientError::Config(format!(
        "idempotent produce needs acks = {ACKS_ALL} (the full ISR), not {acks}"
    )))
}

/// Claim the next sequence range for one partition.
///
/// Split out from the identity handshake so the arithmetic is testable
/// without a broker: which sequence a batch carries is the part that has
/// to be right, and it is pure.
fn next_stamp(
    identity: ProducerIdentity,
    sequences: &mut HashMap<(String, i32), i32>,
    key: &(String, i32),
    records: usize,
) -> BatchStamp {
    let base_sequence = *sequences.get(key).unwrap_or(&0);
    let count = i32::try_from(records).unwrap_or(i32::MAX);
    // Sequences wrap at i32::MAX, which is what the broker expects;
    // saturating would stall a long-lived producer instead.
    sequences.insert(key.clone(), base_sequence.wrapping_add(count));
    BatchStamp {
        producer_id: identity.id,
        producer_epoch: identity.epoch,
        base_sequence,
    }
}

/// What one batch carries so the broker can recognize a repeat of it.
///
/// The triple is the whole mechanism: a broker keeps the last few
/// sequences per (producer id, partition) and drops a batch whose
/// numbers it has already applied. Which is why a retry must carry the
/// *same* stamp, and why a gap is an error rather than something to
/// paper over — the broker cannot tell a gap from a batch it lost.
#[derive(Debug, Clone, Copy)]
struct BatchStamp {
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
}

/// What the broker knows this producer as.
///
/// The epoch is the broker's way of retiring an id: a producer that
/// re-initializes gets a higher one, and batches carrying the old epoch
/// are refused rather than interleaved with the new producer's.
#[derive(Debug, Clone, Copy)]
struct ProducerIdentity {
    id: i64,
    epoch: i16,
}

impl Producer {
    pub fn new(cluster: Cluster) -> Producer {
        Producer::with_config(cluster, ProducerConfig::default())
    }

    pub fn with_config(cluster: Cluster, config: ProducerConfig) -> Producer {
        Producer {
            cluster,
            config,
            pending: HashMap::new(),
            next_round_robin: 0,
            identity: None,
            sequences: HashMap::new(),
        }
    }

    /// The underlying cluster, e.g. for metadata queries.
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// Buffer one record for `topic[partition]`. Delivers the partition's
    /// whole buffer (as one batch) when it passes
    /// [`ProducerConfig::batch_max_bytes`]; otherwise records wait for
    /// [`Producer::flush`].
    pub async fn enqueue(
        &mut self,
        topic: &str,
        partition: i32,
        record: Record,
    ) -> Result<Option<Delivery>, ClientError> {
        let pending = self
            .pending
            .entry((topic.to_owned(), partition))
            .or_default();
        pending.estimated_bytes += estimate_record_size(&record);
        pending.records.push(record);
        if pending.estimated_bytes >= self.config.batch_max_bytes {
            return Ok(Some(self.flush_partition(topic, partition).await?));
        }
        Ok(None)
    }

    /// Buffer one record for `topic`, picking the partition from the
    /// record's key with Kafka's default partitioner — murmur2 over the
    /// key bytes, then `(hash & 0x7fffffff) % partition_count` — so
    /// records with equal keys land on the same partition as they would
    /// from the Java client. Keyless records round-robin across the
    /// topic's partitions. Refreshes metadata once when the topic's
    /// partition count is unknown; otherwise delivery follows the
    /// [`Producer::enqueue`] rules.
    pub async fn enqueue_keyed(
        &mut self,
        topic: &str,
        record: Record,
    ) -> Result<Option<Delivery>, ClientError> {
        // Per record, so it must stay cheap: a count, not a copy of the
        // topic's whole partition vector.
        let mut count = self.cluster.partition_count(topic);
        if count.is_none() {
            self.cluster.refresh_metadata(&[topic]).await?;
            count = self.cluster.partition_count(topic);
        }
        let count = count
            .filter(|count| *count > 0)
            .ok_or_else(|| ClientError::UnknownLeader {
                topic: topic.to_owned(),
                partition: -1,
            })?;
        let count = i32::try_from(count).unwrap_or(i32::MAX);
        let partition = match &record.key {
            Some(key) => partition_for_key(key, count),
            None => {
                let cursor = self.next_round_robin;
                self.next_round_robin = self.next_round_robin.wrapping_add(1);
                let index = cursor % u64::try_from(count).expect("partition count is positive");
                i32::try_from(index).expect("index below partition count fits i32")
            }
        };
        self.enqueue(topic, partition, record).await
    }

    /// Records currently buffered across all partitions.
    pub fn buffered(&self) -> usize {
        self.pending.values().map(|p| p.records.len()).sum()
    }

    /// Deliver every buffered partition, one batch each, **all at
    /// once**: every partition's request goes on the wire before any
    /// response comes back, so a flush costs one round trip rather than
    /// one per partition.
    ///
    /// Returns the deliveries sorted by `(topic, partition)`, whatever
    /// order the brokers answered in. On error the partitions that
    /// failed keep their buffers, so a later flush or a caller-level
    /// retry does not lose records; partitions that succeeded are
    /// delivered (their offsets are lost with the error, as before).
    /// The reported error is the failing partition's lowest by
    /// `(topic, partition)`, so a repeated failure reports repeatably.
    pub async fn flush(&mut self) -> Result<Vec<Delivery>, ClientError> {
        let batches: Vec<((String, i32), PendingBatch)> = self.pending.drain().collect();
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        // Stamps are reserved here, before anything is sent: these
        // deliveries run concurrently and `batch_stamp` needs the
        // producer, so the sequence for every partition is claimed up
        // front and the parallel half only spends what it was given.
        let mut stamped = Vec::with_capacity(batches.len());
        for (key, pending) in batches {
            match self.batch_stamp(&key, pending.records.len()).await {
                Ok(stamp) => stamped.push((key, pending, stamp)),
                Err(e) => {
                    // Whatever was drained but never sent goes back, or
                    // a failure to get an id would lose every buffer.
                    self.restore(key, pending);
                    for (key, pending, _) in stamped {
                        self.restore(key, pending);
                    }
                    return Err(e);
                }
            }
        }

        let cluster = &self.cluster;
        let config = &self.config;
        let attempts: Vec<_> = stamped
            .into_iter()
            .map(|(key, pending, stamp)| async move {
                // The clone is what lets a failed partition keep its
                // records: encoding consumes them.
                let outcome = deliver(
                    cluster,
                    config,
                    &key.0,
                    key.1,
                    pending.records.clone(),
                    stamp,
                )
                .await;
                (key, pending, stamp, outcome)
            })
            .collect();
        let results = join_all(attempts).await;

        let mut deliveries = Vec::with_capacity(results.len());
        let mut failure: Option<((String, i32), ClientError)> = None;
        for (key, pending, stamp, outcome) in results {
            match outcome {
                Ok(base_offset) => deliveries.push(Delivery {
                    topic: key.0,
                    partition: key.1,
                    base_offset,
                    records: pending.records.len(),
                }),
                Err(e) => {
                    // Rewind the sequence so the retry carries the same
                    // numbers; advancing past a batch the broker never
                    // took would leave a gap it rejects everything after.
                    if let Some(stamp) = stamp {
                        self.sequences.insert(key.clone(), stamp.base_sequence);
                    }
                    self.restore(key.clone(), pending);
                    if failure.as_ref().is_none_or(|(worst, _)| key < *worst) {
                        failure = Some((key, e));
                    }
                }
            }
        }
        if let Some((_, e)) = failure {
            return Err(e);
        }
        deliveries.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
        Ok(deliveries)
    }

    async fn flush_partition(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<Delivery, ClientError> {
        let key = (topic.to_owned(), partition);
        let pending = self.pending.remove(&key).unwrap_or_default();
        let count = pending.records.len();
        // The identity is taken once and kept: a new one would restart
        // every sequence, which is exactly what makes a retry look like
        // a new write.
        let stamp = match self.batch_stamp(&key, count).await {
            Ok(stamp) => stamp,
            Err(e) => {
                self.restore(key, pending);
                return Err(e);
            }
        };
        match deliver(
            &self.cluster,
            &self.config,
            topic,
            partition,
            pending.records.clone(),
            stamp,
        )
        .await
        {
            Ok(base_offset) => Ok(Delivery {
                topic: topic.to_owned(),
                partition,
                base_offset,
                records: count,
            }),
            Err(e) => {
                // The sequence is *not* advanced past a failed batch:
                // the retry has to carry the same numbers, or the broker
                // sees a gap and rejects everything after it.
                if let Some(stamp) = stamp {
                    self.sequences.insert(key.clone(), stamp.base_sequence);
                }
                self.restore(key, pending);
                Err(e)
            }
        }
    }

    /// The producer id, epoch and base sequence this batch carries, or
    /// `None` when idempotence is off.
    ///
    /// Reserves the sequence range before the batch goes out, so a
    /// concurrent flush of another partition cannot take the same
    /// numbers — sequences are per partition, but the counter map is
    /// shared.
    async fn batch_stamp(
        &mut self,
        key: &(String, i32),
        records: usize,
    ) -> Result<Option<BatchStamp>, ClientError> {
        if !self.config.idempotent {
            return Ok(None);
        }
        check_idempotent_acks(self.config.acks)?;
        if self.identity.is_none() {
            self.identity = Some(init_producer_id(&self.cluster, &self.config).await?);
        }
        let identity = self.identity.expect("set immediately above");
        Ok(Some(next_stamp(
            identity,
            &mut self.sequences,
            key,
            records,
        )))
    }

    /// Put an undelivered batch back in front of whatever has been
    /// buffered for that partition since, so a caller-level retry or a
    /// later flush does not lose it — or reorder it.
    fn restore(&mut self, key: (String, i32), pending: PendingBatch) {
        let slot = self.pending.entry(key).or_default();
        slot.estimated_bytes += pending.estimated_bytes;
        let mut records = pending.records;
        records.append(&mut slot.records);
        slot.records = records;
    }

    /// Produce `records` as one batch to `topic[partition]`, compressed
    /// per the config; returns the broker-assigned base offset.
    pub async fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        records: Vec<Record>,
    ) -> Result<i64, ClientError> {
        let key = (topic.to_owned(), partition);
        let stamp = self.batch_stamp(&key, records.len()).await?;
        let outcome = deliver(
            &self.cluster,
            &self.config,
            topic,
            partition,
            records,
            stamp,
        )
        .await;
        if outcome.is_err() {
            // Hand the sequence back: a retry has to reuse it.
            if let Some(stamp) = stamp {
                self.sequences.insert(key, stamp.base_sequence);
            }
        }
        outcome
    }
}

/// InitProducerId versions this client speaks. v0 is enough for the
/// idempotent case; the later ones add transactional fields.
const INIT_PRODUCER_ID_SUPPORTED: (i16, i16) = (0, InitProducerIdRequest::MAX_VERSION);

/// Take an id from the broker, once, for this producer.
///
/// Idempotence is per (producer id, partition, sequence), so the id has
/// to be stable for the life of the producer: acquiring a new one would
/// restart every sequence at zero and make the broker treat a retry as
/// a fresh write, which is the thing being prevented.
async fn init_producer_id(
    cluster: &Cluster,
    config: &ProducerConfig,
) -> Result<ProducerIdentity, ClientError> {
    // A broker that has just started is still loading its transaction
    // coordinator, and says so. That is the same "ask again shortly"
    // the produce path already retries — and it happens on exactly the
    // first produce against a fresh cluster, which is when a user is
    // most likely to be watching.
    retry_loop(
        &mut &*cluster,
        config.max_attempts,
        config.retry_backoff,
        |cluster| {
            let cluster: &Cluster = cluster;
            Box::pin(async move {
                match init_producer_id_once(cluster).await {
                    Ok(identity) => Attempt::Done(identity),
                    // `is_retriable` already knows the coordinator codes;
                    // nothing here is partition-scoped, so no metadata to
                    // invalidate.
                    Err(e) if e.is_retriable() => Attempt::Retry(e),
                    Err(e) => Attempt::Fatal(e),
                }
            })
        },
    )
    .await
}

async fn init_producer_id_once(cluster: &Cluster) -> Result<ProducerIdentity, ClientError> {
    let broker = cluster.control_broker().await?;
    let version = broker
        .ranges
        .pick(InitProducerIdRequest::API_KEY, INIT_PRODUCER_ID_SUPPORTED)
        .map_err(|_| {
            // A bare NoCommonVersion(22) is true and useless. The
            // caller asked for a guarantee this broker cannot give, and
            // the fix is a configuration change they can make.
            ClientError::Config(
                "this broker does not support InitProducerId, so idempotent produce is \
                 unavailable; set ProducerConfig::idempotent = false to produce without it"
                    .into(),
            )
        })?;
    let mut request = InitProducerIdRequest::default();
    // No transactional id: this is the idempotent producer, not the
    // transactional one. The broker issues an id scoped to the session
    // rather than one it will recover after a restart.
    request.transactional_id = None;
    request.transaction_timeout_ms = -1;
    request.producer_id = -1;
    request.producer_epoch = -1;
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = broker
        .conn
        .request(InitProducerIdRequest::API_KEY, version, &body)
        .await?;
    let resp = conn::decode_body::<InitProducerIdResponse>(&mut resp, version)?;
    let code = ErrorCode(resp.error_code);
    if !code.is_ok() {
        return Err(ClientError::Broker(code));
    }
    Ok(ProducerIdentity {
        id: resp.producer_id,
        epoch: resp.producer_epoch,
    })
}

/// Deliver one batch to one partition, retrying through leadership
/// changes.
///
/// Free of `&mut Producer` on purpose: what a delivery needs is the
/// cluster and the config, nothing from the producer's buffers. That is
/// what lets [`Producer::flush`] have a delivery per partition in
/// flight at the same time.
async fn deliver(
    cluster: &Cluster,
    config: &ProducerConfig,
    topic: &str,
    partition: i32,
    records: Vec<Record>,
    stamp: Option<BatchStamp>,
) -> Result<i64, ClientError> {
    // Owned per-round captures keep the attempt future free of
    // outer borrows; the Bytes clone is a refcount bump.
    let set = encode_batch(records, config.compression, stamp)?;
    // A retriable failure means leadership (or the broker itself)
    // may have moved on; refetch rather than resend into the wall.
    retry_loop(
        &mut &*cluster,
        config.max_attempts,
        config.retry_backoff,
        |cluster| {
            let cluster: &Cluster = cluster;
            let set = set.clone();
            Box::pin(async move {
                let result = try_once(cluster, config, topic, partition, set).await;
                or_mark_stale(cluster, topic, partition, result)
            })
        },
    )
    .await
}

async fn try_once(
    cluster: &Cluster,
    config: &ProducerConfig,
    topic: &str,
    partition: i32,
    set: Bytes,
) -> Result<i64, ClientError> {
    let broker = cluster.partition_leader(topic, partition).await?;
    let leader = cluster.leader_id(topic, partition);
    let version = broker
        .ranges
        .pick(ProduceRequest::API_KEY, PRODUCE_SUPPORTED)?;

    let mut partition_data = PartitionProduceData::default();
    partition_data.index = partition;
    partition_data.records = Some(set);
    let mut topic_data = TopicProduceData::default();
    topic_data.name = topic.to_owned();
    topic_data.partition_data = vec![partition_data];
    let mut request = ProduceRequest::default();
    request.transactional_id = None;
    request.acks = config.acks;
    request.timeout_ms = config.request_timeout_ms;
    request.topic_data = vec![topic_data];
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;

    let mut resp = match broker
        .conn
        .request(ProduceRequest::API_KEY, version, &body)
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            // A dead pooled connection must not poison later retries.
            if matches!(
                e,
                ClientError::ConnectionClosed | ClientError::Io(_) | ClientError::Timeout(_)
            ) {
                if let Some(id) = leader {
                    cluster.forget_broker(id);
                }
            }
            return Err(e);
        }
    };
    let resp = conn::decode_body::<ProduceResponse>(&mut resp, version)?;
    let entry = resp
        .responses
        .iter()
        .find(|t| t.name == topic)
        .and_then(|t| t.partition_responses.iter().find(|p| p.index == partition))
        .ok_or_else(|| {
            ClientError::ProtocolViolation(format!("produce response omits {topic}[{partition}]"))
        })?;
    let code = ErrorCode(entry.error_code);
    if code.is_ok() {
        Ok(entry.base_offset)
    } else {
        Err(ClientError::Broker(code))
    }
}

/// The partition Kafka's default partitioner picks for `key` among
/// `count` partitions: `(murmur2(key) & 0x7fffffff) % count`.
fn partition_for_key(key: &[u8], count: i32) -> i32 {
    (murmur2(key) & 0x7fff_ffff) % count
}

/// Kafka's murmur2 (`org.apache.kafka.common.utils.Utils.murmur2`):
/// 32-bit MurmurHash2 with seed `0x9747b28c`, bit-identical to the Java
/// client so keyed records land on the same partitions.
fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let len = u32::try_from(data.len()).expect("key length fits u32");
    let mut h: u32 = SEED ^ len;

    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes(chunk.try_into().expect("chunk of 4"));
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    let tail = chunks.remainder();
    if tail.len() >= 3 {
        h ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        h ^= u32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        h ^= u32::from(tail[0]);
        h = h.wrapping_mul(M);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    i32::from_le_bytes(h.to_le_bytes())
}

/// Rough wire footprint of one record, for the batch-size trigger.
fn estimate_record_size(record: &Record) -> usize {
    let payload = record.key.as_ref().map_or(0, |k| k.len())
        + record.value.as_ref().map_or(0, |v| v.len())
        + record
            .headers
            .iter()
            .map(|h| h.key.len() + h.value.as_ref().map_or(0, |v| v.len()) + 8)
            .sum::<usize>();
    payload + 24
}

/// Assemble one record batch: offset deltas by position, timestamps
/// anchored at now, producer id -1 (not idempotent), records compressed
/// with `codec`.
fn encode_batch(
    mut records: Vec<Record>,
    codec: Compression,
    stamp: Option<BatchStamp>,
) -> Result<Bytes, ClientError> {
    if records.is_empty() {
        return Err(ClientError::ProtocolViolation(
            "cannot produce an empty record set".into(),
        ));
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    for (i, record) in records.iter_mut().enumerate() {
        record.offset_delta = i32::try_from(i).map_err(|_| {
            ClientError::ProtocolViolation("more than i32::MAX records in one batch".into())
        })?;
    }
    let max_delta = records.iter().map(|r| r.timestamp_delta).max().unwrap_or(0);
    let last = records.len() - 1;
    let batch_records = if codec == Compression::None {
        Records::Plain(records)
    } else {
        // Compression covers the serialized records, not the count.
        let count = i32::try_from(records.len()).unwrap_or(i32::MAX);
        let mut payload = BytesMut::new();
        for record in &records {
            record.encode(&mut payload)?;
        }
        Records::Compressed {
            count,
            payload: compress(codec, &payload)?,
        }
    };
    let batch = RecordBatch {
        base_offset: 0,
        attributes: codec.attribute_bits(),
        last_offset_delta: i32::try_from(last).unwrap_or(i32::MAX),
        base_timestamp: now_ms,
        max_timestamp: now_ms + max_delta,
        // -1 throughout is "not idempotent"; the broker then has no way
        // to recognize a retry and appends it as a new batch.
        producer_id: stamp.map_or(-1, |s| s.producer_id),
        producer_epoch: stamp.map_or(-1, |s| s.producer_epoch),
        base_sequence: stamp.map_or(-1, |s| s.base_sequence),
        records: batch_records,
        ..Default::default()
    };
    let mut out = BytesMut::new();
    // encode_to, not encode: this destination *is* a BytesMut, and the
    // generic path would encode into a scratch buffer and copy the whole
    // batch across — an extra allocation and memcpy of every byte
    // produced, on every produce.
    batch.encode_to(&mut out)?;
    // freeze() hands the buffer over; to_vec() would copy it.
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference vectors from Apache Kafka's own murmur2 test data
    /// (org.apache.kafka.common.utils.UtilsTest).
    const JAVA_VECTORS: &[(&[u8], i32)] = &[
        (b"21", -973932308),
        (b"foobar", -790332482),
        (b"a-little-bit-long-string", -985981536),
        (b"a-little-bit-longer-string", -1486304829),
        (
            b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
            -58897971,
        ),
        (b"", 275646681),
    ];

    #[test]
    fn murmur2_matches_the_java_client() {
        for (input, expected) in JAVA_VECTORS {
            assert_eq!(
                murmur2(input),
                *expected,
                "murmur2({:?})",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn keyed_partition_selection_matches_the_java_client() {
        // (hash & 0x7fffffff) % n, checked against the vectors above.
        assert_eq!(
            partition_for_key(b"21", 12),
            (-973932308i32 & 0x7fffffff) % 12
        );
        assert_eq!(
            partition_for_key(b"foobar", 7),
            (-790332482i32 & 0x7fffffff) % 7
        );
        // Concrete values, so a broken mask or modulus cannot cancel out.
        assert_eq!(partition_for_key(b"21", 12), 0);
        assert_eq!(partition_for_key(b"foobar", 7), 0);
        assert_eq!(partition_for_key(b"a-little-bit-long-string", 5), 2);
        assert_eq!(partition_for_key(b"a-little-bit-longer-string", 3), {
            (-1486304829i32 & 0x7fffffff) % 3
        });
        // Same key, same partition, always.
        assert_eq!(partition_for_key(b"21", 12), partition_for_key(b"21", 12));
    }
}

#[cfg(test)]
mod idempotence_tests {
    use odradek_protocol::records::decode_set;

    use super::*;

    fn record(value: &str) -> Record {
        Record {
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            ..Default::default()
        }
    }

    /// Without a stamp the batch says "-1" in all three fields, which is
    /// how a non-idempotent producer identifies itself: the broker then
    /// has no way to recognize a retry and appends it again.
    #[test]
    fn an_unstamped_batch_disclaims_a_producer_id() {
        let set = encode_batch(vec![record("a")], Compression::None, None).unwrap();
        let batch = &decode_set(&mut set.clone()).unwrap()[0];
        assert_eq!(batch.producer_id, -1);
        assert_eq!(batch.producer_epoch, -1);
        assert_eq!(batch.base_sequence, -1);
    }

    /// And a stamped one carries exactly what it was given — the triple
    /// the broker dedupes on.
    #[test]
    fn a_stamped_batch_carries_the_triple() {
        let stamp = BatchStamp {
            producer_id: 4242,
            producer_epoch: 7,
            base_sequence: 19,
        };
        let set = encode_batch(
            vec![record("a"), record("b")],
            Compression::None,
            Some(stamp),
        )
        .unwrap();
        let batch = &decode_set(&mut set.clone()).unwrap()[0];
        assert_eq!(batch.producer_id, 4242);
        assert_eq!(batch.producer_epoch, 7);
        assert_eq!(batch.base_sequence, 19);
        // last_offset_delta tells the broker the range this batch
        // covers, and so which sequences it consumed.
        assert_eq!(batch.last_offset_delta, 1);
    }

    /// Sequences advance by the record count, so consecutive batches
    /// leave no gap. A gap is not a smaller problem than a duplicate:
    /// the broker cannot tell it from a batch it lost, and refuses
    /// everything after it.
    #[test]
    fn sequences_advance_by_the_record_count() {
        let identity = ProducerIdentity { id: 1, epoch: 0 };
        let mut sequences = HashMap::new();
        let key = ("t".to_owned(), 0);

        assert_eq!(
            next_stamp(identity, &mut sequences, &key, 3).base_sequence,
            0
        );
        assert_eq!(
            next_stamp(identity, &mut sequences, &key, 2).base_sequence,
            3
        );
        // Per partition, not per producer: another partition starts over.
        let other = ("t".to_owned(), 1);
        assert_eq!(
            next_stamp(identity, &mut sequences, &other, 1).base_sequence,
            0
        );
    }

    /// A failed batch puts its sequence back, so the retry carries the
    /// same numbers. This is the whole point: a retry with a *new*
    /// sequence is a second write, which is what idempotence exists to
    /// prevent.
    #[test]
    fn a_rewound_sequence_is_reused() {
        let identity = ProducerIdentity { id: 1, epoch: 0 };
        let mut sequences = HashMap::new();
        let key = ("t".to_owned(), 0);

        let attempt = next_stamp(identity, &mut sequences, &key, 4);
        // What the failure path does.
        sequences.insert(key.clone(), attempt.base_sequence);
        let retry = next_stamp(identity, &mut sequences, &key, 4);
        assert_eq!(retry.base_sequence, attempt.base_sequence);

        // And the batch after the successful retry continues from there.
        let next = next_stamp(identity, &mut sequences, &key, 1);
        assert_eq!(next.base_sequence, 4);
    }

    /// Idempotence with weaker acks is refused rather than pretended.
    #[test]
    fn idempotence_requires_full_acks() {
        assert!(check_idempotent_acks(ACKS_ALL).is_ok());
        for acks in [0, 1] {
            assert!(
                matches!(check_idempotent_acks(acks), Err(ClientError::Config(_))),
                "acks={acks} should be refused"
            );
        }
    }

    /// The default configuration is the safe one: on, with the acks it
    /// needs.
    #[test]
    fn the_default_is_idempotent_and_consistent() {
        let config = ProducerConfig::default();
        assert!(config.idempotent);
        assert!(check_idempotent_acks(config.acks).is_ok());
    }
}
