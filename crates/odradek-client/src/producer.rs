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
use crate::retry::{or_mark_stale, retry_loop};

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

        let cluster = &self.cluster;
        let config = &self.config;
        let attempts: Vec<_> = batches
            .into_iter()
            .map(|(key, pending)| async move {
                // The clone is what lets a failed partition keep its
                // records: encoding consumes them.
                let outcome =
                    deliver(cluster, config, &key.0, key.1, pending.records.clone()).await;
                (key, pending, outcome)
            })
            .collect();
        let results = join_all(attempts).await;

        let mut deliveries = Vec::with_capacity(results.len());
        let mut failure: Option<((String, i32), ClientError)> = None;
        for (key, pending, outcome) in results {
            match outcome {
                Ok(base_offset) => deliveries.push(Delivery {
                    topic: key.0,
                    partition: key.1,
                    base_offset,
                    records: pending.records.len(),
                }),
                Err(e) => {
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
        match deliver(
            &self.cluster,
            &self.config,
            topic,
            partition,
            pending.records.clone(),
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
                self.restore(key, pending);
                Err(e)
            }
        }
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
        deliver(&self.cluster, &self.config, topic, partition, records).await
    }
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
) -> Result<i64, ClientError> {
    // Owned per-round captures keep the attempt future free of
    // outer borrows; the Bytes clone is a refcount bump.
    let set = encode_batch(records, config.compression)?;
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
fn encode_batch(mut records: Vec<Record>, codec: Compression) -> Result<Bytes, ClientError> {
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
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: batch_records,
        ..Default::default()
    };
    let mut out = BytesMut::new();
    batch.encode(&mut out)?;
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
