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

use std::collections::HashMap;
use std::time::Duration;

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use odradek_protocol::messages::produce_response::ProduceResponse;
use odradek_protocol::records::{Compression, Record, RecordBatch, Records};

use crate::cluster::Cluster;
use crate::compression::{attribute_bits, compress};
use crate::error::ClientError;

/// Produce versions this producer speaks: name-addressed (v13+ switches
/// to topic ids).
const PRODUCE_SUPPORTED: (i16, i16) = (3, 12);

/// Delivery knobs.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Acknowledgement level; -1 = full ISR.
    pub acks: i16,
    /// Broker-side timeout per produce request.
    pub request_timeout_ms: i32,
    /// Total delivery attempts per call (first try included).
    pub max_attempts: u32,
    /// Pause between attempts.
    pub retry_backoff: Duration,
    /// Codec for produced batches (gzip and lz4 supported).
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
        }
    }

    /// The underlying cluster, e.g. for metadata queries.
    pub fn cluster(&mut self) -> &mut Cluster {
        &mut self.cluster
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

    /// Records currently buffered across all partitions.
    pub fn buffered(&self) -> usize {
        self.pending.values().map(|p| p.records.len()).sum()
    }

    /// Deliver every buffered partition, one batch each. On error,
    /// undelivered partitions keep their buffers.
    pub async fn flush(&mut self) -> Result<Vec<Delivery>, ClientError> {
        let mut keys: Vec<(String, i32)> = self.pending.keys().cloned().collect();
        keys.sort();
        let mut deliveries = Vec::new();
        for (topic, partition) in keys {
            deliveries.push(self.flush_partition(&topic, partition).await?);
        }
        Ok(deliveries)
    }

    async fn flush_partition(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<Delivery, ClientError> {
        let pending = self
            .pending
            .remove(&(topic.to_owned(), partition))
            .unwrap_or_default();
        let count = pending.records.len();
        match self
            .produce(topic, partition, pending.records.clone())
            .await
        {
            Ok(base_offset) => Ok(Delivery {
                topic: topic.to_owned(),
                partition,
                base_offset,
                records: count,
            }),
            Err(e) => {
                // Put the batch back so a caller-level retry or later
                // flush does not lose it.
                let slot = self
                    .pending
                    .entry((topic.to_owned(), partition))
                    .or_default();
                slot.estimated_bytes += pending.estimated_bytes;
                let mut records = pending.records;
                records.append(&mut slot.records);
                slot.records = records;
                Err(e)
            }
        }
    }

    /// Produce `records` as one batch to `topic[partition]`, compressed
    /// per the config; returns the broker-assigned base offset.
    pub async fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        records: Vec<Record>,
    ) -> Result<i64, ClientError> {
        let set = encode_batch(records, self.config.compression)?;
        let mut last = None;
        for attempt in 0..self.config.max_attempts {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_backoff).await;
            }
            match self.try_once(topic, partition, &set).await {
                Ok(offset) => return Ok(offset),
                Err(e) if e.is_retriable() => {
                    // Leadership (or the broker itself) may have moved on;
                    // refetch rather than resend into the same wall.
                    self.cluster.mark_stale(topic);
                    last = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or(ClientError::ConnectionClosed))
    }

    async fn try_once(
        &mut self,
        topic: &str,
        partition: i32,
        set: &[u8],
    ) -> Result<i64, ClientError> {
        let broker = self
            .cluster
            .partition_leader(topic, partition)
            .await?
            .clone();
        let leader = self.cluster.leader_id(topic, partition);
        let version = broker
            .ranges
            .pick(ProduceRequest::API_KEY, PRODUCE_SUPPORTED)?;

        let request = ProduceRequest {
            transactional_id: None,
            acks: self.config.acks,
            timeout_ms: self.config.request_timeout_ms,
            topic_data: vec![TopicProduceData {
                name: topic.to_owned(),
                partition_data: vec![PartitionProduceData {
                    index: partition,
                    records: Some(bytes::Bytes::copy_from_slice(set)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
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
                if matches!(e, ClientError::ConnectionClosed | ClientError::Io(_)) {
                    if let Some(id) = leader {
                        self.cluster.forget_broker(id);
                    }
                }
                return Err(e);
            }
        };
        let resp = ProduceResponse::decode(&mut resp, version)?;
        let entry = resp
            .responses
            .iter()
            .find(|t| t.name == topic)
            .and_then(|t| t.partition_responses.iter().find(|p| p.index == partition))
            .ok_or_else(|| {
                ClientError::ProtocolViolation(format!(
                    "produce response omits {topic}[{partition}]"
                ))
            })?;
        let code = ErrorCode(entry.error_code);
        if code.is_ok() {
            Ok(entry.base_offset)
        } else {
            Err(ClientError::Broker(code))
        }
    }
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
fn encode_batch(mut records: Vec<Record>, codec: Compression) -> Result<Vec<u8>, ClientError> {
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
        attributes: attribute_bits(codec),
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
    Ok(out.to_vec())
}
