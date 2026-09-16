//! Minimal producer: encode records as one batch, route it to the
//! partition leader, and retry through leadership changes.
//!
//! No batching-across-calls or compression yet — each call produces one
//! record batch synchronously. What it does own is delivery: version
//! selection per broker, leader routing via the [`Cluster`] cache, and
//! retries that invalidate stale leadership rather than hammering the
//! same broker.

use std::time::Duration;

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use odradek_protocol::messages::produce_response::ProduceResponse;
use odradek_protocol::records::{Record, RecordBatch, Records};

use crate::cluster::Cluster;
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
}

impl Default for ProducerConfig {
    fn default() -> Self {
        ProducerConfig {
            acks: -1,
            request_timeout_ms: 10_000,
            max_attempts: 5,
            retry_backoff: Duration::from_millis(100),
        }
    }
}

/// A producer over a connected [`Cluster`].
#[derive(Debug)]
pub struct Producer {
    cluster: Cluster,
    config: ProducerConfig,
}

impl Producer {
    pub fn new(cluster: Cluster) -> Producer {
        Producer::with_config(cluster, ProducerConfig::default())
    }

    pub fn with_config(cluster: Cluster, config: ProducerConfig) -> Producer {
        Producer { cluster, config }
    }

    /// The underlying cluster, e.g. for metadata queries.
    pub fn cluster(&mut self) -> &mut Cluster {
        &mut self.cluster
    }

    /// Produce `records` as one batch to `topic[partition]`; returns the
    /// broker-assigned base offset.
    pub async fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        records: Vec<Record>,
    ) -> Result<i64, ClientError> {
        let set = encode_batch(records)?;
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

/// Assemble one record batch: offset deltas by position, timestamps
/// anchored at now, producer id -1 (not idempotent).
fn encode_batch(mut records: Vec<Record>) -> Result<Vec<u8>, ClientError> {
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
    let batch = RecordBatch {
        base_offset: 0,
        last_offset_delta: i32::try_from(last).unwrap_or(i32::MAX),
        base_timestamp: now_ms,
        max_timestamp: now_ms + max_delta,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: Records::Plain(records),
        ..Default::default()
    };
    let mut out = BytesMut::new();
    batch.encode(&mut out)?;
    Ok(out.to_vec())
}
