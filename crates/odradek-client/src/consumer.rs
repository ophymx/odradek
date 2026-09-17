//! Minimal consumer: fetch record batches from a partition leader and
//! materialize them as records with absolute offsets.
//!
//! No consumer groups, subscriptions, or offset commits yet — the caller
//! owns its position. What this layer owns is correctness of the fetch
//! path: leader routing with retry, batch decoding (crc verified by the
//! protocol layer), skipping records below the requested offset (brokers
//! return whole batches, which may start earlier), and skipping control
//! batches (transaction markers are not data).

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use odradek_protocol::messages::fetch_response::FetchResponse;
use odradek_protocol::messages::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use odradek_protocol::messages::list_offsets_response::ListOffsetsResponse;
use odradek_protocol::messages::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use odradek_protocol::messages::offset_commit_response::OffsetCommitResponse;
use odradek_protocol::messages::offset_fetch_request::{
    OffsetFetchRequest, OffsetFetchRequestTopic,
};
use odradek_protocol::messages::offset_fetch_response::OffsetFetchResponse;
use odradek_protocol::records::{Compression, RecordHeader, Records, decode_set};

use crate::cluster::Cluster;
use crate::error::ClientError;

/// Fetch versions this consumer speaks: name-addressed (v13+ switches to
/// topic ids).
const FETCH_SUPPORTED: (i16, i16) = (4, 12);

/// ListOffsets versions this consumer speaks: v1+ (v0 predates the
/// timestamp/offset response shape).
const LIST_OFFSETS_SUPPORTED: (i16, i16) = (1, ListOffsetsRequest::MAX_VERSION);

/// ListOffsets sentinel timestamps.
const EARLIEST: i64 = -2;
const LATEST: i64 = -1;

/// OffsetCommit versions this client speaks: the classic name-addressed
/// shape (v9+ carries member epochs for KIP-848 groups, v10 topic ids).
const OFFSET_COMMIT_SUPPORTED: (i16, i16) = (2, 8);

/// OffsetFetch versions this client speaks: the single-group shape
/// (v8+ switches to batched groups).
const OFFSET_FETCH_SUPPORTED: (i16, i16) = (1, 7);

/// Fetch tuning knobs.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// How long the broker may hold the fetch waiting for data.
    pub max_wait_ms: i32,
    /// Minimum bytes before the broker answers (1 = immediately).
    pub min_bytes: i32,
    /// Per-partition response size cap.
    pub partition_max_bytes: i32,
    /// Total delivery attempts per call (first try included).
    pub max_attempts: u32,
    /// Pause between attempts.
    pub retry_backoff: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        ConsumerConfig {
            max_wait_ms: 500,
            min_bytes: 1,
            partition_max_bytes: 1 << 20,
            max_attempts: 5,
            retry_backoff: Duration::from_millis(100),
        }
    }
}

/// One record, materialized with absolute coordinates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedRecord {
    pub offset: i64,
    /// Milliseconds since epoch (batch base + record delta).
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
}

/// What one fetch returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResult {
    pub records: Vec<ConsumedRecord>,
    /// Where the next fetch should start.
    pub next_offset: i64,
    pub high_watermark: i64,
}

/// A consumer over a connected [`Cluster`].
#[derive(Debug)]
pub struct Consumer {
    cluster: Cluster,
    config: ConsumerConfig,
}

impl Consumer {
    pub fn new(cluster: Cluster) -> Consumer {
        Consumer::with_config(cluster, ConsumerConfig::default())
    }

    pub fn with_config(cluster: Cluster, config: ConsumerConfig) -> Consumer {
        Consumer { cluster, config }
    }

    /// The underlying cluster, e.g. for metadata queries.
    pub fn cluster(&mut self) -> &mut Cluster {
        &mut self.cluster
    }

    /// Fetch records from `topic[partition]` starting at `offset`.
    pub async fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<FetchResult, ClientError> {
        self.with_retries(topic, |this| {
            Box::pin(this.fetch_once(topic.to_owned(), partition, offset))
        })
        .await
    }

    /// The partition's oldest available offset (the log start).
    pub async fn earliest_offset(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<i64, ClientError> {
        self.with_retries(topic, |this| {
            Box::pin(this.list_offset_once(topic.to_owned(), partition, EARLIEST))
        })
        .await
    }

    /// The partition's next-to-be-assigned offset (the log end).
    pub async fn latest_offset(&mut self, topic: &str, partition: i32) -> Result<i64, ClientError> {
        self.with_retries(topic, |this| {
            Box::pin(this.list_offset_once(topic.to_owned(), partition, LATEST))
        })
        .await
    }

    /// Durably commit `offset` for `topic[partition]` under `group`, as a
    /// simple (non-member) consumer: no group membership required, the
    /// caller owns partition assignment.
    pub async fn commit_offset(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), ClientError> {
        self.with_group_retries(group, |this| {
            Box::pin(this.commit_once(group.to_owned(), topic.to_owned(), partition, offset))
        })
        .await
    }

    /// The offset last committed for `topic[partition]` under `group`, or
    /// `None` when nothing was ever committed.
    pub async fn committed_offset(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, ClientError> {
        self.with_group_retries(group, |this| {
            Box::pin(this.committed_once(group.to_owned(), topic.to_owned(), partition))
        })
        .await
    }

    /// Like [`Self::with_retries`], but coordinator-scoped: a retriable
    /// error invalidates the discovered coordinator, not topic metadata.
    async fn with_group_retries<T>(
        &mut self,
        group: &str,
        mut attempt: impl for<'a> FnMut(
            &'a mut Consumer,
        ) -> std::pin::Pin<
            Box<dyn Future<Output = Result<T, ClientError>> + 'a>,
        >,
    ) -> Result<T, ClientError> {
        let mut last = None;
        for round in 0..self.config.max_attempts {
            if round > 0 {
                tokio::time::sleep(self.config.retry_backoff).await;
            }
            match attempt(self).await {
                Ok(v) => return Ok(v),
                Err(e) if e.is_retriable() => {
                    self.cluster.forget_coordinator(group);
                    last = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or(ClientError::ConnectionClosed))
    }

    async fn commit_once(
        &mut self,
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Result<(), ClientError> {
        let broker = self.cluster.coordinator(&group).await?.clone();
        let version = broker
            .ranges
            .pick(OffsetCommitRequest::API_KEY, OFFSET_COMMIT_SUPPORTED)?;
        let request = OffsetCommitRequest {
            group_id: group,
            // Simple consumer: no generation, no member.
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![OffsetCommitRequestTopic {
                name: topic.clone(),
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: partition,
                    committed_offset: offset,
                    committed_leader_epoch: -1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(OffsetCommitRequest::API_KEY, version, &body)
            .await?;
        let resp = OffsetCommitResponse::decode(&mut resp, version)?;
        let entry = resp
            .topics
            .iter()
            .find(|t| t.name == topic)
            .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
            .ok_or_else(|| {
                ClientError::ProtocolViolation(format!(
                    "offset commit response omits {topic}[{partition}]"
                ))
            })?;
        let code = ErrorCode(entry.error_code);
        if code.is_ok() {
            Ok(())
        } else {
            Err(ClientError::Broker(code))
        }
    }

    async fn committed_once(
        &mut self,
        group: String,
        topic: String,
        partition: i32,
    ) -> Result<Option<i64>, ClientError> {
        let broker = self.cluster.coordinator(&group).await?.clone();
        let version = broker
            .ranges
            .pick(OffsetFetchRequest::API_KEY, OFFSET_FETCH_SUPPORTED)?;
        let request = OffsetFetchRequest {
            group_id: group,
            topics: Some(vec![OffsetFetchRequestTopic {
                name: topic.clone(),
                partition_indexes: vec![partition],
                ..Default::default()
            }]),
            ..Default::default()
        };
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(OffsetFetchRequest::API_KEY, version, &body)
            .await?;
        let resp = OffsetFetchResponse::decode(&mut resp, version)?;
        let code = ErrorCode(resp.error_code);
        if !code.is_ok() {
            return Err(ClientError::Broker(code));
        }
        let entry = resp
            .topics
            .iter()
            .find(|t| t.name == topic)
            .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
            .ok_or_else(|| {
                ClientError::ProtocolViolation(format!(
                    "offset fetch response omits {topic}[{partition}]"
                ))
            })?;
        let code = ErrorCode(entry.error_code);
        if !code.is_ok() {
            return Err(ClientError::Broker(code));
        }
        Ok((entry.committed_offset >= 0).then_some(entry.committed_offset))
    }

    async fn with_retries<T>(
        &mut self,
        topic: &str,
        mut attempt: impl for<'a> FnMut(
            &'a mut Consumer,
        ) -> std::pin::Pin<
            Box<dyn Future<Output = Result<T, ClientError>> + 'a>,
        >,
    ) -> Result<T, ClientError> {
        let mut last = None;
        for round in 0..self.config.max_attempts {
            if round > 0 {
                tokio::time::sleep(self.config.retry_backoff).await;
            }
            match attempt(self).await {
                Ok(v) => return Ok(v),
                Err(e) if e.is_retriable() => {
                    self.cluster.mark_stale(topic);
                    last = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or(ClientError::ConnectionClosed))
    }

    async fn fetch_once(
        &mut self,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Result<FetchResult, ClientError> {
        let broker = self
            .cluster
            .partition_leader(&topic, partition)
            .await?
            .clone();
        let leader = self.cluster.leader_id(&topic, partition);
        let version = broker.ranges.pick(FetchRequest::API_KEY, FETCH_SUPPORTED)?;

        let request = FetchRequest {
            max_wait_ms: self.config.max_wait_ms,
            min_bytes: self.config.min_bytes,
            max_bytes: self.config.partition_max_bytes.saturating_mul(4),
            session_id: 0,
            session_epoch: -1, // sessionless full fetch
            topics: vec![FetchTopic {
                topic: topic.clone(),
                partitions: vec![FetchPartition {
                    partition,
                    current_leader_epoch: -1,
                    fetch_offset: offset,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: self.config.partition_max_bytes,
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
            .request(FetchRequest::API_KEY, version, &body)
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                if matches!(e, ClientError::ConnectionClosed | ClientError::Io(_)) {
                    if let Some(id) = leader {
                        self.cluster.forget_broker(id);
                    }
                }
                return Err(e);
            }
        };
        let resp = FetchResponse::decode(&mut resp, version)?;
        let code = ErrorCode(resp.error_code);
        if !code.is_ok() {
            return Err(ClientError::Broker(code));
        }
        let entry = resp
            .responses
            .iter()
            .find(|t| t.topic == topic)
            .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
            .ok_or_else(|| {
                ClientError::ProtocolViolation(format!("fetch response omits {topic}[{partition}]"))
            })?;
        let code = ErrorCode(entry.error_code);
        if !code.is_ok() {
            return Err(ClientError::Broker(code));
        }

        let mut set = entry.records.clone().unwrap_or_default();
        let batches = decode_set(&mut set)?;
        let mut records = Vec::new();
        let mut next_offset = offset;
        for batch in &batches {
            next_offset =
                next_offset.max(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
            if batch.is_control() {
                continue;
            }
            let plain = match &batch.records {
                Records::Plain(records) => records,
                Records::Compressed { .. } => {
                    return Err(ClientError::UnsupportedCompression(
                        match batch.compression() {
                            Compression::Gzip => "gzip",
                            Compression::Snappy => "snappy",
                            Compression::Lz4 => "lz4",
                            Compression::Zstd => "zstd",
                            _ => "unknown codec",
                        },
                    ));
                }
            };
            for record in plain {
                let absolute = batch.base_offset + i64::from(record.offset_delta);
                if absolute < offset {
                    // Brokers return whole batches; the head may predate
                    // the requested offset.
                    continue;
                }
                records.push(ConsumedRecord {
                    offset: absolute,
                    timestamp: batch.base_timestamp + record.timestamp_delta,
                    key: record.key.clone(),
                    value: record.value.clone(),
                    headers: record.headers.clone(),
                });
            }
        }
        Ok(FetchResult {
            records,
            next_offset,
            high_watermark: entry.high_watermark,
        })
    }

    async fn list_offset_once(
        &mut self,
        topic: String,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64, ClientError> {
        let broker = self
            .cluster
            .partition_leader(&topic, partition)
            .await?
            .clone();
        let version = broker
            .ranges
            .pick(ListOffsetsRequest::API_KEY, LIST_OFFSETS_SUPPORTED)?;

        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsTopic {
                name: topic.clone(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: partition,
                    current_leader_epoch: -1,
                    timestamp,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(ListOffsetsRequest::API_KEY, version, &body)
            .await?;
        let resp = ListOffsetsResponse::decode(&mut resp, version)?;
        let entry = resp
            .topics
            .iter()
            .find(|t| t.name == topic)
            .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
            .ok_or_else(|| {
                ClientError::ProtocolViolation(format!(
                    "list offsets response omits {topic}[{partition}]"
                ))
            })?;
        let code = ErrorCode(entry.error_code);
        if code.is_ok() {
            Ok(entry.offset)
        } else {
            Err(ClientError::Broker(code))
        }
    }
}
