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
use odradek_protocol::records::{Record, RecordHeader, Records, decode_set};

use crate::cluster::Cluster;
use crate::error::ClientError;
use crate::offsets::{self, CommitIdentity};

/// Fetch versions this consumer speaks: name-addressed (v13+ switches to
/// topic ids).
const FETCH_SUPPORTED: (i16, i16) = (4, 12);

/// ListOffsets versions this consumer speaks: v1+ (v0 predates the
/// timestamp/offset response shape).
const LIST_OFFSETS_SUPPORTED: (i16, i16) = (1, ListOffsetsRequest::MAX_VERSION);

/// ListOffsets sentinel timestamps.
const EARLIEST: i64 = -2;
const LATEST: i64 = -1;

/// Fetch tuning knobs.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConsumerConfig {
    /// How long the broker may hold the fetch waiting for data
    /// (default: 500ms).
    ///
    /// Must stay comfortably under
    /// [`crate::ClientConfig::request_timeout`] (default 30s): the
    /// client-side timeout covers the whole response wait, long-poll
    /// included, and a fetch that long-polls past it is treated as a
    /// hung request — the connection is closed.
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
            // Coordinator bootstrap (the broker creating its internal
            // offsets topic on first use) can take seconds; budget for it.
            max_attempts: 20,
            retry_backoff: Duration::from_millis(250),
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
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// Fetch records from `topic[partition]` starting at `offset`.
    pub async fn fetch(
        &self,
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
    pub async fn earliest_offset(&self, topic: &str, partition: i32) -> Result<i64, ClientError> {
        self.with_retries(topic, |this| {
            Box::pin(this.list_offset_once(topic.to_owned(), partition, EARLIEST))
        })
        .await
    }

    /// The partition's next-to-be-assigned offset (the log end).
    pub async fn latest_offset(&self, topic: &str, partition: i32) -> Result<i64, ClientError> {
        self.with_retries(topic, |this| {
            Box::pin(this.list_offset_once(topic.to_owned(), partition, LATEST))
        })
        .await
    }

    /// Durably commit `offset` for `topic[partition]` under `group`, as a
    /// simple (non-member) consumer: no group membership required, the
    /// caller owns partition assignment.
    pub async fn commit_offset(
        &self,
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
        &self,
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
        &self,
        group: &str,
        mut attempt: impl for<'a> FnMut(
            &'a Consumer,
        ) -> std::pin::Pin<
            Box<dyn Future<Output = Result<T, ClientError>> + Send + 'a>,
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
        &self,
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Result<(), ClientError> {
        // Simple consumer: no generation, no member, no fencing. A group
        // member commits through `GroupMember::commit_offset`, which
        // carries its real generation so the coordinator fences zombies.
        offsets::commit_once(
            &self.cluster,
            &group,
            CommitIdentity::SIMPLE,
            &topic,
            partition,
            offset,
        )
        .await
    }

    async fn committed_once(
        &self,
        group: String,
        topic: String,
        partition: i32,
    ) -> Result<Option<i64>, ClientError> {
        offsets::committed_once(&self.cluster, &group, &topic, partition).await
    }

    async fn with_retries<T>(
        &self,
        topic: &str,
        mut attempt: impl for<'a> FnMut(
            &'a Consumer,
        ) -> std::pin::Pin<
            Box<dyn Future<Output = Result<T, ClientError>> + Send + 'a>,
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
        &self,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Result<FetchResult, ClientError> {
        let broker = self.cluster.partition_leader(&topic, partition).await?;
        let leader = self.cluster.leader_id(&topic, partition);
        let version = broker.ranges.pick(FetchRequest::API_KEY, FETCH_SUPPORTED)?;

        let mut fetch_partition = FetchPartition::default();
        fetch_partition.partition = partition;
        fetch_partition.current_leader_epoch = -1;
        fetch_partition.fetch_offset = offset;
        fetch_partition.last_fetched_epoch = -1;
        fetch_partition.log_start_offset = -1;
        fetch_partition.partition_max_bytes = self.config.partition_max_bytes;
        let mut fetch_topic = FetchTopic::default();
        fetch_topic.topic = topic.clone();
        fetch_topic.partitions = vec![fetch_partition];
        let mut request = FetchRequest::default();
        request.max_wait_ms = self.config.max_wait_ms;
        request.min_bytes = self.config.min_bytes;
        request.max_bytes = self.config.partition_max_bytes.saturating_mul(4);
        request.session_id = 0;
        request.session_epoch = -1; // sessionless full fetch
        request.topics = vec![fetch_topic];
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;

        let mut resp = match broker
            .conn
            .request(FetchRequest::API_KEY, version, &body)
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                if matches!(
                    e,
                    ClientError::ConnectionClosed | ClientError::Io(_) | ClientError::Timeout(_)
                ) {
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
            let materialized;
            let plain: &[Record] = match &batch.records {
                Records::Plain(records) => records,
                Records::Compressed { count, payload } => {
                    let mut data = crate::compression::decompress(batch.compression(), payload)?;
                    let mut records = Vec::new();
                    for _ in 0..*count {
                        records.push(Record::decode(&mut data)?);
                    }
                    if !data.is_empty() {
                        return Err(ClientError::ProtocolViolation(format!(
                            "{} byte(s) left after the batch's {count} compressed records",
                            data.len()
                        )));
                    }
                    materialized = records;
                    &materialized
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
        &self,
        topic: String,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64, ClientError> {
        let broker = self.cluster.partition_leader(&topic, partition).await?;
        let version = broker
            .ranges
            .pick(ListOffsetsRequest::API_KEY, LIST_OFFSETS_SUPPORTED)?;

        let mut request_partition = ListOffsetsPartition::default();
        request_partition.partition_index = partition;
        request_partition.current_leader_epoch = -1;
        request_partition.timestamp = timestamp;
        let mut request_topic = ListOffsetsTopic::default();
        request_topic.name = topic.clone();
        request_topic.partitions = vec![request_partition];
        let mut request = ListOffsetsRequest::default();
        request.replica_id = -1;
        request.isolation_level = 0;
        request.topics = vec![request_topic];
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
