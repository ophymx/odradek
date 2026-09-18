//! Minimal consumer: fetch record batches from a partition leader and
//! materialize them as records with absolute offsets.
//!
//! No consumer groups, subscriptions, or offset commits yet — the caller
//! owns its position. What this layer owns is correctness of the fetch
//! path: leader routing with retry, batch decoding (crc verified by the
//! protocol layer), skipping records below the requested offset (brokers
//! return whole batches, which may start earlier), and skipping control
//! batches (transaction markers are not data).
//!
//! # Bounding what a fetch can materialize
//!
//! A fetch response is broker-controlled input, and a compressed batch
//! is a lever: a few hundred kilobytes on the wire can claim to hold
//! millions of records, each of which costs ~112 bytes of `Vec` once
//! materialized even when its wire form is 7 bytes. Three limits box
//! that in, and they compose:
//!
//! 1. Decompression of any one batch stops at 64 MiB, the frame cap.
//! 2. Across all batches of one response, decompressed bytes are capped
//!    at 64 MiB in total, so a response full of small compressed batches
//!    cannot get that much each.
//! 3. A batch's claimed record count is checked against what its own
//!    decompressed bytes could possibly encode (7 bytes minimum per
//!    record) *before* any record is decoded, and the running total is
//!    capped at [`ConsumerConfig::max_fetch_records`].
//!
//! The result is a stated ceiling per [`Consumer::fetch`] call: at most
//! `max_fetch_records` records (~112 MiB of [`ConsumedRecord`] at the
//! default) over at most 64 MiB of record bytes, from a response that
//! was itself at most 64 MiB on the wire.

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
use crate::conn;
use crate::error::ClientError;
use crate::offsets::{self, CommitIdentity};
use crate::retry::{or_forget_coordinator, or_mark_stale, retry_loop};

/// Fetch versions this consumer speaks: name-addressed (v13+ switches to
/// topic ids).
const FETCH_SUPPORTED: (i16, i16) = (4, 12);

/// ListOffsets versions this consumer speaks: v1+ (v0 predates the
/// timestamp/offset response shape).
const LIST_OFFSETS_SUPPORTED: (i16, i16) = (1, ListOffsetsRequest::MAX_VERSION);

/// ListOffsets sentinel timestamps.
const EARLIEST: i64 = -2;
const LATEST: i64 = -1;

/// The fewest bytes a record can occupy on the wire.
///
/// A record is a varint length followed by that many bytes, and the body
/// needs at least: attributes (1), timestamp delta (1), offset delta
/// (1), a null key (1), a null value (1), and a zero header count (1) —
/// six bytes, which the leading length varint encodes in one more. So a
/// decompressed payload of `n` bytes cannot hold more than `n / 7`
/// records, whatever the batch header claims. Asserted in the tests
/// against the protocol crate's encoder.
const MIN_RECORD_WIRE_LEN: usize = 7;

/// Decompressed record bytes one fetch response may produce in total,
/// summed over its batches.
///
/// Equal to the connection layer's frame ceiling: a response is allowed
/// to expand to, at most, what a response is allowed to *be*. Without
/// the cumulative accounting, a 64 MiB frame packed with thousands of
/// tiny compressed batches could each expand to the per-batch cap.
const MAX_FETCH_DECOMPRESSED: usize = odradek_protocol::frame::DEFAULT_MAX_FRAME;

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
    /// Ceiling on the records one [`Consumer::fetch`] will materialize
    /// (default: 1_048_576).
    ///
    /// This is a safety bound on broker-controlled input, not a paging
    /// knob: a response that asks for more is rejected as a
    /// [`ClientError::ProtocolViolation`], not truncated. The count a
    /// compressed batch declares is checked against it *before* records
    /// are decoded, so a batch claiming millions costs nothing to
    /// refuse.
    ///
    /// Every materialized record costs about 112 bytes of
    /// [`ConsumedRecord`] on top of its payload (payloads are slices of
    /// the decompressed buffer, not copies), so the default caps one
    /// fetch at roughly 112 MiB of record structs. Compression is what
    /// makes this necessary: a minimal record is 7 bytes decompressed
    /// and gzip shrinks a run of them by several hundred times, so
    /// without the bound a ~200 KiB response can ask for gigabytes.
    ///
    /// Raise it only alongside [`ConsumerConfig::partition_max_bytes`],
    /// and only if a legitimate broker actually returns that many
    /// records per fetch; the default is already ~7x what a 1 MiB
    /// partition fetch of minimum-size records could hold uncompressed.
    pub max_fetch_records: usize,
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
            max_fetch_records: 1 << 20,
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
        retry_loop(
            &mut &*self,
            self.config.max_attempts,
            self.config.retry_backoff,
            |this| {
                Box::pin(async move {
                    let result = this.fetch_once(topic, partition, offset).await;
                    or_mark_stale(&this.cluster, topic, partition, result)
                })
            },
        )
        .await
    }

    /// The partition's oldest available offset (the log start).
    pub async fn earliest_offset(&self, topic: &str, partition: i32) -> Result<i64, ClientError> {
        retry_loop(
            &mut &*self,
            self.config.max_attempts,
            self.config.retry_backoff,
            |this| {
                Box::pin(async move {
                    let result = this.list_offset_once(topic, partition, EARLIEST).await;
                    or_mark_stale(&this.cluster, topic, partition, result)
                })
            },
        )
        .await
    }

    /// The partition's next-to-be-assigned offset (the log end).
    pub async fn latest_offset(&self, topic: &str, partition: i32) -> Result<i64, ClientError> {
        retry_loop(
            &mut &*self,
            self.config.max_attempts,
            self.config.retry_backoff,
            |this| {
                Box::pin(async move {
                    let result = this.list_offset_once(topic, partition, LATEST).await;
                    or_mark_stale(&this.cluster, topic, partition, result)
                })
            },
        )
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
        retry_loop(
            &mut &*self,
            self.config.max_attempts,
            self.config.retry_backoff,
            |this| {
                Box::pin(async move {
                    let result = offsets::commit_once(
                        &this.cluster,
                        group,
                        CommitIdentity::SIMPLE,
                        topic,
                        partition,
                        offset,
                    )
                    .await;
                    or_forget_coordinator(&this.cluster, group, result)
                })
            },
        )
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
        retry_loop(
            &mut &*self,
            self.config.max_attempts,
            self.config.retry_backoff,
            |this| {
                Box::pin(async move {
                    let result =
                        offsets::committed_once(&this.cluster, group, topic, partition).await;
                    or_forget_coordinator(&this.cluster, group, result)
                })
            },
        )
        .await
    }

    async fn fetch_once(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<FetchResult, ClientError> {
        // Long-poll fetches hold their connection for up to max_wait_ms;
        // a leased connection keeps them off the shared fast lane.
        let lease = self
            .cluster
            .blocking_partition_leader(topic, partition)
            .await?;
        let leader = self.cluster.leader_id(topic, partition);
        let broker = lease.broker();
        let version = broker.ranges.pick(FetchRequest::API_KEY, FETCH_SUPPORTED)?;

        let mut fetch_partition = FetchPartition::default();
        fetch_partition.partition = partition;
        fetch_partition.current_leader_epoch = -1;
        fetch_partition.fetch_offset = offset;
        fetch_partition.last_fetched_epoch = -1;
        fetch_partition.log_start_offset = -1;
        fetch_partition.partition_max_bytes = self.config.partition_max_bytes;
        let mut fetch_topic = FetchTopic::default();
        fetch_topic.topic = topic.to_owned();
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
                // The lease drops here, discarding the possibly-poisoned
                // connection; also drop the fast-lane pool entry.
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
        // The exchange completed; the connection is clean for reuse.
        lease.release();
        let resp = conn::decode_body::<FetchResponse>(&mut resp, version)?;
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
        let mut budget = FetchBudget {
            records_left: self.config.max_fetch_records,
            decompressed_left: MAX_FETCH_DECOMPRESSED,
        };
        let mut next_offset = offset;
        for batch in &batches {
            next_offset =
                next_offset.max(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
            if batch.is_control() {
                continue;
            }
            match &batch.records {
                Records::Plain(plain) => {
                    budget.claim_records(plain.len())?;
                    for record in plain {
                        push_record(&mut records, batch, record.clone(), offset);
                    }
                }
                Records::Compressed { count, payload } => {
                    decode_compressed(&mut records, &mut budget, batch, *count, payload, offset)?;
                }
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
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64, ClientError> {
        let broker = self.cluster.partition_leader(topic, partition).await?;
        let version = broker
            .ranges
            .pick(ListOffsetsRequest::API_KEY, LIST_OFFSETS_SUPPORTED)?;

        let mut request_partition = ListOffsetsPartition::default();
        request_partition.partition_index = partition;
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
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(ListOffsetsRequest::API_KEY, version, &body)
            .await?;
        let resp = conn::decode_body::<ListOffsetsResponse>(&mut resp, version)?;
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

/// What one fetch response is still allowed to spend.
///
/// Both counters run for the whole response rather than per batch, so a
/// response cannot multiply its allowance by splitting itself up.
#[derive(Debug)]
struct FetchBudget {
    records_left: usize,
    decompressed_left: usize,
}

impl FetchBudget {
    /// Charge `count` records, refusing rather than truncating.
    fn claim_records(&mut self, count: usize) -> Result<(), ClientError> {
        if count > self.records_left {
            return Err(ClientError::ProtocolViolation(format!(
                "fetch response wants to materialize more than this client's \
                 limit of records (ConsumerConfig::max_fetch_records); \
                 {count} more asked for with room for {}",
                self.records_left
            )));
        }
        self.records_left -= count;
        Ok(())
    }

    /// Charge `len` decompressed bytes.
    fn claim_decompressed(&mut self, len: usize) -> Result<(), ClientError> {
        if len > self.decompressed_left {
            return Err(ClientError::ProtocolViolation(format!(
                "fetch response decompresses past this client's limit of \
                 {MAX_FETCH_DECOMPRESSED} bytes per response"
            )));
        }
        self.decompressed_left -= len;
        Ok(())
    }
}

/// Decode a compressed batch's records straight into `out`.
///
/// Single pass on purpose: an intermediate `Vec<Record>` would double
/// the peak for no benefit, since every record here is consumed exactly
/// once. The payload bytes are not copied — keys and values are slices
/// of the decompressed buffer.
fn decode_compressed(
    out: &mut Vec<ConsumedRecord>,
    budget: &mut FetchBudget,
    batch: &odradek_protocol::records::RecordBatch,
    count: i32,
    payload: &[u8],
    min_offset: i64,
) -> Result<(), ClientError> {
    let count = usize::try_from(count).map_err(|_| {
        ClientError::ProtocolViolation(format!("batch claims a negative record count ({count})"))
    })?;
    let mut data = crate::compression::decompress(batch.compression(), payload)?;
    budget.claim_decompressed(data.len())?;
    // The count is the broker's claim; the decompressed length is the
    // arithmetic limit on what that claim can possibly be true about.
    // Check it before decoding, so a lie costs one division, not a
    // multi-gigabyte `Vec`.
    let possible = data.len() / MIN_RECORD_WIRE_LEN;
    if count > possible {
        return Err(ClientError::ProtocolViolation(format!(
            "batch claims {count} records but its {} decompressed byte(s) \
             can hold at most {possible}",
            data.len()
        )));
    }
    budget.claim_records(count)?;
    for _ in 0..count {
        let record = Record::decode(&mut data)?;
        push_record(out, batch, record, min_offset);
    }
    if !data.is_empty() {
        return Err(ClientError::ProtocolViolation(format!(
            "{} byte(s) left after the batch's {count} compressed records",
            data.len()
        )));
    }
    Ok(())
}

/// Give `record` its absolute coordinates and keep it, unless it
/// predates the requested offset.
fn push_record(
    out: &mut Vec<ConsumedRecord>,
    batch: &odradek_protocol::records::RecordBatch,
    record: Record,
    min_offset: i64,
) {
    let absolute = batch.base_offset + i64::from(record.offset_delta);
    if absolute < min_offset {
        // Brokers return whole batches; the head may predate the
        // requested offset.
        return;
    }
    out.push(ConsumedRecord {
        offset: absolute,
        timestamp: batch.base_timestamp + record.timestamp_delta,
        key: record.key,
        value: record.value,
        headers: record.headers,
    });
}

#[cfg(test)]
mod tests {
    use odradek_protocol::records::{Compression, RecordBatch};

    use super::*;

    fn budget() -> FetchBudget {
        FetchBudget {
            records_left: ConsumerConfig::default().max_fetch_records,
            decompressed_left: MAX_FETCH_DECOMPRESSED,
        }
    }

    /// [`MIN_RECORD_WIRE_LEN`] is load-bearing arithmetic, not a guess:
    /// the smallest record the protocol crate can encode must be exactly
    /// that long.
    #[test]
    fn minimum_record_is_seven_bytes() {
        let mut buf = BytesMut::new();
        Record::default().encode(&mut buf).unwrap();
        assert_eq!(buf.len(), MIN_RECORD_WIRE_LEN);
    }

    /// The amplification: ~200 KiB of gzip can claim millions of
    /// records. The claim has to be refused from the payload's own
    /// length, before anything is allocated for it.
    #[test]
    fn huge_claimed_count_against_a_small_payload_is_rejected() {
        let mut out = Vec::new();
        let batch = RecordBatch::default();
        // Compression::None keeps this test independent of which codec
        // features are built; the count check is codec-agnostic.
        let payload = b"a short payload";
        let err =
            decode_compressed(&mut out, &mut budget(), &batch, 19_173_961, payload, 0).unwrap_err();
        assert!(
            matches!(&err, ClientError::ProtocolViolation(m) if m.contains("can hold at most 2")),
            "unexpected error: {err}"
        );
        assert!(out.is_empty(), "nothing should have been materialized");
    }

    /// A count that fits the payload arithmetically but blows the
    /// per-response record budget is refused too, and again before
    /// decoding.
    #[test]
    fn count_over_the_response_budget_is_rejected() {
        let mut out = Vec::new();
        let mut budget = FetchBudget {
            records_left: 3,
            decompressed_left: MAX_FETCH_DECOMPRESSED,
        };
        let payload = vec![0u8; 10 * MIN_RECORD_WIRE_LEN];
        let err = decode_compressed(
            &mut out,
            &mut budget,
            &RecordBatch::default(),
            10,
            &payload,
            0,
        )
        .unwrap_err();
        assert!(
            matches!(&err, ClientError::ProtocolViolation(m) if m.contains("max_fetch_records")),
            "unexpected error: {err}"
        );
        assert!(out.is_empty());
        assert_eq!(budget.records_left, 3, "a refused claim spends nothing");
    }

    #[test]
    fn decompressed_bytes_are_charged_against_the_response_budget() {
        let mut budget = FetchBudget {
            records_left: 100,
            decompressed_left: 8,
        };
        let payload = vec![0u8; 9];
        let err = decode_compressed(
            &mut Vec::new(),
            &mut budget,
            &RecordBatch::default(),
            1,
            &payload,
            0,
        )
        .unwrap_err();
        assert!(
            matches!(&err, ClientError::ProtocolViolation(m) if m.contains("decompresses past")),
            "unexpected error: {err}"
        );
    }

    /// Honest batches still decode, with the offsets and timestamps made
    /// absolute and the pre-offset head skipped.
    #[test]
    fn honest_compressed_batch_still_materializes() {
        let records = [
            Record {
                offset_delta: 0,
                timestamp_delta: 5,
                value: Some(Bytes::from_static(b"first")),
                ..Default::default()
            },
            Record {
                offset_delta: 1,
                timestamp_delta: 6,
                value: Some(Bytes::from_static(b"second")),
                ..Default::default()
            },
        ];
        let mut payload = BytesMut::new();
        for record in &records {
            record.encode(&mut payload).unwrap();
        }
        let batch = RecordBatch {
            base_offset: 100,
            base_timestamp: 1_000,
            ..Default::default()
        };
        assert_eq!(batch.compression(), Compression::None);

        let mut out = Vec::new();
        let mut spent = budget();
        decode_compressed(&mut out, &mut spent, &batch, 2, &payload, 0).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].offset, 100);
        assert_eq!(out[0].timestamp, 1_005);
        assert_eq!(out[1].value.as_deref(), Some(b"second".as_slice()));
        assert_eq!(
            spent.records_left,
            ConsumerConfig::default().max_fetch_records - 2
        );

        // Records below the requested offset are skipped, not an error.
        let mut out = Vec::new();
        decode_compressed(&mut out, &mut budget(), &batch, 2, &payload, 101).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].offset, 101);
    }

    #[test]
    fn negative_claimed_count_is_rejected() {
        let err = decode_compressed(
            &mut Vec::new(),
            &mut budget(),
            &RecordBatch::default(),
            -1,
            b"",
            0,
        )
        .unwrap_err();
        assert!(matches!(err, ClientError::ProtocolViolation(_)));
    }
}
