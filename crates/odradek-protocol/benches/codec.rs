//! Codec throughput benchmarks.
//!
//! The record path is on every hot path of every consumer, so these
//! measure the two kernels that dominate it — CRC-32C and per-record
//! encode/decode — plus a whole 1 MiB fetch response end to end.
//!
//! Two record shapes are measured deliberately: with large values the
//! CRC pass dominates, with small ones the per-record overhead does, and
//! a single-shape benchmark would hide whichever half regressed.

use bytes::{Bytes, BytesMut};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use odradek_protocol::messages::fetch_response::FetchResponse;
use odradek_protocol::records::{Record, RecordBatch, Records, crc32c, decode_set, encode_set};

/// Roughly one megabyte of records with `value_len`-byte values.
fn batch_of(value_len: usize) -> RecordBatch {
    let target = 1 << 20;
    let count = target / (value_len + 24);
    let value = Bytes::from(vec![0xa5u8; value_len]);
    let records: Vec<Record> = (0..count)
        .map(|i| Record {
            attributes: 0,
            timestamp_delta: i64::try_from(i).expect("fits"),
            offset_delta: i32::try_from(i).expect("fits"),
            key: Some(Bytes::from(format!("key-{i}"))),
            value: Some(value.clone()),
            headers: Vec::new(),
        })
        .collect();
    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: -1,
        attributes: 0,
        last_offset_delta: i32::try_from(records.len() - 1).expect("fits"),
        base_timestamp: 1_700_000_000_000,
        max_timestamp: 1_700_000_000_000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: Records::Plain(records),
    }
}

fn encoded(batch: &RecordBatch) -> Bytes {
    let mut buf = BytesMut::new();
    encode_set(&mut buf, std::slice::from_ref(batch)).expect("encodes");
    buf.freeze()
}

/// A fetch response carrying `set` as its one partition's records.
fn fetch_response_bytes(set: &Bytes) -> Bytes {
    use odradek_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
    let mut partition = PartitionData::default();
    partition.partition_index = 0;
    partition.high_watermark = 1_000_000;
    partition.records = Some(set.clone());
    let mut topic = FetchableTopicResponse::default();
    topic.topic = "bench".into();
    topic.partitions = vec![partition];
    let mut response = FetchResponse::default();
    response.responses = vec![topic];
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 12).expect("encodes");
    buf.freeze()
}

fn bench_crc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crc32c");
    for size in [4 << 10, 64 << 10, 1 << 20] {
        let data = vec![0x5au8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{}KiB", size >> 10), |b| {
            b.iter(|| crc32c(std::hint::black_box(&data)))
        });
    }
    group.finish();
}

fn bench_records(c: &mut Criterion) {
    for (label, value_len) in [("value256", 256), ("value20", 20)] {
        let batch = batch_of(value_len);
        let set = encoded(&batch);
        let count = match &batch.records {
            Records::Plain(r) => r.len(),
            Records::Compressed { count, .. } => usize::try_from(*count).expect("fits"),
        };

        let mut group = c.benchmark_group(format!("records/{label}"));
        group.throughput(Throughput::Bytes(set.len() as u64));
        group.bench_function("decode_set", |b| {
            b.iter(|| decode_set(&mut std::hint::black_box(set.clone())).expect("decodes"))
        });
        group.bench_function("encode_set", |b| {
            b.iter(|| {
                let mut buf = BytesMut::new();
                encode_set(&mut buf, std::slice::from_ref(&batch)).expect("encodes");
                buf
            })
        });
        group.finish();

        // Whole fetch response: message decode (which must stay O(fields),
        // leaving `records` an opaque slice) and then materialization.
        let response = fetch_response_bytes(&set);
        let mut group = c.benchmark_group(format!("fetch/{label}"));
        group.throughput(Throughput::Bytes(response.len() as u64));
        group.bench_function("message_decode", |b| {
            b.iter(|| {
                FetchResponse::decode(&mut std::hint::black_box(response.clone()), 12)
                    .expect("decodes")
            })
        });
        group.bench_function("full_decode", |b| {
            b.iter(|| {
                let decoded = FetchResponse::decode(&mut response.clone(), 12).expect("decodes");
                let mut raw = decoded.responses[0].partitions[0]
                    .records
                    .clone()
                    .expect("records");
                decode_set(&mut raw).expect("decodes")
            })
        });
        group.finish();
        assert!(count > 0);
    }
}

criterion_group!(benches, bench_crc, bench_records);
criterion_main!(benches);
