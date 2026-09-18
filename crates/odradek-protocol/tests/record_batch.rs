//! Record batch (v2) round-trips, crc verification, and hostile input.

use bytes::{BufMut, Bytes, BytesMut};
use odradek_protocol::DecodeError;
use odradek_protocol::records::{
    Compression, MAGIC, Record, RecordBatch, RecordHeader, Records, crc32c, decode_set, encode_set,
};
use odradek_protocol::wire;

fn sample_batch() -> RecordBatch {
    RecordBatch {
        base_offset: 42,
        partition_leader_epoch: 3,
        attributes: 0,
        last_offset_delta: 1,
        base_timestamp: 1_726_000_000_000,
        max_timestamp: 1_726_000_000_250,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: Records::Plain(vec![
            Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(Bytes::from_static(b"k1")),
                value: Some(Bytes::from_static(b"first value")),
                headers: vec![RecordHeader {
                    key: "trace-id".into(),
                    value: Some(Bytes::from_static(b"abc123")),
                }],
            },
            Record {
                attributes: 0,
                timestamp_delta: 250,
                offset_delta: 1,
                key: None,
                value: None,
                headers: vec![RecordHeader {
                    key: "tombstone-reason".into(),
                    value: None,
                }],
            },
        ]),
    }
}

fn encode(batch: &RecordBatch) -> Bytes {
    let mut buf = BytesMut::new();
    batch.encode(&mut buf).unwrap();
    buf.freeze()
}

#[test]
fn crc32c_known_vectors() {
    // The standard CRC-32C check value, plus the empty string.
    assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    assert_eq!(crc32c(b""), 0);
}

/// The textbook byte-at-a-time CRC-32C, which the shipped slicing-by-8
/// implementation must agree with on every input.
fn reference_crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82f6_3b78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[test]
fn crc32c_agrees_with_the_bitwise_reference() {
    // Lengths on and around every eight-byte boundary the wide loop
    // cares about, so a mishandled remainder cannot hide.
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        u8::try_from((state >> 33) & 0xff).expect("masked to a byte")
    };
    for len in [
        0, 1, 2, 7, 8, 9, 15, 16, 17, 31, 63, 64, 65, 127, 1000, 1023, 1024, 1025, 4097,
    ] {
        let data: Vec<u8> = (0..len).map(|_| next()).collect();
        assert_eq!(
            crc32c(&data),
            reference_crc32c(&data),
            "crc mismatch at {len} bytes"
        );
        // All-zero and all-one runs of the same length too: the tables
        // are indexed by byte value, not just by position.
        let zeros = vec![0u8; len];
        assert_eq!(crc32c(&zeros), reference_crc32c(&zeros), "zeros at {len}");
        let ones = vec![0xffu8; len];
        assert_eq!(crc32c(&ones), reference_crc32c(&ones), "ones at {len}");
    }
}

#[test]
fn roundtrip_plain_batch() {
    let batch = sample_batch();
    let mut bytes = encode(&batch);
    let decoded = RecordBatch::decode(&mut bytes).unwrap();
    assert!(bytes.is_empty());
    assert_eq!(decoded, batch);
    assert_eq!(decoded.compression(), Compression::None);
    assert!(!decoded.is_transactional());
    assert!(!decoded.is_control());
}

#[test]
fn roundtrip_negative_deltas_and_flags() {
    // Delete-horizon batches carry negative timestamp deltas; flag bits
    // and unusual values must survive the trip.
    let batch = RecordBatch {
        attributes: (1 << 4) | (1 << 6), // transactional + delete horizon
        base_timestamp: i64::MAX,
        records: Records::Plain(vec![Record {
            timestamp_delta: -12_345,
            offset_delta: 0,
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }]),
        ..Default::default()
    };
    let mut bytes = encode(&batch);
    let decoded = RecordBatch::decode(&mut bytes).unwrap();
    assert_eq!(decoded, batch);
    assert!(decoded.is_transactional());
    assert!(decoded.has_delete_horizon());
    assert!(!decoded.is_log_append_time());
}

#[test]
fn compressed_payload_roundtrips_byte_identical() {
    // Codec bits set: the payload is opaque — no decompression, and
    // re-encoding reproduces the input exactly (the proxy guarantee).
    // The payload here is not real gzip; it must not matter.
    let batch = RecordBatch {
        attributes: 1, // gzip
        records: Records::Compressed {
            count: 7,
            payload: Bytes::from_static(b"\x1f\x8b-opaque-compressed-noise"),
        },
        ..Default::default()
    };
    let wire = encode(&batch);
    let decoded = RecordBatch::decode(&mut wire.clone()).unwrap();
    assert_eq!(decoded.compression(), Compression::Gzip);
    assert_eq!(decoded, batch);
    assert_eq!(encode(&decoded), wire);
}

#[test]
fn unknown_codec_is_carried_not_rejected() {
    let batch = RecordBatch {
        attributes: 7,
        records: Records::Compressed {
            count: 1,
            payload: Bytes::from_static(b"future-codec-bytes"),
        },
        ..Default::default()
    };
    let wire = encode(&batch);
    let decoded = RecordBatch::decode(&mut wire.clone()).unwrap();
    assert_eq!(decoded.compression(), Compression::Unknown(7));
    assert_eq!(encode(&decoded), wire);
}

#[test]
fn corrupt_byte_fails_crc() {
    let mut wire = BytesMut::from(&encode(&sample_batch())[..]);
    let last = wire.len() - 1;
    wire[last] ^= 0x01;
    match RecordBatch::decode(&mut wire.freeze()) {
        Err(DecodeError::CrcMismatch { .. }) => {}
        other => panic!("expected crc mismatch, got {other:?}"),
    }
}

#[test]
fn wrong_magic_is_rejected() {
    let mut wire = BytesMut::from(&encode(&sample_batch())[..]);
    wire[16] = 1; // magic byte: base_offset(8) + batch_length(4) + epoch(4)
    match RecordBatch::decode(&mut wire.freeze()) {
        Err(DecodeError::UnknownDiscriminant { kind, value }) => {
            assert_eq!(kind, "record batch magic");
            assert_eq!(value, 1);
        }
        other => panic!("expected magic rejection, got {other:?}"),
    }
    assert_eq!(MAGIC, 2);
}

#[test]
fn record_set_roundtrip_and_truncated_tail() {
    let a = sample_batch();
    let b = RecordBatch {
        base_offset: 44,
        ..RecordBatch::default()
    };
    let mut wire = BytesMut::new();
    encode_set(&mut wire, &[a.clone(), b.clone()]).unwrap();

    // Whole set decodes to both batches.
    let full = decode_set(&mut wire.clone().freeze()).unwrap();
    assert_eq!(full, vec![a.clone(), b.clone()]);

    // A fetch-style cut inside the second batch discards it silently.
    let cut = wire.len() - 5;
    let mut truncated = Bytes::copy_from_slice(&wire[..cut]);
    let partial = decode_set(&mut truncated).unwrap();
    assert_eq!(partial, vec![a]);
    assert!(truncated.is_empty());
}

/// A log segment produced by Apache Kafka 4.1.0 (console producer, two
/// keyed records, one batch): on-disk segments hold record batches in
/// exactly the on-wire format, so this is ground truth, crc included.
const KAFKA_SEGMENT_HEX: &str = "00000000000000000000005d00000000028b7c13e00000000000010000\
                                 01a0abfa7edd000001a0abfa7ef600000000000000000000000000000000\
                                 00022a000000046b311a68656c6c6f206f64726164656b002a003202046b\
                                 321a7365636f6e64207265636f726400";

#[test]
fn decodes_real_kafka_segment_and_reencodes_byte_identical() {
    let hex: String = KAFKA_SEGMENT_HEX.split_whitespace().collect();
    let wire: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    let wire = Bytes::from(wire);

    let batches = decode_set(&mut wire.clone()).unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.base_offset, 0);
    assert_eq!(batch.last_offset_delta, 1);
    assert_eq!(batch.compression(), Compression::None);
    // The console producer is idempotent by default in 4.x: a real
    // producer id, not -1.
    assert_eq!(batch.producer_id, 0);
    assert_eq!(batch.base_sequence, 0);

    let Records::Plain(records) = &batch.records else {
        panic!("expected plain records");
    };
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].key.as_deref(), Some(b"k1".as_slice()));
    assert_eq!(
        records[0].value.as_deref(),
        Some(b"hello odradek".as_slice())
    );
    assert_eq!(records[0].offset_delta, 0);
    assert_eq!(records[1].key.as_deref(), Some(b"k2".as_slice()));
    assert_eq!(
        records[1].value.as_deref(),
        Some(b"second record".as_slice())
    );
    assert_eq!(records[1].offset_delta, 1);
    assert_eq!(
        records[1].timestamp_delta,
        batch.max_timestamp - batch.base_timestamp
    );

    // The proxy guarantee against ground truth: re-encoding a broker's
    // bytes reproduces them exactly, crc and all.
    let mut reencoded = BytesMut::new();
    encode_set(&mut reencoded, &batches).unwrap();
    assert_eq!(reencoded.freeze(), wire);
}

#[test]
fn hostile_prefixes_error_cleanly() {
    // Every prefix of a valid batch must decode to an error, not a panic
    // or a bogus success (except the full batch itself).
    let wire = encode(&sample_batch());
    for cut in 0..wire.len() {
        let mut prefix = wire.slice(..cut);
        assert!(
            RecordBatch::decode(&mut prefix).is_err(),
            "prefix of {cut} bytes decoded successfully"
        );
    }
    // So must a batch_length that lies about the record contents.
    let mut lying = BytesMut::from(&wire[..]);
    lying[60] ^= 0x40; // somewhere inside the first record's bytes
    assert!(RecordBatch::decode(&mut lying.freeze()).is_err());
}

/// A batch whose records exercise every field shape the encoder has to
/// size analytically: absent, empty and present key/value, multi-byte
/// zigzag varints of both signs, a value long enough to need a two-byte
/// length varint, and headers with and without values.
fn wide_batch() -> RecordBatch {
    RecordBatch {
        base_offset: 9_000_000_000,
        partition_leader_epoch: 17,
        attributes: 1 << 3, // log-append time
        last_offset_delta: 4,
        base_timestamp: 1_726_000_000_000,
        max_timestamp: 1_726_000_009_999,
        producer_id: 77,
        producer_epoch: 2,
        base_sequence: 13,
        records: Records::Plain(vec![
            Record::default(),
            Record {
                attributes: -1,
                timestamp_delta: -1_000_000,
                offset_delta: 1,
                key: Some(Bytes::from_static(b"")),
                value: Some(Bytes::from_static(b"")),
                headers: Vec::new(),
            },
            Record {
                attributes: 0,
                timestamp_delta: 9_999,
                offset_delta: 2,
                key: Some(Bytes::from_static(b"k")),
                value: Some(Bytes::from(vec![0x5au8; 300])),
                headers: vec![
                    RecordHeader {
                        key: String::new(),
                        value: None,
                    },
                    RecordHeader {
                        key: "h".into(),
                        value: Some(Bytes::from_static(b"v")),
                    },
                ],
            },
            Record {
                attributes: 0,
                timestamp_delta: i64::MAX,
                offset_delta: 3,
                key: None,
                value: Some(Bytes::from_static(b"tail")),
                // 64 headers: one past the point where the zigzag header
                // count itself needs a second varint byte.
                headers: (0..64)
                    .map(|i| RecordHeader {
                        key: format!("h{i}"),
                        value: (i % 2 == 0).then(|| Bytes::from(vec![u8::try_from(i).unwrap(); 3])),
                    })
                    .collect(),
            },
            Record {
                attributes: 0,
                timestamp_delta: i64::MIN,
                offset_delta: 4,
                key: Some(Bytes::from_static(b"last")),
                value: None,
                headers: Vec::new(),
            },
        ]),
    }
}

/// The record encoder as it was before it sized bodies analytically:
/// build the body in a side buffer, then prefix its measured length.
/// Kept here as the reference the fast path must match byte for byte.
fn reference_encode_record(record: &Record, buf: &mut BytesMut) {
    fn put_bytes(buf: &mut BytesMut, value: Option<&[u8]>) {
        match value {
            Some(v) => {
                wire::put_varint(buf, v.len() as i64);
                buf.put_slice(v);
            }
            None => wire::put_varint(buf, -1),
        }
    }

    let mut body = BytesMut::new();
    body.put_i8(record.attributes);
    wire::put_varint(&mut body, record.timestamp_delta);
    wire::put_varint(&mut body, i64::from(record.offset_delta));
    put_bytes(&mut body, record.key.as_deref());
    put_bytes(&mut body, record.value.as_deref());
    wire::put_varint(&mut body, record.headers.len() as i64);
    for header in &record.headers {
        put_bytes(&mut body, Some(header.key.as_bytes()));
        put_bytes(&mut body, header.value.as_deref());
    }
    wire::put_varint(buf, body.len() as i64);
    buf.put_slice(&body);
}

/// The same for a whole batch: body first, then the prefix whose crc
/// covers it.
fn reference_encode_batch(batch: &RecordBatch, buf: &mut BytesMut) {
    let mut tail = BytesMut::new();
    tail.put_i16(batch.attributes);
    tail.put_i32(batch.last_offset_delta);
    tail.put_i64(batch.base_timestamp);
    tail.put_i64(batch.max_timestamp);
    tail.put_i64(batch.producer_id);
    tail.put_i16(batch.producer_epoch);
    tail.put_i32(batch.base_sequence);
    tail.put_i32(batch.record_count().unwrap());
    match &batch.records {
        Records::Plain(records) => {
            for record in records {
                reference_encode_record(record, &mut tail);
            }
        }
        Records::Compressed { payload, .. } => tail.extend_from_slice(payload),
    }
    buf.put_i64(batch.base_offset);
    buf.put_i32(i32::try_from(4 + 1 + 4 + tail.len()).unwrap());
    buf.put_i32(batch.partition_leader_epoch);
    buf.put_i8(MAGIC);
    buf.put_u32(crc32c(&tail));
    buf.put_slice(&tail);
}

#[test]
fn encoding_is_byte_identical_to_the_side_buffer_reference() {
    // Analytic body lengths are only correct if they agree with the
    // measured ones for every field shape, in every record of a batch.
    for batch in [
        wide_batch(),
        sample_batch(),
        RecordBatch::default(),
        RecordBatch {
            attributes: 2, // snappy: the payload stays opaque
            records: Records::Compressed {
                count: 3,
                payload: Bytes::from_static(b"opaque"),
            },
            ..RecordBatch::default()
        },
    ] {
        let mut expected = BytesMut::new();
        reference_encode_batch(&batch, &mut expected);
        assert_eq!(encode(&batch), expected.clone().freeze(), "generic encode");

        // The in-place encoder must agree too, including when it is
        // appending after bytes that are already in the buffer.
        let mut in_place = BytesMut::from(&b"prefix"[..]);
        batch.encode_to(&mut in_place).unwrap();
        assert_eq!(&in_place[6..], &expected[..], "encode_to");

        // And the round trip still closes.
        let decoded = RecordBatch::decode(&mut expected.freeze()).unwrap();
        assert_eq!(decoded, batch);
    }
}
