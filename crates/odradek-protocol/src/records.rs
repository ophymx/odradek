//! Record batch (v2) encoding — the record set carried in Produce and
//! Fetch `records` fields.
//!
//! This format predates and sits below the message schemas: a record set
//! is an opaque `BYTES` field there, holding a sequence of batches. Each
//! batch is CRC-protected (CRC-32C over everything after the crc field)
//! and may hold its records compressed.
//!
//! Compressed batches are kept as raw bytes rather than decompressed:
//! decoding then re-encoding one is byte-identical, which is what proxying
//! requires, and it keeps this crate free of compression dependencies. The
//! same applies to unknown compression codecs from the future.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{DecodeError, EncodeError};
use crate::wire;

/// The only record batch magic this crate speaks. Magic 0 and 1 (the
/// pre-0.11 "message set" formats) were removed from brokers in Kafka 4.0.
pub const MAGIC: i8 = 2;

/// Byte length of a batch's fields from `partition_leader_epoch` through
/// the record count — everything `batch_length` counts except the records
/// themselves.
const BATCH_OVERHEAD: usize = 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4;

/// Compression codec, from the low three bits of batch attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
    /// A codec this crate does not know. Carried losslessly: the batch's
    /// payload stays raw and re-encodes byte-identically.
    Unknown(u8),
}

impl Compression {
    fn from_attributes(attributes: i16) -> Compression {
        match (attributes & 0x7) as u8 {
            0 => Compression::None,
            1 => Compression::Gzip,
            2 => Compression::Snappy,
            3 => Compression::Lz4,
            4 => Compression::Zstd,
            other => Compression::Unknown(other),
        }
    }

    /// The attribute bits that select this codec — the inverse of
    /// [`Compression::from_attributes`], kept beside it so a new codec
    /// number cannot land in one direction only.
    pub fn attribute_bits(self) -> i16 {
        match self {
            Compression::None => 0,
            Compression::Gzip => 1,
            Compression::Snappy => 2,
            Compression::Lz4 => 3,
            Compression::Zstd => 4,
            Compression::Unknown(bits) => i16::from(bits),
        }
    }
}

/// One record batch (magic 2).
///
/// `attributes` is kept raw for lossless round-trips; the typed accessors
/// read its bits. There is no `magic` or `crc` field: magic is always
/// [`MAGIC`] and the crc is computed on encode and verified on decode.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Records,
}

/// The records of a batch: parsed when uncompressed, raw otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Records {
    /// Uncompressed records, fully parsed.
    Plain(Vec<Record>),
    /// Compressed records: the wire record count and the compressed bytes
    /// exactly as they appeared after it.
    Compressed { count: i32, payload: Bytes },
}

impl Default for Records {
    fn default() -> Self {
        Records::Plain(Vec::new())
    }
}

/// One record within an uncompressed batch.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Record {
    pub attributes: i8,
    /// Timestamp as an offset from the batch's `base_timestamp`.
    pub timestamp_delta: i64,
    /// Offset as a delta from the batch's `base_offset`.
    pub offset_delta: i32,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
}

/// A record header: string key, nullable bytes value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecordHeader {
    pub key: String,
    pub value: Option<Bytes>,
}

impl RecordBatch {
    pub fn compression(&self) -> Compression {
        Compression::from_attributes(self.attributes)
    }

    /// True when timestamps are log-append time (bit 3); false = create time.
    pub fn is_log_append_time(&self) -> bool {
        self.attributes & (1 << 3) != 0
    }

    pub fn is_transactional(&self) -> bool {
        self.attributes & (1 << 4) != 0
    }

    pub fn is_control(&self) -> bool {
        self.attributes & (1 << 5) != 0
    }

    pub fn has_delete_horizon(&self) -> bool {
        self.attributes & (1 << 6) != 0
    }

    /// The wire record count: parsed length for plain records, the carried
    /// count for compressed payloads.
    pub fn record_count(&self) -> Result<i32, EncodeError> {
        match &self.records {
            Records::Plain(records) => {
                i32::try_from(records.len()).map_err(|_| EncodeError::TooLong {
                    len: records.len(),
                    max: i32::MAX as usize,
                })
            }
            Records::Compressed { count, .. } => Ok(*count),
        }
    }

    pub fn encode(&self, buf: &mut impl BufMut) -> Result<(), EncodeError> {
        // The crc covers attributes through the end of the batch, so the
        // tail must exist before the prefix can be written.
        let mut tail = BytesMut::new();
        tail.put_i16(self.attributes);
        tail.put_i32(self.last_offset_delta);
        tail.put_i64(self.base_timestamp);
        tail.put_i64(self.max_timestamp);
        tail.put_i64(self.producer_id);
        tail.put_i16(self.producer_epoch);
        tail.put_i32(self.base_sequence);
        tail.put_i32(self.record_count()?);
        match &self.records {
            Records::Plain(records) => {
                for record in records {
                    record.encode(&mut tail)?;
                }
            }
            Records::Compressed { payload, .. } => tail.extend_from_slice(payload),
        }

        let batch_length = 4 + 1 + 4 + tail.len(); // epoch + magic + crc + tail
        let batch_length = i32::try_from(batch_length).map_err(|_| EncodeError::TooLong {
            len: batch_length,
            max: i32::MAX as usize,
        })?;
        buf.put_i64(self.base_offset);
        buf.put_i32(batch_length);
        buf.put_i32(self.partition_leader_epoch);
        buf.put_i8(MAGIC);
        buf.put_u32(crc32c(&tail));
        buf.put_slice(&tail);
        Ok(())
    }

    /// Decode one batch, verifying magic and crc. Errors leave `buf` in an
    /// unspecified position.
    pub fn decode(buf: &mut Bytes) -> Result<RecordBatch, DecodeError> {
        if buf.len() < 12 {
            return Err(DecodeError::Truncated {
                needed: 12 - buf.len(),
            });
        }
        let base_offset = buf.get_i64();
        let batch_length = buf.get_i32();
        let Ok(batch_length) = usize::try_from(batch_length) else {
            return Err(DecodeError::InvalidLength(i64::from(batch_length)));
        };
        if batch_length < BATCH_OVERHEAD {
            return Err(DecodeError::InvalidLength(batch_length as i64));
        }
        if buf.len() < batch_length {
            return Err(DecodeError::Truncated {
                needed: batch_length - buf.len(),
            });
        }
        let partition_leader_epoch = buf.get_i32();
        let magic = buf.get_i8();
        if magic != MAGIC {
            return Err(DecodeError::UnknownDiscriminant {
                kind: "record batch magic",
                value: i64::from(magic),
            });
        }
        let crc = buf.get_u32();
        // Everything after the crc field, i.e. attributes through the end.
        let mut body = buf.split_to(batch_length - 9);
        let actual = crc32c(&body);
        if actual != crc {
            return Err(DecodeError::CrcMismatch {
                stored: crc,
                computed: actual,
            });
        }

        let attributes = body.get_i16();
        let last_offset_delta = body.get_i32();
        let base_timestamp = body.get_i64();
        let max_timestamp = body.get_i64();
        let producer_id = body.get_i64();
        let producer_epoch = body.get_i16();
        let base_sequence = body.get_i32();
        let count = body.get_i32();
        if count < 0 {
            return Err(DecodeError::InvalidLength(i64::from(count)));
        }
        let records = if Compression::from_attributes(attributes) == Compression::None {
            let mut records = Vec::new();
            for _ in 0..count {
                records.push(Record::decode(&mut body)?);
            }
            if !body.is_empty() {
                // Bytes inside the crc-covered region that no record
                // accounts for: the count or the record lengths lie.
                return Err(DecodeError::InvalidLength(body.len() as i64));
            }
            Records::Plain(records)
        } else {
            Records::Compressed {
                count,
                payload: body,
            }
        };
        Ok(RecordBatch {
            base_offset,
            partition_leader_epoch,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

impl Record {
    pub fn encode(&self, buf: &mut impl BufMut) -> Result<(), EncodeError> {
        let mut body = BytesMut::new();
        body.put_i8(self.attributes);
        wire::put_varint(&mut body, self.timestamp_delta);
        wire::put_varint(&mut body, i64::from(self.offset_delta));
        put_varint_bytes(&mut body, self.key.as_deref());
        put_varint_bytes(&mut body, self.value.as_deref());
        let header_count = i64::try_from(self.headers.len()).map_err(|_| EncodeError::TooLong {
            len: self.headers.len(),
            max: usize::try_from(i64::MAX).unwrap_or(usize::MAX),
        })?;
        wire::put_varint(&mut body, header_count);
        for header in &self.headers {
            put_varint_bytes(&mut body, Some(header.key.as_bytes()));
            put_varint_bytes(&mut body, header.value.as_deref());
        }
        wire::put_varint(buf, body.len() as i64);
        buf.put_slice(&body);
        Ok(())
    }

    pub fn decode(buf: &mut Bytes) -> Result<Record, DecodeError> {
        let length = wire::get_varint(buf)?;
        let Ok(length) = usize::try_from(length) else {
            return Err(DecodeError::InvalidLength(length));
        };
        if buf.len() < length {
            return Err(DecodeError::Truncated {
                needed: length - buf.len(),
            });
        }
        let mut body = buf.split_to(length);
        if body.is_empty() {
            return Err(DecodeError::Truncated { needed: 1 });
        }
        let attributes = body.get_i8();
        let timestamp_delta = wire::get_varint(&mut body)?;
        let offset_delta = wire::get_varint(&mut body)?;
        let Ok(offset_delta) = i32::try_from(offset_delta) else {
            return Err(DecodeError::InvalidLength(offset_delta));
        };
        let key = get_varint_bytes(&mut body)?;
        let value = get_varint_bytes(&mut body)?;
        let header_count = wire::get_varint(&mut body)?;
        if header_count < 0 {
            return Err(DecodeError::InvalidLength(header_count));
        }
        let mut headers = Vec::new();
        for _ in 0..header_count {
            let key = get_varint_bytes(&mut body)?.ok_or(DecodeError::InvalidLength(-1))?;
            let key = String::from_utf8(key.into()).map_err(|_| DecodeError::InvalidUtf8)?;
            let value = get_varint_bytes(&mut body)?;
            headers.push(RecordHeader { key, value });
        }
        if !body.is_empty() {
            return Err(DecodeError::InvalidLength(body.len() as i64));
        }
        Ok(Record {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        })
    }
}

/// Decode a whole record set: batches back to back.
///
/// A fetch response may truncate the final batch mid-bytes (brokers send
/// whole segments sliced by size); a partial trailing batch is discarded,
/// not an error. Corruption inside a complete batch is still an error.
pub fn decode_set(buf: &mut Bytes) -> Result<Vec<RecordBatch>, DecodeError> {
    let mut batches = Vec::new();
    loop {
        if buf.len() < 12 {
            buf.clear();
            return Ok(batches);
        }
        let batch_length = i32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let Ok(batch_length) = usize::try_from(batch_length) else {
            return Err(DecodeError::InvalidLength(i64::from(batch_length)));
        };
        if buf.len() < 12 + batch_length {
            buf.clear();
            return Ok(batches);
        }
        batches.push(RecordBatch::decode(buf)?);
    }
}

/// Encode a record set: batches back to back.
pub fn encode_set(buf: &mut impl BufMut, batches: &[RecordBatch]) -> Result<(), EncodeError> {
    for batch in batches {
        batch.encode(buf)?;
    }
    Ok(())
}

fn put_varint_bytes(buf: &mut impl BufMut, value: Option<&[u8]>) {
    match value {
        Some(v) => {
            wire::put_varint(buf, v.len() as i64);
            buf.put_slice(v);
        }
        None => wire::put_varint(buf, -1),
    }
}

fn get_varint_bytes(buf: &mut Bytes) -> Result<Option<Bytes>, DecodeError> {
    let length = wire::get_varint(buf)?;
    if length == -1 {
        return Ok(None);
    }
    let Ok(length) = usize::try_from(length) else {
        return Err(DecodeError::InvalidLength(length));
    };
    if buf.len() < length {
        return Err(DecodeError::Truncated {
            needed: length - buf.len(),
        });
    }
    Ok(Some(buf.split_to(length)))
}

/// CRC-32C (Castagnoli), the checksum record batches use. Distinct from
/// the CRC-32 that magic 0/1 message sets used.
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc = (crc >> 8) ^ CRC32C_TABLE[((crc ^ u32::from(byte)) & 0xff) as usize];
    }
    !crc
}

static CRC32C_TABLE: [u32; 256] = crc32c_table();

#[expect(clippy::cast_possible_truncation)] // i < 256
const fn crc32c_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82f6_3b78
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}
