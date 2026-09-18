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
    /// `Compression::from_attributes`, kept beside it so a new codec
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

    /// Encode into any [`BufMut`].
    ///
    /// The crc covers everything after itself, so a destination that
    /// cannot be read back forces the batch body through a scratch
    /// buffer first. Callers that already hold a `BytesMut` should use
    /// [`RecordBatch::encode_to`], which writes in place and skips both
    /// the allocation and the copy.
    pub fn encode(&self, buf: &mut impl BufMut) -> Result<(), EncodeError> {
        let mut scratch = BytesMut::new();
        self.encode_to(&mut scratch)?;
        buf.put_slice(&scratch);
        Ok(())
    }

    /// Append this batch to `buf`, computing the crc over the bytes just
    /// written rather than over a side buffer.
    ///
    /// The batch length and crc are back-patched once the body is in
    /// place. On error `buf` is truncated back to the length it had on
    /// entry, so a failed batch leaves no partial bytes behind.
    pub fn encode_to(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        let start = buf.len();
        match self.encode_in_place(buf, start) {
            Ok(()) => Ok(()),
            Err(error) => {
                buf.truncate(start);
                Err(error)
            }
        }
    }

    fn encode_in_place(&self, buf: &mut BytesMut, start: usize) -> Result<(), EncodeError> {
        let record_count = self.record_count()?;
        buf.put_i64(self.base_offset);
        buf.put_i32(0); // batch_length, back-patched below
        buf.put_i32(self.partition_leader_epoch);
        buf.put_i8(MAGIC);
        buf.put_u32(0); // crc, back-patched below
        let body = buf.len();

        buf.put_i16(self.attributes);
        buf.put_i32(self.last_offset_delta);
        buf.put_i64(self.base_timestamp);
        buf.put_i64(self.max_timestamp);
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        buf.put_i32(self.base_sequence);
        buf.put_i32(record_count);
        match &self.records {
            Records::Plain(records) => {
                for record in records {
                    record.encode(buf)?;
                }
            }
            Records::Compressed { payload, .. } => buf.extend_from_slice(payload),
        }

        let batch_length = 4 + 1 + 4 + (buf.len() - body); // epoch + magic + crc + body
        let batch_length = i32::try_from(batch_length).map_err(|_| EncodeError::TooLong {
            len: batch_length,
            max: i32::MAX as usize,
        })?;
        let crc = crc32c(&buf[body..]);
        buf[start + 8..start + 12].copy_from_slice(&batch_length.to_be_bytes());
        buf[body - 4..body].copy_from_slice(&crc.to_be_bytes());
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
    /// Encoded length of everything after this record's length prefix.
    ///
    /// Computed rather than measured: the length prefix comes first on
    /// the wire, and sizing the body analytically is what lets the
    /// fields go straight to the destination instead of through a
    /// per-record side buffer.
    fn body_len(&self, header_count: i64) -> usize {
        fn bytes_len(value: Option<&[u8]>) -> usize {
            match value {
                Some(v) => wire::varint_len(v.len() as i64) + v.len(),
                None => wire::varint_len(-1),
            }
        }

        let mut len = 1; // attributes
        len += wire::varint_len(self.timestamp_delta);
        len += wire::varint_len(i64::from(self.offset_delta));
        len += bytes_len(self.key.as_deref());
        len += bytes_len(self.value.as_deref());
        len += wire::varint_len(header_count);
        for header in &self.headers {
            len += bytes_len(Some(header.key.as_bytes()));
            len += bytes_len(header.value.as_deref());
        }
        len
    }

    pub fn encode(&self, buf: &mut impl BufMut) -> Result<(), EncodeError> {
        let header_count = i64::try_from(self.headers.len()).map_err(|_| EncodeError::TooLong {
            len: self.headers.len(),
            max: usize::try_from(i64::MAX).unwrap_or(usize::MAX),
        })?;
        let body_len = self.body_len(header_count);
        let body_len = i64::try_from(body_len).map_err(|_| EncodeError::TooLong {
            len: body_len,
            max: usize::try_from(i64::MAX).unwrap_or(usize::MAX),
        })?;

        wire::put_varint(buf, body_len);
        buf.put_i8(self.attributes);
        wire::put_varint(buf, self.timestamp_delta);
        wire::put_varint(buf, i64::from(self.offset_delta));
        put_varint_bytes(buf, self.key.as_deref());
        put_varint_bytes(buf, self.value.as_deref());
        wire::put_varint(buf, header_count);
        for header in &self.headers {
            put_varint_bytes(buf, Some(header.key.as_bytes()));
            put_varint_bytes(buf, header.value.as_deref());
        }
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
    // One scratch buffer for the whole set, reused batch to batch: a
    // generic BufMut cannot be read back, and the crc must be written
    // before the body it covers.
    let mut scratch = BytesMut::new();
    for batch in batches {
        scratch.clear();
        batch.encode_to(&mut scratch)?;
        buf.put_slice(&scratch);
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
///
/// Slicing-by-8: each iteration folds eight input bytes at once through
/// eight tables, breaking the serial dependency that makes the classic
/// one-byte loop wait a table lookup per byte. Pure safe Rust, so it
/// needs no runtime feature detection — a machine with the SSE4.2 `crc32`
/// instruction would still be faster, but that needs `unsafe`.
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        // The reflected algorithm consumes bytes little-endian first.
        let low = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) ^ crc;
        let high = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        crc = CRC32C_TABLES[7][(low & 0xff) as usize]
            ^ CRC32C_TABLES[6][((low >> 8) & 0xff) as usize]
            ^ CRC32C_TABLES[5][((low >> 16) & 0xff) as usize]
            ^ CRC32C_TABLES[4][(low >> 24) as usize]
            ^ CRC32C_TABLES[3][(high & 0xff) as usize]
            ^ CRC32C_TABLES[2][((high >> 8) & 0xff) as usize]
            ^ CRC32C_TABLES[1][((high >> 16) & 0xff) as usize]
            ^ CRC32C_TABLES[0][(high >> 24) as usize];
    }
    // Up to seven bytes the wide loop could not take, a byte at a time.
    for &byte in chunks.remainder() {
        crc = (crc >> 8) ^ CRC32C_TABLES[0][((crc ^ u32::from(byte)) & 0xff) as usize];
    }
    !crc
}

static CRC32C_TABLES: [[u32; 256]; 8] = crc32c_tables();

/// Table `n` holds the residue of a byte shifted `n` places further into
/// the message, so table 0 is the classic byte-at-a-time table and each
/// later one advances it by another byte.
#[expect(clippy::cast_possible_truncation)] // i < 256
const fn crc32c_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
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
        tables[0][i] = crc;
        i += 1;
    }
    let mut slice = 1;
    while slice < 8 {
        let mut i = 0;
        while i < 256 {
            let previous = tables[slice - 1][i];
            tables[slice][i] = (previous >> 8) ^ tables[0][(previous & 0xff) as usize];
            i += 1;
        }
        slice += 1;
    }
    tables
}
