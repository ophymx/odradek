//! Primitive codecs for the Kafka wire format.
//!
//! Fixed-width integers are big-endian and provided directly by [`bytes`];
//! this module adds the Kafka-specific primitives: unsigned varints
//! (protobuf-style LEB128), zigzag-encoded signed varints, length-prefixed
//! strings/bytes in both classic and compact ("flexible version") forms, and
//! tagged fields.
//!
//! Decoders take `&mut impl Buf` and never panic on malformed input; every
//! failure is a [`DecodeError`]. Encoders take `&mut impl BufMut` and only
//! fail when a value cannot be represented (see [`EncodeError`]).
//!
//! The one decoder here that allocates per wire element —
//! [`get_tagged_fields`] — spends against a [`Budget`]; see
//! [`crate::budget`] for the bound that puts on it.

use bytes::{Buf, BufMut, Bytes};

use crate::budget::{Budget, Limits};
use crate::error::{DecodeError, EncodeError};

/// Ensure `buf` has at least `needed` readable bytes.
fn ensure(buf: &impl Buf, needed: usize) -> Result<(), DecodeError> {
    if buf.remaining() < needed {
        Err(DecodeError::Truncated {
            needed: needed - buf.remaining(),
        })
    } else {
        Ok(())
    }
}

fn len_from(raw: i64) -> Result<usize, DecodeError> {
    usize::try_from(raw).map_err(|_| DecodeError::InvalidLength(raw))
}

/// Convert a compact length (already offset by -1) to `usize`, rejecting
/// values that cannot fit in memory on this platform.
fn compact_len(raw: u64) -> Result<usize, DecodeError> {
    usize::try_from(raw)
        .map_err(|_| DecodeError::InvalidLength(i64::try_from(raw).unwrap_or(i64::MAX)))
}

// ---------------------------------------------------------------------------
// Fixed-width values
// ---------------------------------------------------------------------------
//
// `Buf`'s own getters panic when the buffer is short; codecs must never do
// that on wire input, so every fixed-width read goes through these.

macro_rules! checked_get {
    ($(#[$doc:meta] $name:ident -> $ty:ty [$len:expr] via $method:ident;)+) => {
        $(
            #[$doc]
            pub fn $name(buf: &mut impl Buf) -> Result<$ty, DecodeError> {
                ensure(buf, $len)?;
                Ok(buf.$method())
            }
        )+
    };
}

checked_get! {
    /// Decode an `INT8`.
    get_i8 -> i8 [1] via get_i8;
    /// Decode an `INT16` (big-endian).
    get_i16 -> i16 [2] via get_i16;
    /// Decode a `UINT16` (big-endian).
    get_u16 -> u16 [2] via get_u16;
    /// Decode an `INT32` (big-endian).
    get_i32 -> i32 [4] via get_i32;
    /// Decode a `UINT32` (big-endian).
    get_u32 -> u32 [4] via get_u32;
    /// Decode an `INT64` (big-endian).
    get_i64 -> i64 [8] via get_i64;
    /// Decode a `FLOAT64` (big-endian IEEE 754).
    get_f64 -> f64 [8] via get_f64;
}

/// Decode a `BOOLEAN` (any non-zero byte is true).
pub fn get_bool(buf: &mut impl Buf) -> Result<bool, DecodeError> {
    ensure(buf, 1)?;
    Ok(buf.get_u8() != 0)
}

// ---------------------------------------------------------------------------
// Varints
// ---------------------------------------------------------------------------

/// Encode an `UNSIGNED_VARINT` (LEB128, low 7 bits first).
pub fn put_unsigned_varint(buf: &mut impl BufMut, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if value == 0 {
            return;
        }
    }
}

/// Decode an `UNSIGNED_VARINT`. At most 10 bytes; rejects encodings that
/// overflow 64 bits.
pub fn get_unsigned_varint(buf: &mut impl Buf) -> Result<u64, DecodeError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        ensure(buf, 1)?;
        let byte = buf.get_u8();
        if shift == 63 && byte & 0xfe != 0 {
            return Err(DecodeError::VarintOverflow);
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift > 63 {
            return Err(DecodeError::VarintOverflow);
        }
    }
}

/// Encode a signed `VARINT`/`VARLONG` using zigzag encoding.
pub fn put_varint(buf: &mut impl BufMut, value: i64) {
    put_unsigned_varint(buf, zigzag_encode(value));
}

/// Decode a signed `VARINT`/`VARLONG` using zigzag encoding.
pub fn get_varint(buf: &mut impl Buf) -> Result<i64, DecodeError> {
    Ok(zigzag_decode(get_unsigned_varint(buf)?))
}

fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

/// Number of bytes [`put_varint`] emits for `value`. Lets an encoder
/// size a body before writing it instead of measuring a side buffer.
pub(crate) fn varint_len(value: i64) -> usize {
    unsigned_varint_len(zigzag_encode(value))
}

/// Number of bytes [`put_unsigned_varint`] emits for `value`.
pub fn unsigned_varint_len(value: u64) -> usize {
    // 1 byte per started 7-bit group; value 0 still takes one byte.
    let bits = 64 - value.leading_zeros() as usize;
    bits.div_ceil(7).max(1)
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

/// Encode a classic `STRING` (i16 length prefix).
pub fn put_string(buf: &mut impl BufMut, value: &str) -> Result<(), EncodeError> {
    let len = i16::try_from(value.len()).map_err(|_| EncodeError::TooLong {
        len: value.len(),
        max: i16::MAX as usize,
    })?;
    buf.put_i16(len);
    buf.put_slice(value.as_bytes());
    Ok(())
}

/// Encode a classic `NULLABLE_STRING` (length -1 marks null).
pub fn put_nullable_string(buf: &mut impl BufMut, value: Option<&str>) -> Result<(), EncodeError> {
    match value {
        Some(s) => put_string(buf, s),
        None => {
            buf.put_i16(-1);
            Ok(())
        }
    }
}

/// Decode a classic `STRING`; a null marker is an error here.
pub fn get_string(buf: &mut impl Buf) -> Result<String, DecodeError> {
    get_nullable_string(buf)?.ok_or(DecodeError::InvalidLength(-1))
}

/// Decode a classic `NULLABLE_STRING`.
pub fn get_nullable_string(buf: &mut impl Buf) -> Result<Option<String>, DecodeError> {
    ensure(buf, 2)?;
    let raw = buf.get_i16();
    if raw == -1 {
        return Ok(None);
    }
    let len = len_from(raw.into())?;
    read_utf8(buf, len).map(Some)
}

/// Encode a `COMPACT_STRING` (unsigned varint of length + 1).
pub fn put_compact_string(buf: &mut impl BufMut, value: &str) {
    put_unsigned_varint(buf, value.len() as u64 + 1);
    buf.put_slice(value.as_bytes());
}

/// Encode a `COMPACT_NULLABLE_STRING` (0 marks null).
pub fn put_compact_nullable_string(buf: &mut impl BufMut, value: Option<&str>) {
    match value {
        Some(s) => put_compact_string(buf, s),
        None => put_unsigned_varint(buf, 0),
    }
}

/// Decode a `COMPACT_STRING`; a null marker is an error here.
pub fn get_compact_string(buf: &mut impl Buf) -> Result<String, DecodeError> {
    get_compact_nullable_string(buf)?.ok_or(DecodeError::InvalidLength(-1))
}

/// Decode a `COMPACT_NULLABLE_STRING`.
pub fn get_compact_nullable_string(buf: &mut impl Buf) -> Result<Option<String>, DecodeError> {
    match get_unsigned_varint(buf)? {
        0 => Ok(None),
        n => read_utf8(buf, compact_len(n - 1)?).map(Some),
    }
}

fn read_utf8(buf: &mut impl Buf, len: usize) -> Result<String, DecodeError> {
    ensure(buf, len)?;
    let bytes = buf.copy_to_bytes(len);
    String::from_utf8(bytes.into()).map_err(|_| DecodeError::InvalidUtf8)
}

// ---------------------------------------------------------------------------
// Bytes
// ---------------------------------------------------------------------------

/// Encode classic `BYTES` (i32 length prefix).
pub fn put_bytes(buf: &mut impl BufMut, value: &[u8]) -> Result<(), EncodeError> {
    let len = i32::try_from(value.len()).map_err(|_| EncodeError::TooLong {
        len: value.len(),
        max: i32::MAX as usize,
    })?;
    buf.put_i32(len);
    buf.put_slice(value);
    Ok(())
}

/// Encode classic `NULLABLE_BYTES` (length -1 marks null).
pub fn put_nullable_bytes(buf: &mut impl BufMut, value: Option<&[u8]>) -> Result<(), EncodeError> {
    match value {
        Some(b) => put_bytes(buf, b),
        None => {
            buf.put_i32(-1);
            Ok(())
        }
    }
}

/// Decode classic `NULLABLE_BYTES`.
pub fn get_nullable_bytes(buf: &mut impl Buf) -> Result<Option<Bytes>, DecodeError> {
    ensure(buf, 4)?;
    let raw = buf.get_i32();
    if raw == -1 {
        return Ok(None);
    }
    let len = len_from(raw.into())?;
    ensure(buf, len)?;
    Ok(Some(buf.copy_to_bytes(len)))
}

/// Encode `COMPACT_BYTES` (unsigned varint of length + 1).
pub fn put_compact_bytes(buf: &mut impl BufMut, value: &[u8]) {
    put_unsigned_varint(buf, value.len() as u64 + 1);
    buf.put_slice(value);
}

/// Encode `COMPACT_NULLABLE_BYTES` (0 marks null).
pub fn put_compact_nullable_bytes(buf: &mut impl BufMut, value: Option<&[u8]>) {
    match value {
        Some(b) => put_compact_bytes(buf, b),
        None => put_unsigned_varint(buf, 0),
    }
}

/// Decode `COMPACT_NULLABLE_BYTES`.
pub fn get_compact_nullable_bytes(buf: &mut impl Buf) -> Result<Option<Bytes>, DecodeError> {
    match get_unsigned_varint(buf)? {
        0 => Ok(None),
        n => {
            let len = compact_len(n - 1)?;
            ensure(buf, len)?;
            Ok(Some(buf.copy_to_bytes(len)))
        }
    }
}

// ---------------------------------------------------------------------------
// Array length prefixes
// ---------------------------------------------------------------------------

/// Encode a classic `ARRAY` length prefix (`None` marks a null array).
pub fn put_array_len(buf: &mut impl BufMut, len: Option<usize>) -> Result<(), EncodeError> {
    match len {
        Some(n) => {
            let n = i32::try_from(n).map_err(|_| EncodeError::TooLong {
                len: n,
                max: i32::MAX as usize,
            })?;
            buf.put_i32(n);
        }
        None => buf.put_i32(-1),
    }
    Ok(())
}

/// Decode a classic `ARRAY` length prefix (`None` marks a null array).
pub fn get_array_len(buf: &mut impl Buf) -> Result<Option<usize>, DecodeError> {
    ensure(buf, 4)?;
    let raw = buf.get_i32();
    if raw == -1 {
        return Ok(None);
    }
    len_from(raw.into()).map(Some)
}

/// Encode a `COMPACT_ARRAY` length prefix (varint of length + 1, 0 is null).
pub fn put_compact_array_len(buf: &mut impl BufMut, len: Option<usize>) {
    match len {
        Some(n) => put_unsigned_varint(buf, n as u64 + 1),
        None => put_unsigned_varint(buf, 0),
    }
}

/// Decode a `COMPACT_ARRAY` length prefix (`None` marks a null array).
pub fn get_compact_array_len(buf: &mut impl Buf) -> Result<Option<usize>, DecodeError> {
    match get_unsigned_varint(buf)? {
        0 => Ok(None),
        n => compact_len(n - 1).map(Some),
    }
}

// ---------------------------------------------------------------------------
// UUID
// ---------------------------------------------------------------------------

/// Encode a `UUID` (16 raw bytes, big-endian field order as sent by Kafka).
pub fn put_uuid(buf: &mut impl BufMut, value: [u8; 16]) {
    buf.put_slice(&value);
}

/// Decode a `UUID`.
pub fn get_uuid(buf: &mut impl Buf) -> Result<[u8; 16], DecodeError> {
    ensure(buf, 16)?;
    let mut out = [0u8; 16];
    buf.copy_to_slice(&mut out);
    Ok(out)
}

// ---------------------------------------------------------------------------
// Tagged fields (KIP-482 flexible versions)
// ---------------------------------------------------------------------------

/// A single tagged field, kept as raw bytes so unknown tags round-trip
/// losslessly — required for proxying and for acceptance testing against
/// implementations newer than this crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTaggedField {
    pub tag: u32,
    pub data: Bytes,
}

/// Encode a tagged-field section. Fields must be sorted by tag per the spec;
/// this function encodes in the order given.
pub fn put_tagged_fields(buf: &mut impl BufMut, fields: &[RawTaggedField]) {
    put_unsigned_varint(buf, fields.len() as u64);
    for field in fields {
        put_unsigned_varint(buf, u64::from(field.tag));
        put_unsigned_varint(buf, field.data.len() as u64);
        buf.put_slice(&field.data);
    }
}

/// Decode a tagged-field section, preserving unknown tags as raw bytes.
///
/// Allocation is bounded by a [`Budget`] derived from `buf`'s remaining
/// length; see [`get_tagged_fields_with_budget`] to share one budget
/// across a whole message.
pub fn get_tagged_fields(buf: &mut impl Buf) -> Result<Vec<RawTaggedField>, DecodeError> {
    let mut budget = Limits::default().budget(buf.remaining());
    get_tagged_fields_with_budget(buf, &mut budget)
}

/// Decode a tagged-field section, charging `budget` for what it keeps.
///
/// Tags must be **strictly ascending**, which the spec requires and the
/// Java implementation enforces. Accepting anything else is a
/// parser-differential primitive: a section carrying tag 0 twice would
/// decode to whichever copy came last, with the first vanishing — not
/// even preserved in `unknown_tagged_fields` — so the bytes this crate
/// re-encoded would not be the bytes it was handed, and two
/// implementations reading the same frame would disagree about its
/// contents.
pub fn get_tagged_fields_with_budget(
    buf: &mut impl Buf,
    budget: &mut Budget,
) -> Result<Vec<RawTaggedField>, DecodeError> {
    let count = get_unsigned_varint(buf)?;
    let mut fields: Vec<RawTaggedField> = Vec::new();
    let mut previous: Option<u32> = None;
    for _ in 0..count {
        // No progress check here: a tag costs at least its own varint
        // plus a length varint, so this loop always consumes input.
        let tag =
            u32::try_from(get_unsigned_varint(buf)?).map_err(|_| DecodeError::VarintOverflow)?;
        if let Some(previous) = previous {
            if tag <= previous {
                return Err(DecodeError::TaggedFieldOrder { previous, tag });
            }
        }
        previous = Some(tag);
        let len = len_from(i64::try_from(get_unsigned_varint(buf)?).unwrap_or(-1))?;
        ensure(buf, len)?;
        budget.push(
            &mut fields,
            RawTaggedField {
                tag,
                data: buf.copy_to_bytes(len),
            },
        )?;
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[track_caller]
    fn roundtrip_unsigned(value: u64) {
        let mut buf = BytesMut::new();
        put_unsigned_varint(&mut buf, value);
        assert_eq!(buf.len(), unsigned_varint_len(value), "length of {value}");
        let mut read = buf.freeze();
        assert_eq!(get_unsigned_varint(&mut read), Ok(value));
        assert!(!read.has_remaining());
    }

    #[test]
    fn unsigned_varint_roundtrip() {
        for value in [0, 1, 127, 128, 300, 16383, 16384, u32::MAX as u64, u64::MAX] {
            roundtrip_unsigned(value);
        }
    }

    #[test]
    fn unsigned_varint_known_encoding() {
        let mut buf = BytesMut::new();
        put_unsigned_varint(&mut buf, 300);
        assert_eq!(&buf[..], &[0xac, 0x02]);
    }

    #[test]
    fn unsigned_varint_overflow_rejected() {
        // 11 continuation bytes can never be a valid u64.
        let mut buf = Bytes::from_static(&[0x80; 11]);
        assert_eq!(
            get_unsigned_varint(&mut buf),
            Err(DecodeError::VarintOverflow)
        );
        // 10th byte with value bits above the 64th bit.
        let mut buf =
            Bytes::from_static(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]);
        assert_eq!(
            get_unsigned_varint(&mut buf),
            Err(DecodeError::VarintOverflow)
        );
    }

    #[test]
    fn unsigned_varint_truncated() {
        let mut buf = Bytes::from_static(&[0x80]);
        assert_eq!(
            get_unsigned_varint(&mut buf),
            Err(DecodeError::Truncated { needed: 1 })
        );
    }

    #[test]
    fn zigzag_known_values() {
        for (plain, encoded) in [(0i64, 0u64), (-1, 1), (1, 2), (-2, 3), (2, 4)] {
            assert_eq!(zigzag_encode(plain), encoded);
            assert_eq!(zigzag_decode(encoded), plain);
        }
        assert_eq!(zigzag_decode(zigzag_encode(i64::MIN)), i64::MIN);
        assert_eq!(zigzag_decode(zigzag_encode(i64::MAX)), i64::MAX);
    }

    #[test]
    fn signed_varint_roundtrip() {
        for value in [0i64, -1, 1, -300, 300, i32::MIN as i64, i64::MAX, i64::MIN] {
            let mut buf = BytesMut::new();
            put_varint(&mut buf, value);
            assert_eq!(get_varint(&mut buf.freeze()), Ok(value));
        }
    }

    #[test]
    fn string_roundtrip() {
        let mut buf = BytesMut::new();
        put_string(&mut buf, "hello").unwrap();
        assert_eq!(&buf[..], b"\x00\x05hello");
        assert_eq!(get_string(&mut buf.freeze()).unwrap(), "hello");
    }

    #[test]
    fn nullable_string_null() {
        let mut buf = BytesMut::new();
        put_nullable_string(&mut buf, None).unwrap();
        assert_eq!(&buf[..], &[0xff, 0xff]);
        assert_eq!(get_nullable_string(&mut buf.freeze()), Ok(None));
    }

    #[test]
    fn compact_string_roundtrip() {
        let mut buf = BytesMut::new();
        put_compact_string(&mut buf, "topic-a");
        assert_eq!(buf[0], 8); // len + 1
        assert_eq!(get_compact_string(&mut buf.freeze()).unwrap(), "topic-a");

        let mut buf = BytesMut::new();
        put_compact_nullable_string(&mut buf, None);
        assert_eq!(&buf[..], &[0x00]);
        assert_eq!(get_compact_nullable_string(&mut buf.freeze()), Ok(None));
    }

    #[test]
    fn invalid_utf8_rejected() {
        let mut buf = Bytes::from_static(&[0x00, 0x02, 0xff, 0xfe]);
        assert_eq!(get_string(&mut buf), Err(DecodeError::InvalidUtf8));
    }

    #[test]
    fn bytes_roundtrip() {
        let payload = &[1u8, 2, 3][..];
        let mut buf = BytesMut::new();
        put_bytes(&mut buf, payload).unwrap();
        assert_eq!(
            get_nullable_bytes(&mut buf.freeze()).unwrap().as_deref(),
            Some(payload)
        );

        let mut buf = BytesMut::new();
        put_nullable_bytes(&mut buf, None).unwrap();
        assert_eq!(get_nullable_bytes(&mut buf.freeze()), Ok(None));

        let mut buf = BytesMut::new();
        put_compact_bytes(&mut buf, payload);
        assert_eq!(
            get_compact_nullable_bytes(&mut buf.freeze())
                .unwrap()
                .as_deref(),
            Some(payload)
        );
    }

    #[test]
    fn array_len_roundtrip() {
        for len in [None, Some(0), Some(5), Some(100_000)] {
            let mut buf = BytesMut::new();
            put_array_len(&mut buf, len).unwrap();
            assert_eq!(get_array_len(&mut buf.freeze()), Ok(len));

            let mut buf = BytesMut::new();
            put_compact_array_len(&mut buf, len);
            assert_eq!(get_compact_array_len(&mut buf.freeze()), Ok(len));
        }
    }

    #[test]
    fn negative_array_len_rejected() {
        let mut buf = Bytes::from_static(&[0xff, 0xff, 0xff, 0xfe]); // -2
        assert_eq!(get_array_len(&mut buf), Err(DecodeError::InvalidLength(-2)));
    }

    #[test]
    fn uuid_roundtrip() {
        let id: [u8; 16] = *b"0123456789abcdef";
        let mut buf = BytesMut::new();
        put_uuid(&mut buf, id);
        assert_eq!(get_uuid(&mut buf.freeze()), Ok(id));
    }

    #[test]
    fn tagged_fields_roundtrip() {
        let fields = vec![
            RawTaggedField {
                tag: 0,
                data: Bytes::from_static(b"abc"),
            },
            RawTaggedField {
                tag: 7,
                data: Bytes::new(),
            },
        ];
        let mut buf = BytesMut::new();
        put_tagged_fields(&mut buf, &fields);
        assert_eq!(get_tagged_fields(&mut buf.freeze()), Ok(fields));

        // Empty section is a single zero byte.
        let mut buf = BytesMut::new();
        put_tagged_fields(&mut buf, &[]);
        assert_eq!(&buf[..], &[0x00]);
    }

    /// Build a tagged-field section from `(tag, data)` pairs verbatim,
    /// bypassing the ordering `put_tagged_fields` callers observe.
    fn raw_section(fields: &[(u32, &[u8])]) -> Bytes {
        let mut buf = BytesMut::new();
        put_unsigned_varint(&mut buf, fields.len() as u64);
        for (tag, data) in fields {
            put_unsigned_varint(&mut buf, u64::from(*tag));
            put_unsigned_varint(&mut buf, data.len() as u64);
            buf.put_slice(data);
        }
        buf.freeze()
    }

    #[test]
    fn tagged_fields_must_ascend() {
        assert_eq!(
            get_tagged_fields(&mut raw_section(&[(7, b"a"), (2, b"b")])),
            Err(DecodeError::TaggedFieldOrder {
                previous: 7,
                tag: 2
            })
        );
    }

    #[test]
    fn duplicate_tags_are_rejected() {
        // Unknown tag twice: the second would shadow the first in any
        // consumer that indexes by tag.
        assert_eq!(
            get_tagged_fields(&mut raw_section(&[(99, b"first"), (99, b"second")])),
            Err(DecodeError::TaggedFieldOrder {
                previous: 99,
                tag: 99
            })
        );
        // Adjacent distinct tags are still fine.
        assert!(get_tagged_fields(&mut raw_section(&[(0, b"a"), (1, b"b")])).is_ok());
    }

    #[test]
    fn tagged_fields_are_bounded_by_input_size() {
        // 4096 empty tags: 2 bytes each on the wire, 40 in memory.
        // Unbounded, that is a 20x amplification; the budget stops it.
        let fields: Vec<(u32, &[u8])> = (0..4096u32).map(|tag| (tag, &[][..])).collect();
        let section = raw_section(&fields);
        let mut tight = Limits::default()
            .with_min_alloc_bytes(0)
            .with_alloc_factor(4)
            .budget(section.len());
        assert!(matches!(
            get_tagged_fields_with_budget(&mut section.clone(), &mut tight),
            Err(DecodeError::AllocationLimit { .. })
        ));
        assert!(tight.used() <= tight.limit());
        // Raising the factor decodes the same bytes: the limit is a
        // policy, not a wire rule.
        let mut roomy = Limits::default()
            .with_alloc_factor(64)
            .budget(section.len());
        assert_eq!(
            get_tagged_fields_with_budget(&mut section.clone(), &mut roomy)
                .expect("decodes")
                .len(),
            4096
        );
    }
}
