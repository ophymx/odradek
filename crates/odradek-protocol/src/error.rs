//! Error types shared by the wire codecs.

/// A value could not be encoded into the Kafka wire format.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum EncodeError {
    /// A string or byte sequence exceeds the maximum length its wire
    /// representation can carry (e.g. a `STRING` longer than `i16::MAX`).
    #[error("value of length {len} exceeds the wire format limit of {max}")]
    TooLong { len: usize, max: usize },
    /// A field was `None` but the message version being encoded does not
    /// permit null for it.
    #[error("field {0} may not be null in this message version")]
    NullField(&'static str),
}

/// Bytes on the wire could not be decoded.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeError {
    /// The buffer ended before the value was complete.
    #[error("buffer truncated: needed {needed} more byte(s)")]
    Truncated { needed: usize },
    /// A varint ran past the maximum width for its type.
    #[error("varint overflows a 64-bit value")]
    VarintOverflow,
    /// A length prefix was negative where null is not permitted, or does not
    /// fit in memory on this platform.
    #[error("invalid length prefix: {0}")]
    InvalidLength(i64),
    /// A `STRING` value was not valid UTF-8.
    #[error("string is not valid UTF-8")]
    InvalidUtf8,
    /// An API key, error code, or other enum discriminant is unknown.
    #[error("unknown {kind} discriminant: {value}")]
    UnknownDiscriminant { kind: &'static str, value: i64 },
    /// A record batch's stored checksum does not match its contents.
    #[error("crc mismatch: batch stores {stored:#010x}, contents hash to {computed:#010x}")]
    CrcMismatch { stored: u32, computed: u32 },
    /// Decoding this input would allocate more than its
    /// [`Budget`](crate::budget::Budget) allows — a count-prefixed array
    /// asking for far more memory than the bytes that carry it.
    #[error("decode would allocate {wanted} bytes, over this input's {limit}-byte budget")]
    AllocationLimit { limit: usize, wanted: usize },
    /// The allocator refused a request the budget had approved.
    #[error("allocator refused {bytes} bytes while decoding (budget {limit})")]
    AllocationFailed { bytes: usize, limit: usize },
    /// A count-driven loop's element consumed no input. Legal counts
    /// cannot outrun the bytes that back them; continuing would spin.
    #[error("array element consumed no input")]
    NoProgress,
    /// A tagged-field section was not strictly ascending by tag. The
    /// spec requires ascending order, and a repeated tag would let one
    /// occurrence silently overwrite another.
    #[error("tagged field {tag} does not follow {previous} in ascending order")]
    TaggedFieldOrder { previous: u32, tag: u32 },
}
