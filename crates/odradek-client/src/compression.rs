//! Record batch compression codecs.
//!
//! Compression is a client concern: the protocol crate deliberately
//! carries compressed payloads raw (the proxy guarantee), so the
//! producer compresses here and the consumer decompresses here.
//!
//! Supported: gzip and lz4 (Kafka uses the lz4 *frame* format), both via
//! pure-Rust codecs. Snappy needs the xerial framing and zstd a C
//! binding; both are still typed errors.

use std::io::{Read, Write};

use bytes::Bytes;
use odradek_protocol::records::Compression;

use crate::error::ClientError;

/// Guard decompression against bombs: a record batch's decompressed
/// records must still fit in a sane fetch response.
const MAX_DECOMPRESSED: u64 = 128 << 20;

pub(crate) fn compress(codec: Compression, payload: &[u8]) -> Result<Bytes, ClientError> {
    match codec {
        Compression::None => Ok(Bytes::copy_from_slice(payload)),
        Compression::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(payload)?;
            Ok(encoder.finish()?.into())
        }
        Compression::Lz4 => {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(payload)?;
            let out = encoder
                .finish()
                .map_err(|e| ClientError::ProtocolViolation(format!("lz4 encode: {e}")))?;
            Ok(out.into())
        }
        Compression::Snappy => Err(ClientError::UnsupportedCompression("snappy")),
        Compression::Zstd => Err(ClientError::UnsupportedCompression("zstd")),
        Compression::Unknown(_) => Err(ClientError::UnsupportedCompression("unknown codec")),
    }
}

pub(crate) fn decompress(codec: Compression, payload: &[u8]) -> Result<Bytes, ClientError> {
    match codec {
        Compression::None => Ok(Bytes::copy_from_slice(payload)),
        Compression::Gzip => {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(payload)
                .take(MAX_DECOMPRESSED)
                .read_to_end(&mut out)?;
            Ok(out.into())
        }
        Compression::Lz4 => {
            let mut out = Vec::new();
            lz4_flex::frame::FrameDecoder::new(payload)
                .take(MAX_DECOMPRESSED)
                .read_to_end(&mut out)?;
            Ok(out.into())
        }
        Compression::Snappy => Err(ClientError::UnsupportedCompression("snappy")),
        Compression::Zstd => Err(ClientError::UnsupportedCompression("zstd")),
        Compression::Unknown(_) => Err(ClientError::UnsupportedCompression("unknown codec")),
    }
}

/// The attribute bits selecting `codec` in a record batch.
pub(crate) fn attribute_bits(codec: Compression) -> i16 {
    match codec {
        Compression::None => 0,
        Compression::Gzip => 1,
        Compression::Snappy => 2,
        Compression::Lz4 => 3,
        Compression::Zstd => 4,
        Compression::Unknown(bits) => i16::from(bits),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gzip_and_lz4_roundtrip() {
        let payload = b"a record batch payload, repetitive enough to shrink \
                        shrink shrink shrink shrink shrink shrink shrink";
        for codec in [Compression::Gzip, Compression::Lz4] {
            let packed = compress(codec, payload).unwrap();
            assert_ne!(&packed[..], &payload[..]);
            let unpacked = decompress(codec, &packed).unwrap();
            assert_eq!(&unpacked[..], &payload[..]);
        }
    }

    #[test]
    fn unsupported_codecs_are_typed_errors() {
        for codec in [
            Compression::Snappy,
            Compression::Zstd,
            Compression::Unknown(7),
        ] {
            assert!(matches!(
                compress(codec, b"x"),
                Err(ClientError::UnsupportedCompression(_))
            ));
            assert!(matches!(
                decompress(codec, b"x"),
                Err(ClientError::UnsupportedCompression(_))
            ));
        }
    }
}
