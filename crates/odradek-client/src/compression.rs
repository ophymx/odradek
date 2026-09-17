//! Record batch compression codecs.
//!
//! Compression is a client concern: the protocol crate deliberately
//! carries compressed payloads raw (the proxy guarantee), so the
//! producer compresses here and the consumer decompresses here.
//!
//! Supported: gzip, lz4 (Kafka uses the lz4 *frame* format), snappy
//! (in the xerial stream framing Kafka's Java client writes, with a
//! bare-block fallback on decode for payloads from older librdkafka),
//! and zstd (via the libzstd binding).

#[cfg(any(feature = "gzip", feature = "lz4", feature = "zstd"))]
use std::io::Read;
#[cfg(any(feature = "gzip", feature = "lz4"))]
use std::io::Write;

use bytes::Bytes;
use odradek_protocol::records::Compression;

use crate::error::ClientError;

/// Guard decompression against bombs: a record batch's decompressed
/// records must still fit in a sane fetch response.
#[cfg(any(
    feature = "gzip",
    feature = "lz4",
    feature = "snappy",
    feature = "zstd"
))]
const MAX_DECOMPRESSED: u64 = 128 << 20;

/// Header of the xerial snappy stream format: magic, then version and
/// minimum compatible version (both big-endian 1), then a sequence of
/// big-endian-length-prefixed raw snappy blocks.
#[cfg(feature = "snappy")]
const XERIAL_HEADER: [u8; 16] = [
    0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00, 0, 0, 0, 1, 0, 0, 0, 1,
];

/// Uncompressed bytes per xerial block, matching the Java
/// `SnappyOutputStream` default.
#[cfg(feature = "snappy")]
const XERIAL_BLOCK: usize = 32 << 10;

pub(crate) fn compress(codec: Compression, payload: &[u8]) -> Result<Bytes, ClientError> {
    match codec {
        Compression::None => Ok(Bytes::copy_from_slice(payload)),
        #[cfg(feature = "gzip")]
        Compression::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(payload)?;
            Ok(encoder.finish()?.into())
        }
        #[cfg(not(feature = "gzip"))]
        Compression::Gzip => Err(ClientError::UnsupportedCompression(
            "gzip (feature disabled)",
        )),
        #[cfg(feature = "lz4")]
        Compression::Lz4 => {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(payload)?;
            let out = encoder
                .finish()
                .map_err(|e| ClientError::ProtocolViolation(format!("lz4 encode: {e}")))?;
            Ok(out.into())
        }
        #[cfg(not(feature = "lz4"))]
        Compression::Lz4 => Err(ClientError::UnsupportedCompression(
            "lz4 (feature disabled)",
        )),
        #[cfg(feature = "snappy")]
        Compression::Snappy => {
            let mut out = Vec::from(XERIAL_HEADER);
            let mut encoder = snap::raw::Encoder::new();
            for chunk in payload.chunks(XERIAL_BLOCK) {
                let block = encoder
                    .compress_vec(chunk)
                    .map_err(|e| ClientError::ProtocolViolation(format!("snappy encode: {e}")))?;
                let len = u32::try_from(block.len())
                    .map_err(|_| ClientError::ProtocolViolation("snappy block too large".into()))?;
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(&block);
            }
            Ok(out.into())
        }
        #[cfg(not(feature = "snappy"))]
        Compression::Snappy => Err(ClientError::UnsupportedCompression(
            "snappy (feature disabled)",
        )),
        #[cfg(feature = "zstd")]
        Compression::Zstd => {
            let out = zstd::stream::encode_all(payload, zstd::DEFAULT_COMPRESSION_LEVEL)?;
            Ok(out.into())
        }
        #[cfg(not(feature = "zstd"))]
        Compression::Zstd => Err(ClientError::UnsupportedCompression(
            "zstd (feature disabled)",
        )),
        Compression::Unknown(_) => Err(ClientError::UnsupportedCompression("unknown codec")),
    }
}

pub(crate) fn decompress(codec: Compression, payload: &[u8]) -> Result<Bytes, ClientError> {
    match codec {
        Compression::None => Ok(Bytes::copy_from_slice(payload)),
        #[cfg(feature = "gzip")]
        Compression::Gzip => {
            capped_read_to_end(flate2::read::GzDecoder::new(payload)).map(Bytes::from)
        }
        #[cfg(not(feature = "gzip"))]
        Compression::Gzip => Err(ClientError::UnsupportedCompression(
            "gzip (feature disabled)",
        )),
        #[cfg(feature = "lz4")]
        Compression::Lz4 => {
            capped_read_to_end(lz4_flex::frame::FrameDecoder::new(payload)).map(Bytes::from)
        }
        #[cfg(not(feature = "lz4"))]
        Compression::Lz4 => Err(ClientError::UnsupportedCompression(
            "lz4 (feature disabled)",
        )),
        #[cfg(feature = "snappy")]
        Compression::Snappy => snappy_decompress(payload).map(Bytes::from),
        #[cfg(not(feature = "snappy"))]
        Compression::Snappy => Err(ClientError::UnsupportedCompression(
            "snappy (feature disabled)",
        )),
        #[cfg(feature = "zstd")]
        Compression::Zstd => {
            capped_read_to_end(zstd::stream::read::Decoder::new(payload)?).map(Bytes::from)
        }
        #[cfg(not(feature = "zstd"))]
        Compression::Zstd => Err(ClientError::UnsupportedCompression(
            "zstd (feature disabled)",
        )),
        Compression::Unknown(_) => Err(ClientError::UnsupportedCompression("unknown codec")),
    }
}

/// Read a decompression stream fully, erroring (never truncating) if it
/// exceeds [`MAX_DECOMPRESSED`].
#[cfg(any(feature = "gzip", feature = "lz4", feature = "zstd"))]
fn capped_read_to_end(reader: impl Read) -> Result<Vec<u8>, ClientError> {
    let mut out = Vec::new();
    reader.take(MAX_DECOMPRESSED + 1).read_to_end(&mut out)?;
    if out.len() as u64 > MAX_DECOMPRESSED {
        return Err(ClientError::ProtocolViolation(
            "decompressed batch exceeds size cap".into(),
        ));
    }
    Ok(out)
}

#[cfg(feature = "snappy")]
fn snappy_decompress(payload: &[u8]) -> Result<Vec<u8>, ClientError> {
    let snappy_err = |e: snap::Error| ClientError::ProtocolViolation(format!("snappy decode: {e}"));
    let mut decoder = snap::raw::Decoder::new();
    if payload.len() < XERIAL_HEADER.len() || payload[..8] != XERIAL_HEADER[..8] {
        // No xerial magic: a bare snappy block, as older librdkafka wrote.
        if snap::raw::decompress_len(payload).map_err(snappy_err)? as u64 > MAX_DECOMPRESSED {
            return Err(ClientError::ProtocolViolation(
                "decompressed batch exceeds size cap".into(),
            ));
        }
        return decoder.decompress_vec(payload).map_err(snappy_err);
    }
    let mut rest = &payload[XERIAL_HEADER.len()..];
    let mut out = Vec::new();
    while !rest.is_empty() {
        let (len_bytes, tail) = rest
            .split_first_chunk::<4>()
            .ok_or_else(|| ClientError::ProtocolViolation("truncated xerial block".into()))?;
        let len = u32::from_be_bytes(*len_bytes) as usize;
        let block = tail
            .get(..len)
            .ok_or_else(|| ClientError::ProtocolViolation("truncated xerial block".into()))?;
        rest = &tail[len..];
        let uncompressed = snap::raw::decompress_len(block).map_err(snappy_err)?;
        if out.len() as u64 + uncompressed as u64 > MAX_DECOMPRESSED {
            return Err(ClientError::ProtocolViolation(
                "decompressed batch exceeds size cap".into(),
            ));
        }
        out.append(&mut decoder.decompress_vec(block).map_err(snappy_err)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(all(
        feature = "gzip",
        feature = "lz4",
        feature = "snappy",
        feature = "zstd"
    ))]
    fn all_codecs_roundtrip() {
        let payload = b"a record batch payload, repetitive enough to shrink \
                        shrink shrink shrink shrink shrink shrink shrink";
        for codec in [
            Compression::Gzip,
            Compression::Lz4,
            Compression::Snappy,
            Compression::Zstd,
        ] {
            let packed = compress(codec, payload).unwrap();
            assert_ne!(&packed[..], &payload[..]);
            let unpacked = decompress(codec, &packed).unwrap();
            assert_eq!(&unpacked[..], &payload[..]);
        }
    }

    #[test]
    #[cfg(feature = "snappy")]
    fn snappy_writes_xerial_framing() {
        let packed = compress(Compression::Snappy, b"hello").unwrap();
        assert_eq!(&packed[..16], &XERIAL_HEADER[..]);
    }

    #[test]
    #[cfg(feature = "snappy")]
    fn snappy_multi_block_roundtrip() {
        // Payloads past the block size must chunk and reassemble.
        let payload: Vec<u8> = (0..XERIAL_BLOCK * 2 + 17)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let packed = compress(Compression::Snappy, &payload).unwrap();
        let unpacked = decompress(Compression::Snappy, &packed).unwrap();
        assert_eq!(&unpacked[..], &payload[..]);
    }

    #[test]
    #[cfg(feature = "snappy")]
    fn snappy_bare_block_fallback() {
        // Older librdkafka wrote raw snappy without the xerial header.
        let bare = snap::raw::Encoder::new()
            .compress_vec(b"bare block payload")
            .unwrap();
        let unpacked = decompress(Compression::Snappy, &bare).unwrap();
        assert_eq!(&unpacked[..], b"bare block payload");
    }

    #[test]
    #[cfg(feature = "snappy")]
    fn snappy_truncated_xerial_is_an_error() {
        let packed = compress(Compression::Snappy, b"hello").unwrap();
        for cut in [XERIAL_HEADER.len() + 2, packed.len() - 1] {
            assert!(matches!(
                decompress(Compression::Snappy, &packed[..cut]),
                Err(ClientError::ProtocolViolation(_))
            ));
        }
    }

    #[test]
    fn unknown_codec_is_a_typed_error() {
        assert!(matches!(
            compress(Compression::Unknown(7), b"x"),
            Err(ClientError::UnsupportedCompression(_))
        ));
        assert!(matches!(
            decompress(Compression::Unknown(7), b"x"),
            Err(ClientError::UnsupportedCompression(_))
        ));
    }
}
