//! The Kafka frame envelope: an `i32` length prefix around every
//! request and response.
//!
//! Sans-I/O, like the rest of this crate: [`frame`] wraps a payload
//! being encoded into a buffer, [`check_len`] validates a length prefix
//! for read-exact style consumers, [`try_split`] extracts complete
//! frames from an accumulating buffer, and [`peek_correlation_id`]
//! reads the id every frame payload leads with. Before this module,
//! five consumers carried private copies of this logic with three
//! mutually inconsistent length policies — the frame envelope is wire
//! format, and wire format lives here.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{DecodeError, EncodeError};

/// The default ceiling on one frame's payload, matching Kafka's
/// `socket.request.max.bytes` spirit: large enough for any sane batch,
/// small enough that a hostile length prefix cannot balloon memory.
pub const DEFAULT_MAX_FRAME: usize = 64 << 20;

/// Append one frame to `buf`: an `i32` length prefix backfilled around
/// whatever `payload` encodes (header then body, by convention).
///
/// On error `buf` is truncated back to the length it had on entry, as
/// [`RecordBatch::encode_to`](crate::records::RecordBatch::encode_to)
/// does. A caller's send buffer usually outlives the frame being written
/// into it, so leaving a length prefix and a partial payload behind
/// after a handled [`EncodeError`] would desynchronize the stream — the
/// peer would read that debris as the start of the next frame.
pub fn frame(
    buf: &mut BytesMut,
    payload: impl FnOnce(&mut BytesMut) -> Result<(), EncodeError>,
) -> Result<(), EncodeError> {
    let start = buf.len();
    match frame_in_place(buf, start, payload) {
        Ok(()) => Ok(()),
        Err(error) => {
            buf.truncate(start);
            Err(error)
        }
    }
}

fn frame_in_place(
    buf: &mut BytesMut,
    start: usize,
    payload: impl FnOnce(&mut BytesMut) -> Result<(), EncodeError>,
) -> Result<(), EncodeError> {
    buf.put_i32(0);
    payload(buf)?;
    let len = buf.len() - start - 4;
    let prefix = i32::try_from(len).map_err(|_| EncodeError::TooLong {
        len,
        max: i32::MAX as usize,
    })?;
    buf[start..start + 4].copy_from_slice(&prefix.to_be_bytes());
    Ok(())
}

/// Validate a frame's length prefix: negative or above `max_len` is a
/// wire violation, never a value to clamp.
///
/// The return is an upper bound to *stream against*, not a size to
/// allocate. `vec![0u8; check_len(prefix, max)?]` before the body has
/// arrived lets four attacker-chosen bytes reserve 64 MiB per
/// connection, and a few hundred idle connections exhaust a host
/// without either side sending a payload. Accumulate what the socket
/// actually delivers and let [`try_split`] decide when a frame is whole.
pub fn check_len(prefix: [u8; 4], max_len: usize) -> Result<usize, DecodeError> {
    let len = i32::from_be_bytes(prefix);
    if len < 0 {
        return Err(DecodeError::InvalidLength(i64::from(len)));
    }
    let len = len as usize;
    if len > max_len {
        return Err(DecodeError::InvalidLength(i64::from(i32::from_be_bytes(
            prefix,
        ))));
    }
    Ok(len)
}

/// If `buf` holds at least one complete frame, split its payload off
/// (prefix consumed) and return it; `Ok(None)` means read more bytes.
pub fn try_split(buf: &mut BytesMut, max_len: usize) -> Result<Option<Bytes>, DecodeError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = check_len([buf[0], buf[1], buf[2], buf[3]], max_len)?;
    if buf.len() < 4 + len {
        return Ok(None);
    }
    buf.advance(4);
    Ok(Some(buf.split_to(len).freeze()))
}

/// The correlation id a frame payload leads with (request payloads
/// after the api key/version, response payloads immediately) — here,
/// the response form: the first four bytes.
pub fn peek_correlation_id(payload: &Bytes) -> Result<i32, DecodeError> {
    if payload.len() < 4 {
        return Err(DecodeError::Truncated {
            needed: 4 - payload.len(),
        });
    }
    Ok(i32::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrips_through_try_split() {
        let mut buf = BytesMut::new();
        frame(&mut buf, |b| {
            b.put_slice(b"hello");
            Ok(())
        })
        .unwrap();
        frame(&mut buf, |b| {
            b.put_slice(b"world!");
            Ok(())
        })
        .unwrap();

        let first = try_split(&mut buf, DEFAULT_MAX_FRAME).unwrap().unwrap();
        assert_eq!(&first[..], b"hello");
        let second = try_split(&mut buf, DEFAULT_MAX_FRAME).unwrap().unwrap();
        assert_eq!(&second[..], b"world!");
        assert!(buf.is_empty());
        assert!(try_split(&mut buf, DEFAULT_MAX_FRAME).unwrap().is_none());
    }

    #[test]
    fn partial_frames_ask_for_more() {
        let mut buf = BytesMut::new();
        frame(&mut buf, |b| {
            b.put_slice(b"abcdef");
            Ok(())
        })
        .unwrap();
        let whole = buf.clone();
        for cut in 0..whole.len() {
            let mut partial = BytesMut::from(&whole[..cut]);
            assert!(
                try_split(&mut partial, DEFAULT_MAX_FRAME)
                    .unwrap()
                    .is_none(),
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn hostile_lengths_are_violations_not_clamps() {
        // Negative length: an error, never "zero".
        assert!(matches!(
            check_len((-1i32).to_be_bytes(), DEFAULT_MAX_FRAME),
            Err(DecodeError::InvalidLength(-1))
        ));
        // Oversized length: an error before any allocation happens.
        assert!(check_len(i32::MAX.to_be_bytes(), DEFAULT_MAX_FRAME).is_err());
        let mut buf = BytesMut::from(&i32::MAX.to_be_bytes()[..]);
        assert!(try_split(&mut buf, DEFAULT_MAX_FRAME).is_err());
    }

    #[test]
    fn failed_frame_leaves_no_debris() {
        // A send buffer with an already-queued frame in it; the second
        // frame's payload fails halfway through writing.
        let mut buf = BytesMut::new();
        frame(&mut buf, |b| {
            b.put_slice(b"first");
            Ok(())
        })
        .unwrap();
        let queued = buf.clone();

        let error = frame(&mut buf, |b| {
            b.put_slice(b"half-written");
            Err(EncodeError::NullField("boom"))
        })
        .unwrap_err();
        assert_eq!(error, EncodeError::NullField("boom"));
        assert_eq!(buf, queued, "failed frame left bytes in the send buffer");

        // The buffer is still a valid stream: the queued frame reads
        // back, and nothing follows it.
        let first = try_split(&mut buf, DEFAULT_MAX_FRAME).unwrap().unwrap();
        assert_eq!(&first[..], b"first");
        assert!(try_split(&mut buf, DEFAULT_MAX_FRAME).unwrap().is_none());
    }

    #[test]
    fn correlation_id_peeks_without_consuming() {
        let payload = Bytes::from_static(&[0, 0, 0, 42, 9, 9]);
        assert_eq!(peek_correlation_id(&payload).unwrap(), 42);
        assert_eq!(payload.len(), 6);
    }
}
