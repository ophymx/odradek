//! Raw wire access for checks.
//!
//! Deliberately independent of `odradek-client`: the suite must be able to
//! validate *that* client too, and checks need control over every byte they
//! send — including bytes a well-behaved client would never produce.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use odradek_protocol::EncodeError;
use odradek_protocol::frame;
use odradek_protocol::messages::request_header::RequestHeader;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Frames larger than this are treated as a subject failure, not a reason
/// to allocate unboundedly.
pub use odradek_protocol::frame::DEFAULT_MAX_FRAME as MAX_FRAME_SIZE;

/// How long a check waits for the subject to produce a frame.
pub const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Errors while exchanging raw frames with the subject.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WireError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode error: {0}")]
    Encode(#[from] EncodeError),
    #[error("timed out after {READ_TIMEOUT:?} waiting for a response frame")]
    Timeout,
    #[error("frame length {0} out of range")]
    BadFrameLength(i32),
}

/// A raw client connection to a server under test.
#[derive(Debug)]
pub struct RawConnection {
    stream: TcpStream,
}

impl RawConnection {
    pub async fn connect(addr: &str) -> Result<RawConnection, WireError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(RawConnection { stream })
    }

    /// Write one length-prefixed frame.
    pub async fn send_frame(&mut self, payload: &[u8]) -> Result<(), WireError> {
        let mut framed = BytesMut::with_capacity(payload.len() + 4);
        frame::frame(&mut framed, |buf| {
            buf.extend_from_slice(payload);
            Ok(())
        })?;
        self.stream.write_all(&framed).await?;
        Ok(())
    }

    /// Read one length-prefixed frame, bounded by [`READ_TIMEOUT`].
    pub async fn read_frame(&mut self) -> Result<Bytes, WireError> {
        tokio::time::timeout(READ_TIMEOUT, self.read_frame_inner())
            .await
            .map_err(|_| WireError::Timeout)?
    }

    async fn read_frame_inner(&mut self) -> Result<Bytes, WireError> {
        let mut len_bytes = [0u8; 4];
        self.stream.read_exact(&mut len_bytes).await?;
        let len = frame::check_len(len_bytes, MAX_FRAME_SIZE)
            .map_err(|_| WireError::BadFrameLength(i32::from_be_bytes(len_bytes)))?;
        let mut frame = vec![0u8; len];
        self.stream.read_exact(&mut frame).await?;
        Ok(Bytes::from(frame))
    }

    /// Send a request assembled from a header and pre-encoded body, then
    /// read one response frame.
    pub async fn round_trip(
        &mut self,
        header: &RequestHeader,
        header_version: i16,
        body: &[u8],
    ) -> Result<Bytes, WireError> {
        let mut payload = BytesMut::new();
        header.encode(&mut payload, header_version)?;
        payload.extend_from_slice(body);
        self.send_frame(&payload).await?;
        self.read_frame().await
    }
}
