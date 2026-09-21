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
    #[cfg(feature = "tls")]
    #[error("tls: {0}")]
    Tls(String),
}

/// What a [`RawConnection`] is actually carried over.
///
/// An enum rather than a boxed `AsyncRead + AsyncWrite`, so the plain
/// case stays exactly what it was: a `TcpStream`, no vtable, no
/// indirection, for the several hundred exchanges a suite run makes
/// against a broker that wants none of this.
#[derive(Debug)]
enum Transport {
    Plain(TcpStream),
    /// Boxed because a rustls stream is large — around 10 KiB of
    /// buffers — and every `RawConnection` would otherwise be that big
    /// whether or not it speaks TLS.
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl Transport {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Transport::Plain(stream) => stream.write_all(buf).await,
            #[cfg(feature = "tls")]
            Transport::Tls(stream) => stream.write_all(buf).await,
        }
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            Transport::Plain(stream) => stream.read_exact(buf).await.map(|_| ()),
            #[cfg(feature = "tls")]
            Transport::Tls(stream) => stream.read_exact(buf).await.map(|_| ()),
        }
    }
}

/// A raw client connection to a server under test.
#[derive(Debug)]
pub struct RawConnection {
    stream: Transport,
    peer: String,
}

impl RawConnection {
    /// The address this connection was opened to.
    ///
    /// Kept as it was given rather than read back from the socket: a
    /// caller that wants a second connection to the same broker wants
    /// the address it would dial, not the resolved one.
    pub fn peer(&self) -> &str {
        &self.peer
    }

    pub async fn connect(addr: &str) -> Result<RawConnection, WireError> {
        Ok(RawConnection {
            stream: Transport::Plain(tcp(addr).await?),
            peer: addr.to_owned(),
        })
    }

    /// The same, wrapped in TLS and verified against `trust`.
    ///
    /// The name the certificate is checked against comes from `trust`
    /// rather than from `addr`, because a broker's certificate names
    /// the host an operator configured and the suite reaches it at
    /// whatever `127.0.0.1:<ephemeral>` the container was published on.
    /// Verifying against the dialled address would mean either a
    /// certificate per run or no verification at all, and a suite that
    /// skips verification is not exercising TLS, it is exercising a
    /// socket.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(addr: &str, trust: &TlsTrust) -> Result<RawConnection, WireError> {
        use tokio_rustls::TlsConnector;
        let stream = tcp(addr).await?;
        let name = trust.server_name.clone();
        let stream = TlsConnector::from(trust.config.clone())
            .connect(name, stream)
            .await?;
        Ok(RawConnection {
            stream: Transport::Tls(Box::new(stream)),
            peer: addr.to_owned(),
        })
    }

    /// Write one length-prefixed frame (TLS or not).
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

/// A connected, nodelay TCP socket.
///
/// `set_nodelay` matters more here than in a client: every check is a
/// request and its answer, so Nagle would add a round trip's delay to
/// exchanges that are measuring round trips.
async fn tcp(addr: &str) -> Result<TcpStream, WireError> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// What the suite trusts, and the name it holds a broker to.
#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct TlsTrust {
    config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
    server_name: tokio_rustls::rustls::pki_types::ServerName<'static>,
}

#[cfg(feature = "tls")]
impl std::fmt::Debug for TlsTrust {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsTrust")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "tls")]
impl TlsTrust {
    /// Trust exactly the certificates in `ca_pem`, and require the
    /// broker to present one valid for `server_name`.
    ///
    /// Exactly those: not the system roots as well. A suite pointed at
    /// a throwaway container should fail if that container presents a
    /// publicly trusted certificate, because something has then gone
    /// very strange indeed.
    pub fn from_ca_pem(ca_pem: &[u8], server_name: &str) -> Result<TlsTrust, WireError> {
        use tokio_rustls::rustls::pki_types::pem::PemObject;
        use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
        use tokio_rustls::rustls::{ClientConfig, RootCertStore};

        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(ca_pem) {
            let cert = cert.map_err(|e| WireError::Tls(format!("bad ca pem: {e}")))?;
            roots
                .add(cert)
                .map_err(|e| WireError::Tls(format!("bad ca certificate: {e}")))?;
        }
        if roots.is_empty() {
            return Err(WireError::Tls("ca pem holds no certificates".into()));
        }
        let server_name = ServerName::try_from(server_name.to_owned())
            .map_err(|e| WireError::Tls(format!("bad server name: {e}")))?;
        Ok(TlsTrust {
            config: std::sync::Arc::new(
                ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            ),
            server_name,
        })
    }
}
