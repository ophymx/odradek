//! A single broker connection.
//!
//! Kafka connections are length-prefixed frames carrying a versioned header
//! and message body. Responses may complete out of order relative to other
//! requests; they are matched back to callers by correlation id, so requests
//! pipeline naturally: many can be in flight at once.
//!
//! One reader task per connection owns the read half of the socket and the
//! in-flight table; writers serialize on a mutex around the write half.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use odradek_protocol::frame;
use odradek_protocol::header::{request_header_version, response_header_version};
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
#[cfg(feature = "tls")]
use tokio_rustls::TlsConnector;
#[cfg(feature = "tls")]
use tokio_rustls::rustls::pki_types::ServerName;

use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio::task::JoinHandle;

use crate::ClientConfig;
use crate::error::ClientError;
#[cfg(feature = "tls")]
use crate::tls::Tls;

/// The wire under a connection: plaintext TCP or TLS over it.
enum Transport {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl Transport {
    /// Whether this wire encrypts what is written to it. Callers that
    /// are about to send a secret need to know.
    fn is_encrypted(&self) -> bool {
        match self {
            Transport::Plain(_) => false,
            #[cfg(feature = "tls")]
            Transport::Tls(_) => true,
        }
    }
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

type ReadHalf = tokio::io::ReadHalf<Transport>;
type WriteHalf = tokio::io::WriteHalf<Transport>;

struct Pending {
    response_header_version: i16,
    reply: oneshot::Sender<Result<Bytes, ClientError>>,
}

struct Shared {
    in_flight: StdMutex<HashMap<i32, Pending>>,
    closed: AtomicBool,
}

impl Shared {
    fn fail_all(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let pending = std::mem::take(&mut *self.in_flight.lock().unwrap());
        for (_, p) in pending {
            let _ = p.reply.send(Err(ClientError::ConnectionClosed));
        }
    }
}

struct Inner {
    shared: Arc<Shared>,
    writer: AsyncMutex<WriteHalf>,
    next_correlation: AtomicI32,
    client_id: String,
    request_timeout: std::time::Duration,
    encrypted: bool,
    reader: JoinHandle<()>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.reader.abort();
        self.shared.fail_all();
    }
}

/// A connection to one broker. Cheap to clone; all clones share the socket.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish_non_exhaustive()
    }
}

impl Connection {
    /// Open a connection to `addr` (`host:port`), wrapped in TLS when
    /// the config says so. No Kafka handshake is performed here; call
    /// [`Connection::negotiate`] to exchange ApiVersions.
    pub async fn connect(addr: &str, config: &ClientConfig) -> Result<Connection, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        #[cfg(feature = "tls")]
        let transport = match &config.tls {
            Tls::None => Transport::Plain(stream),
            Tls::Rustls(tls_config) => {
                let host = host_of(addr);
                let server_name = ServerName::try_from(host.to_owned())
                    .map_err(|e| ClientError::Tls(format!("bad server name {host:?}: {e}")))?;
                let connector = TlsConnector::from(Arc::clone(tls_config));
                let tls = connector
                    .connect(server_name, stream)
                    .await
                    .map_err(|e| ClientError::Tls(format!("handshake with {addr}: {e}")))?;
                Transport::Tls(Box::new(tls))
            }
        };
        #[cfg(not(feature = "tls"))]
        let transport = Transport::Plain(stream);
        let encrypted = transport.is_encrypted();
        let (read_half, write_half) = tokio::io::split(transport);

        let shared = Arc::new(Shared {
            in_flight: StdMutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        });
        let reader = tokio::spawn(reader_loop(read_half, Arc::clone(&shared)));

        Ok(Connection {
            inner: Arc::new(Inner {
                shared,
                writer: AsyncMutex::new(write_half),
                next_correlation: AtomicI32::new(1),
                client_id: config.client_id.clone(),
                request_timeout: config.request_timeout,
                encrypted,
                reader,
            }),
        })
    }

    /// Whether this connection is encrypted (TLS) rather than plaintext
    /// TCP.
    ///
    /// Anything about to put a secret on the wire — SASL PLAIN, say —
    /// should consult this first; see the `sasl` module. It reflects only
    /// what *this* client negotiated: encryption terminated elsewhere
    /// (a sidecar proxy, a tunnel) is invisible here and reads as
    /// `false`.
    pub fn is_encrypted(&self) -> bool {
        self.inner.encrypted
    }

    /// Send a request body (already encoded at `api_version`) and await the
    /// matching response body. The request/response headers and framing are
    /// handled here.
    ///
    /// The wait is bounded by [`crate::ClientConfig::request_timeout`]. A
    /// request that blows it fails with [`ClientError::Timeout`] and closes
    /// the connection: the broker answers a connection's requests strictly
    /// in order, so everything pipelined behind a hung request is hung too.
    pub async fn request(
        &self,
        api_key: i16,
        api_version: i16,
        body: &[u8],
    ) -> Result<Bytes, ClientError> {
        match tokio::time::timeout(
            self.inner.request_timeout,
            self.request_unbounded(api_key, api_version, body),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                // The pipeline behind the hung request is dead with it;
                // fail everything so callers redial rather than queue.
                self.inner.shared.fail_all();
                Err(ClientError::Timeout("response"))
            }
        }
    }

    async fn request_unbounded(
        &self,
        api_key: i16,
        api_version: i16,
        body: &[u8],
    ) -> Result<Bytes, ClientError> {
        let inner = &self.inner;
        if inner.shared.closed.load(Ordering::SeqCst) {
            return Err(ClientError::ConnectionClosed);
        }
        let header_version = request_header_version(api_key, api_version)
            .ok_or(ClientError::UnsupportedApi(api_key))?;
        let resp_header_version = response_header_version(api_key, api_version)
            .ok_or(ClientError::UnsupportedApi(api_key))?;

        let correlation_id = inner.next_correlation.fetch_add(1, Ordering::Relaxed);
        let mut header = RequestHeader::default();
        header.request_api_key = api_key;
        header.request_api_version = api_version;
        header.correlation_id = correlation_id;
        header.client_id = Some(inner.client_id.clone());
        let mut framed = BytesMut::new();
        frame::frame(&mut framed, |buf| {
            header.encode(buf, header_version)?;
            buf.extend_from_slice(body);
            Ok(())
        })?;

        let (reply_tx, reply_rx) = oneshot::channel();
        inner.shared.in_flight.lock().unwrap().insert(
            correlation_id,
            Pending {
                response_header_version: resp_header_version,
                reply: reply_tx,
            },
        );

        // Register-then-write: the response cannot beat the table entry.
        {
            let mut writer = inner.writer.lock().await;
            if let Err(e) = writer.write_all(&framed).await {
                inner
                    .shared
                    .in_flight
                    .lock()
                    .unwrap()
                    .remove(&correlation_id);
                return Err(e.into());
            }
        }

        reply_rx.await.map_err(|_| ClientError::ConnectionClosed)?
    }
}

async fn reader_loop(mut read_half: ReadHalf, shared: Arc<Shared>) {
    loop {
        match read_response(&mut read_half, &shared).await {
            Ok(()) => {}
            Err(_) => {
                shared.fail_all();
                return;
            }
        }
    }
}

/// Decode a response body and require that it accounted for every byte
/// of the frame.
///
/// A message that decodes while leaving a remainder is not a message
/// this client understood. The usual cause is a version disagreement —
/// decoding a v9 response against the v8 schema stops early and hands
/// back a struct whose fields are silently the wrong ones — and the
/// symptom without this check is not an error but wrong data: offsets
/// and error codes read out of fields the peer meant for something
/// else. Kafka encodes a response at exactly the version the request
/// named, so a remainder is always a disagreement, never slack; unknown
/// *additions* are what the tagged-field section exists to carry, and
/// that is consumed by the decode.
///
/// The check is on the client and not in
/// [`Message::decode`](odradek_protocol::message::Message::decode),
/// which is correct to leave the buffer alone: a decoder can legitimately
/// be handed a buffer holding more than one thing — the response header
/// and the body that follows it, here.
pub(crate) fn decode_body<M: odradek_protocol::message::Message>(
    body: &mut Bytes,
    version: i16,
) -> Result<M, ClientError> {
    let message = M::decode(body, version)?;
    if !body.is_empty() {
        return Err(ClientError::ProtocolViolation(format!(
            "api key {} v{version} response leaves {} undecoded byte(s)",
            M::API_KEY,
            body.len()
        )));
    }
    Ok(message)
}

/// The host part of a `host:port` endpoint, for TLS server-name
/// verification.
///
/// An IPv6 literal is bracketed and full of colons — `[::1]:9092` — so
/// splitting on the last colon yields `[::1]`, which is not a name
/// rustls will accept. That fails closed (a handshake error, never an
/// unverified connection), but it fails on every IPv6 broker a cluster
/// advertises, which is not a thing to leave to a bug report. The
/// brackets are endpoint syntax, not part of the address, so they come
/// off before rustls decides whether this is a DNS name or an IP.
#[cfg(feature = "tls")]
fn host_of(addr: &str) -> &str {
    match addr.strip_prefix('[') {
        Some(rest) => rest.split_once(']').map_or(addr, |(host, _)| host),
        None => addr.rsplit_once(':').map_or(addr, |(host, _)| host),
    }
}

/// How much of a frame body to allocate at a time.
///
/// The peer's declared length is a claim, not an arrival. Growing in
/// steps means a claim only costs what the peer actually backs with
/// bytes, and the step is large enough that an honest 1 MiB fetch
/// response costs a handful of amortized reallocations.
const FRAME_CHUNK: usize = 64 * 1024;

/// Read exactly `len` bytes of frame body, allocating as they arrive.
///
/// [`frame::check_len`] returns a bound to stream against, and says in
/// its own documentation that `vec![0u8; len]` is the thing not to do:
/// four attacker-chosen bytes would otherwise reserve
/// [`frame::DEFAULT_MAX_FRAME`] (64 MiB) per connection before the peer
/// sends any payload at all. A hostile broker needs no more than that
/// header, and a cluster's metadata response is what tells this client
/// how many brokers to connect to — so the multiplier is also the
/// peer's to choose.
async fn read_frame_body<R: AsyncRead + Unpin>(
    read_half: &mut R,
    len: usize,
) -> Result<Bytes, ClientError> {
    let mut frame = BytesMut::new();
    while frame.len() < len {
        let start = frame.len();
        let chunk = (len - start).min(FRAME_CHUNK);
        frame.resize(start + chunk, 0);
        // read_exact, not read_buf: reading past `chunk` would consume
        // the head of the next frame.
        read_half.read_exact(&mut frame[start..]).await?;
    }
    Ok(frame.freeze())
}

async fn read_response(read_half: &mut ReadHalf, shared: &Shared) -> Result<(), ClientError> {
    let mut len_bytes = [0u8; 4];
    read_half.read_exact(&mut len_bytes).await?;
    // Refuse hostile length prefixes (negative, or above 64 MiB) as a
    // protocol violation rather than attempting the allocation.
    let len = frame::check_len(len_bytes, frame::DEFAULT_MAX_FRAME)
        .map_err(|e| ClientError::ProtocolViolation(format!("response frame: {e}")))?;
    let mut frame = read_frame_body(read_half, len).await?;

    // Correlation id leads every response header version; peek it, then
    // decode the header at the version recorded for that request.
    let correlation_id = frame::peek_correlation_id(&frame)
        .map_err(|e| ClientError::ProtocolViolation(format!("short response header: {e}")))?;
    let pending = shared.in_flight.lock().unwrap().remove(&correlation_id);
    let Some(pending) = pending else {
        return Err(ClientError::ProtocolViolation(format!(
            "response with unknown correlation id {correlation_id}"
        )));
    };

    let result = ResponseHeader::decode(&mut frame, pending.response_header_version)
        .map(|_| frame)
        .map_err(ClientError::from);
    let _ = pending.reply.send(result);
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "tls")]
    #[test]
    fn host_of_handles_both_address_families() {
        use super::host_of;
        assert_eq!(host_of("broker.example.com:9093"), "broker.example.com");
        assert_eq!(host_of("10.0.0.7:9093"), "10.0.0.7");
        // The brackets are endpoint syntax; rustls wants the address.
        assert_eq!(host_of("[::1]:9093"), "::1");
        assert_eq!(host_of("[2001:db8::a]:9093"), "2001:db8::a");
        // No port at all: the whole string is the host.
        assert_eq!(host_of("broker.example.com"), "broker.example.com");
    }

    #[tokio::test]
    async fn frame_body_allocates_only_what_arrives() {
        use super::{FRAME_CHUNK, read_frame_body};

        // A peer that declares a huge frame and then stalls must not
        // get that size allocated up front. Nothing is written to the
        // pipe, so the read blocks; the test passes by *not* having
        // reserved 64 MiB to reach that point.
        let (client, _server) = tokio::io::duplex(64);
        let (mut read_half, _write_half) = tokio::io::split(client);
        let huge = 64 << 20;
        let read = read_frame_body(&mut read_half, huge);
        tokio::pin!(read);
        let stalled = tokio::time::timeout(std::time::Duration::from_millis(50), &mut read).await;
        assert!(stalled.is_err(), "should still be waiting for the body");

        // And an honest frame still arrives whole, across chunks.
        let (mut client, mut server) = tokio::io::duplex(1 << 20);
        let len = FRAME_CHUNK + 1234;
        let writer = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            server.write_all(&vec![7u8; len]).await.unwrap();
        });
        let body = read_frame_body(&mut client, len).await.unwrap();
        writer.await.unwrap();
        assert_eq!(body.len(), len);
        assert!(body.iter().all(|b| *b == 7));
    }
}
