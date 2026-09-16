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

use bytes::{BufMut, Bytes, BytesMut};
use odradek_protocol::header::{request_header_version, response_header_version};
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use odradek_protocol::wire;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio::task::JoinHandle;

use crate::ClientConfig;
use crate::error::ClientError;

/// Refuse frames larger than this (64 MiB) as a protocol violation rather
/// than attempting the allocation.
const MAX_FRAME_SIZE: i32 = 64 * 1024 * 1024;

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
    writer: AsyncMutex<OwnedWriteHalf>,
    next_correlation: AtomicI32,
    client_id: String,
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
    /// Open a TCP connection to `addr` (`host:port`). No handshake is
    /// performed here; call [`Connection::negotiate`] to exchange
    /// ApiVersions.
    pub async fn connect(addr: &str, config: &ClientConfig) -> Result<Connection, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (read_half, write_half) = stream.into_split();

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
                reader,
            }),
        })
    }

    /// Send a request body (already encoded at `api_version`) and await the
    /// matching response body. The request/response headers and framing are
    /// handled here.
    pub async fn request(
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
        let header = RequestHeader {
            request_api_key: api_key,
            request_api_version: api_version,
            correlation_id,
            client_id: Some(inner.client_id.clone()),
            unknown_tagged_fields: Vec::new(),
        };
        let mut frame = BytesMut::new();
        frame.put_i32(0); // frame length, patched below
        header.encode(&mut frame, header_version)?;
        frame.extend_from_slice(body);
        let frame_len = i32::try_from(frame.len() - 4).expect("frame length fits i32");
        frame[..4].copy_from_slice(&frame_len.to_be_bytes());

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
            if let Err(e) = writer.write_all(&frame).await {
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

async fn reader_loop(mut read_half: OwnedReadHalf, shared: Arc<Shared>) {
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

async fn read_response(read_half: &mut OwnedReadHalf, shared: &Shared) -> Result<(), ClientError> {
    let mut len_bytes = [0u8; 4];
    read_half.read_exact(&mut len_bytes).await?;
    let len = i32::from_be_bytes(len_bytes);
    if !(0..=MAX_FRAME_SIZE).contains(&len) {
        return Err(ClientError::ProtocolViolation(format!(
            "response frame length {len} out of range"
        )));
    }
    let mut frame = vec![0u8; len as usize];
    read_half.read_exact(&mut frame).await?;
    let mut frame = Bytes::from(frame);

    // Correlation id leads every response header version; peek it, then
    // decode the header at the version recorded for that request.
    let correlation_id = wire::get_i32(&mut frame.clone())
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
