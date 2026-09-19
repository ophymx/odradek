//! A pass-through Kafka proxy, in about two hundred lines of this
//! crate and nothing else.
//!
//! ```sh
//! cargo run -p odradek-protocol --example proxy -- \
//!     --listen 127.0.0.1:19092 --upstream 127.0.0.1:9092
//! ```
//!
//! This exists to be a *witness*. The crate's design rests on a claim
//! it repeats everywhere — unknown tagged fields round-trip raw, record
//! batches re-encode byte-identically, decoding costs O(fields) rather
//! than O(payload) — and the justification given for all of it is "a
//! proxy needs this". Nothing in the workspace proxied. An argument
//! with no witness is a nice argument.
//!
//! # What a proxy actually has to do
//!
//! Almost nothing, and that is the point. It forwards bytes it never
//! looks at. Two things force it to look:
//!
//! 1. **Response header versions are not in the response.** A Kafka
//!    response frame is a correlation id and then a body whose header
//!    shape depends on the *request's* api key and version — which the
//!    response does not carry. So the proxy remembers, per correlation
//!    id, what went out. That is [`header::response_header_version`],
//!    including the ApiVersions quirk where the error response is
//!    always header v0 whatever the request asked for.
//!
//! 2. **Metadata advertises where to connect next.** A client asks for
//!    metadata, is told the brokers' real addresses, and connects to
//!    them directly — around the proxy. So Metadata responses are
//!    decoded, the endpoints rewritten to point here, and re-encoded.
//!
//! Everything else — Produce, Fetch, the group protocols, SASL,
//! transactions — is forwarded without being parsed at all.
//!
//! # Where the proxy guarantee earns its keep
//!
//! The Metadata rewrite is the interesting case, because a decode and
//! re-encode is exactly where a lossy codec shows up. The response may
//! carry tagged fields this crate has never heard of, from a broker
//! newer than it, and they have to survive the round trip or the client
//! loses them. `unknown_tagged_fields` is what makes that work, and
//! this proxy is where it stops being a unit test and starts being load
//! bearing: run a real client through here to a real broker, and every
//! byte the client sees of every message but Metadata is the broker's
//! own.
//!
//! # What this is not
//!
//! Not a product. One upstream broker, no TLS, no SASL rewriting, no
//! connection pooling, no attempt to be fast. A proxy that wanted to be
//! any of those would still forward the same way; it would just have
//! more to say about connections.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use odradek_protocol::frame;
use odradek_protocol::header;
use odradek_protocol::messages::metadata_response::MetadataResponse;
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};

/// What a request was, so its response can be read.
#[derive(Clone, Copy)]
struct Pending {
    api_key: i16,
    api_version: i16,
}

/// Correlation id → the request that is still outstanding for it.
///
/// A client may have many in flight; brokers answer a connection in
/// order, but nothing here depends on that.
type Outstanding = Arc<Mutex<HashMap<i32, Pending>>>;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut listen = "127.0.0.1:19092".to_owned();
    let mut upstream = "127.0.0.1:9092".to_owned();
    let mut advertise: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => listen = args.next().unwrap_or_default(),
            "--upstream" => upstream = args.next().unwrap_or_default(),
            // Where clients should be told to connect. Defaults to the
            // listen address, which is right whenever the proxy is
            // reachable at the same address it binds.
            "--advertise" => advertise = args.next(),
            other => {
                eprintln!("usage: proxy [--listen addr] [--upstream addr] [--advertise addr]");
                return Err(format!("unknown argument {other:?}").into());
            }
        }
    }
    let advertise = advertise.unwrap_or_else(|| listen.clone());
    let (host, port) = split_endpoint(&advertise)?;

    let listener = TcpListener::bind(&listen).await?;
    eprintln!("proxying {listen} -> {upstream}, advertising {advertise}");

    loop {
        let (client, peer) = listener.accept().await?;
        let upstream = upstream.clone();
        let host = host.clone();
        tokio::spawn(async move {
            if let Err(e) = serve(client, &upstream, host, port).await {
                eprintln!("{peer}: {e}");
            }
        });
    }
}

/// One client connection, and the upstream connection it gets.
async fn serve(
    client: TcpStream,
    upstream: &str,
    host: String,
    port: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    client.set_nodelay(true)?;
    let broker = TcpStream::connect(upstream).await?;
    broker.set_nodelay(true)?;

    let (client_read, client_write) = tokio::io::split(client);
    let (broker_read, broker_write) = tokio::io::split(broker);
    let outstanding: Outstanding = Arc::new(Mutex::new(HashMap::new()));

    // Both directions at once: a client that pipelines has requests on
    // the wire while responses are still coming back, and a proxy that
    // took turns would serialize what the broker is happy to overlap.
    let up = tokio::spawn(pump_requests(
        client_read,
        broker_write,
        Arc::clone(&outstanding),
    ));
    let down = tokio::spawn(pump_responses(
        broker_read,
        client_write,
        outstanding,
        host,
        port,
    ));
    // Whichever side ends first ends the connection; dropping the other
    // task's halves closes it.
    tokio::select! {
        result = up => result??,
        result = down => result??,
    }
    Ok(())
}

/// Client → broker. Remembers what each correlation id asked for, and
/// forwards the frame exactly as it arrived.
async fn pump_requests(
    mut from: ReadHalf<TcpStream>,
    mut to: WriteHalf<TcpStream>,
    outstanding: Outstanding,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let Some(payload) = read_frame(&mut from).await? else {
            return Ok(());
        };
        // The first four bytes are api key and version; that is all the
        // proxy needs from a request it is not rewriting.
        if payload.len() >= 4 {
            let api_key = i16::from_be_bytes([payload[0], payload[1]]);
            let api_version = i16::from_be_bytes([payload[2], payload[3]]);
            if let Some(hv) = header::request_header_version(api_key, api_version) {
                let mut head = payload.clone();
                if let Ok(header) = RequestHeader::decode(&mut head, hv) {
                    outstanding.lock().unwrap().insert(
                        header.correlation_id,
                        Pending {
                            api_key,
                            api_version,
                        },
                    );
                }
            }
        }
        write_frame(&mut to, &payload).await?;
    }
}

/// Broker → client. Forwards everything untouched except Metadata,
/// whose endpoints have to point back here.
async fn pump_responses(
    mut from: ReadHalf<TcpStream>,
    mut to: WriteHalf<TcpStream>,
    outstanding: Outstanding,
    host: String,
    port: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let Some(payload) = read_frame(&mut from).await? else {
            return Ok(());
        };
        let correlation = frame::peek_correlation_id(&payload)?;
        let pending = outstanding.lock().unwrap().remove(&correlation);

        let forwarded = match pending {
            Some(pending) if pending.api_key == MetadataResponse::API_KEY => {
                match rewrite_metadata(&payload, pending.api_version, &host, port) {
                    Ok(rewritten) => rewritten,
                    // A Metadata shape this crate cannot read is still
                    // the broker's answer; forwarding it unchanged is
                    // wrong for routing but better than dropping the
                    // client's connection over it.
                    Err(e) => {
                        eprintln!("metadata v{}: {e}", pending.api_version);
                        payload
                    }
                }
            }
            _ => payload,
        };
        write_frame(&mut to, &forwarded).await?;
    }
}

/// Decode a Metadata response, point every broker at this proxy, and
/// re-encode it.
///
/// The round trip is the load-bearing part. Everything the crate does
/// not model — tagged fields from a newer broker, in the response and
/// in every broker, topic and partition inside it — has to come out
/// again, or the client silently loses whatever the upstream was
/// telling it.
fn rewrite_metadata(
    payload: &Bytes,
    api_version: i16,
    host: &str,
    port: i32,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    let header_version = header::response_header_version(MetadataResponse::API_KEY, api_version)
        .ok_or("no response header version for Metadata")?;
    let mut body = payload.clone();
    let header = ResponseHeader::decode(&mut body, header_version)?;
    let mut response = MetadataResponse::decode(&mut body, api_version)?;
    if !body.is_empty() {
        return Err(format!("metadata response left {} undecoded byte(s)", body.len()).into());
    }

    for broker in &mut response.brokers {
        broker.host = host.to_owned();
        broker.port = port;
    }

    let mut out = BytesMut::new();
    header.encode(&mut out, header_version)?;
    response.encode(&mut out, api_version)?;
    Ok(out.freeze())
}

/// One length-prefixed frame, or `None` at a clean end of stream.
async fn read_frame(
    from: &mut ReadHalf<TcpStream>,
) -> Result<Option<Bytes>, Box<dyn std::error::Error + Send + Sync>> {
    let mut prefix = [0u8; 4];
    match from.read_exact(&mut prefix).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = frame::check_len(prefix, frame::DEFAULT_MAX_FRAME)?;
    let mut payload = vec![0u8; len];
    from.read_exact(&mut payload).await?;
    Ok(Some(Bytes::from(payload)))
}

async fn write_frame(
    to: &mut WriteHalf<TcpStream>,
    payload: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut out = BytesMut::with_capacity(payload.len() + 4);
    frame::frame(&mut out, |buf| {
        buf.extend_from_slice(payload);
        Ok(())
    })?;
    to.write_all(&out).await?;
    Ok(())
}

/// `host:port`, with IPv6 literals bracketed.
fn split_endpoint(addr: &str) -> Result<(String, i32), Box<dyn std::error::Error>> {
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| format!("{addr:?} is not host:port"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    Ok((host.to_owned(), port.parse()?))
}
