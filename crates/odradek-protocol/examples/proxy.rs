//! A pass-through Kafka proxy, in about two hundred lines of this
//! crate and nothing else.
//!
//! ```sh
//! # one broker
//! cargo run -p odradek-protocol --example proxy -- \
//!     --listen 127.0.0.1:19092 --upstream 127.0.0.1:9092
//! # a cluster: one listener per broker
//! cargo run -p odradek-protocol --example proxy -- \
//!     --map 127.0.0.1:19092=127.0.0.1:9092 \
//!     --map 127.0.0.1:19093=127.0.0.1:9093 \
//!     --map 127.0.0.1:19094=127.0.0.1:9094
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
//!    id, the version each Metadata request went out at. That is
//!    [`header::response_header_version`], including the ApiVersions
//!    quirk where the error response is always header v0 whatever the
//!    request asked for.
//!
//!    Only Metadata is remembered, and the reason is worth knowing
//!    before you write your own: an `acks=0` produce is answered with
//!    silence, so a proxy that recorded every request would keep those
//!    entries forever and leak on every fire-and-forget write.
//!
//! 2. **Metadata advertises where to connect next.** A client asks for
//!    metadata, is told the brokers' real addresses, and connects to
//!    them directly — around the proxy. So Metadata responses are
//!    decoded, the endpoints rewritten to point here, and re-encoded.
//!
//!    For a cluster that means one listener per broker and a rewrite
//!    that is a *lookup*, not a substitution: broker 2 must be
//!    advertised as the listener that fronts broker 2, or the client
//!    sends partition 2's writes to whichever broker the proxy happens
//!    to hold. A proxy in front of a cluster that collapsed every
//!    broker onto one address would not be a proxy, it would be a
//!    reassignment.
//!
//!    An endpoint the map does not know is left exactly as the broker
//!    gave it — and said so on stderr, because the client will then
//!    connect around the proxy and everything after that is measuring
//!    the wrong thing.
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
//! Not a product. No TLS, no SASL rewriting, no connection pooling, no
//! attempt to be fast. Its idea of a cluster is a fixed list given on
//! the command line: brokers that join later are not fronted, and it
//! says so rather than pretending. A proxy that wanted to be
//! any of those would still forward the same way; it would just have
//! more to say about connections.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use odradek_protocol::frame;
use odradek_protocol::header;
use odradek_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use odradek_protocol::messages::metadata_response::MetadataResponse;
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};

/// Correlation ids of requests whose answers this proxy has to read,
/// and the `(api key, version)` each was asked at.
///
/// Two apis, because two apis tell a client where to connect: Metadata
/// names the brokers, and FindCoordinator names the one broker that
/// holds a group or a transaction. Everything else is forwarded without
/// being looked at, so there is nothing to remember about it. That is
/// not only economy. A produce with `acks=0` is answered with *silence*: the
/// broker sends nothing at all, by design. A proxy that recorded every
/// request would keep those entries for the life of the connection and
/// leak a little on every fire-and-forget write. Remembering only what
/// has to be remembered makes the leak impossible rather than rare.
type Outstanding = Arc<Mutex<HashMap<i32, (i16, i16)>>>;

/// Where each upstream broker should be advertised instead.
///
/// Keyed by the endpoint the broker puts in its own Metadata —
/// `host:port` as the client would read it — because that is the only
/// thing the proxy can match a broker entry against. Node ids are not
/// known until a Metadata response arrives, and by then the answer is
/// needed.
type Endpoints = Arc<HashMap<String, (String, i32)>>;

/// One listener and the broker behind it.
struct Route {
    listen: String,
    upstream: String,
    /// What clients are told, which is the listen address unless the
    /// proxy is reachable somewhere else.
    advertise: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut listen = "127.0.0.1:19092".to_owned();
    let mut upstream = "127.0.0.1:9092".to_owned();
    let mut advertise: Option<String> = None;
    let mut routes: Vec<Route> = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => listen = args.next().unwrap_or_default(),
            "--upstream" => upstream = args.next().unwrap_or_default(),
            // Where clients should be told to connect. Defaults to the
            // listen address, which is right whenever the proxy is
            // reachable at the same address it binds.
            "--advertise" => advertise = args.next(),
            // One broker, repeatable. `--map <listen>=<upstream>`.
            "--map" => {
                let pair = args.next().unwrap_or_default();
                let (listen, upstream) = pair
                    .split_once('=')
                    .ok_or_else(|| format!("--map wants <listen>=<upstream>, got {pair:?}"))?;
                routes.push(Route {
                    listen: listen.to_owned(),
                    upstream: upstream.to_owned(),
                    advertise: listen.to_owned(),
                });
            }
            other => {
                eprintln!(
                    "usage: proxy [--listen addr] [--upstream addr] [--advertise addr]\n\
                     \x20      proxy --map <listen>=<upstream> [--map ...]"
                );
                return Err(format!("unknown argument {other:?}").into());
            }
        }
    }
    if routes.is_empty() {
        routes.push(Route {
            advertise: advertise.unwrap_or_else(|| listen.clone()),
            listen,
            upstream,
        });
    }

    // Built once and shared by every connection: a client that reaches
    // broker 1 may ask it about broker 3, and must be told about the
    // listener in front of broker 3 rather than about broker 3.
    let mut table = HashMap::new();
    for route in &routes {
        let (host, port) = split_endpoint(&route.advertise)?;
        table.insert(route.upstream.clone(), (host, port));
    }
    let endpoints: Endpoints = Arc::new(table);

    let mut listeners = Vec::new();
    for route in &routes {
        listeners.push((
            TcpListener::bind(&route.listen).await?,
            route.upstream.clone(),
        ));
        eprintln!(
            "proxying {} -> {}, advertising {}",
            route.listen, route.upstream, route.advertise
        );
    }

    // One accept loop per listener; the process ends when any of them
    // does, which for an example is the right amount of ceremony.
    let mut loops = Vec::new();
    for (listener, upstream) in listeners {
        let endpoints = Arc::clone(&endpoints);
        loops.push(tokio::spawn(async move {
            loop {
                let (client, peer) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(e) => {
                        eprintln!("accept on {upstream}: {e}");
                        return;
                    }
                };
                let upstream = upstream.clone();
                let endpoints = Arc::clone(&endpoints);
                tokio::spawn(async move {
                    if let Err(e) = serve(client, &upstream, endpoints).await {
                        eprintln!("{peer}: {e}");
                    }
                });
            }
        }));
    }
    for task in loops {
        task.await?;
    }
    Ok(())
}

/// One client connection, and the upstream connection it gets.
async fn serve(
    client: TcpStream,
    upstream: &str,
    endpoints: Endpoints,
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
        endpoints,
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
        // The first four bytes are the api key and version, which is
        // all the proxy needs to decide whether it cares at all.
        if payload.len() >= 4 {
            let api_key = i16::from_be_bytes([payload[0], payload[1]]);
            let api_version = i16::from_be_bytes([payload[2], payload[3]]);
            if api_key == MetadataResponse::API_KEY || api_key == FindCoordinatorResponse::API_KEY {
                if let Some(hv) = header::request_header_version(api_key, api_version) {
                    let mut head = payload.clone();
                    if let Ok(header) = RequestHeader::decode(&mut head, hv) {
                        outstanding
                            .lock()
                            .unwrap()
                            .insert(header.correlation_id, (api_key, api_version));
                    }
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
    endpoints: Endpoints,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let Some(payload) = read_frame(&mut from).await? else {
            return Ok(());
        };
        let correlation = frame::peek_correlation_id(&payload)?;
        // Present only for Metadata; everything else falls through
        // without being parsed.
        let asked_at = outstanding.lock().unwrap().remove(&correlation);

        let forwarded = match asked_at {
            Some((api_key, api_version)) => {
                let rewritten = if api_key == MetadataResponse::API_KEY {
                    rewrite_metadata(&payload, api_version, &endpoints)
                } else {
                    rewrite_find_coordinator(&payload, api_version, &endpoints)
                };
                match rewritten {
                    Ok(rewritten) => rewritten,
                    // A shape this crate cannot read is still the
                    // broker's answer; forwarding it unchanged is wrong
                    // for routing but better than dropping the client's
                    // connection over it.
                    Err(e) => {
                        eprintln!("api {api_key} v{api_version}: {e}");
                        payload
                    }
                }
            }
            None => payload,
        };
        write_frame(&mut to, &forwarded).await?;
    }
}

/// Decode a Metadata response, point each broker at the listener that
/// fronts it, and re-encode it.
///
/// The round trip is the load-bearing part. Everything the crate does
/// not model — tagged fields from a newer broker, in the response and
/// in every broker, topic and partition inside it — has to come out
/// again, or the client silently loses whatever the upstream was
/// telling it.
fn rewrite_metadata(
    payload: &Bytes,
    api_version: i16,
    endpoints: &Endpoints,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    let header_version = header::response_header_version(MetadataResponse::API_KEY, api_version)
        .ok_or("no response header version for Metadata")?;
    let mut body = payload.clone();
    let header = ResponseHeader::decode(&mut body, header_version)?;
    let mut response = MetadataResponse::decode(&mut body, api_version)?;
    if !body.is_empty() {
        return Err(format!("metadata response left {} undecoded byte(s)", body.len()).into());
    }

    // A lookup rather than a substitution. With one broker the two are
    // the same thing, which is why this went unnoticed until there were
    // three: pointing every broker at one listener tells the client
    // that whichever broker the proxy happens to hold leads every
    // partition, and the first write to a partition it does not lead is
    // answered NOT_LEADER_OR_FOLLOWER for as long as the client cares
    // to retry.
    // Anything not in the map is left as the broker gave it, so the
    // client can still reach it — around the proxy, which is worth
    // saying out loud: everything measured through here afterwards is
    // measuring the broker directly.
    let mut unmapped = Vec::new();
    for broker in &mut response.brokers {
        map_endpoint(&mut broker.host, &mut broker.port, endpoints, &mut unmapped);
    }
    warn_unmapped(&unmapped);

    let mut out = BytesMut::new();
    header.encode(&mut out, header_version)?;
    response.encode(&mut out, api_version)?;
    Ok(out.freeze())
}

/// The same, for the other api that says where to connect.
///
/// FindCoordinator is Metadata's quieter twin: it names the one broker
/// that holds a group or a transaction, and a client goes there next. A
/// proxy that rewrites Metadata and not this one is transparent for
/// produce and fetch and invisible for everything a consumer group
/// does — the client is handed the broker's own address and connects
/// around it.
///
/// That was true here until a cluster made it visible. With a single
/// upstream the coordinator's address *is* the upstream's, so the
/// client went straight to the broker and every answer was still
/// correct; nothing was wrong except that the proxy was not in the
/// path. Three brokers turned it into a wrong answer instead of an
/// invisible one, because the coordinator was then a broker the client
/// had not been told about.
///
/// The address moves from scalar fields to a `coordinators` list at
/// v4; both are rewritten, since the schema models both and a proxy
/// does not get to pick which versions its clients speak.
fn rewrite_find_coordinator(
    payload: &Bytes,
    api_version: i16,
    endpoints: &Endpoints,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    let header_version =
        header::response_header_version(FindCoordinatorResponse::API_KEY, api_version)
            .ok_or("no response header version for FindCoordinator")?;
    let mut body = payload.clone();
    let header = ResponseHeader::decode(&mut body, header_version)?;
    let mut response = FindCoordinatorResponse::decode(&mut body, api_version)?;
    if !body.is_empty() {
        return Err(format!(
            "find coordinator response left {} undecoded byte(s)",
            body.len()
        )
        .into());
    }

    let mut unmapped = Vec::new();
    // The scalar pair (v0-v3). An error response leaves them empty,
    // which matches nothing and is left alone.
    map_endpoint(
        &mut response.host,
        &mut response.port,
        endpoints,
        &mut unmapped,
    );
    for coordinator in &mut response.coordinators {
        map_endpoint(
            &mut coordinator.host,
            &mut coordinator.port,
            endpoints,
            &mut unmapped,
        );
    }
    warn_unmapped(&unmapped);

    let mut out = BytesMut::new();
    header.encode(&mut out, header_version)?;
    response.encode(&mut out, api_version)?;
    Ok(out.freeze())
}

/// Point one `host`/`port` pair at the listener fronting it, or record
/// that nothing does.
///
/// An empty host is what an error response carries; there is nothing to
/// map and nothing to complain about.
fn map_endpoint(
    host: &mut String,
    port: &mut i32,
    endpoints: &Endpoints,
    unmapped: &mut Vec<String>,
) {
    if host.is_empty() {
        return;
    }
    let endpoint = format!("{host}:{port}");
    match endpoints.get(&endpoint) {
        Some((mapped_host, mapped_port)) => {
            *host = mapped_host.clone();
            *port = *mapped_port;
        }
        None => unmapped.push(endpoint),
    }
}

/// Say, once per response, which endpoints the client will reach around
/// this proxy.
fn warn_unmapped(unmapped: &[String]) {
    if unmapped.is_empty() {
        return;
    }
    eprintln!(
        "not fronting {}; clients will connect to {} around this proxy",
        unmapped.join(", "),
        if unmapped.len() == 1 { "it" } else { "them" }
    );
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
