//! End-to-end WebSocket tests: a real listener, a hand-rolled WS
//! client (HTTP upgrade + frame reader — server frames are unmasked,
//! so parsing is small), and the in-memory log behind the hub.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use odradek_web_core::memory::{MemoryFactory, MemoryLog};
use odradek_web_core::{PumpConfig, RecordSource, SourceBatch, SourceError};
use odradek_web_ws::{SourceFactory, WsState, router};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TOPIC: &str = "bridge";

async fn serve_state<F: SourceFactory>(state: Arc<WsState<F>>) -> SocketAddr {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

/// A server over the whole in-memory log. The gate is an argument to
/// `WsState::new`, so even a test has to say what it serves; these
/// tests serve everything the memory factory knows, and (by default)
/// deny cross-origin handshakes.
async fn serve(log: MemoryLog) -> SocketAddr {
    serve_state(WsState::new(
        MemoryFactory::new(log),
        PumpConfig::default(),
        |_| true,
    ))
    .await
}

/// A source whose partition metadata resolves but whose reads fail
/// permanently — the shape of "auth revoked under a live stream".
#[derive(Debug, Clone)]
struct RevokedFactory;

struct RevokedSource;

impl RecordSource for RevokedSource {
    async fn fetch(
        &mut self,
        _topic: &str,
        _partition: i32,
        _offset: i64,
    ) -> Result<SourceBatch, SourceError> {
        Err(SourceError::auth("TOPIC_AUTHORIZATION_FAILED"))
    }

    async fn earliest_offset(&mut self, _topic: &str, _partition: i32) -> Result<i64, SourceError> {
        Err(SourceError::auth("TOPIC_AUTHORIZATION_FAILED"))
    }

    async fn latest_offset(&mut self, _topic: &str, _partition: i32) -> Result<i64, SourceError> {
        Err(SourceError::auth("TOPIC_AUTHORIZATION_FAILED"))
    }
}

impl SourceFactory for RevokedFactory {
    type Source = RevokedSource;

    async fn create(&self, _topic: &str, _partition: i32) -> Result<RevokedSource, SourceError> {
        Ok(RevokedSource)
    }

    async fn partitions(&self, _topic: &str) -> Result<Vec<i32>, SourceError> {
        Ok(vec![0])
    }
}

struct WsClient {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl WsClient {
    /// Open the connection and send the upgrade request; returns the
    /// HTTP status line without asserting on it.
    async fn connect(addr: SocketAddr, path: &str) -> (WsClient, String) {
        let (client, head) = WsClient::handshake(addr, path, &[]).await;
        (client, head.lines().next().unwrap().to_owned())
    }

    /// Like [`WsClient::connect`], with extra request headers (an
    /// `Origin`, say); returns the whole response head.
    async fn handshake(
        addr: SocketAddr,
        path: &str,
        extra_headers: &[(&str, &str)],
    ) -> (WsClient, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut request = format!(
            "GET {path} HTTP/1.1\r\n\
             Host: localhost\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==\r\n\
             Sec-WebSocket-Version: 13\r\n"
        );
        for (name, value) in extra_headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();

        let mut client = WsClient {
            stream,
            buffer: Vec::new(),
        };
        let head = loop {
            if let Some(pos) = find(&client.buffer, b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&client.buffer[..pos]).into_owned();
                client.buffer.drain(..pos + 4);
                break head;
            }
            client.fill().await;
        };
        (client, head)
    }

    async fn fill(&mut self) {
        let mut chunk = [0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(5), self.stream.read(&mut chunk))
            .await
            .expect("timed out reading from the websocket")
            .unwrap();
        assert!(n > 0, "server closed the connection");
        self.buffer.extend_from_slice(&chunk[..n]);
    }

    /// The next text frame as JSON (skips pings; panics on close).
    async fn next_json(&mut self) -> serde_json::Value {
        loop {
            if let Some((opcode, payload, consumed)) = parse_frame(&self.buffer) {
                self.buffer.drain(..consumed);
                match opcode {
                    0x1 => return serde_json::from_slice(&payload).unwrap(),
                    0x8 => panic!("server closed the websocket"),
                    _ => continue, // ping/pong or continuation: skip
                }
            }
            self.fill().await;
        }
    }

    /// The next close frame as (code, reason); skips data frames.
    async fn next_close(&mut self) -> (u16, String) {
        loop {
            if let Some((opcode, payload, consumed)) = parse_frame(&self.buffer) {
                self.buffer.drain(..consumed);
                if opcode == 0x8 {
                    assert!(payload.len() >= 2, "close frame without a code");
                    let code = u16::from_be_bytes([payload[0], payload[1]]);
                    let reason = String::from_utf8(payload[2..].to_vec()).unwrap();
                    return (code, reason);
                }
                continue;
            }
            self.fill().await;
        }
    }
}

/// Parse one complete unmasked server frame: (opcode, payload, bytes
/// consumed), or None if the buffer holds only part of one.
fn parse_frame(buffer: &[u8]) -> Option<(u8, Vec<u8>, usize)> {
    if buffer.len() < 2 {
        return None;
    }
    let opcode = buffer[0] & 0x0f;
    assert_eq!(buffer[1] & 0x80, 0, "server frames must be unmasked");
    let (len, header) = match buffer[1] & 0x7f {
        126 => {
            if buffer.len() < 4 {
                return None;
            }
            (usize::from(u16::from_be_bytes([buffer[2], buffer[3]])), 4)
        }
        127 => {
            if buffer.len() < 10 {
                return None;
            }
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&buffer[2..10]);
            (usize::try_from(u64::from_be_bytes(raw)).unwrap(), 10)
        }
        short => (usize::from(short), 2),
    };
    if buffer.len() < header + len {
        return None;
    }
    Some((opcode, buffer[header..header + len].to_vec(), header + len))
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn value_of(json: &serde_json::Value) -> String {
    json["value"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn replays_then_streams_live() {
    let log = MemoryLog::new();
    for i in 0..3 {
        log.append(TOPIC, 0, None, format!("old-{i}").as_bytes(), Vec::new());
    }
    let addr = serve(log.clone()).await;

    let (mut client, status) = WsClient::connect(
        addr,
        &format!("/topics/{TOPIC}/partitions/0/ws?from=earliest"),
    )
    .await;
    assert!(status.contains("101"), "{status}");

    for i in 0..3 {
        let json = client.next_json().await;
        assert_eq!(json["offset"], i);
        assert_eq!(value_of(&json), format!("old-{i}"));
        assert_eq!(json["topic"], TOPIC);
    }

    log.append(TOPIC, 0, None, b"fresh", Vec::new());
    let json = client.next_json().await;
    assert_eq!(json["offset"], 3);
    assert_eq!(value_of(&json), "fresh");
}

#[tokio::test]
async fn offset_resume_is_exact() {
    let log = MemoryLog::new();
    for i in 0..5 {
        log.append(TOPIC, 0, None, format!("v{i}").as_bytes(), Vec::new());
    }
    let addr = serve(log.clone()).await;

    // A reconnecting client resumes with from=<last seen + 1>.
    let (mut client, status) =
        WsClient::connect(addr, &format!("/topics/{TOPIC}/partitions/0/ws?from=3")).await;
    assert!(status.contains("101"), "{status}");
    let json = client.next_json().await;
    assert_eq!(json["offset"], 3);
    assert_eq!(value_of(&json), "v3");
}

#[tokio::test]
async fn key_prefix_filter_applies() {
    let log = MemoryLog::new();
    log.append(TOPIC, 0, Some(b"user:1"), b"keep", Vec::new());
    log.append(TOPIC, 0, Some(b"cart:2"), b"drop", Vec::new());
    log.append(TOPIC, 0, Some(b"user:3"), b"keep-too", Vec::new());
    let addr = serve(log.clone()).await;

    let (mut client, status) = WsClient::connect(
        addr,
        &format!("/topics/{TOPIC}/partitions/0/ws?from=earliest&key_prefix=user:"),
    )
    .await;
    assert!(status.contains("101"), "{status}");
    let json = client.next_json().await;
    assert_eq!(
        (json["offset"].as_i64(), value_of(&json)),
        (Some(0), "keep".into())
    );
    let json = client.next_json().await;
    assert_eq!(
        (json["offset"].as_i64(), value_of(&json)),
        (Some(2), "keep-too".into())
    );
}

#[tokio::test]
async fn bad_parameters_fail_before_the_upgrade() {
    let addr = serve(MemoryLog::new()).await;
    let (_client, status) = WsClient::connect(
        addr,
        &format!("/topics/{TOPIC}/partitions/0/ws?from=yesterday"),
    )
    .await;
    assert!(status.contains("400"), "{status}");
}

#[tokio::test]
async fn topic_stream_merges_partitions_and_resumes_by_cursor() {
    let log = MemoryLog::with_partitions(2);
    log.append(TOPIC, 0, None, b"p0-a", Vec::new());
    log.append(TOPIC, 1, None, b"p1-a", Vec::new());
    log.append(TOPIC, 0, None, b"p0-b", Vec::new());
    let addr = serve(log.clone()).await;

    let (mut client, status) =
        WsClient::connect(addr, &format!("/topics/{TOPIC}/ws?from=earliest")).await;
    assert!(status.contains("101"), "{status}");
    let mut seen: Vec<(i64, i64)> = Vec::new();
    for _ in 0..3 {
        let json = client.next_json().await;
        assert_eq!(json["topic"], TOPIC);
        seen.push((
            json["partition"].as_i64().unwrap(),
            json["offset"].as_i64().unwrap(),
        ));
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![(0, 0), (0, 1), (1, 0)]);

    // Resume with the cursor those events imply: only new data flows.
    let (mut resumed, status) =
        WsClient::connect(addr, &format!("/topics/{TOPIC}/ws?from=0:2,1:1")).await;
    assert!(status.contains("101"), "{status}");
    log.append(TOPIC, 1, None, b"p1-b", Vec::new());
    let json = resumed.next_json().await;
    assert_eq!(json["partition"], 1);
    assert_eq!(json["offset"], 1);
    assert_eq!(value_of(&json), "p1-b");
}

#[tokio::test]
async fn gated_topics_fail_403_before_the_upgrade() {
    use odradek_web_ws::web_core::Hub;

    let log = MemoryLog::new();
    let hub = Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default())
        .with_topic_gate(|topic| !topic.starts_with("internal-"));
    let addr = serve_state(WsState::from_hub(hub)).await;

    let (_denied, status) = WsClient::connect(addr, "/topics/internal-audit/partitions/0/ws").await;
    assert!(status.contains("403"), "{status}");
    let (_denied_topic, status) = WsClient::connect(addr, "/topics/internal-audit/ws").await;
    assert!(status.contains("403"), "{status}");

    // The gate does not get in the way of allowed topics.
    let (mut allowed, status) =
        WsClient::connect(addr, &format!("/topics/{TOPIC}/partitions/0/ws")).await;
    assert!(status.contains("101"), "{status}");
    log.append(TOPIC, 0, None, b"through-the-gate", Vec::new());
    let json = allowed.next_json().await;
    assert_eq!(value_of(&json), "through-the-gate");
}

/// Topics *and* partitions the source does not have are `404`s before
/// the upgrade — including on the default position, which needs no
/// source call and so used to upgrade happily and die later.
#[tokio::test]
async fn unknown_topics_and_partitions_fail_404_before_the_upgrade() {
    let log = MemoryLog::new();
    let factory = MemoryFactory::new(log).known_topics([TOPIC]);
    let addr = serve_state(WsState::new(factory, PumpConfig::default(), |_| true)).await;

    for path in [
        "/topics/ghost/partitions/0/ws".to_owned(),
        "/topics/ghost/ws".to_owned(),
        format!("/topics/{TOPIC}/partitions/7/ws"),
        format!("/topics/{TOPIC}/partitions/2147483647/ws"),
    ] {
        let (_client, status) = WsClient::connect(addr, &path).await;
        assert!(status.contains("404"), "{path}: {status}");
    }
}

/// CORS does not apply to WebSockets, so `Origin` is the only thing
/// between a victim's browser and a cross-origin socket carrying their
/// cookies. The default policy refuses every browser origin.
#[tokio::test]
async fn cross_origin_handshakes_are_refused() {
    let addr = serve(MemoryLog::new()).await;

    for origin in [
        "https://evil.example",
        "http://evil.example",
        "null",
        // Not a prefix game: a suffix of an allowed origin is not it.
        "https://app.example.com.evil.example",
    ] {
        let (_client, head) = WsClient::handshake(
            addr,
            &format!("/topics/{TOPIC}/partitions/0/ws"),
            &[("Origin", origin)],
        )
        .await;
        let status = head.lines().next().unwrap();
        assert!(status.contains("403"), "origin {origin}: {status}");
    }

    // Both routes, not just the partition one.
    let (_client, head) = WsClient::handshake(
        addr,
        "/topics/bridge/ws",
        &[("Origin", "https://evil.example")],
    )
    .await;
    assert!(head.lines().next().unwrap().contains("403"), "{head}");
}

/// A named origin connects and streams; the refusal is a policy, not a
/// wall.
#[tokio::test]
async fn allowed_origins_connect_and_stream() {
    use odradek_web_ws::{OriginPolicy, web_core::Hub};

    let log = MemoryLog::new();
    let hub = Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default()).allow_all_topics();
    let addr = serve_state(WsState::from_hub_with_origins(
        hub,
        OriginPolicy::allow(["https://app.example.com"]),
    ))
    .await;

    // Case-insensitive on the scheme and host, as origins are.
    let (mut client, head) = WsClient::handshake(
        addr,
        &format!("/topics/{TOPIC}/partitions/0/ws"),
        &[("Origin", "https://APP.example.com")],
    )
    .await;
    assert!(head.lines().next().unwrap().contains("101"), "{head}");
    log.append(TOPIC, 0, None, b"same-origin", Vec::new());
    assert_eq!(value_of(&client.next_json().await), "same-origin");

    // Anything else still gets nothing.
    let (_denied, head) = WsClient::handshake(
        addr,
        &format!("/topics/{TOPIC}/partitions/0/ws"),
        &[("Origin", "https://evil.example")],
    )
    .await;
    assert!(head.lines().next().unwrap().contains("403"), "{head}");
}

/// A handshake with no `Origin` at all is not a browser, so it is not
/// the attack this defends against: allowed by default, refusable with
/// `require_origin` for embedders whose ambient credentials make an
/// anonymous handshake privileged.
#[tokio::test]
async fn missing_origin_is_allowed_by_default_and_refusable() {
    use odradek_web_ws::{OriginPolicy, web_core::Hub};

    let log = MemoryLog::new();
    let addr = serve(log.clone()).await;
    let (mut client, status) =
        WsClient::connect(addr, &format!("/topics/{TOPIC}/partitions/0/ws")).await;
    assert!(status.contains("101"), "{status}");
    log.append(TOPIC, 0, None, b"not-a-browser", Vec::new());
    assert_eq!(value_of(&client.next_json().await), "not-a-browser");

    let strict = Hub::new(MemoryFactory::new(log), PumpConfig::default()).allow_all_topics();
    let strict_addr = serve_state(WsState::from_hub_with_origins(
        strict,
        OriginPolicy::allow(["https://app.example.com"]).require_origin(),
    ))
    .await;
    let (_client, status) =
        WsClient::connect(strict_addr, &format!("/topics/{TOPIC}/partitions/0/ws")).await;
    assert!(status.contains("403"), "{status}");
}

/// The handshake response carries the same hardening header the error
/// responses do.
#[tokio::test]
async fn responses_are_not_sniffable() {
    let addr = serve(MemoryLog::new()).await;

    let (_client, head) =
        WsClient::handshake(addr, &format!("/topics/{TOPIC}/partitions/0/ws"), &[]).await;
    let head = head.to_ascii_lowercase();
    assert!(head.contains("101"), "{head}");
    assert!(head.contains("x-content-type-options: nosniff"), "{head}");

    let (_client, head) = WsClient::handshake(
        addr,
        &format!("/topics/{TOPIC}/partitions/0/ws?from=yesterday"),
        &[],
    )
    .await;
    let head = head.to_ascii_lowercase();
    assert!(head.contains("400"), "{head}");
    assert!(head.contains("x-content-type-options: nosniff"), "{head}");
}

#[tokio::test]
async fn failed_stream_closes_with_a_reasoned_close_frame() {
    let addr = serve_state(WsState::new(RevokedFactory, PumpConfig::default(), |_| {
        true
    }))
    .await;

    // Latest subscribes without touching the source, so the upgrade
    // succeeds — then the pump hits the permanent auth failure.
    let (mut client, status) =
        WsClient::connect(addr, &format!("/topics/{TOPIC}/partitions/0/ws")).await;
    assert!(status.contains("101"), "{status}");

    let (code, reason) = client.next_close().await;
    assert_eq!(code, 1008, "auth failures use policy-violation");
    // The kind is the contract; the upstream's own words stay in the
    // operator's logs.
    assert_eq!(reason, "auth: not authorized for this topic");
    assert!(
        !reason.contains("TOPIC_AUTHORIZATION_FAILED"),
        "upstream error text leaked into the close reason: {reason}"
    );
}

#[tokio::test]
async fn shutdown_closes_sockets_going_away_and_refuses_new_upgrades() {
    let log = MemoryLog::new();
    let state = WsState::new(MemoryFactory::new(log), PumpConfig::default(), |_| true);
    let addr = serve_state(state.clone()).await;

    let (mut client, status) =
        WsClient::connect(addr, &format!("/topics/{TOPIC}/partitions/0/ws")).await;
    assert!(status.contains("101"), "{status}");

    state.shutdown().await;
    let (code, reason) = client.next_close().await;
    assert_eq!(code, 1001);
    assert_eq!(reason, "going away");

    let (_refused, status) =
        WsClient::connect(addr, &format!("/topics/{TOPIC}/partitions/0/ws")).await;
    assert!(status.contains("503"), "{status}");
}
