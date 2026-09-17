//! End-to-end WebSocket tests: a real listener, a hand-rolled WS
//! client (HTTP upgrade + frame reader — server frames are unmasked,
//! so parsing is small), and the in-memory log behind the hub.

use std::net::SocketAddr;
use std::time::Duration;

use odradek_web_core::PumpConfig;
use odradek_web_core::memory::{MemoryFactory, MemoryLog};
use odradek_web_ws::{WsState, router};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TOPIC: &str = "bridge";

async fn serve(log: MemoryLog) -> SocketAddr {
    let state = WsState::new(MemoryFactory { log }, PumpConfig::default());
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

struct WsClient {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl WsClient {
    /// Open the connection and send the upgrade request; returns the
    /// HTTP status line without asserting on it.
    async fn connect(addr: SocketAddr, path: &str) -> (WsClient, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request = format!(
            "GET {path} HTTP/1.1\r\n\
             Host: localhost\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==\r\n\
             Sec-WebSocket-Version: 13\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.unwrap();

        let mut client = WsClient {
            stream,
            buffer: Vec::new(),
        };
        let status = loop {
            if let Some(pos) = find(&client.buffer, b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&client.buffer[..pos]).into_owned();
                client.buffer.drain(..pos + 4);
                break head.lines().next().unwrap().to_owned();
            }
            client.fill().await;
        };
        (client, status)
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
