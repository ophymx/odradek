//! End-to-end SSE tests: a real listener, a raw HTTP client, and the
//! in-memory log behind the hub.
//!
//! Requests go out as HTTP/1.0 so the response streams close-delimited
//! (no chunked framing) — the tests read SSE blocks straight off the
//! socket.

use std::net::SocketAddr;
use std::time::Duration;

use odradek_web_core::PumpConfig;
use odradek_web_core::memory::{MemoryFactory, MemoryLog};
use odradek_web_sse::{SseState, router};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TOPIC: &str = "bridge";

async fn serve(log: MemoryLog) -> SocketAddr {
    let state = SseState::new(MemoryFactory { log }, PumpConfig::default());
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

struct SseClient {
    stream: TcpStream,
    buffer: Vec<u8>,
    body_at: Option<usize>,
    consumed: usize,
}

impl SseClient {
    async fn get(addr: SocketAddr, path: &str, extra_headers: &[(&str, &str)]) -> SseClient {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut request = format!("GET {path} HTTP/1.0\r\nHost: localhost\r\n");
        for (name, value) in extra_headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        SseClient {
            stream,
            buffer: Vec::new(),
            body_at: None,
            consumed: 0,
        }
    }

    async fn fill(&mut self) {
        let mut chunk = [0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(5), self.stream.read(&mut chunk))
            .await
            .expect("timed out reading from the sse stream")
            .unwrap();
        assert!(n > 0, "server closed the stream");
        self.buffer.extend_from_slice(&chunk[..n]);
    }

    /// The response status line (reads until headers are complete).
    async fn status(&mut self) -> String {
        loop {
            if let Some(pos) = find(&self.buffer, b"\r\n\r\n") {
                self.body_at = Some(pos + 4);
                self.consumed = pos + 4;
                let head = String::from_utf8_lossy(&self.buffer[..pos]);
                return head.lines().next().unwrap().to_owned();
            }
            self.fill().await;
        }
    }

    /// The next SSE data event as (id, json), skipping keep-alive
    /// comments.
    async fn next_event(&mut self) -> (i64, serde_json::Value) {
        if self.body_at.is_none() {
            let status = self.status().await;
            assert!(status.contains("200"), "unexpected status: {status}");
        }
        loop {
            // A complete SSE block ends with a blank line.
            if let Some(end) = find(&self.buffer[self.consumed..], b"\n\n") {
                let block =
                    String::from_utf8_lossy(&self.buffer[self.consumed..self.consumed + end])
                        .into_owned();
                self.consumed += end + 2;
                let mut id = None;
                let mut data = None;
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("id:") {
                        id = v.trim().parse().ok();
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data = Some(v.trim().to_owned());
                    }
                }
                match (id, data) {
                    (Some(id), Some(data)) => {
                        return (id, serde_json::from_str(&data).unwrap());
                    }
                    // Keep-alive comment or partial block: skip.
                    _ => continue,
                }
            }
            self.fill().await;
        }
    }
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
        log.append(TOPIC, None, format!("old-{i}").as_bytes(), Vec::new());
    }
    let addr = serve(log.clone()).await;

    let mut client = SseClient::get(
        addr,
        &format!("/topics/{TOPIC}/partitions/0/events?from=earliest"),
        &[],
    )
    .await;
    for i in 0..3 {
        let (id, json) = client.next_event().await;
        assert_eq!(id, i);
        assert_eq!(value_of(&json), format!("old-{i}"));
        assert_eq!(json["topic"], TOPIC);
        assert_eq!(json["offset"], i);
    }

    log.append(TOPIC, None, b"fresh", Vec::new());
    let (id, json) = client.next_event().await;
    assert_eq!(id, 3);
    assert_eq!(value_of(&json), "fresh");
}

#[tokio::test]
async fn last_event_id_resumes_exactly_after() {
    let log = MemoryLog::new();
    for i in 0..5 {
        log.append(TOPIC, None, format!("v{i}").as_bytes(), Vec::new());
    }
    let addr = serve(log.clone()).await;

    // A reconnecting EventSource sends the last id it saw; the stream
    // must resume at the next offset even though `from` says earliest.
    let mut client = SseClient::get(
        addr,
        &format!("/topics/{TOPIC}/partitions/0/events?from=earliest"),
        &[("Last-Event-ID", "2")],
    )
    .await;
    let (id, json) = client.next_event().await;
    assert_eq!(id, 3);
    assert_eq!(value_of(&json), "v3");
}

#[tokio::test]
async fn default_position_is_latest() {
    let log = MemoryLog::new();
    log.append(TOPIC, None, b"history", Vec::new());
    let addr = serve(log.clone()).await;

    let mut client =
        SseClient::get(addr, &format!("/topics/{TOPIC}/partitions/0/events"), &[]).await;
    // Ensure the subscription is live before appending.
    let status = client.status().await;
    assert!(status.contains("200"), "{status}");

    log.append(TOPIC, None, b"new", Vec::new());
    let (id, json) = client.next_event().await;
    assert_eq!(id, 1);
    assert_eq!(value_of(&json), "new");
}

#[tokio::test]
async fn key_prefix_filter_applies() {
    let log = MemoryLog::new();
    log.append(TOPIC, Some(b"user:1"), b"keep", Vec::new());
    log.append(TOPIC, Some(b"cart:2"), b"drop", Vec::new());
    log.append(TOPIC, Some(b"user:3"), b"keep-too", Vec::new());
    let addr = serve(log.clone()).await;

    let mut client = SseClient::get(
        addr,
        &format!("/topics/{TOPIC}/partitions/0/events?from=earliest&key_prefix=user:"),
        &[],
    )
    .await;
    let (id, json) = client.next_event().await;
    assert_eq!((id, value_of(&json)), (0, "keep".into()));
    assert_eq!(json["key"], "user:1");
    let (id, json) = client.next_event().await;
    assert_eq!((id, value_of(&json)), (2, "keep-too".into()));
}

#[tokio::test]
async fn bad_parameters_are_rejected() {
    let addr = serve(MemoryLog::new()).await;
    let mut client = SseClient::get(
        addr,
        &format!("/topics/{TOPIC}/partitions/0/events?from=yesterday"),
        &[],
    )
    .await;
    let status = client.status().await;
    assert!(status.contains("400"), "{status}");
}
