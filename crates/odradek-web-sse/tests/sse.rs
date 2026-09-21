//! End-to-end SSE tests: a real listener, a raw HTTP client, and the
//! in-memory log behind the hub.
//!
//! Requests go out as HTTP/1.0 so the response streams close-delimited
//! (no chunked framing) — the tests read SSE blocks straight off the
//! socket.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use odradek_web_core::memory::{MemoryFactory, MemoryLog};
use odradek_web_core::{PumpConfig, RecordSource, SourceBatch, SourceError};
use odradek_web_sse::{SourceFactory, SseState, router};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TOPIC: &str = "bridge";

async fn serve_state<F: SourceFactory>(state: Arc<SseState<F>>) -> SocketAddr {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

/// A server over the whole in-memory log. The gate is an argument to
/// `SseState::new`, so even a test has to say what it serves; these
/// tests serve everything the memory factory knows.
async fn serve(log: MemoryLog) -> SocketAddr {
    serve_state(SseState::new(
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
        _after: Option<i64>,
    ) -> Result<SourceBatch, SourceError> {
        Err(SourceError::auth("TOPIC_AUTHORIZATION_FAILED"))
    }

    async fn live_start(
        &mut self,
        _topic: &str,
        _partition: i32,
    ) -> Result<Option<i64>, SourceError> {
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

    /// The whole response head (reads until the headers are complete).
    async fn head(&mut self) -> String {
        loop {
            if let Some(pos) = find(&self.buffer, b"\r\n\r\n") {
                self.body_at = Some(pos + 4);
                self.consumed = pos + 4;
                return String::from_utf8_lossy(&self.buffer[..pos]).into_owned();
            }
            self.fill().await;
        }
    }

    /// The response status line (reads until headers are complete).
    async fn status(&mut self) -> String {
        let head = self.head().await;
        head.lines().next().unwrap().to_owned()
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

    /// Like [`Self::next_event`], but returns the raw (cursor) id.
    async fn next_cursor_event(&mut self) -> (String, serde_json::Value) {
        if self.body_at.is_none() {
            let status = self.status().await;
            assert!(status.contains("200"), "unexpected status: {status}");
        }
        loop {
            if let Some(end) = find(&self.buffer[self.consumed..], b"\n\n") {
                let block =
                    String::from_utf8_lossy(&self.buffer[self.consumed..self.consumed + end])
                        .into_owned();
                self.consumed += end + 2;
                let mut id = None;
                let mut data = None;
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("id:") {
                        id = Some(v.trim().to_owned());
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data = Some(v.trim().to_owned());
                    }
                }
                match (id, data) {
                    (Some(id), Some(data)) => {
                        return (id, serde_json::from_str(&data).unwrap());
                    }
                    _ => continue,
                }
            }
            self.fill().await;
        }
    }

    /// The next SSE block's (event name, data json), skipping
    /// keep-alive comments. Unlike [`Self::next_event`] it does not
    /// require an id, so it also sees `event: error` frames.
    async fn next_named(&mut self) -> (String, serde_json::Value) {
        if self.body_at.is_none() {
            let status = self.status().await;
            assert!(status.contains("200"), "unexpected status: {status}");
        }
        loop {
            if let Some(end) = find(&self.buffer[self.consumed..], b"\n\n") {
                let block =
                    String::from_utf8_lossy(&self.buffer[self.consumed..self.consumed + end])
                        .into_owned();
                self.consumed += end + 2;
                let mut event = None;
                let mut data = None;
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("event:") {
                        event = Some(v.trim().to_owned());
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data = Some(v.trim().to_owned());
                    }
                }
                match (event, data) {
                    (Some(event), Some(data)) => {
                        return (event, serde_json::from_str(&data).unwrap());
                    }
                    _ => continue,
                }
            }
            self.fill().await;
        }
    }

    /// Wait for the server to close the connection.
    async fn wait_close(&mut self) {
        loop {
            let mut chunk = [0u8; 4096];
            let n = tokio::time::timeout(Duration::from_secs(5), self.stream.read(&mut chunk))
                .await
                .expect("timed out waiting for the stream to close")
                .unwrap();
            if n == 0 {
                return;
            }
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
        log.append(TOPIC, 0, None, format!("old-{i}").as_bytes(), Vec::new());
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
        // The id is the resume token, and it is this event's own
        // offset: echoing it back asks for what comes after.
        assert_eq!(id, i);
        assert_eq!(value_of(&json), format!("old-{i}"));
        assert_eq!(json["topic"], TOPIC);
        assert_eq!(json["offset"], i);
    }

    log.append(TOPIC, 0, None, b"fresh", Vec::new());
    let (id, json) = client.next_event().await;
    assert_eq!(id, 3);
    assert_eq!(value_of(&json), "fresh");
}

#[tokio::test]
async fn last_event_id_resumes_exactly_after() {
    let log = MemoryLog::new();
    for i in 0..5 {
        log.append(TOPIC, 0, None, format!("v{i}").as_bytes(), Vec::new());
    }
    let addr = serve(log.clone()).await;

    // A reconnecting EventSource sends the last id it saw, and an id is
    // the offset of the event that carried it: id 2 came with the event
    // at offset 2, so the stream resumes at offset 3 — the next one —
    // even though `from` says earliest. The token is used verbatim, so
    // what the client echoes is exactly what it was given.
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
    log.append(TOPIC, 0, None, b"history", Vec::new());
    let addr = serve(log.clone()).await;

    let mut client =
        SseClient::get(addr, &format!("/topics/{TOPIC}/partitions/0/events"), &[]).await;
    // Ensure the subscription is live before appending.
    let status = client.status().await;
    assert!(status.contains("200"), "{status}");

    log.append(TOPIC, 0, None, b"new", Vec::new());
    let (id, json) = client.next_event().await;
    assert_eq!(id, 1); // the resume token is the offset itself
    assert_eq!(value_of(&json), "new");
}

#[tokio::test]
async fn key_prefix_filter_applies() {
    let log = MemoryLog::new();
    log.append(TOPIC, 0, Some(b"user:1"), b"keep", Vec::new());
    log.append(TOPIC, 0, Some(b"cart:2"), b"drop", Vec::new());
    log.append(TOPIC, 0, Some(b"user:3"), b"keep-too", Vec::new());
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
    // The record at offset 1 was filtered out, so the token jumps
    // straight from 0 to 2: a resume token names an offset the client
    // has been carried past, not one it was shown.
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

#[tokio::test]
async fn topic_stream_merges_partitions_with_cursor_ids() {
    let log = MemoryLog::with_partitions(2);
    log.append(TOPIC, 0, None, b"p0-a", Vec::new());
    log.append(TOPIC, 1, None, b"p1-a", Vec::new());
    log.append(TOPIC, 0, None, b"p0-b", Vec::new());
    let addr = serve(log.clone()).await;

    let mut client =
        SseClient::get(addr, &format!("/topics/{TOPIC}/events?from=earliest"), &[]).await;
    let mut last_cursor = String::new();
    let mut seen: Vec<(i64, i64)> = Vec::new();
    for _ in 0..3 {
        let (cursor, json) = client.next_cursor_event().await;
        seen.push((
            json["partition"].as_i64().unwrap(),
            json["offset"].as_i64().unwrap(),
        ));
        last_cursor = cursor;
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![(0, 0), (0, 1), (1, 0)]);
    // After all three, the cursor names the last offset seen in each
    // partition.
    assert_eq!(last_cursor, "0:1,1:0");

    // Reconnect with that cursor: nothing replays, only new arrives.
    let mut resumed = SseClient::get(
        addr,
        &format!("/topics/{TOPIC}/events"),
        &[("Last-Event-ID", &last_cursor)],
    )
    .await;
    log.append(TOPIC, 1, None, b"p1-b", Vec::new());
    let (cursor, json) = resumed.next_cursor_event().await;
    assert_eq!(json["partition"], 1);
    assert_eq!(json["offset"], 1);
    assert_eq!(cursor, "0:1,1:1");
}

/// The event id of a topic stream is bounded by the topic's own
/// partitions, not by what the client put in `Last-Event-ID`.
///
/// The id is re-encoded per event, so an unpruned cursor is an
/// amplifier whose factor is the number of records in the topic: a
/// 20 KB resume token against a 200-record topic used to come back as
/// megabytes. Here the seed names 2000 partitions, two of which exist.
#[tokio::test]
async fn topic_cursor_ids_are_bounded_by_the_topics_partitions() {
    let log = MemoryLog::with_partitions(2);
    for i in 0..100 {
        log.append(TOPIC, 0, None, format!("p0-{i}").as_bytes(), Vec::new());
        log.append(TOPIC, 1, None, format!("p1-{i}").as_bytes(), Vec::new());
    }
    let addr = serve(log.clone()).await;

    // Two real positions, and 2000 partitions that do not exist.
    // Positions are exclusive, so these resume at 98 and 99.
    let mut seed = String::from("0:97,1:98");
    for partition in 1000..3000 {
        seed.push_str(&format!(",{partition}:{}", i64::MAX));
    }
    assert!(
        seed.len() > 20_000,
        "the seed should be big: {}",
        seed.len()
    );

    let mut client = SseClient::get(
        addr,
        &format!("/topics/{TOPIC}/events"),
        &[("Last-Event-ID", &seed)],
    )
    .await;

    // Resume is still exact: partition 0 replays from 98, partition 1
    // from 99, and nothing else appears.
    let mut seen = Vec::new();
    for _ in 0..3 {
        let (cursor, json) = client.next_cursor_event().await;
        assert!(
            cursor.len() < 64,
            "cursor id grew with the client's seed: {} bytes",
            cursor.len()
        );
        assert_eq!(
            cursor.split(',').count(),
            2,
            "cursor names partitions this stream does not cover: {cursor}"
        );
        seen.push((
            json["partition"].as_i64().unwrap(),
            json["offset"].as_i64().unwrap(),
        ));
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![(0, 98), (0, 99), (1, 99)]);
}

#[tokio::test]
async fn gated_topics_return_403_and_allowed_topics_still_stream() {
    use odradek_web_sse::web_core::Hub;

    let log = MemoryLog::new();
    let hub = Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default())
        .with_topic_gate(|topic| !topic.starts_with("internal-"));
    let addr = serve_state(SseState::from_hub(hub)).await;

    let mut denied = SseClient::get(
        addr,
        "/topics/internal-audit/partitions/0/events?from=earliest",
        &[],
    )
    .await;
    let status = denied.status().await;
    assert!(status.contains("403"), "{status}");

    let mut denied_topic = SseClient::get(addr, "/topics/internal-audit/events", &[]).await;
    let status = denied_topic.status().await;
    assert!(status.contains("403"), "{status}");

    // The gate does not get in the way of allowed topics.
    let mut allowed =
        SseClient::get(addr, &format!("/topics/{TOPIC}/partitions/0/events"), &[]).await;
    let status = allowed.status().await;
    assert!(status.contains("200"), "{status}");
    log.append(TOPIC, 0, None, b"through-the-gate", Vec::new());
    let (_, json) = allowed.next_event().await;
    assert_eq!(value_of(&json), "through-the-gate");
}

/// Both stream shapes answer `404` for a topic the source does not
/// have — and, crucially, on the *default* position as well as
/// `from=earliest`. `latest` needs no source call, so this path used to
/// answer `200` and then die mid-stream, which is both a broken
/// contract and how a pump-map entry got created for a topic that does
/// not exist.
#[tokio::test]
async fn unknown_topics_and_partitions_return_404() {
    let log = MemoryLog::new();
    let factory = MemoryFactory::new(log).known_topics([TOPIC]);
    let addr = serve_state(SseState::new(factory, PumpConfig::default(), |_| true)).await;

    for path in [
        "/topics/ghost/partitions/0/events?from=earliest",
        "/topics/ghost/partitions/0/events",
        "/topics/ghost/events",
        // The topic exists; the partition does not. Nothing but the
        // source's partition list can tell the difference.
        &format!("/topics/{TOPIC}/partitions/7/events"),
        &format!("/topics/{TOPIC}/partitions/2147483647/events"),
    ] {
        let mut client = SseClient::get(addr, path, &[]).await;
        let status = client.status().await;
        assert!(status.contains("404"), "{path}: {status}");
    }
}

/// Responses carry `X-Content-Type-Options: nosniff` — the streaming
/// one and the error one alike.
#[tokio::test]
async fn responses_are_not_sniffable() {
    let addr = serve(MemoryLog::new()).await;

    let mut streaming =
        SseClient::get(addr, &format!("/topics/{TOPIC}/partitions/0/events"), &[]).await;
    let head = streaming.head().await.to_ascii_lowercase();
    assert!(head.contains("200"), "{head}");
    assert!(head.contains("x-content-type-options: nosniff"), "{head}");

    let mut bad = SseClient::get(
        addr,
        &format!("/topics/{TOPIC}/partitions/0/events?from=yesterday"),
        &[],
    )
    .await;
    let head = bad.head().await.to_ascii_lowercase();
    assert!(head.contains("400"), "{head}");
    assert!(head.contains("x-content-type-options: nosniff"), "{head}");
}

#[tokio::test]
async fn failed_stream_ends_with_an_error_event() {
    let addr = serve_state(SseState::new(RevokedFactory, PumpConfig::default(), |_| {
        true
    }))
    .await;

    // Latest subscribes without touching the source, so the stream
    // opens — then the pump hits the permanent auth failure.
    let mut client =
        SseClient::get(addr, &format!("/topics/{TOPIC}/partitions/0/events"), &[]).await;
    let (event, json) = client.next_named().await;
    assert_eq!(event, "error");
    assert_eq!(json["kind"], "auth");
    // The kind is the contract; the upstream's own words ("TOPIC_
    // AUTHORIZATION_FAILED", and on a real cluster broker hostnames and
    // ports) stay in the operator's logs.
    assert_eq!(json["message"], "not authorized for this topic");
    assert!(
        !json["message"]
            .as_str()
            .unwrap()
            .contains("TOPIC_AUTHORIZATION_FAILED"),
        "upstream error text leaked to the client: {json}"
    );
    client.wait_close().await;
}

#[tokio::test]
async fn shutdown_ends_streams_cleanly_and_refuses_new_subscribes() {
    let log = MemoryLog::new();
    let state = SseState::new(MemoryFactory::new(log), PumpConfig::default(), |_| true);
    let addr = serve_state(state.clone()).await;

    let mut client =
        SseClient::get(addr, &format!("/topics/{TOPIC}/partitions/0/events"), &[]).await;
    let status = client.status().await;
    assert!(status.contains("200"), "{status}");

    state.shutdown().await;
    // A clean end: no error frame, the response simply completes.
    client.wait_close().await;

    let mut refused =
        SseClient::get(addr, &format!("/topics/{TOPIC}/partitions/0/events"), &[]).await;
    let status = refused.status().await;
    assert!(status.contains("503"), "{status}");
}
