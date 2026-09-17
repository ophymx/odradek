//! Server-Sent Events over [`odradek_web_core`]: the thin transport the
//! engine was built for.
//!
//! [`router`] returns an embeddable [`axum::Router`] — mount it in your
//! own service and layer your own auth/middleware over it (that is the
//! point of being a library, not a gateway):
//!
//! ```text
//! GET /topics/{topic}/partitions/{partition}/events
//!     ?from=earliest|latest|<offset>     start position (default: latest)
//!     &key_prefix=<utf8>                 only records whose key starts so
//!     &header=<name>:<value>             only records with this header
//! ```
//!
//! Each record becomes one SSE event: `event: record`, `data` = one
//! JSON object (which carries the record's own offset), and `id` = the
//! *resume token*: the next offset after this event. Browsers reconnect
//! with `Last-Event-ID`, which overrides `from` and is used verbatim as
//! the start position — so a reconnecting `EventSource` never misses or
//! repeats a record. Every resume token in the constellation means
//! "start here": SSE ids, WebSocket `from=`, and the topic-level
//! cursors are interchangeable across transports.
//!
//! Keys, values, and header values arrive as UTF-8 strings when they
//! are valid UTF-8 (`key`, `value`), else base64 (`key_base64`,
//! `value_base64`) — web-friendly without lying about binary data.

use std::convert::Infallible;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::routing::get;
use bytes::Bytes;
use serde::Deserialize;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use odradek_web_core::json::event_json;
use odradek_web_core::{Filter, Hub, Position, TopicPosition, cursor};

/// Everything needed to stand the router up, re-exported so embedders
/// depend on this crate alone; the full engine is under [`web_core`].
pub use odradek_web_core as web_core;
#[cfg(feature = "kafka")]
pub use odradek_web_core::{ClientConfig, KafkaSourceFactory};
pub use odradek_web_core::{PumpConfig, SourceFactory};

/// Shared state behind the routes: the hub, guarded for subscribe-time
/// mutation only (streams run lock-free once created).
#[derive(Debug)]
pub struct SseState<F: SourceFactory> {
    hub: tokio::sync::Mutex<Hub<F>>,
}

impl<F: SourceFactory> SseState<F> {
    pub fn new(factory: F, config: PumpConfig) -> Arc<SseState<F>> {
        Arc::new(SseState {
            hub: tokio::sync::Mutex::new(Hub::new(factory, config)),
        })
    }
}

/// The SSE routes over `state`; merge into your own [`Router`].
pub fn router<F: SourceFactory>(state: Arc<SseState<F>>) -> Router {
    Router::new()
        .route(
            "/topics/{topic}/partitions/{partition}/events",
            get(stream_partition::<F>),
        )
        .route("/topics/{topic}/events", get(stream_topic::<F>))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct StreamParams {
    from: Option<String>,
    key_prefix: Option<String>,
    header: Option<String>,
}

impl StreamParams {
    /// `Last-Event-ID` (a reconnect) wins over `from`.
    fn position(&self, headers: &HeaderMap) -> Result<Position, String> {
        if let Some(last) = headers.get("last-event-id") {
            // Ids are resume tokens (next offset), used verbatim.
            let last: i64 = last
                .to_str()
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| "Last-Event-ID must be an offset".to_owned())?;
            return Ok(Position::Offset(last));
        }
        match self.from.as_deref() {
            None | Some("latest") => Ok(Position::Latest),
            Some("earliest") => Ok(Position::Earliest),
            Some(raw) => raw
                .parse()
                .map(Position::Offset)
                .map_err(|_| format!("from must be earliest, latest, or an offset (got {raw:?})")),
        }
    }

    fn filter(&self) -> Result<Filter, String> {
        let header = match &self.header {
            None => None,
            Some(raw) => {
                let (name, value) = raw
                    .split_once(':')
                    .ok_or_else(|| "header filter must be <name>:<value>".to_owned())?;
                Some((name.to_owned(), Bytes::copy_from_slice(value.as_bytes())))
            }
        };
        Ok(Filter {
            key_prefix: self
                .key_prefix
                .as_ref()
                .map(|p| Bytes::copy_from_slice(p.as_bytes())),
            header,
        })
    }
}

async fn stream_partition<F: SourceFactory>(
    State(state): State<Arc<SseState<F>>>,
    Path((topic, partition)): Path<(String, i32)>,
    Query(params): Query<StreamParams>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<SseEvent, Infallible>>>, (StatusCode, String)>
{
    let position = params
        .position(&headers)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let filter = params.filter().map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    let subscription = state
        .hub
        .lock()
        .await
        .subscribe(&topic, partition, position, filter)
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;

    let stream = ReceiverStream::new(subscription.into_receiver()).map(|event| {
        Ok(SseEvent::default()
            .event("record")
            .id((event.offset + 1).to_string())
            .data(event_json(&event).to_string()))
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// The whole topic, all partitions merged. The event id is the
/// multi-partition cursor (`partition:next_offset,...`), updated per
/// event, so `Last-Event-ID` on reconnect resumes every partition
/// loss-free (partitions the cursor has not seen replay from earliest).
async fn stream_topic<F: SourceFactory>(
    State(state): State<Arc<SseState<F>>>,
    Path(topic): Path<String>,
    Query(params): Query<StreamParams>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<SseEvent, Infallible>>>, (StatusCode, String)>
{
    let position = topic_position(&params, &headers).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let filter = params.filter().map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    // Seed the running cursor from the resume point, so an id always
    // carries every partition the client has a position for.
    let mut running = match &position {
        TopicPosition::Offsets(cursor) => cursor.clone(),
        _ => std::collections::BTreeMap::new(),
    };
    let subscription = state
        .hub
        .lock()
        .await
        .subscribe_topic(&topic, position, filter)
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;

    let stream = ReceiverStream::new(subscription.into_receiver()).map(move |event| {
        running.insert(event.partition, event.offset + 1);
        Ok(SseEvent::default()
            .event("record")
            .id(cursor::encode(&running))
            .data(event_json(&event).to_string()))
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// `Last-Event-ID` (a cursor) wins over `from`; `from` accepts
/// `earliest`, `latest`, or a cursor.
fn topic_position(params: &StreamParams, headers: &HeaderMap) -> Result<TopicPosition, String> {
    if let Some(last) = headers.get("last-event-id") {
        let raw = last
            .to_str()
            .map_err(|_| "Last-Event-ID must be a cursor".to_owned())?;
        return Ok(TopicPosition::Offsets(cursor::parse(raw)?));
    }
    match params.from.as_deref() {
        None | Some("latest") => Ok(TopicPosition::Latest),
        Some("earliest") => Ok(TopicPosition::Earliest),
        Some(raw) => Ok(TopicPosition::Offsets(cursor::parse(raw)?)),
    }
}
