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
//!
//! One record's `data` is rendered once for all of that partition's
//! subscribers, however many streams are open. The frame around it is
//! per-stream: the `id` of a partition stream is that record's next
//! offset, but a topic stream's `id` is the reader's own multi-partition
//! cursor, so each stream writes its own.
//!
//! Subscribe failures are plain HTTP errors before the stream starts:
//! `403` for a topic the hub's gate denies, `404` for a topic the
//! source does not have, `503` after shutdown, `502` for other source
//! trouble. If the stream fails *later* (topic deleted, auth revoked,
//! error budget exhausted), the client receives one final
//! `event: error` frame whose data is `{"kind": "...", "message":
//! "..."}` and the stream ends; a clean shutdown just ends the stream.
//!
//! Graceful shutdown: keep the [`SseState`] `Arc` you built the router
//! from and call [`SseState::shutdown`] when your server begins to
//! drain (e.g. from the future you hand to axum's
//! `with_graceful_shutdown`) — every pump stops, open streams end
//! cleanly, and new subscribes are refused with `503`.

use std::convert::Infallible;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::routing::get;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use odradek_web_core::pump::StreamItem;
use odradek_web_core::{
    Hub, Rejection, RejectionKind, SharedHub, StreamParams, TopicPosition, cursor,
};

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
    hub: SharedHub<F>,
}

impl<F: SourceFactory> SseState<F> {
    pub fn new(factory: F, config: PumpConfig) -> Arc<SseState<F>> {
        SseState::from_hub(Hub::new(factory, config))
    }

    /// Wrap a pre-built hub — the way in for hub-level options such as
    /// [`Hub::with_topic_gate`].
    pub fn from_hub(hub: Hub<F>) -> Arc<SseState<F>> {
        Arc::new(SseState {
            hub: SharedHub::from_hub(hub),
        })
    }

    /// Stop every pump and refuse further subscribes with `503`; open
    /// streams end cleanly. Call this from your server's graceful
    /// shutdown (axum's `with_graceful_shutdown`).
    pub async fn shutdown(&self) {
        self.hub.shutdown().await;
    }
}

/// A refused subscribe as the plain HTTP error it becomes: `400` for
/// bad parameters, `403` for gated topics, `404` for missing ones,
/// `503` after shutdown, `502` for the rest.
fn reject(rejection: Rejection) -> (StatusCode, String) {
    let status = match rejection.kind {
        RejectionKind::BadRequest => StatusCode::BAD_REQUEST,
        RejectionKind::Denied => StatusCode::FORBIDDEN,
        RejectionKind::NotFound => StatusCode::NOT_FOUND,
        RejectionKind::ShutDown => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::BAD_GATEWAY,
    };
    (status, rejection.message)
}

/// One channel item as an SSE frame; errors become a final
/// `event: error` frame right before the stream ends.
fn error_frame(err: &odradek_web_core::StreamError) -> SseEvent {
    SseEvent::default().event("error").data(
        serde_json::json!({
            "kind": err.kind.as_str(),
            "message": err.message,
        })
        .to_string(),
    )
}

/// The raw `Last-Event-ID` header value, if any; non-UTF-8 is a 400.
fn last_event_id(headers: &HeaderMap) -> Result<Option<String>, (StatusCode, String)> {
    match headers.get("last-event-id") {
        None => Ok(None),
        Some(v) => v.to_str().map(|s| Some(s.to_owned())).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "Last-Event-ID must be UTF-8".to_owned(),
            )
        }),
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

async fn stream_partition<F: SourceFactory>(
    State(state): State<Arc<SseState<F>>>,
    Path((topic, partition)): Path<(String, i32)>,
    Query(params): Query<StreamParams>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<SseEvent, Infallible>>>, (StatusCode, String)>
{
    let resume = last_event_id(&headers)?;
    let subscription = state
        .hub
        .stream(&topic, partition, &params, resume.as_deref())
        .await
        .map_err(reject)?;

    let stream = ReceiverStream::new(subscription.into_receiver()).map(|item: StreamItem| {
        Ok(match item {
            // `event.json()` is rendered by whichever subscriber of this
            // partition reaches it first; the rest copy the finished
            // bytes into their own frame.
            Ok(event) => SseEvent::default()
                .event("record")
                .id((event.offset + 1).to_string())
                .data(event.json()),
            // The final item of a failed stream; the channel closes
            // right after, ending the response.
            Err(err) => error_frame(&err),
        })
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
    let resume = last_event_id(&headers)?;
    let (subscription, position) = state
        .hub
        .stream_topic(&topic, &params, resume.as_deref())
        .await
        .map_err(reject)?;

    // Seed the running cursor from the resume point, so an id always
    // carries every partition the client has a position for.
    let mut running = match position {
        TopicPosition::Offsets(cursor) => cursor,
        _ => std::collections::BTreeMap::new(),
    };

    let stream = ReceiverStream::new(subscription.into_receiver()).map(move |item: StreamItem| {
        Ok(match item {
            Ok(event) => {
                running.insert(event.partition, event.offset + 1);
                SseEvent::default()
                    .event("record")
                    .id(cursor::encode(&running))
                    .data(event.json())
            }
            // A partition pump failed; report it and let the stream
            // wind down.
            Err(err) => error_frame(&err),
        })
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
