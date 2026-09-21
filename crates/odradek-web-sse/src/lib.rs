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
//! *resume token*: the offset it was read at, which the stream
//! resumes after. Browsers
//! reconnect with `Last-Event-ID`, which overrides `from` and is
//! honoured exactly as sent — so a reconnecting `EventSource` never
//! misses or repeats a record. Every resume token in the constellation
//! means "start here": SSE ids, WebSocket `from=`, and the topic-level
//! cursors are interchangeable across transports.
//!
//! The token is **opaque**: echo it back, do not parse or increment it.
//! A client that derives the next id itself rather than reading the one
//! it was sent is coupled to an internal format — and to a `data`
//! offset that is the *record's*, not the resume position's.
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
//! `403` for a topic the hub's gate denies, `404` for a topic or
//! partition the source does not have, `503` after shutdown or at pump
//! capacity, `502` for other source trouble. If the stream fails
//! *later* (topic deleted, auth revoked, error budget exhausted), the
//! client receives one final `event: error` frame whose data is
//! `{"kind": "...", "message": "..."}` and the stream ends; a clean
//! shutdown just ends the stream. Both the status body and that frame
//! carry the *classified* reason only — the upstream's own error text
//! goes to `tracing`, because on a real cluster it names brokers,
//! ports, and ACL state.
//!
//! # Security
//!
//! These routes serve Kafka data to whoever reaches them. Two things
//! are yours to set, and this crate will not guess:
//!
//! - **Which topics** — [`SseState::new`] takes the hub's topic gate
//!   as an argument for that reason; there is no ungated shortcut.
//! - **Who** — mount the router under your own authentication, and do
//!   not put a permissive CORS layer over it: the response bodies are
//!   your Kafka records, and `Access-Control-Allow-Origin: *` hands
//!   them to every page in the world. `EventSource` is same-origin by
//!   default, which is the protection you would be removing.
//!
//! See the crate README for the rest (connection limits, the memory
//! formula).
//!
//! Graceful shutdown: keep the [`SseState`] `Arc` you built the router
//! from and call [`SseState::shutdown`] when your server begins to
//! drain (e.g. from the future you hand to axum's
//! `with_graceful_shutdown`) — every pump stops, open streams end
//! cleanly, and new subscribes are refused with `503`.

// This crate carries no `unsafe` block and has never needed one:
// forbid rather than deny, so the decision cannot be reversed by a
// local `allow` in a module nobody re-reads.
#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::header::{HeaderName, X_CONTENT_TYPE_OPTIONS};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
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
    /// State over a hub serving exactly the topics `gate` approves.
    ///
    /// The gate is an argument, not an option, because these routes are
    /// reachable by whoever can reach the port: the default
    /// configuration should not be one that serves every topic on the
    /// cluster (`__consumer_offsets` included). `|_| true` is
    /// available, but you have to write it.
    ///
    /// ```no_run
    /// # use odradek_web_sse::{PumpConfig, SseState, router};
    /// # fn demo<F: odradek_web_sse::SourceFactory>(factory: F) {
    /// let state = SseState::new(factory, PumpConfig::default(), |topic| {
    ///     topic.starts_with("public.")
    /// });
    /// let app = router(state);
    /// # }
    /// ```
    ///
    /// For hub-level options — [`Hub::with_max_pumps`],
    /// [`Hub::allow_all_topics`] — build the [`Hub`] yourself and use
    /// [`SseState::from_hub`].
    pub fn new(
        factory: F,
        config: PumpConfig,
        gate: impl Fn(&str) -> bool + Send + Sync + 'static,
    ) -> Arc<SseState<F>> {
        SseState::from_hub(Hub::new(factory, config).with_topic_gate(gate))
    }

    /// Wrap a pre-built hub — the way in for hub-level options such as
    /// [`Hub::with_max_pumps`]. A hub with no gate denies every topic,
    /// so this path still requires a decision.
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

/// An error response: a status, the hardening headers, and a body.
type HttpError = (StatusCode, Headers, String);

/// The response headers every route sets.
type Headers = [(HeaderName, &'static str); 1];

/// `X-Content-Type-Options: nosniff`.
///
/// Defense in depth: error bodies are `text/plain` and can echo the
/// client's own parameters back, so nothing should be left to a
/// browser's content sniffing.
fn headers() -> Headers {
    [(X_CONTENT_TYPE_OPTIONS, "nosniff")]
}

/// A refused subscribe as the plain HTTP error it becomes: `400` for
/// bad parameters, `403` for gated topics, `404` for missing topics and
/// partitions, `503` after shutdown or at pump capacity, `502` for the
/// rest. The body is [`Rejection::message`], which is classified rather
/// than narrated — see [`odradek_web_core::RejectionKind`].
fn reject(rejection: Rejection) -> HttpError {
    let status = match rejection.kind {
        RejectionKind::BadRequest => StatusCode::BAD_REQUEST,
        RejectionKind::Denied => StatusCode::FORBIDDEN,
        RejectionKind::NotFound => StatusCode::NOT_FOUND,
        RejectionKind::ShutDown | RejectionKind::AtCapacity => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::BAD_GATEWAY,
    };
    (status, headers(), rejection.message)
}

/// One channel item as an SSE frame; errors become a final
/// `event: error` frame right before the stream ends.
///
/// The frame carries the kind and its fixed message, never the
/// upstream's own text: the pump has already logged that, and a stream
/// that anyone can open is not the place to publish broker hostnames.
fn error_frame(err: &odradek_web_core::StreamError) -> SseEvent {
    SseEvent::default().event("error").data(
        serde_json::json!({
            "kind": err.kind.as_str(),
            "message": err.public_message(),
        })
        .to_string(),
    )
}

/// The raw `Last-Event-ID` header value, if any; non-UTF-8 is a 400.
fn last_event_id(headers_in: &HeaderMap) -> Result<Option<String>, HttpError> {
    match headers_in.get("last-event-id") {
        None => Ok(None),
        Some(v) => v.to_str().map(|s| Some(s.to_owned())).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                headers(),
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
    headers_in: HeaderMap,
) -> Result<impl IntoResponse, HttpError> {
    let resume = last_event_id(&headers_in)?;
    let subscription = state
        .hub
        .stream(&topic, partition, &params, resume.as_deref())
        .await
        .map_err(reject)?;

    let stream = ReceiverStream::new(subscription.into_receiver()).map(|item: StreamItem| {
        Ok::<_, Infallible>(match item {
            // `event.json()` is rendered by whichever subscriber of this
            // partition reaches it first; the rest copy the finished
            // bytes into their own frame.
            Ok(event) => SseEvent::default()
                .event("record")
                .id(event.offset.to_string())
                .data(event.json()),
            // The final item of a failed stream; the channel closes
            // right after, ending the response.
            Err(err) => error_frame(&err),
        })
    });
    Ok((headers(), Sse::new(stream).keep_alive(KeepAlive::default())))
}

/// The whole topic, all partitions merged. The event id is the
/// multi-partition cursor (`partition:offset,...`) of what the client
/// has now seen, updated per event, so `Last-Event-ID` on reconnect
/// resumes every partition loss-free (partitions the cursor has not
/// seen replay from earliest).
async fn stream_topic<F: SourceFactory>(
    State(state): State<Arc<SseState<F>>>,
    Path(topic): Path<String>,
    Query(params): Query<StreamParams>,
    headers_in: HeaderMap,
) -> Result<impl IntoResponse, HttpError> {
    let resume = last_event_id(&headers_in)?;
    let (subscription, position) = state
        .hub
        .stream_topic(&topic, &params, resume.as_deref())
        .await
        .map_err(reject)?;

    // Seed the running cursor from the resume point, so an id always
    // carries every partition the client has a position for — but only
    // the partitions this subscription actually covers.
    //
    // The seed is client-supplied and this cursor is re-encoded into
    // *every* event's id, so an unpruned seed is an amplifier: the
    // client sends one oversized `Last-Event-ID` and gets it back once
    // per record in the topic. Pruned, an id is bounded by the topic's
    // real partition count, whatever the client sent.
    let covered = subscription.partitions().to_vec();
    let mut running = match position {
        TopicPosition::After(cursor) => cursor,
        _ => BTreeMap::new(),
    };
    running.retain(|partition, _| covered.contains(partition));

    let stream = ReceiverStream::new(subscription.into_receiver()).map(move |item: StreamItem| {
        Ok::<_, Infallible>(match item {
            Ok(event) => {
                running.insert(event.partition, event.offset);
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
    Ok((headers(), Sse::new(stream).keep_alive(KeepAlive::default())))
}
