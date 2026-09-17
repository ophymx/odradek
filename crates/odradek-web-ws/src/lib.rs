//! WebSocket transport over [`odradek_web_core`].
//!
//! [`router`] returns an embeddable [`axum::Router`] — mount it in your
//! own service and layer your own auth/middleware over it:
//!
//! ```text
//! GET /topics/{topic}/partitions/{partition}/ws   one partition
//! GET /topics/{topic}/ws                          all partitions, merged
//!     ?from=earliest|latest|<offset or cursor>   start (default: latest)
//!     &key_prefix=<utf8>                 only records whose key starts so
//!     &header=<name>:<value>             only records with this header
//! ```
//!
//! Each record arrives as one JSON text frame (the same shape as the
//! SSE transport's `data`), carrying its partition and offset.
//! WebSocket has no `Last-Event-ID`, so resume is explicit: reconnect
//! with the resume token — `from=<next offset>` (partition streams) or
//! `from=<partition:next_offset,...>` (topic streams). Tokens mean
//! "start here" and are interchangeable with the SSE transport's event
//! ids. Parameter errors are rejected as plain HTTP responses before
//! the upgrade.
//!
//! Subscribe failures are plain HTTP errors before the upgrade: `403`
//! for a topic the hub's gate denies, `404` for a topic the source
//! does not have, `503` after shutdown, `502` for other source
//! trouble. If the stream fails *after* the upgrade (topic deleted,
//! auth revoked, error budget exhausted), the socket closes with code
//! `1008` (auth) or `1011` (everything else) and `kind: message` as
//! the close reason; a clean end closes with `1001` ("going away").
//!
//! Graceful shutdown: keep the [`WsState`] `Arc` you built the router
//! from and call [`WsState::shutdown`] when your server begins to
//! drain (e.g. from the future you hand to axum's
//! `with_graceful_shutdown`) — every pump stops, open sockets close
//! with `1001`, and new subscribes are refused with `503`.

use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use bytes::Bytes;
use serde::Deserialize;

use odradek_web_core::json::event_json;
use odradek_web_core::pump::{HubError, StreamError, StreamItem};
use odradek_web_core::{Filter, Hub, Position, SourceErrorKind, TopicPosition, cursor};

/// Everything needed to stand the router up, re-exported so embedders
/// depend on this crate alone; the full engine is under [`web_core`].
pub use odradek_web_core as web_core;
#[cfg(feature = "kafka")]
pub use odradek_web_core::{ClientConfig, KafkaSourceFactory};
pub use odradek_web_core::{PumpConfig, SourceFactory};

/// Shared state behind the routes: the hub, guarded for subscribe-time
/// mutation only (each socket runs lock-free once subscribed).
#[derive(Debug)]
pub struct WsState<F: SourceFactory> {
    hub: tokio::sync::Mutex<Hub<F>>,
}

impl<F: SourceFactory> WsState<F> {
    pub fn new(factory: F, config: PumpConfig) -> Arc<WsState<F>> {
        WsState::from_hub(Hub::new(factory, config))
    }

    /// Wrap a pre-built hub — the way in for hub-level options such as
    /// [`Hub::with_topic_gate`].
    pub fn from_hub(hub: Hub<F>) -> Arc<WsState<F>> {
        Arc::new(WsState {
            hub: tokio::sync::Mutex::new(hub),
        })
    }

    /// Stop every pump and refuse further subscribes with `503`; open
    /// sockets close with `1001` ("going away"). Call this from your
    /// server's graceful shutdown (axum's `with_graceful_shutdown`).
    pub async fn shutdown(&self) {
        self.hub.lock().await.shutdown().await;
    }
}

/// The HTTP status a failed subscribe maps to: gated topics are `403`,
/// missing ones `404`, a shut-down hub `503`, the rest `502`.
fn subscribe_status(error: &HubError) -> StatusCode {
    match error {
        HubError::Denied(_) => StatusCode::FORBIDDEN,
        HubError::ShutDown => StatusCode::SERVICE_UNAVAILABLE,
        HubError::Source(source) if source.kind == SourceErrorKind::NotFound => {
            StatusCode::NOT_FOUND
        }
        _ => StatusCode::BAD_GATEWAY,
    }
}

/// The WebSocket routes over `state`; merge into your own [`Router`].
pub fn router<F: SourceFactory>(state: Arc<WsState<F>>) -> Router {
    Router::new()
        .route(
            "/topics/{topic}/partitions/{partition}/ws",
            get(upgrade_partition::<F>),
        )
        .route("/topics/{topic}/ws", get(upgrade_topic::<F>))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct StreamParams {
    from: Option<String>,
    key_prefix: Option<String>,
    header: Option<String>,
}

impl StreamParams {
    fn position(&self) -> Result<Position, String> {
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

async fn upgrade_partition<F: SourceFactory>(
    State(state): State<Arc<WsState<F>>>,
    Path((topic, partition)): Path<(String, i32)>,
    Query(params): Query<StreamParams>,
    ws: WebSocketUpgrade,
) -> Result<Response, (StatusCode, String)> {
    // Validate and subscribe before upgrading, so failures are ordinary
    // HTTP errors a client can read.
    let position = params
        .position()
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let filter = params.filter().map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let subscription = state
        .hub
        .lock()
        .await
        .subscribe(&topic, partition, position, filter)
        .await
        .map_err(|e| (subscribe_status(&e), e.to_string()))?;

    Ok(ws.on_upgrade(move |socket| stream_events(socket, subscription.into_receiver())))
}

/// The whole topic, all partitions merged as one frame stream. Resume
/// is `from=<partition:next_offset,...>` — each frame carries its
/// partition and offset, so the client tracks its own cursor.
async fn upgrade_topic<F: SourceFactory>(
    State(state): State<Arc<WsState<F>>>,
    Path(topic): Path<String>,
    Query(params): Query<StreamParams>,
    ws: WebSocketUpgrade,
) -> Result<Response, (StatusCode, String)> {
    let position = match params.from.as_deref() {
        None | Some("latest") => TopicPosition::Latest,
        Some("earliest") => TopicPosition::Earliest,
        Some(raw) => {
            TopicPosition::Offsets(cursor::parse(raw).map_err(|e| (StatusCode::BAD_REQUEST, e))?)
        }
    };
    let filter = params.filter().map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let subscription = state
        .hub
        .lock()
        .await
        .subscribe_topic(&topic, position, filter)
        .await
        .map_err(|e| (subscribe_status(&e), e.to_string()))?;

    Ok(ws.on_upgrade(move |socket| stream_events(socket, subscription.into_receiver())))
}

/// Close code 1001: the endpoint is going away (a clean shutdown).
const CLOSE_GOING_AWAY: u16 = 1001;
/// Close code 1008: policy violation — used for auth failures.
const CLOSE_POLICY_VIOLATION: u16 = 1008;
/// Close code 1011: the server hit an unexpected condition.
const CLOSE_INTERNAL_ERROR: u16 = 1011;

/// The close frame for a stream that failed: `1008` for auth, `1011`
/// for everything else, with the kind and message as the reason
/// (truncated to the 123-byte close-reason limit).
fn close_frame(error: &StreamError) -> CloseFrame {
    let code = match error.kind {
        SourceErrorKind::Auth => CLOSE_POLICY_VIOLATION,
        _ => CLOSE_INTERNAL_ERROR,
    };
    let mut reason = format!("{}: {}", error.kind.as_str(), error.message);
    // A close reason holds at most 123 bytes; cut at a char boundary.
    if reason.len() > 123 {
        let mut end = 123;
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        reason.truncate(end);
    }
    CloseFrame {
        code,
        reason: reason.into(),
    }
}

async fn stream_events(mut socket: WebSocket, mut events: tokio::sync::mpsc::Receiver<StreamItem>) {
    loop {
        tokio::select! {
            item = events.recv() => match item {
                Some(Ok(event)) => {
                    let frame = Message::Text(event_json(&event).to_string().into());
                    if socket.send(frame).await.is_err() {
                        return;
                    }
                }
                // The stream failed; say why, then close.
                Some(Err(error)) => {
                    let _ = socket.send(Message::Close(Some(close_frame(&error)))).await;
                    return;
                }
                // The pump shut down cleanly; the bridge is going away.
                None => {
                    let frame = CloseFrame {
                        code: CLOSE_GOING_AWAY,
                        reason: "going away".into(),
                    };
                    let _ = socket.send(Message::Close(Some(frame))).await;
                    return;
                }
            },
            // Keep reading so pings are answered and closes are seen.
            message = socket.recv() => match message {
                None | Some(Ok(Message::Close(_))) | Some(Err(_)) => return,
                Some(Ok(_)) => {}
            },
        }
    }
}
