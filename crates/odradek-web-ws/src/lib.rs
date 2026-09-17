//! WebSocket transport over [`odradek_web_core`].
//!
//! [`router`] returns an embeddable [`axum::Router`] — mount it in your
//! own service and layer your own auth/middleware over it:
//!
//! ```text
//! GET /topics/{topic}/partitions/{partition}/ws
//!     ?from=earliest|latest|<offset>     start position (default: latest)
//!     &key_prefix=<utf8>                 only records whose key starts so
//!     &header=<name>:<value>             only records with this header
//! ```
//!
//! Each record arrives as one JSON text frame (the same shape as the
//! SSE transport's `data`), carrying its offset. WebSocket has no
//! `Last-Event-ID`, so resume is explicit: reconnect with
//! `from=<last offset + 1>`. Parameter errors are rejected as plain
//! HTTP responses before the upgrade.

use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use bytes::Bytes;
use serde::Deserialize;

use odradek_web_core::json::event_json;
use odradek_web_core::{Filter, Hub, Position, PumpConfig, SourceFactory, Subscription};

/// Shared state behind the routes: the hub, guarded for subscribe-time
/// mutation only (each socket runs lock-free once subscribed).
#[derive(Debug)]
pub struct WsState<F: SourceFactory> {
    hub: tokio::sync::Mutex<Hub<F>>,
}

impl<F: SourceFactory> WsState<F> {
    pub fn new(factory: F, config: PumpConfig) -> Arc<WsState<F>> {
        Arc::new(WsState {
            hub: tokio::sync::Mutex::new(Hub::new(factory, config)),
        })
    }
}

/// The WebSocket routes over `state`; merge into your own [`Router`].
pub fn router<F: SourceFactory>(state: Arc<WsState<F>>) -> Router {
    Router::new()
        .route(
            "/topics/{topic}/partitions/{partition}/ws",
            get(upgrade_partition::<F>),
        )
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
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;

    Ok(ws.on_upgrade(move |socket| stream_events(socket, subscription)))
}

async fn stream_events(mut socket: WebSocket, mut subscription: Subscription) {
    loop {
        tokio::select! {
            event = subscription.recv() => match event {
                Some(event) => {
                    let frame = Message::Text(event_json(&event).to_string().into());
                    if socket.send(frame).await.is_err() {
                        return;
                    }
                }
                // The pump shut down; tell the client cleanly.
                None => {
                    let _ = socket.send(Message::Close(None)).await;
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
