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
//! SSE transport's `data`), carrying its partition and offset. That
//! JSON is rendered once per record and shared by every socket reading
//! the partition, so a frame costs each socket a refcount bump.
//! WebSocket has no `Last-Event-ID`, so resume is explicit: reconnect
//! with `from=<next offset>` (partition streams) or
//! `from=<partition:next_offset,...>` (topic streams). Tokens mean
//! "start here" and are interchangeable with the SSE transport's event
//! ids. Parameter errors are rejected as plain HTTP responses before
//! the upgrade.
//!
//! Note the asymmetry with SSE, which sends each event's resume token
//! as its `id` and can therefore treat the token as opaque. This
//! transport sends no token: a client derives `from` by adding one to
//! the `offset` of the last frame it kept. That arithmetic is part of
//! this transport's contract, and it is the one place in the
//! constellation where a resume token's shape is not the server's alone
//! to change. Putting a resume field in the frame would close the gap —
//! deliberately not done yet, because it is a wire change to the
//! envelope SSE shares.
//!
//! Subscribe failures are plain HTTP errors before the upgrade: `403`
//! for a topic the hub's gate denies or an `Origin` the state's
//! [`OriginPolicy`] does not, `404` for a topic or partition the source
//! does not have, `503` after shutdown or at pump capacity, `502` for
//! other source trouble. If the stream fails *after* the upgrade (topic
//! deleted, auth revoked, error budget exhausted), the socket closes
//! with code `1008` (auth) or `1011` (everything else) and
//! `kind: message` as the close reason; a clean end closes with `1001`
//! ("going away"). The reason is the *classified* failure only — the
//! upstream's own error text goes to `tracing`.
//!
//! # Security
//!
//! **Cross-origin.** CORS does not apply to WebSockets: a browser will
//! happily let any page open a socket to any host, and it attaches the
//! victim's cookies to the handshake, so an embedder's cookie auth
//! authenticates the *attacker's* connection (CSWSH). The only thing
//! standing in the way is the `Origin` header, and checking it is this
//! crate's job, not a CORS layer's. [`WsState`] therefore **denies
//! cross-origin handshakes by default** — name the origins your own
//! pages are served from with [`OriginPolicy::allow`] before a browser
//! client will work. See [`OriginPolicy`] for what happens when the
//! header is absent.
//!
//! **Topics.** [`WsState::new`] takes the hub's topic gate as an
//! argument: these routes are as reachable as the port they are mounted
//! on, and the default should not be "every topic on the cluster".
//!
//! **CORS.** Do not put a permissive CORS layer over these routes to
//! "fix" anything — the response bodies are your Kafka records.
//!
//! See the crate README for connection limits and the memory formula.
//!
//! Graceful shutdown: keep the [`WsState`] `Arc` you built the router
//! from and call [`WsState::shutdown`] when your server begins to
//! drain (e.g. from the future you hand to axum's
//! `with_graceful_shutdown`) — every pump stops, open sockets close
//! with `1001`, and new subscribes are refused with `503`.

// This crate carries no `unsafe` block and has never needed one:
// forbid rather than deny, so the decision cannot be reversed by a
// local `allow` in a module nobody re-reads.
#![forbid(unsafe_code)]

use std::collections::HashSet;
use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::header::{HeaderName, ORIGIN, X_CONTENT_TYPE_OPTIONS};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::get;

use odradek_web_core::pump::{StreamError, StreamItem};
use odradek_web_core::{Hub, Rejection, RejectionKind, SharedHub, SourceErrorKind, StreamParams};

/// Everything needed to stand the router up, re-exported so embedders
/// depend on this crate alone; the full engine is under [`web_core`].
pub use odradek_web_core as web_core;
#[cfg(feature = "kafka")]
pub use odradek_web_core::{ClientConfig, KafkaSourceFactory};
pub use odradek_web_core::{PumpConfig, SourceFactory};

/// Which `Origin`s may complete a handshake.
///
/// A browser sends `Origin` on every WebSocket handshake it makes, and
/// a page cannot forge it. That makes the header a reliable answer to
/// "which site is this socket for", and this policy is where the answer
/// is enforced — *before* the upgrade, so a rejected page gets a `403`
/// and never a socket.
///
/// # The missing-header choice
///
/// Non-browser clients (`websocat`, a service, a load test) usually
/// send no `Origin` at all. [`allow`](OriginPolicy::allow) and
/// [`deny_cross_origin`](OriginPolicy::deny_cross_origin) **permit a
/// handshake with no `Origin`**, because the attack this defends
/// against — a page the victim visits opening a socket with the
/// victim's cookies — is a browser attack, and browsers always send the
/// header. Denying it would lock out every non-browser client while
/// stopping nothing.
///
/// That reasoning holds only while `Origin`-less requests are not
/// *privileged*. If your embedder authenticates by ambient credential —
/// a cookie, a client certificate, a source IP allowlist — such that a
/// request with no `Origin` is authorized as somebody, call
/// [`require_origin`](OriginPolicy::require_origin) and let those
/// clients send one.
#[derive(Debug, Clone)]
pub struct OriginPolicy {
    /// Lowercased origins that may connect; `None` means any.
    allowed: Option<HashSet<String>>,
    /// Whether a handshake carrying no `Origin` header is allowed.
    allow_missing: bool,
}

impl Default for OriginPolicy {
    fn default() -> OriginPolicy {
        OriginPolicy::deny_cross_origin()
    }
}

impl OriginPolicy {
    /// Refuse every browser origin (the default): only clients that
    /// send no `Origin` at all get through.
    ///
    /// This is the safe starting point, not a useful destination — it
    /// is what a bridge with no browser clients wants, and what a
    /// bridge that has not decided yet should do rather than serve
    /// them all.
    pub fn deny_cross_origin() -> OriginPolicy {
        OriginPolicy {
            allowed: Some(HashSet::new()),
            allow_missing: true,
        }
    }

    /// Allow exactly these origins, e.g. `["https://app.example.com"]`.
    ///
    /// Compared case-insensitively against the whole header value, so
    /// write the scheme and (if it is not the default) the port —
    /// `https://app.example.com:8443`. An origin is not a hostname: a
    /// bare `app.example.com` matches nothing.
    pub fn allow<I, S>(origins: I) -> OriginPolicy
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        OriginPolicy {
            allowed: Some(
                origins
                    .into_iter()
                    .map(|o| o.as_ref().trim().to_ascii_lowercase())
                    .collect(),
            ),
            allow_missing: true,
        }
    }

    /// Allow any origin — the explicit opt-out.
    ///
    /// Sound only where something else already decides who may connect
    /// *per request* (a gateway that authenticates the handshake and
    /// does not rely on ambient cookies). Otherwise this is exactly the
    /// hole: any page the user visits can open a socket to this bridge
    /// and read the topics it serves.
    pub fn any() -> OriginPolicy {
        OriginPolicy {
            allowed: None,
            allow_missing: true,
        }
    }

    /// Also refuse handshakes that carry no `Origin` header. Locks out
    /// non-browser clients; see the type docs for when that is the
    /// right trade.
    #[must_use]
    pub fn require_origin(mut self) -> OriginPolicy {
        self.allow_missing = false;
        self
    }

    /// Whether this handshake may proceed.
    fn permits(&self, origin: Option<&HeaderValue>) -> bool {
        match origin {
            None => self.allow_missing,
            // A non-UTF-8 Origin cannot match anything we were told to
            // allow, and no browser sends one.
            Some(value) => match (value.to_str(), &self.allowed) {
                (Err(_), _) => false,
                (Ok(_), None) => true,
                (Ok(raw), Some(allowed)) => allowed.contains(&raw.trim().to_ascii_lowercase()),
            },
        }
    }
}

/// Shared state behind the routes: the hub, guarded for subscribe-time
/// mutation only (each socket runs lock-free once subscribed), and the
/// [`OriginPolicy`] every handshake is checked against.
#[derive(Debug)]
pub struct WsState<F: SourceFactory> {
    hub: SharedHub<F>,
    origins: OriginPolicy,
}

impl<F: SourceFactory> WsState<F> {
    /// State over a hub serving exactly the topics `gate` approves,
    /// denying cross-origin handshakes.
    ///
    /// Both defaults are deliberate: the gate is an argument because
    /// these routes are reachable by whoever can reach the port, and
    /// the origin policy starts at
    /// [`OriginPolicy::deny_cross_origin`] because CORS will not save
    /// a WebSocket route. For browser clients, build the hub yourself
    /// and use [`WsState::from_hub_with_origins`].
    ///
    /// ```no_run
    /// # use odradek_web_ws::{PumpConfig, WsState, router};
    /// # fn demo<F: odradek_web_ws::SourceFactory>(factory: F) {
    /// let state = WsState::new(factory, PumpConfig::default(), |topic| {
    ///     topic.starts_with("public.")
    /// });
    /// let app = router(state);
    /// # }
    /// ```
    pub fn new(
        factory: F,
        config: PumpConfig,
        gate: impl Fn(&str) -> bool + Send + Sync + 'static,
    ) -> Arc<WsState<F>> {
        WsState::from_hub(Hub::new(factory, config).with_topic_gate(gate))
    }

    /// Wrap a pre-built hub — the way in for hub-level options such as
    /// [`Hub::with_max_pumps`] — denying cross-origin handshakes.
    pub fn from_hub(hub: Hub<F>) -> Arc<WsState<F>> {
        WsState::from_hub_with_origins(hub, OriginPolicy::default())
    }

    /// Wrap a pre-built hub and say which origins may connect.
    ///
    /// ```no_run
    /// # use odradek_web_ws::{OriginPolicy, PumpConfig, WsState};
    /// # use odradek_web_ws::web_core::Hub;
    /// # fn demo<F: odradek_web_ws::SourceFactory>(factory: F) {
    /// let hub = Hub::new(factory, PumpConfig::default())
    ///     .with_topic_gate(|topic| topic.starts_with("public."));
    /// let state = WsState::from_hub_with_origins(
    ///     hub,
    ///     OriginPolicy::allow(["https://app.example.com"]),
    /// );
    /// # }
    /// ```
    pub fn from_hub_with_origins(hub: Hub<F>, origins: OriginPolicy) -> Arc<WsState<F>> {
        Arc::new(WsState {
            hub: SharedHub::from_hub(hub),
            origins,
        })
    }

    /// Stop every pump and refuse further subscribes with `503`; open
    /// sockets close with `1001` ("going away"). Call this from your
    /// server's graceful shutdown (axum's `with_graceful_shutdown`).
    pub async fn shutdown(&self) {
        self.hub.shutdown().await;
    }
}

/// An error response: a status, the hardening headers, and a body.
type HttpError = (StatusCode, Headers, String);

/// The response headers every route sets.
type Headers = [(HeaderName, &'static str); 1];

/// `X-Content-Type-Options: nosniff` — defense in depth for the
/// `text/plain` bodies of pre-upgrade errors, which can echo the
/// client's own parameters back.
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

/// The most a client may send us in one message.
///
/// This protocol is send-only: a client's frames exist to keep the
/// connection alive (pings, the close handshake), and nothing it sends
/// is read. Without a limit, tungstenite's defaults let every socket
/// buffer up to 64 MiB of data we would then throw away — free memory
/// for anyone who can complete a handshake. 4 KiB is far more than a
/// control frame's 125 bytes and far less than a useful amplifier.
const MAX_CLIENT_MESSAGE: usize = 4 * 1024;

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

/// Refuse a handshake the [`OriginPolicy`] does not allow, before the
/// hub is touched: a cross-origin page gets a `403` and no socket, no
/// subscription, and no pump.
fn check_origin<F: SourceFactory>(
    state: &WsState<F>,
    headers_in: &HeaderMap,
) -> Result<(), HttpError> {
    if state.origins.permits(headers_in.get(ORIGIN)) {
        return Ok(());
    }
    Err((
        StatusCode::FORBIDDEN,
        headers(),
        "origin not allowed".to_owned(),
    ))
}

/// The handshake response, with the same hardening header the error
/// paths set.
fn upgrade(ws: WebSocketUpgrade, receiver: tokio::sync::mpsc::Receiver<StreamItem>) -> Response {
    let mut response = ws
        .max_message_size(MAX_CLIENT_MESSAGE)
        .max_frame_size(MAX_CLIENT_MESSAGE)
        .on_upgrade(move |socket| stream_events(socket, receiver));
    response
        .headers_mut()
        .insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    response
}

async fn upgrade_partition<F: SourceFactory>(
    State(state): State<Arc<WsState<F>>>,
    Path((topic, partition)): Path<(String, i32)>,
    Query(params): Query<StreamParams>,
    headers_in: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, HttpError> {
    check_origin(&state, &headers_in)?;
    // Validate and subscribe before upgrading, so failures are ordinary
    // HTTP errors a client can read.
    let subscription = state
        .hub
        .stream(&topic, partition, &params, None)
        .await
        .map_err(reject)?;

    Ok(upgrade(ws, subscription.into_receiver()))
}

/// The whole topic, all partitions merged as one frame stream. Resume
/// is `from=<partition:next_offset,...>` — each frame carries its
/// partition and offset, so the client tracks its own cursor.
async fn upgrade_topic<F: SourceFactory>(
    State(state): State<Arc<WsState<F>>>,
    Path(topic): Path<String>,
    Query(params): Query<StreamParams>,
    headers_in: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, HttpError> {
    check_origin(&state, &headers_in)?;
    let (subscription, _) = state
        .hub
        .stream_topic(&topic, &params, None)
        .await
        .map_err(reject)?;

    Ok(upgrade(ws, subscription.into_receiver()))
}

/// Close code 1001: the endpoint is going away (a clean shutdown).
const CLOSE_GOING_AWAY: u16 = 1001;
/// Close code 1008: policy violation — used for auth failures.
const CLOSE_POLICY_VIOLATION: u16 = 1008;
/// Close code 1011: the server hit an unexpected condition.
const CLOSE_INTERNAL_ERROR: u16 = 1011;

/// The close frame for a stream that failed: `1008` for auth, `1011`
/// for everything else, with the kind and its fixed message as the
/// reason (truncated to the 123-byte close-reason limit).
///
/// The upstream's own text stays out of it — the pump logs that, and a
/// socket anyone can open is not the place for broker hostnames or ACL
/// state.
fn close_frame(error: &StreamError) -> CloseFrame {
    let code = match error.kind {
        SourceErrorKind::Auth => CLOSE_POLICY_VIOLATION,
        _ => CLOSE_INTERNAL_ERROR,
    };
    let mut reason = format!("{}: {}", error.kind, error.public_message());
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
                    // The JSON is rendered once per event, by whichever
                    // socket gets there first; this is a refcount bump
                    // plus the UTF-8 check `Utf8Bytes` insists on.
                    let body = Utf8Bytes::try_from(event.json_bytes().clone())
                        .expect("rendered json is utf-8");
                    if socket.send(Message::Text(body)).await.is_err() {
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
            // Anything else a client sends is discarded — bounded by
            // `MAX_CLIENT_MESSAGE`, set on the upgrade.
            message = socket.recv() => match message {
                None | Some(Ok(Message::Close(_))) | Some(Err(_)) => return,
                Some(Ok(_)) => {}
            },
        }
    }
}
