//! The partition pump: one task per (topic, partition) that owns the
//! source connection, keeps a ring of recent events, and feeds every
//! subscriber at its own pace.
//!
//! Backpressure is self-healing rather than lossy: a subscriber whose
//! queue fills simply falls out of the live path and is caught back up
//! from the ring (or, past the ring, from the source itself) as its
//! queue drains. Events are delivered in offset order with no gaps and
//! no duplicates, however slow the consumer.
//!
//! Fan-out is by [`SharedEvent`]: one event is read once, rendered (if
//! a transport asks) once, and handed to every subscriber as a refcount
//! bump — the per-subscriber cost of a live event is a channel send.
//!
//! Catch-up is *scheduled*, not unbounded, because it shares the loop
//! with the live fetch:
//!
//! - Subscribers whose cursor is still inside the ring are served from
//!   memory every iteration — no source call, so no effect on the live
//!   path.
//! - Subscribers that have fallen behind the ring need a source fetch,
//!   and the pump serves **at most one such fetch per iteration**,
//!   round-robin. Without that bound, K laggards put K broker round
//!   trips between consecutive live fetches, which widens the live gap,
//!   pushes more subscribers out of the ring, and spirals.
//!
//! The honest cost of the bound: when K subscribers are behind the
//! ring, each is served roughly every K iterations, so deep replays
//! finish slower the more of them there are (they still lose nothing —
//! this is latency, not loss). The live path pays one catch-up round
//! trip per iteration regardless of K, and a laggard in the ring pays
//! nothing at all.
//!
//! Lifecycle: dead subscribers (dropped receivers) are noticed every
//! loop iteration, not just on delivery, so a quiet topic is never
//! fetched for nobody; a pump with no subscribers for
//! [`PumpConfig::idle_shutdown`] exits, and the hub respawns it on the
//! next subscribe. A *permanent* source error ([`SourceError`] whose
//! kind is `NotFound` or `Auth`) stops the pump immediately; transient
//! errors retry with backoff until [`PumpConfig::max_consecutive_errors`]
//! runs out. Either way, every subscriber receives one final
//! [`StreamError`] item explaining why before its stream closes. A
//! clean stop ([`PumpHandle::shutdown`], reached via `Hub::shutdown`)
//! closes subscriber streams without an error item.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::event::{Filter, Position, SharedEvent};
use crate::source::{RecordSource, SourceBatch, SourceError, SourceErrorKind};

/// Tuning for one pump.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PumpConfig {
    /// Recent events kept in memory for catch-up without re-fetching.
    pub ring_capacity: usize,
    /// Per-subscriber queue depth; a full queue demotes the subscriber
    /// to catch-up, it never drops events.
    pub queue_capacity: usize,
    /// Pause after an empty live fetch (the source's own long-poll wait
    /// does the heavy lifting; this only paces busy in-memory sources).
    pub idle_poll: Duration,
    /// Pause after a transient source error.
    pub error_backoff: Duration,
    /// Consecutive transient source errors before the pump gives up and
    /// closes every subscription with a terminal [`StreamError`].
    /// Permanent errors (`NotFound`, `Auth`) skip the budget entirely.
    pub max_consecutive_errors: u32,
    /// How long a pump lingers with zero subscribers before exiting
    /// (the hub respawns it on the next subscribe). `None` keeps idle
    /// pumps alive forever.
    pub idle_shutdown: Option<Duration>,
}

impl Default for PumpConfig {
    fn default() -> Self {
        PumpConfig {
            ring_capacity: 1024,
            queue_capacity: 256,
            idle_poll: Duration::from_millis(10),
            error_backoff: Duration::from_millis(500),
            max_consecutive_errors: 20,
            idle_shutdown: Some(Duration::from_secs(30)),
        }
    }
}

/// Errors surfaced to subscribers at subscribe time.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HubError {
    #[error("source error: {0}")]
    Source(#[from] SourceError),
    #[error("the pump for this partition has shut down")]
    PumpClosed,
    #[error("the hub has been shut down")]
    ShutDown,
    #[error("access to topic {0:?} denied")]
    Denied(String),
}

/// The transport-facing classification of a refused subscribe — what
/// to tell the client, without re-deriving it from [`HubError`]'s
/// shape. Transports map these straight to their status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RejectionKind {
    /// The request's own parameters were malformed (bad `from`, bad
    /// filter). Produced only by the
    /// [`SharedHub`](crate::hub::SharedHub) front door — never by
    /// [`HubError::rejection_kind`], which classifies subscribe
    /// failures, not parse failures.
    BadRequest,
    /// The hub's topic gate refused this topic (HTTP `403`).
    Denied,
    /// The source does not have this topic or partition (HTTP `404`).
    NotFound,
    /// The hub has shut down and refuses new subscribes (HTTP `503`).
    ShutDown,
    /// The source failed in some other way (HTTP `502`).
    Upstream,
}

impl HubError {
    /// How a transport should refuse the subscribe this error failed.
    pub fn rejection_kind(&self) -> RejectionKind {
        match self {
            HubError::Denied(_) => RejectionKind::Denied,
            HubError::ShutDown => RejectionKind::ShutDown,
            HubError::Source(source) if source.kind == SourceErrorKind::NotFound => {
                RejectionKind::NotFound
            }
            _ => RejectionKind::Upstream,
        }
    }
}

/// Why a stream ended, delivered as the final item before the channel
/// closes. Streams that end *cleanly* (pump or hub shutdown) close
/// without one.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{kind}: {message}")]
#[non_exhaustive]
pub struct StreamError {
    pub kind: SourceErrorKind,
    pub message: String,
}

impl From<SourceError> for StreamError {
    fn from(e: SourceError) -> StreamError {
        StreamError {
            kind: e.kind,
            message: e.message,
        }
    }
}

/// What a subscriber's channel carries: events until the stream ends,
/// with an optional final error explaining an abnormal end.
///
/// The event is a [`SharedEvent`] — one allocation shared by every
/// subscriber of the partition. It derefs to
/// [`Event`](crate::event::Event), so reading fields is unchanged.
pub type StreamItem = Result<SharedEvent, StreamError>;

/// A live subscription: a stream of [`SharedEvent`]s in offset order,
/// possibly ending with one [`StreamError`].
#[derive(Debug)]
pub struct Subscription {
    pub topic: String,
    pub partition: i32,
    receiver: mpsc::Receiver<StreamItem>,
}

impl Subscription {
    /// The next event; `Some(Err(_))` is the final item of a stream
    /// that failed, `None` means the stream ended cleanly (or after
    /// that error).
    pub async fn recv(&mut self) -> Option<StreamItem> {
        self.receiver.recv().await
    }

    /// The underlying receiver, for `select!`-style composition.
    pub fn into_receiver(self) -> mpsc::Receiver<StreamItem> {
        self.receiver
    }
}

pub(crate) enum Command {
    Subscribe {
        position: Position,
        filter: Filter,
        reply: oneshot::Sender<Result<Subscription, HubError>>,
    },
    /// Stop cleanly: subscriber streams close without an error item.
    Shutdown,
}

/// A handle to a running pump; cheap to clone. Dropping every handle
/// shuts the pump down once its subscribers are gone.
#[derive(Debug, Clone)]
pub struct PumpHandle {
    topic: String,
    partition: i32,
    commands: mpsc::Sender<Command>,
}

impl PumpHandle {
    /// Spawn a pump over `source` and return its handle.
    pub fn spawn<S: RecordSource>(
        source: S,
        topic: &str,
        partition: i32,
        config: PumpConfig,
    ) -> PumpHandle {
        let (commands, command_rx) = mpsc::channel(16);
        tokio::spawn(run_pump(
            source,
            topic.to_owned(),
            partition,
            config,
            command_rx,
        ));
        PumpHandle {
            topic: topic.to_owned(),
            partition,
            commands,
        }
    }

    /// Subscribe from `position`, seeing only events `filter` matches.
    pub async fn subscribe(
        &self,
        position: Position,
        filter: Filter,
    ) -> Result<Subscription, HubError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Subscribe {
                position,
                filter,
                reply,
            })
            .await
            .map_err(|_| HubError::PumpClosed)?;
        response.await.map_err(|_| HubError::PumpClosed)?
    }

    /// Stop the pump cleanly: remaining subscriber streams close
    /// without an error item. Idempotent; a no-op on a dead pump.
    pub async fn shutdown(&self) {
        let _ = self.commands.send(Command::Shutdown).await;
    }

    /// True when both handles drive the same pump task — how the hub
    /// tells "the pump I saw" from "the replacement another subscriber
    /// installed while I was unlocked".
    pub fn same_pump(&self, other: &PumpHandle) -> bool {
        self.commands.same_channel(&other.commands)
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Replaying toward the live edge; `cursor` is the next offset owed.
    CatchingUp { cursor: i64 },
    /// At the edge: new events are pushed as they arrive.
    Live,
}

struct SubState {
    /// Identity within this pump, for round-robin catch-up service.
    id: u64,
    sender: mpsc::Sender<StreamItem>,
    filter: Filter,
    mode: Mode,
    closed: bool,
}

impl SubState {
    /// Push one event, demoting to catch-up when the queue is full.
    /// The filter runs before the handle is cloned, so a subscriber
    /// that does not want the event costs nothing but the test.
    fn push_live(&mut self, event: &SharedEvent) {
        if self.mode != Mode::Live || self.closed {
            return;
        }
        if !self.filter.matches(event) {
            return;
        }
        match self.sender.try_send(Ok(event.clone())) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.mode = Mode::CatchingUp {
                    cursor: event.offset,
                };
            }
            Err(mpsc::error::TrySendError::Closed(_)) => self.closed = true,
        }
    }

    /// True when this subscriber can only be advanced by a source fetch:
    /// it is behind the live edge, its cursor predates the ring, and it
    /// has room for what the fetch would bring. A subscriber whose queue
    /// is full would stall on the first record, so spending the
    /// iteration's one fetch on it would waste the turn.
    fn needs_fetch(&self, ring_start: Option<i64>, live_edge: i64) -> bool {
        if self.closed || self.sender.capacity() == 0 {
            return false;
        }
        match self.mode {
            Mode::Live => false,
            Mode::CatchingUp { cursor } => {
                cursor < live_edge && ring_start.is_none_or(|start| cursor < start)
            }
        }
    }
}

/// The pump's subscribers, plus the bookkeeping that keeps catch-up
/// fair: ids to take turns by, and the turn to resume from.
#[derive(Default)]
struct Subscribers {
    list: Vec<SubState>,
    next_id: u64,
    /// The id the next out-of-ring catch-up round starts looking from.
    rotor: u64,
}

impl Subscribers {
    fn add(&mut self, sender: mpsc::Sender<StreamItem>, filter: Filter, mode: Mode) {
        let id = self.next_id;
        self.next_id += 1;
        self.list.push(SubState {
            id,
            sender,
            filter,
            mode,
            closed: false,
        });
    }

    /// Drop subscribers whose receivers are gone.
    fn retain_live(&mut self) {
        self.list.retain(|s| !s.closed && !s.sender.is_closed());
    }

    fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Take every subscriber out, for a terminal error.
    fn take(&mut self) -> Vec<SubState> {
        std::mem::take(&mut self.list)
    }

    /// Whose turn it is for the iteration's single catch-up fetch:
    /// the next one owed a fetch at or after the rotor, wrapping to the
    /// start, so no laggard starves behind another.
    fn next_fetch(&mut self, ring_start: Option<i64>, live_edge: i64) -> Option<usize> {
        let rotor = self.rotor;
        let (index, id) = self
            .list
            .iter()
            .enumerate()
            .filter(|(_, sub)| sub.needs_fetch(ring_start, live_edge))
            // `false < true`: ids from the rotor on come first, and the
            // search wraps to the lowest id only if none remain.
            .min_by_key(|(_, sub)| (sub.id < rotor, sub.id))
            .map(|(index, sub)| (index, sub.id))?;
        self.rotor = id + 1;
        Some(index)
    }
}

/// Deliver a terminal error to every subscriber, each on its own task
/// so one stalled queue cannot delay the others, then let the channels
/// close.
fn fail_subs(subs: Vec<SubState>, error: &SourceError) {
    let error = StreamError::from(error.clone());
    for sub in subs {
        if sub.closed {
            continue;
        }
        let sender = sub.sender;
        let error = error.clone();
        tokio::spawn(async move {
            let _ = sender.send(Err(error)).await;
        });
    }
}

async fn run_pump<S: RecordSource>(
    mut source: S,
    topic: String,
    partition: i32,
    config: PumpConfig,
    mut commands: mpsc::Receiver<Command>,
) {
    let mut ring: VecDeque<SharedEvent> = VecDeque::new();
    let mut subs = Subscribers::default();
    // The next offset the live fetch reads; None until first needed.
    let mut live_cursor: Option<i64> = None;
    let mut consecutive_errors = 0u32;

    loop {
        // Notice dead subscribers every iteration — not just on
        // delivery — so a quiet topic is never fetched for nobody.
        subs.retain_live();

        // With nobody listening, sit idle on the command channel; give
        // up entirely after `idle_shutdown`.
        if subs.is_empty() {
            let command = match config.idle_shutdown {
                Some(idle) => match tokio::time::timeout(idle, commands.recv()).await {
                    Ok(command) => command,
                    Err(_) => {
                        tracing::debug!(topic, partition, "pump idle too long; exiting");
                        return;
                    }
                },
                None => commands.recv().await,
            };
            match command {
                Some(Command::Shutdown) | None => return,
                Some(cmd) => {
                    handle_command(cmd, &mut source, &topic, partition, &config, &mut subs).await;
                }
            }
        }
        while let Ok(cmd) = commands.try_recv() {
            if matches!(cmd, Command::Shutdown) {
                // Drop the senders without an error item: a clean end.
                return;
            }
            handle_command(cmd, &mut source, &topic, partition, &config, &mut subs).await;
        }

        // Establish where "live" starts.
        let cursor = match live_cursor {
            Some(c) => c,
            None => match source.latest_offset(&topic, partition).await {
                Ok(latest) => {
                    live_cursor = Some(latest);
                    latest
                }
                Err(e) => {
                    tracing::warn!(topic, partition, error = %e, "latest offset lookup failed");
                    consecutive_errors += 1;
                    if e.is_permanent() || consecutive_errors > config.max_consecutive_errors {
                        fail_subs(subs.take(), &e);
                        return;
                    }
                    tokio::time::sleep(config.error_backoff).await;
                    continue;
                }
            },
        };

        // One live fetch: extend the ring, push to live subscribers.
        match source.fetch(&topic, partition, cursor).await {
            Ok(SourceBatch {
                events,
                next_offset,
                ..
            }) => {
                consecutive_errors = 0;
                let empty = events.is_empty();
                for event in events {
                    // Read once, shared by everyone: each subscriber
                    // costs a filter test and a refcount bump.
                    let event = SharedEvent::new(event);
                    for sub in &mut subs.list {
                        sub.push_live(&event);
                    }
                    ring.push_back(event);
                    while ring.len() > config.ring_capacity {
                        ring.pop_front();
                    }
                }
                live_cursor = Some(next_offset.max(cursor));
                if empty {
                    tokio::time::sleep(config.idle_poll).await;
                }
            }
            Err(e) => {
                tracing::warn!(topic, partition, error = %e, "live fetch failed");
                consecutive_errors += 1;
                if e.is_permanent() || consecutive_errors > config.max_consecutive_errors {
                    fail_subs(subs.take(), &e);
                    return;
                }
                tokio::time::sleep(config.error_backoff).await;
            }
        }

        // Catch-up, bounded so it cannot stall the live path: serve
        // everyone the ring can serve (memory only), then spend one
        // source fetch on whichever laggard's turn it is.
        let live_edge = live_cursor.unwrap_or(0);
        let ring_start = ring.front().map(|e| e.offset);
        for sub in &mut subs.list {
            advance_from_ring(sub, &ring, live_edge);
        }
        if let Some(index) = subs.next_fetch(ring_start, live_edge) {
            let outcome =
                fetch_catch_up(&mut subs.list[index], &mut source, &topic, partition).await;
            if let Err(e) = outcome {
                fail_subs(subs.take(), &e);
                return;
            }
        }
    }
}

async fn handle_command<S: RecordSource>(
    cmd: Command,
    source: &mut S,
    topic: &str,
    partition: i32,
    config: &PumpConfig,
    subs: &mut Subscribers,
) {
    let Command::Subscribe {
        position,
        filter,
        reply,
    } = cmd
    else {
        return;
    };
    let mode = match position {
        Position::Latest => Ok(Mode::Live),
        Position::Offset(offset) => Ok(Mode::CatchingUp { cursor: offset }),
        Position::Earliest => source
            .earliest_offset(topic, partition)
            .await
            .map(|cursor| Mode::CatchingUp { cursor })
            .map_err(HubError::Source),
    };
    match mode {
        Ok(mode) => {
            let (sender, receiver) = mpsc::channel(config.queue_capacity);
            subs.add(sender, filter, mode);
            let _ = reply.send(Ok(Subscription {
                topic: topic.to_owned(),
                partition,
                receiver,
            }));
        }
        Err(e) => {
            let _ = reply.send(Err(e));
        }
    }
}

/// What happened while pushing a run of events to one subscriber.
enum PushOutcome {
    /// All delivered; cursor is past the run.
    Delivered(i64),
    /// The queue filled at this offset; resume there later.
    Stalled(i64),
    /// The subscriber is gone.
    Closed,
}

/// Push filtered `events` (already in offset order, and already
/// positioned at `cursor`) to one subscriber, advancing the cursor per
/// event.
fn push_run<'a>(
    sub: &mut SubState,
    events: impl Iterator<Item = &'a SharedEvent>,
    mut cursor: i64,
) -> PushOutcome {
    for event in events {
        if sub.filter.matches(event) {
            match sub.sender.try_send(Ok(event.clone())) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    return PushOutcome::Stalled(event.offset);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return PushOutcome::Closed,
            }
        }
        cursor = event.offset + 1;
    }
    PushOutcome::Delivered(cursor)
}

/// Advance one catching-up subscriber as far as the ring allows — pure
/// memory, so every laggard can have this every iteration. Subscribers
/// behind the ring are left for [`fetch_catch_up`].
fn advance_from_ring(sub: &mut SubState, ring: &VecDeque<SharedEvent>, live_edge: i64) {
    let Mode::CatchingUp { cursor } = sub.mode else {
        return;
    };
    if sub.closed {
        return;
    }
    if cursor >= live_edge {
        // Caught up (or asked for a future offset: wait for the live
        // edge to reach it rather than fetching past the log end).
        if cursor == live_edge {
            sub.mode = Mode::Live;
        }
        return;
    }
    // Behind the ring (or the ring is empty): only a fetch can help.
    if ring.front().is_none_or(|first| cursor < first.offset) {
        return;
    }
    // Inside the ring: serve the remainder from the cursor, then go
    // live — the ring always ends at the live edge. The ring is
    // offset-sorted, so finding the cursor is a binary search rather
    // than a scan over everything already sent.
    let from = ring.partition_point(|event| event.offset < cursor);
    match push_run(sub, ring.range(from..), cursor) {
        PushOutcome::Delivered(_) => sub.mode = Mode::Live,
        PushOutcome::Stalled(at) => sub.mode = Mode::CatchingUp { cursor: at },
        PushOutcome::Closed => sub.closed = true,
    }
}

/// Spend one source fetch on a subscriber that has fallen behind the
/// ring. Stops early (without losing its place) when the subscriber's
/// queue fills. A permanent source error is returned to end the whole
/// pump; a transient one is left for the next iteration's turn.
async fn fetch_catch_up<S: RecordSource>(
    sub: &mut SubState,
    source: &mut S,
    topic: &str,
    partition: i32,
) -> Result<(), SourceError> {
    let Mode::CatchingUp { cursor } = sub.mode else {
        return Ok(());
    };
    // The cursor advances through record-less stretches (compaction
    // gaps, control batches) via next_offset.
    match source.fetch(topic, partition, cursor).await {
        Ok(batch) => {
            let events: Vec<SharedEvent> = batch.events.into_iter().map(SharedEvent::new).collect();
            // Sources answer from `cursor`, but skipping any earlier
            // events they do return costs one binary search.
            let from = events.partition_point(|event| event.offset < cursor);
            match push_run(sub, events[from..].iter(), cursor) {
                PushOutcome::Delivered(done) => {
                    sub.mode = Mode::CatchingUp {
                        cursor: done.max(batch.next_offset),
                    };
                }
                PushOutcome::Stalled(at) => sub.mode = Mode::CatchingUp { cursor: at },
                PushOutcome::Closed => sub.closed = true,
            }
        }
        Err(e) if e.is_permanent() => return Err(e),
        Err(e) => {
            // Transient; the next pump iteration retries.
            tracing::debug!(topic, partition, error = %e, "catch-up fetch failed");
        }
    }
    Ok(())
}
