//! The partition pump: one task per (topic, partition) that owns the
//! source connection, keeps a ring of recent events, and feeds every
//! subscriber at its own pace.
//!
//! Backpressure is self-healing rather than lossy: a subscriber whose
//! queue fills simply falls out of the live path and is caught back up
//! from the ring (or, past the ring, from the source itself) as its
//! queue drains. Events are delivered in offset order with no gaps and
//! no duplicates, however slow the consumer.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::event::{Event, Filter, Position};
use crate::source::RecordSource;

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
    /// Pause after a source error.
    pub error_backoff: Duration,
    /// Consecutive source errors before the pump gives up and closes
    /// every subscription.
    pub max_consecutive_errors: u32,
}

impl Default for PumpConfig {
    fn default() -> Self {
        PumpConfig {
            ring_capacity: 1024,
            queue_capacity: 256,
            idle_poll: Duration::from_millis(10),
            error_backoff: Duration::from_millis(500),
            max_consecutive_errors: 20,
        }
    }
}

/// Errors surfaced to subscribers at subscribe time.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HubError {
    #[error("source error: {0}")]
    Source(String),
    #[error("the pump for this partition has shut down")]
    PumpClosed,
}

/// A live subscription: a stream of [`Event`]s in offset order.
#[derive(Debug)]
pub struct Subscription {
    pub topic: String,
    pub partition: i32,
    receiver: mpsc::Receiver<Event>,
}

impl Subscription {
    /// The next event, or `None` when the pump has shut down.
    pub async fn recv(&mut self) -> Option<Event> {
        self.receiver.recv().await
    }

    /// The underlying receiver, for `select!`-style composition.
    pub fn into_receiver(self) -> mpsc::Receiver<Event> {
        self.receiver
    }
}

pub(crate) enum Command {
    Subscribe {
        position: Position,
        filter: Filter,
        reply: oneshot::Sender<Result<Subscription, HubError>>,
    },
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
    sender: mpsc::Sender<Event>,
    filter: Filter,
    mode: Mode,
    closed: bool,
}

impl SubState {
    /// Push one event, demoting to catch-up when the queue is full.
    fn push_live(&mut self, event: &Event) {
        if self.mode != Mode::Live || self.closed {
            return;
        }
        if !self.filter.matches(event) {
            return;
        }
        match self.sender.try_send(event.clone()) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.mode = Mode::CatchingUp {
                    cursor: event.offset,
                };
            }
            Err(mpsc::error::TrySendError::Closed(_)) => self.closed = true,
        }
    }
}

async fn run_pump<S: RecordSource>(
    mut source: S,
    topic: String,
    partition: i32,
    config: PumpConfig,
    mut commands: mpsc::Receiver<Command>,
) {
    let mut ring: VecDeque<Event> = VecDeque::new();
    let mut subs: Vec<SubState> = Vec::new();
    // The next offset the live fetch reads; None until first needed.
    let mut live_cursor: Option<i64> = None;
    let mut consecutive_errors = 0u32;

    loop {
        // With nobody listening, sit idle on the command channel.
        if subs.is_empty() {
            match commands.recv().await {
                Some(cmd) => {
                    handle_command(cmd, &mut source, &topic, partition, &config, &mut subs).await;
                }
                None => return,
            }
        }
        while let Ok(cmd) = commands.try_recv() {
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
                    consecutive_errors += 1;
                    tracing::warn!(topic, partition, error = %e, "latest offset lookup failed");
                    if consecutive_errors > config.max_consecutive_errors {
                        return;
                    }
                    tokio::time::sleep(config.error_backoff).await;
                    continue;
                }
            },
        };

        // One live fetch: extend the ring, push to live subscribers.
        match source.fetch(&topic, partition, cursor).await {
            Ok(batch) => {
                consecutive_errors = 0;
                for event in &batch.events {
                    for sub in &mut subs {
                        sub.push_live(event);
                    }
                    ring.push_back(event.clone());
                    while ring.len() > config.ring_capacity {
                        ring.pop_front();
                    }
                }
                live_cursor = Some(batch.next_offset.max(cursor));
                if batch.events.is_empty() {
                    tokio::time::sleep(config.idle_poll).await;
                }
            }
            Err(e) => {
                consecutive_errors += 1;
                tracing::warn!(topic, partition, error = %e, "live fetch failed");
                if consecutive_errors > config.max_consecutive_errors {
                    return;
                }
                tokio::time::sleep(config.error_backoff).await;
            }
        }

        // Advance every catching-up subscriber by one step.
        let live_edge = live_cursor.unwrap_or(0);
        for sub in &mut subs {
            advance_catch_up(sub, &mut source, &topic, partition, &ring, live_edge).await;
        }
        subs.retain(|s| !s.closed);
    }
}

async fn handle_command<S: RecordSource>(
    cmd: Command,
    source: &mut S,
    topic: &str,
    partition: i32,
    config: &PumpConfig,
    subs: &mut Vec<SubState>,
) {
    let Command::Subscribe {
        position,
        filter,
        reply,
    } = cmd;
    let mode = match position {
        Position::Latest => Ok(Mode::Live),
        Position::Offset(offset) => Ok(Mode::CatchingUp { cursor: offset }),
        Position::Earliest => source
            .earliest_offset(topic, partition)
            .await
            .map(|cursor| Mode::CatchingUp { cursor })
            .map_err(|e| HubError::Source(e.to_string())),
    };
    match mode {
        Ok(mode) => {
            let (sender, receiver) = mpsc::channel(config.queue_capacity);
            subs.push(SubState {
                sender,
                filter,
                mode,
                closed: false,
            });
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

/// Push filtered `events` (already in offset order) starting at
/// `cursor`, advancing it per event.
fn push_run<'a>(
    sub: &mut SubState,
    events: impl Iterator<Item = &'a Event>,
    mut cursor: i64,
) -> PushOutcome {
    for event in events {
        if event.offset < cursor {
            continue;
        }
        if sub.filter.matches(event) {
            match sub.sender.try_send(event.clone()) {
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

/// Move one catching-up subscriber forward: from the ring when its
/// cursor is inside it, else by one direct fetch from the source. Stops
/// early (without losing its place) when the subscriber's queue fills.
async fn advance_catch_up<S: RecordSource>(
    sub: &mut SubState,
    source: &mut S,
    topic: &str,
    partition: i32,
    ring: &VecDeque<Event>,
    live_edge: i64,
) {
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

    let ring_start = ring.front().map(|e| e.offset);
    if ring_start.is_some_and(|start| cursor >= start) {
        // Inside the ring: serve the remainder, then go live — the ring
        // always ends at the live edge.
        match push_run(sub, ring.iter(), cursor) {
            PushOutcome::Delivered(_) => sub.mode = Mode::Live,
            PushOutcome::Stalled(at) => sub.mode = Mode::CatchingUp { cursor: at },
            PushOutcome::Closed => sub.closed = true,
        }
        return;
    }

    // Behind the ring (or the ring is empty): fetch on the subscriber's
    // behalf. The cursor still advances through record-less stretches
    // (compaction gaps, control batches) via next_offset.
    match source.fetch(topic, partition, cursor).await {
        Ok(batch) => match push_run(sub, batch.events.iter(), cursor) {
            PushOutcome::Delivered(done) => {
                sub.mode = Mode::CatchingUp {
                    cursor: done.max(batch.next_offset),
                };
            }
            PushOutcome::Stalled(at) => sub.mode = Mode::CatchingUp { cursor: at },
            PushOutcome::Closed => sub.closed = true,
        },
        Err(e) => {
            // Transient; the next pump iteration retries.
            tracing::debug!(topic, partition, error = %e, "catch-up fetch failed");
        }
    }
}
