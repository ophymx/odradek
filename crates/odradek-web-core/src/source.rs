//! Where events come from: an offset-addressed record source.
//!
//! The pump is written against this trait so the engine tests run
//! against an in-memory log; `KafkaSource` adapts `odradek_client`'s
//! `Consumer` for the real thing — both behind the `kafka` feature,
//! hence named rather than linked.

use std::future::Future;

#[cfg(feature = "kafka")]
use odradek_client::{ClientConfig, Cluster, Consumer};

use crate::event::Event;

/// A fetch's worth of events plus the position that follows it.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SourceBatch {
    pub events: Vec<Event>,
    /// What to pass as `after` on the next fetch.
    ///
    /// Normally the last event's position, but it may be further on:
    /// a stretch of the log that yields no events (compaction gaps,
    /// control batches, records the source filtered) still has to
    /// advance the cursor, or a subscriber fetches the same empty
    /// stretch forever. `None` means the fetch consumed nothing and
    /// the caller's position stands.
    pub next_after: Option<i64>,
    pub high_watermark: i64,
}

/// What broke, coarsely — the part of a source error the pump and the
/// transports act on.
///
/// [`NotFound`](SourceErrorKind::NotFound) and
/// [`Auth`](SourceErrorKind::Auth) are *permanent*: the pump stops at
/// once and tells its subscribers why. Everything else is transient and
/// retried with backoff until the configured error budget runs out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SourceErrorKind {
    /// The topic or partition does not exist.
    NotFound,
    /// The source refused the bridge's credentials for this resource.
    Auth,
    /// The source cannot be reached right now (connection, timeout,
    /// bootstrap).
    Unavailable,
    /// Anything else.
    Other,
}

impl SourceErrorKind {
    /// A stable lowercase name, for wire formats (JSON, close reasons).
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceErrorKind::NotFound => "not_found",
            SourceErrorKind::Auth => "auth",
            SourceErrorKind::Unavailable => "unavailable",
            _ => "other",
        }
    }

    /// The fixed, source-independent sentence a *client* is told.
    ///
    /// [`SourceError::message`] is for operators: it is the upstream's
    /// own text, and a real cluster puts broker hostnames, ports,
    /// protocol error codes, and ACL state in it. Transports send this
    /// instead — the kind is the whole contract — and leave the detail
    /// to `tracing`.
    pub fn public_message(&self) -> &'static str {
        match self {
            SourceErrorKind::NotFound => "topic or partition not found",
            SourceErrorKind::Auth => "not authorized for this topic",
            SourceErrorKind::Unavailable => "upstream unavailable",
            _ => "upstream error",
        }
    }
}

impl std::fmt::Display for SourceErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An error from a source, carrying the [`SourceErrorKind`] that
/// decides whether the pump retries (transient) or stops and tells its
/// subscribers (permanent).
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
#[non_exhaustive]
pub struct SourceError {
    pub kind: SourceErrorKind,
    pub message: String,
}

impl SourceError {
    pub fn new(kind: SourceErrorKind, message: impl Into<String>) -> SourceError {
        SourceError {
            kind,
            message: message.into(),
        }
    }

    /// The topic or partition does not exist (permanent).
    pub fn not_found(message: impl Into<String>) -> SourceError {
        SourceError::new(SourceErrorKind::NotFound, message)
    }

    /// The source refused the bridge's credentials (permanent).
    pub fn auth(message: impl Into<String>) -> SourceError {
        SourceError::new(SourceErrorKind::Auth, message)
    }

    /// The source cannot be reached right now (transient).
    pub fn unavailable(message: impl Into<String>) -> SourceError {
        SourceError::new(SourceErrorKind::Unavailable, message)
    }

    /// Anything else (transient).
    pub fn other(message: impl Into<String>) -> SourceError {
        SourceError::new(SourceErrorKind::Other, message)
    }

    /// True when retrying can never help: the pump stops immediately.
    pub fn is_permanent(&self) -> bool {
        matches!(self.kind, SourceErrorKind::NotFound | SourceErrorKind::Auth)
    }
}

/// Classify an [`odradek_client::ClientError`] into the kinds the pump
/// acts on. The client owns the taxonomy
/// ([`ErrorCategory`](odradek_client::ErrorCategory)); this impl only
/// carries it across the trait boundary.
#[cfg(feature = "kafka")]
impl From<odradek_client::ClientError> for SourceError {
    fn from(e: odradek_client::ClientError) -> SourceError {
        use odradek_client::ErrorCategory;

        let kind = match e.category() {
            ErrorCategory::NotFound => SourceErrorKind::NotFound,
            ErrorCategory::Auth => SourceErrorKind::Auth,
            ErrorCategory::Unavailable => SourceErrorKind::Unavailable,
            _ => SourceErrorKind::Other,
        };
        SourceError::new(kind, e.to_string())
    }
}

/// An offset-addressed stream of records for one partition.
///
/// # Positions are exclusive
///
/// Every position in this trait names a record that has *already been
/// seen*, and asks for what comes after it. `None` is the position
/// before the oldest record the source still holds, so a full replay
/// is `fetch(.., None)` and needs no lookup to begin.
///
/// The engine therefore never computes one position from another: it
/// says "after the event I just delivered" and hands back exactly the
/// position the source gave it. That is what lets a source number its
/// records however it likes — sparsely, with gaps, in strides — rather
/// than densely like Kafka, where the next record after `n` happens to
/// be at `n + 1`. Adapters over a dense log do that arithmetic
/// themselves, where the assumption is a known fact about the store
/// rather than a guess about positions in general.
///
/// Methods return `impl Future + Send` (rather than plain `async fn`) so
/// pumps holding a source can be spawned.
///
/// # Implementing this trait
///
/// Two things to know before you write one, neither of which the
/// signatures tell you.
///
/// **It is not dyn-compatible.** Returning `impl Future` from a trait
/// method rules out `Box<dyn RecordSource>`, so a program that picks its
/// source at runtime — Kafka here, something else there, by
/// configuration — cannot do it with a trait object. Wrap the
/// alternatives in an enum and implement this trait on the enum,
/// forwarding each method. That costs a match per call, against a
/// network round trip.
///
/// **New methods will arrive with defaults.** Adding a required method
/// to a trait breaks every implementor, and the implementors of this one
/// are, by design, code this project cannot see. Anything added here
/// later will have a default body, even where that default is worse than
/// what an implementor could write. `Message::decode_with_limits` in
/// `odradek-protocol` is the pattern already in use, and says so where
/// it is defined.
///
/// **There is a worked example of a source that is not dense.**
/// `tests/sparse_source.rs` implements this trait over a byte-addressed
/// log — positions are byte offsets, so they stride by record length,
/// start after a header rather than at zero, and leave gaps where an
/// entry carries no event. It is written from outside the crate against
/// the public API only, which is what makes it evidence rather than
/// decoration: both sources shipped here are dense, so until it existed
/// the claim in this doc had two witnesses that agreed with each other
/// and nothing that disagreed. It is also where
/// [`SourceBatch::next_after`] is held to anything — a dense source
/// sets that field to the last event's position, so an engine that
/// ignored it passes every test written against one.
pub trait RecordSource: Send + 'static {
    /// Records strictly after `after`, oldest first; `None` starts at
    /// the oldest record the source still holds.
    fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        after: Option<i64>,
    ) -> impl Future<Output = Result<SourceBatch, SourceError>> + Send;

    /// Where a [`Position::Latest`](crate::Position) subscription
    /// starts: every record the source already holds is at or before
    /// this position, so a subscriber resuming after it sees only what
    /// arrives from now on. `None` when the source holds nothing, which
    /// resumes at the beginning — the same place, for a log with no
    /// records in it.
    fn live_start(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> impl Future<Output = Result<Option<i64>, SourceError>> + Send;
}

/// Makes one source per pump; a pump owns its source exclusively.
pub trait SourceFactory: Send + Sync + 'static {
    type Source: RecordSource;

    /// A source for one (topic, partition).
    ///
    /// Returning an error here is the factory's veto: the hub creates
    /// no pump and caches nothing. An implementation *may* rely on the
    /// hub having already checked the pair against
    /// [`partitions`](SourceFactory::partitions) — the hub does that
    /// before every create, so that a request naming a topic or
    /// partition the source does not have can never cost a pump task —
    /// but a factory that can reject more cheaply, or that knows more
    /// than the metadata does, should still reject.
    fn create(
        &self,
        topic: &str,
        partition: i32,
    ) -> impl Future<Output = Result<Self::Source, SourceError>> + Send;

    /// The partitions `topic` currently has.
    ///
    /// This is the hub's existence check for *both* stream shapes: a
    /// topic-level subscribe fans out over the result, and a
    /// partition-level subscribe is refused with
    /// [`SourceErrorKind::NotFound`] unless the partition is in it. It
    /// is cached per topic, so it costs one round trip per topic, not
    /// one per request.
    fn partitions(&self, topic: &str)
    -> impl Future<Output = Result<Vec<i32>, SourceError>> + Send;
}

/// A [`RecordSource`] over a real Kafka cluster.
#[cfg(feature = "kafka")]
#[derive(Debug)]
pub struct KafkaSource {
    consumer: Consumer,
}

#[cfg(feature = "kafka")]
impl KafkaSource {
    pub fn new(consumer: Consumer) -> KafkaSource {
        KafkaSource { consumer }
    }
}

/// Kafka numbers its records densely, so the record after `n` is at
/// `n + 1` and the whole of this adapter's position arithmetic is those
/// three characters. It is correct *here*, where the offset space is a
/// known fact about the store, and would be a guess anywhere above the
/// trait — which is why the engine has none of it.
#[cfg(feature = "kafka")]
impl RecordSource for KafkaSource {
    async fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        after: Option<i64>,
    ) -> Result<SourceBatch, SourceError> {
        // Kafka fetches inclusively from an offset it must be given, so
        // a replay from the beginning has to ask where the beginning
        // currently is — the log start moves with retention, and
        // fetching below it is an error rather than a clamp.
        let from = match after {
            Some(position) => position + 1,
            None => self
                .consumer
                .earliest_offset(topic, partition)
                .await
                .map_err(SourceError::from)?,
        };
        // fetch_with, not fetch: an `Event` is what this bridge wants,
        // and asking for records first means building a whole
        // `Vec<ConsumedRecord>` in order to walk it once and drop it.
        let result = self
            .consumer
            .fetch_with(topic, partition, from, |r| {
                let mut event = Event::at(topic, partition, r.offset, r.timestamp);
                event.key = r.key;
                event.value = r.value;
                event.headers = r.headers.into_iter().map(|h| (h.key, h.value)).collect();
                event
            })
            .await
            .map_err(SourceError::from)?;
        Ok(SourceBatch {
            events: result.records,
            // `next_offset` is where Kafka would have us fetch next, so
            // the position already consumed is the one before it. A
            // fetch that advanced nothing leaves the caller's position
            // alone rather than reporting one below it.
            next_after: (result.next_offset > from).then(|| result.next_offset - 1),
            high_watermark: result.high_watermark,
        })
    }

    async fn live_start(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, SourceError> {
        // The high watermark is one past the newest record, so the
        // newest record is one before it. At zero there has never been
        // a record and there is nothing to resume after.
        let watermark = self
            .consumer
            .latest_offset(topic, partition)
            .await
            .map_err(SourceError::from)?;
        Ok((watermark > 0).then(|| watermark - 1))
    }
}

/// Sources over one shared [`Cluster`]: the first pump (or partition
/// lookup) dials it, and every pump's consumer then rides the same
/// connection pool and metadata cache.
#[cfg(feature = "kafka")]
#[derive(Debug, Clone)]
pub struct KafkaSourceFactory {
    config: ClientConfig,
    cluster: std::sync::Arc<tokio::sync::OnceCell<Cluster>>,
}

#[cfg(feature = "kafka")]
impl KafkaSourceFactory {
    pub fn new(config: ClientConfig) -> KafkaSourceFactory {
        KafkaSourceFactory {
            config,
            cluster: std::sync::Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    async fn cluster(&self) -> Result<Cluster, SourceError> {
        self.cluster
            .get_or_try_init(|| async {
                Cluster::connect(self.config.clone())
                    .await
                    .map_err(SourceError::from)
            })
            .await
            .cloned()
    }
}

#[cfg(feature = "kafka")]
impl SourceFactory for KafkaSourceFactory {
    type Source = KafkaSource;

    async fn create(&self, _topic: &str, _partition: i32) -> Result<KafkaSource, SourceError> {
        Ok(KafkaSource::new(Consumer::new(self.cluster().await?)))
    }

    async fn partitions(&self, topic: &str) -> Result<Vec<i32>, SourceError> {
        // The client owns unknown-topic semantics; its error maps to
        // `NotFound` through the `From` impl above.
        let mut partitions: Vec<i32> = self
            .cluster()
            .await?
            .topic_partitions(topic)
            .await
            .map_err(SourceError::from)?
            .iter()
            .map(|p| p.index)
            .collect();
        partitions.sort_unstable();
        Ok(partitions)
    }
}
