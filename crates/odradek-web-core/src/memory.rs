//! An in-memory [`RecordSource`]: a shared, appendable multi-partition
//! log.
//!
//! For tests and examples — both this crate's and those of anything
//! built on top: transports can exercise replay, live tailing,
//! topic-level merges, and backpressure without a broker.

use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::event::Event;
use crate::source::{RecordSource, SourceBatch, SourceError, SourceFactory};

/// A shared in-memory topic; offsets are per-partition indexes. Every
/// topic name reads the same log, which is plenty for tests.
#[derive(Debug, Clone)]
pub struct MemoryLog {
    partitions: Arc<Vec<Mutex<Vec<Event>>>>,
    /// Events returned per fetch, so catch-up takes several rounds like
    /// a real broker's bounded fetches.
    batch_limit: usize,
}

impl Default for MemoryLog {
    fn default() -> Self {
        MemoryLog::new()
    }
}

impl MemoryLog {
    /// A single-partition log.
    pub fn new() -> MemoryLog {
        MemoryLog::with_partitions(1)
    }

    /// A log with `count` partitions.
    pub fn with_partitions(count: usize) -> MemoryLog {
        MemoryLog {
            partitions: Arc::new((0..count.max(1)).map(|_| Mutex::new(Vec::new())).collect()),
            batch_limit: 3,
        }
    }

    /// Events returned per fetch (default 3, small on purpose so
    /// catch-up takes several rounds). Raise it for throughput work
    /// where the fetch count, not the fan-out, would dominate.
    #[must_use]
    pub fn with_batch_limit(mut self, limit: usize) -> MemoryLog {
        self.batch_limit = limit.max(1);
        self
    }

    /// Append one record to `partition`; returns its offset.
    pub fn append(
        &self,
        topic: &str,
        partition: i32,
        key: Option<&[u8]>,
        value: &[u8],
        headers: Vec<(String, Option<Bytes>)>,
    ) -> i64 {
        let mut events = self.partitions[usize::try_from(partition).unwrap_or(0)]
            .lock()
            .unwrap();
        let offset = i64::try_from(events.len()).unwrap_or(i64::MAX);
        events.push(Event {
            topic: topic.to_owned(),
            partition,
            offset,
            timestamp: 1_000 + offset,
            key: key.map(Bytes::copy_from_slice),
            value: Some(Bytes::copy_from_slice(value)),
            headers,
        });
        offset
    }

    /// A source reading this log.
    pub fn source(&self) -> MemorySource {
        MemorySource { log: self.clone() }
    }

    /// Records in `partition`.
    pub fn len(&self, partition: i32) -> usize {
        self.partitions[usize::try_from(partition).unwrap_or(0)]
            .lock()
            .unwrap()
            .len()
    }

    pub fn is_empty(&self, partition: i32) -> bool {
        self.len(partition) == 0
    }
}

/// A [`RecordSource`] over a [`MemoryLog`].
#[derive(Debug, Clone)]
pub struct MemorySource {
    log: MemoryLog,
}

impl MemorySource {
    fn partition(&self, partition: i32) -> Result<&Mutex<Vec<Event>>, SourceError> {
        usize::try_from(partition)
            .ok()
            .and_then(|p| self.log.partitions.get(p))
            .ok_or_else(|| SourceError::not_found(format!("no partition {partition}")))
    }
}

/// Offsets here are indexes into a `Vec`, which is as dense as a log
/// gets, so this adapter converts from exclusive positions the same way
/// the Kafka one does — and for the same reason: the arithmetic belongs
/// where the numbering is known, not above the trait.
impl RecordSource for MemorySource {
    async fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        after: Option<i64>,
    ) -> Result<SourceBatch, SourceError> {
        let events = self.partition(partition)?.lock().unwrap();
        let len = i64::try_from(events.len()).unwrap_or(i64::MAX);
        let from = after.map_or(0, |position| position + 1);
        let start = usize::try_from(from.clamp(0, len)).unwrap_or(usize::MAX);
        let batch: Vec<Event> = events
            .iter()
            .skip(start)
            .take(self.log.batch_limit)
            .map(|e| Event {
                topic: topic.to_owned(),
                ..e.clone()
            })
            .collect();
        Ok(SourceBatch {
            // The log has no gaps, so the position consumed is simply
            // the last event returned; an empty batch consumed nothing.
            next_after: batch.last().map(|e| e.offset),
            events: batch,
        })
    }

    async fn live_start(
        &mut self,
        _topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, SourceError> {
        let events = self.partition(partition)?.lock().unwrap();
        Ok(events.last().map(|e| e.offset))
    }
}

/// Hands every pump a view of the same [`MemoryLog`].
#[derive(Debug, Clone)]
pub struct MemoryFactory {
    log: MemoryLog,
    /// Topics that exist; `None` means every name reads the log (the
    /// historical behavior, plenty for most tests).
    known_topics: Option<Vec<String>>,
}

impl MemoryFactory {
    pub fn new(log: MemoryLog) -> MemoryFactory {
        MemoryFactory {
            log,
            known_topics: None,
        }
    }

    /// Restrict the factory to these topic names; anything else fails
    /// with a `NotFound` [`SourceError`], like a real broker's unknown
    /// topic.
    #[must_use]
    pub fn known_topics<I, S>(mut self, topics: I) -> MemoryFactory
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.known_topics = Some(topics.into_iter().map(Into::into).collect());
        self
    }

    fn check_topic(&self, topic: &str) -> Result<(), SourceError> {
        match &self.known_topics {
            Some(known) if !known.iter().any(|t| t == topic) => {
                Err(SourceError::not_found(format!("unknown topic {topic}")))
            }
            _ => Ok(()),
        }
    }
}

impl SourceFactory for MemoryFactory {
    type Source = MemorySource;

    async fn create(&self, topic: &str, _partition: i32) -> Result<MemorySource, SourceError> {
        self.check_topic(topic)?;
        Ok(self.log.source())
    }

    async fn partitions(&self, topic: &str) -> Result<Vec<i32>, SourceError> {
        self.check_topic(topic)?;
        Ok((0..i32::try_from(self.log.partitions.len()).unwrap_or(1)).collect())
    }
}
