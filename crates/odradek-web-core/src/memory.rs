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
            .ok_or_else(|| SourceError(format!("no partition {partition}")))
    }
}

impl RecordSource for MemorySource {
    async fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<SourceBatch, SourceError> {
        let events = self.partition(partition)?.lock().unwrap();
        let len = i64::try_from(events.len()).unwrap_or(i64::MAX);
        let start = usize::try_from(offset.clamp(0, len)).unwrap_or(usize::MAX);
        let batch: Vec<Event> = events
            .iter()
            .skip(start)
            .take(self.log.batch_limit)
            .map(|e| Event {
                topic: topic.to_owned(),
                ..e.clone()
            })
            .collect();
        let next_offset = offset.max(0) + i64::try_from(batch.len()).unwrap_or(0);
        Ok(SourceBatch {
            events: batch,
            next_offset,
            high_watermark: len,
        })
    }

    async fn earliest_offset(&mut self, _topic: &str, partition: i32) -> Result<i64, SourceError> {
        self.partition(partition)?;
        Ok(0)
    }

    async fn latest_offset(&mut self, _topic: &str, partition: i32) -> Result<i64, SourceError> {
        Ok(i64::try_from(self.partition(partition)?.lock().unwrap().len()).unwrap_or(i64::MAX))
    }
}

/// Hands every pump a view of the same [`MemoryLog`].
#[derive(Debug, Clone)]
pub struct MemoryFactory {
    pub log: MemoryLog,
}

impl SourceFactory for MemoryFactory {
    type Source = MemorySource;

    async fn create(&self, _topic: &str, _partition: i32) -> Result<MemorySource, SourceError> {
        Ok(self.log.source())
    }

    async fn partitions(&self, _topic: &str) -> Result<Vec<i32>, SourceError> {
        Ok((0..i32::try_from(self.log.partitions.len()).unwrap_or(1)).collect())
    }
}
