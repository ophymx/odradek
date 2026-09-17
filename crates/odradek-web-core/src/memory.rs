//! An in-memory [`RecordSource`]: a shared, appendable partition log.
//!
//! For tests and examples — both this crate's and those of anything
//! built on top: transports can exercise replay, live tailing, and
//! backpressure without a broker.

use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::event::Event;
use crate::source::{RecordSource, SourceBatch, SourceError, SourceFactory};

/// A shared in-memory log; offsets are indexes. Every partition of
/// every topic reads the same log, which is plenty for tests.
#[derive(Debug, Clone, Default)]
pub struct MemoryLog {
    events: Arc<Mutex<Vec<Event>>>,
    /// Events returned per fetch, so catch-up takes several rounds like
    /// a real broker's bounded fetches.
    batch_limit: usize,
}

impl MemoryLog {
    pub fn new() -> MemoryLog {
        MemoryLog {
            events: Arc::default(),
            batch_limit: 3,
        }
    }

    /// Append one record; returns its offset.
    pub fn append(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
        headers: Vec<(String, Option<Bytes>)>,
    ) -> i64 {
        let mut events = self.events.lock().unwrap();
        let offset = i64::try_from(events.len()).unwrap_or(i64::MAX);
        events.push(Event {
            topic: topic.to_owned(),
            partition: 0,
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

    pub fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A [`RecordSource`] over a [`MemoryLog`].
#[derive(Debug, Clone)]
pub struct MemorySource {
    log: MemoryLog,
}

impl RecordSource for MemorySource {
    async fn fetch(
        &mut self,
        _topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<SourceBatch, SourceError> {
        let events = self.log.events.lock().unwrap();
        let len = i64::try_from(events.len()).unwrap_or(i64::MAX);
        let start = usize::try_from(offset.clamp(0, len)).unwrap_or(usize::MAX);
        let batch: Vec<Event> = events
            .iter()
            .skip(start)
            .take(self.log.batch_limit)
            .map(|e| Event {
                partition,
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

    async fn earliest_offset(&mut self, _topic: &str, _partition: i32) -> Result<i64, SourceError> {
        Ok(0)
    }

    async fn latest_offset(&mut self, _topic: &str, _partition: i32) -> Result<i64, SourceError> {
        Ok(i64::try_from(self.log.events.lock().unwrap().len()).unwrap_or(i64::MAX))
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
        Ok(MemorySource {
            log: self.log.clone(),
        })
    }
}
