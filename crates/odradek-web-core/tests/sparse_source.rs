//! A [`RecordSource`] whose positions are not dense, and the engine run
//! against it.
//!
//! The trait says a source may "number its records however it likes —
//! sparsely, with gaps, in strides — rather than densely like Kafka".
//! Both sources this workspace ships are dense: `KafkaSource` and
//! `MemorySource` each carry a `position + 1`, each a line from the
//! numbering it assumes. So the general claim had two witnesses that
//! agree with each other and nothing that disagreed.
//!
//! `SegmentLog` here is the disagreement. It is a byte-addressed log, of
//! the shape a file-backed or WAL-backed adapter would be: a record's
//! position is the byte at which it starts, so positions stride by
//! record length, begin after a segment header rather than at zero, and
//! leave gaps wherever an entry carries no event. No two consecutive
//! positions differ by one anywhere in this file, and nothing in it
//! computes a position from another position.
//!
//! It is written against the public API only, from outside the crate,
//! because that is also the question: could someone who is not us
//! implement this trait with what is exported? Anything reached for and
//! not found is the finding, not the workaround.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use odradek_web_core::{
    Event, Filter, Hub, Position, PumpConfig, PumpHandle, RecordSource, SourceBatch, SourceError,
    SourceFactory, Subscription,
};

const TOPIC: &str = "segments";

/// Bytes before the first record, so nothing is ever at position 0 and
/// a source that quietly assumed the log starts there would be caught.
const HEADER: i64 = 64;

/// One entry in the segment: where it starts, how many bytes it takes,
/// and what it holds.
///
/// `payload: None` is an entry that occupies the log without producing
/// an event — a control batch, a compaction tombstone already collected,
/// an index record. It is the case that makes `next_after` load-bearing:
/// the cursor has to cross it, and no event carries it across.
#[derive(Debug, Clone)]
struct Entry {
    at: i64,
    len: i64,
    payload: Option<String>,
}

/// A byte-addressed log. Appends compute a position from the previous
/// entry's *end*, which is the one arithmetic a real adapter over such a
/// store does and the one this file allows itself.
#[derive(Debug, Default)]
struct Segment {
    entries: Vec<Entry>,
}

impl Segment {
    fn end(&self) -> i64 {
        self.entries
            .last()
            .map_or(HEADER, |entry| entry.at + entry.len)
    }
}

#[derive(Debug, Clone, Default)]
struct SegmentLog {
    segment: Arc<Mutex<Segment>>,
}

impl SegmentLog {
    fn new() -> SegmentLog {
        SegmentLog::default()
    }

    /// Append a record and answer the position it landed at.
    ///
    /// The length is derived from the payload, so positions stride
    /// irregularly: "one" and "seventeen" are not the same distance
    /// apart as "one" and "two".
    fn append(&self, payload: &str) -> i64 {
        let mut segment = self.segment.lock().unwrap();
        let at = segment.end();
        // A framing overhead, so even equal payloads never stride by a
        // number a reader could mistake for a count.
        let len = 11 + i64::try_from(payload.len()).unwrap();
        segment.entries.push(Entry {
            at,
            len,
            payload: Some(payload.to_owned()),
        });
        at
    }

    /// Append an entry that occupies bytes and yields no event.
    fn append_control(&self, len: i64) -> i64 {
        let mut segment = self.segment.lock().unwrap();
        let at = segment.end();
        segment.entries.push(Entry {
            at,
            len,
            payload: None,
        });
        at
    }

    fn source(&self) -> SegmentSource {
        SegmentSource {
            log: self.clone(),
            max_batch: 4,
        }
    }
}

struct SegmentSource {
    log: SegmentLog,
    /// A real adapter reads a bounded window, not the whole segment.
    max_batch: usize,
}

impl RecordSource for SegmentSource {
    async fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        after: Option<i64>,
    ) -> Result<SourceBatch, SourceError> {
        let segment = self.log.segment.lock().unwrap();
        // "Strictly after", by comparison. `after` is a byte position
        // that need not be any entry's — a resume token echoed by a
        // client is whatever the client was last sent, and a client is
        // free to send something else — so this is a scan for the first
        // entry beyond it rather than a lookup of it.
        let first = segment
            .entries
            .iter()
            .position(|entry| Some(entry.at) > after)
            .unwrap_or(segment.entries.len());
        let window = &segment.entries[first..];
        let taken = &window[..window.len().min(self.max_batch)];

        let mut batch = SourceBatch::default();
        batch.events = taken
            .iter()
            .filter_map(|entry| {
                let payload = entry.payload.as_ref()?;
                let mut event = Event::at(topic, partition, entry.at, 1_700_000_000_000);
                event.value = Some(payload.clone().into_bytes().into());
                Some(event)
            })
            .collect();
        // The last entry *consumed*, event or not. A control entry that
        // produced nothing still has to move the cursor past itself, or
        // the next fetch asks the same question and gets the same
        // answer forever.
        batch.next_after = taken.last().map(|entry| entry.at);
        Ok(batch)
    }

    async fn live_start(
        &mut self,
        _topic: &str,
        _partition: i32,
    ) -> Result<Option<i64>, SourceError> {
        // Where every record the log already holds is at or before: the
        // last entry's own position, since positions are exclusive.
        // `None` for an empty log, which is the same place as the
        // beginning.
        let segment = self.log.segment.lock().unwrap();
        Ok(segment.entries.last().map(|entry| entry.at))
    }
}

#[derive(Debug, Clone)]
struct SegmentFactory {
    log: SegmentLog,
}

impl SourceFactory for SegmentFactory {
    type Source = SegmentSource;

    async fn create(&self, topic: &str, partition: i32) -> Result<SegmentSource, SourceError> {
        if topic != TOPIC || partition != 0 {
            return Err(SourceError::not_found(format!("no {topic}[{partition}]")));
        }
        Ok(self.log.source())
    }

    async fn partitions(&self, topic: &str) -> Result<Vec<i32>, SourceError> {
        if topic != TOPIC {
            return Err(SourceError::not_found(format!("no {topic}")));
        }
        Ok(vec![0])
    }
}

fn pump_for(log: &SegmentLog, config: PumpConfig) -> PumpHandle {
    PumpHandle::spawn(log.source(), TOPIC, 0, config)
}

async fn collect(sub: &mut Subscription, n: usize) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for _ in 0..n {
        let event = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("timed out waiting for event")
            .expect("pump closed")
            .expect("stream failed");
        out.push((
            event.offset,
            String::from_utf8(event.value.clone().unwrap().to_vec()).unwrap(),
        ));
    }
    out
}

/// Nothing this log hands out is ever one more than the last thing.
///
/// Asserted rather than assumed, because every other test here is only
/// interesting while it holds: if the strides collapsed to one, the
/// whole file would be a second dense source agreeing with the first
/// two.
#[test]
fn positions_are_never_consecutive() {
    let log = SegmentLog::new();
    let positions: Vec<i64> = ["one", "two", "seventeen", "x"]
        .iter()
        .map(|payload| log.append(payload))
        .collect();
    assert_eq!(positions[0], HEADER, "the log does not begin at zero");
    for pair in positions.windows(2) {
        assert!(
            pair[1] - pair[0] > 1,
            "positions {} and {} are consecutive",
            pair[0],
            pair[1]
        );
    }
}

#[tokio::test]
async fn earliest_replays_a_sparse_log_in_order() {
    let log = SegmentLog::new();
    let first = log.append("alpha");
    let second = log.append("beta");
    let third = log.append("gamma");

    let pump = pump_for(&log, PumpConfig::default());
    let mut sub = pump
        .subscribe(Position::Earliest, Filter::default())
        .await
        .unwrap();

    assert_eq!(
        collect(&mut sub, 3).await,
        vec![
            (first, "alpha".into()),
            (second, "beta".into()),
            (third, "gamma".into()),
        ]
    );
}

#[tokio::test]
async fn latest_starts_after_everything_already_written() {
    let log = SegmentLog::new();
    log.append("old");
    log.append("older");

    let pump = pump_for(&log, PumpConfig::default());
    let mut sub = pump
        .subscribe(Position::Latest, Filter::default())
        .await
        .unwrap();

    let fresh = log.append("new");
    assert_eq!(collect(&mut sub, 1).await, vec![(fresh, "new".into())]);
}

/// A resume token is a position the client was handed, echoed back.
///
/// The point of exclusivity: the client sends the offset of the last
/// event it actually received, and the engine passes it straight to
/// `fetch` without adding anything to it. On this log, adding one would
/// land in the middle of a record.
#[tokio::test]
async fn a_resume_token_is_echoed_back_unmodified() {
    let log = SegmentLog::new();
    log.append("first");
    let seen = log.append("second");
    let next = log.append("third");
    let last = log.append("fourth");

    let pump = pump_for(&log, PumpConfig::default());
    let mut sub = pump
        .subscribe(Position::After(seen), Filter::default())
        .await
        .unwrap();

    assert_eq!(
        collect(&mut sub, 2).await,
        vec![(next, "third".into()), (last, "fourth".into())]
    );
}

/// A position no record occupies still means "after here".
///
/// A client may echo a token from before a compaction, or simply send
/// something of its own. With dense offsets every integer is a
/// plausible position and this question never comes up; here, most
/// integers fall inside a record rather than at one.
#[tokio::test]
async fn a_position_between_records_resumes_at_the_next_one() {
    let log = SegmentLog::new();
    let first = log.append("first");
    let second = log.append("second");
    log.append("third");

    // Strictly between two records, and not either of them.
    let between = first + 1;
    assert!(between < second, "the test's own premise");

    let pump = pump_for(&log, PumpConfig::default());
    let mut sub = pump
        .subscribe(Position::After(between), Filter::default())
        .await
        .unwrap();

    assert_eq!(
        collect(&mut sub, 1).await,
        vec![(second, "second".into())],
        "a position inside a record resumes after it, not before"
    );
}

/// Entries that yield no events still have to move the cursor.
///
/// The stretch between two events is not empty here, it is full of
/// things that are not events. If the cursor could only advance by
/// landing on an event, it would stop at the first control entry and
/// the log would never be read past it.
#[tokio::test]
async fn a_run_of_eventless_entries_does_not_stall_the_cursor() {
    let log = SegmentLog::new();
    let before = log.append("before");
    // More eventless entries than a single fetch window holds, so at
    // least one whole fetch returns no events at all and advances the
    // cursor by `next_after` alone.
    for _ in 0..9 {
        log.append_control(23);
    }
    let after = log.append("after");

    let pump = pump_for(&log, PumpConfig::default());
    let mut sub = pump
        .subscribe(Position::Earliest, Filter::default())
        .await
        .unwrap();

    assert_eq!(
        collect(&mut sub, 2).await,
        vec![(before, "before".into()), (after, "after".into())]
    );
}

/// A laggard that falls out of the ring is served by fetching again.
///
/// The ring's floor is a position taken from an evicted event rather
/// than computed, so this is where a dense assumption would have hidden:
/// "the ring begins after `front().offset - 1`" is true for Kafka and
/// false here.
#[tokio::test]
async fn a_subscriber_evicted_from_the_ring_is_still_served() {
    let log = SegmentLog::new();
    let mut positions = Vec::new();
    for i in 0..12 {
        positions.push(log.append(&format!("event-{i}")));
    }

    let mut config = PumpConfig::default();
    config.ring_capacity = 2;
    let pump = pump_for(&log, config);
    let mut sub = pump
        .subscribe(Position::Earliest, Filter::default())
        .await
        .unwrap();

    let seen = collect(&mut sub, 12).await;
    let expected: Vec<(i64, String)> = positions
        .iter()
        .enumerate()
        .map(|(i, at)| (*at, format!("event-{i}")))
        .collect();
    assert_eq!(seen, expected, "every event, once, in order");
}

/// Catch-up hands over to live delivery without losing or repeating the
/// record at the boundary.
#[tokio::test]
async fn catch_up_meets_live_without_a_gap_or_a_duplicate() {
    let log = SegmentLog::new();
    let mut positions = Vec::new();
    for i in 0..6 {
        positions.push(log.append(&format!("old-{i}")));
    }

    let pump = pump_for(&log, PumpConfig::default());
    let mut sub = pump
        .subscribe(Position::Earliest, Filter::default())
        .await
        .unwrap();

    // Drain the backlog, then write across the boundary.
    let backlog = collect(&mut sub, 6).await;
    for i in 0..4 {
        positions.push(log.append(&format!("new-{i}")));
    }
    let live = collect(&mut sub, 4).await;

    let seen: Vec<i64> = backlog.iter().chain(&live).map(|(at, _)| *at).collect();
    assert_eq!(
        seen, positions,
        "every position once, in order, across the handover"
    );
}

/// A token from before the log's first record resumes at the beginning.
///
/// The realistic version of this is a client that saved a token,
/// disconnected, and came back after the segment it named had been
/// collected. With dense offsets the stale token is still a plausible
/// offset; here it is a byte position that no longer addresses
/// anything, and may be below the header.
#[tokio::test]
async fn a_token_from_before_the_log_starts_at_the_oldest_record() {
    let log = SegmentLog::new();
    let first = log.append("oldest");
    let second = log.append("newer");

    let pump = pump_for(&log, PumpConfig::default());
    let mut sub = pump
        .subscribe(Position::After(1), Filter::default())
        .await
        .unwrap();

    assert_eq!(
        collect(&mut sub, 2).await,
        vec![(first, "oldest".into()), (second, "newer".into())],
        "a position below everything the log holds yields everything it holds"
    );
}

/// The factory half, through the hub: existence checks, fan-out over
/// the partitions a topic reports, and the veto.
///
/// `SourceFactory` is the other trait behind the 1.0 gate, and a source
/// that is only ever constructed directly never exercises it.
#[tokio::test]
async fn the_hub_drives_a_sparse_factory() {
    let log = SegmentLog::new();
    let first = log.append("one");
    let second = log.append("two");

    let mut hub =
        Hub::new(SegmentFactory { log: log.clone() }, PumpConfig::default()).allow_all_topics();

    let mut sub = hub
        .subscribe(TOPIC, 0, Position::Earliest, Filter::default())
        .await
        .unwrap();
    assert_eq!(
        collect(&mut sub, 2).await,
        vec![(first, "one".into()), (second, "two".into())]
    );

    // The veto: a partition the factory does not have.
    let refused = hub
        .subscribe(TOPIC, 7, Position::Earliest, Filter::default())
        .await;
    assert!(refused.is_err(), "a partition the source lacks is refused");

    // And a topic it does not have.
    let refused = hub
        .subscribe("absent", 0, Position::Earliest, Filter::default())
        .await;
    assert!(refused.is_err(), "a topic the source lacks is refused");
}

/// The same eventless run, crossed by a subscriber that is already
/// live.
///
/// Catch-up and live delivery advance the cursor in different places,
/// and only one of them is reached by a subscriber that started at
/// `Earliest`. A sabotage that dropped `next_after` from the live path
/// left the test above passing, which is how this one came to exist.
#[tokio::test]
async fn a_live_subscriber_crosses_an_eventless_run() {
    let log = SegmentLog::new();
    let mut config = PumpConfig::default();
    // The live path sleeps this long whenever a fetch yields no events,
    // and crossing the run below takes several such fetches.
    config.idle_poll = Duration::from_millis(5);
    let pump = pump_for(&log, config);

    let mut sub = pump
        .subscribe(Position::Latest, Filter::default())
        .await
        .unwrap();

    let before = log.append("before");
    for _ in 0..9 {
        log.append_control(23);
    }
    let after = log.append("after");

    assert_eq!(
        collect(&mut sub, 2).await,
        vec![(before, "before".into()), (after, "after".into())]
    );
}
