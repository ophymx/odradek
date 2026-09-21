//! Engine tests over an in-memory log: fan-out, replay, filtering, the
//! no-loss guarantee under backpressure, and the lifecycle — terminal
//! errors, idle exit, shutdown, and topic gating.

use std::time::Duration;

use bytes::Bytes;
use odradek_web_core::memory::{MemoryFactory, MemoryLog};
use odradek_web_core::{
    Filter, Hub, HubError, Position, PumpConfig, PumpHandle, RecordSource, SharedHub, SourceBatch,
    SourceError, SourceErrorKind, SourceFactory, Subscription,
};

const TOPIC: &str = "bridge";

fn pump_for(log: &MemoryLog, config: PumpConfig) -> PumpHandle {
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
        let value = String::from_utf8(event.value.clone().unwrap().to_vec()).unwrap();
        out.push((event.offset, value));
    }
    out
}

/// A source whose every fetch is slow, the way a broker parked in a
/// long poll is: anything that waits on this pump waits a whole poll.
struct SlowSource {
    poll: Duration,
}

impl RecordSource for SlowSource {
    async fn fetch(
        &mut self,
        _topic: &str,
        _partition: i32,
        after: Option<i64>,
    ) -> Result<SourceBatch, SourceError> {
        tokio::time::sleep(self.poll).await;
        // Built the way an out-of-tree adapter must: `SourceBatch` is
        // non-exhaustive, so `Default` plus assignment is the path, and
        // this test is the proof that path is usable.
        let mut batch = SourceBatch::default();
        batch.next_after = after;
        batch.high_watermark = 0;
        Ok(batch)
    }

    async fn live_start(
        &mut self,
        _topic: &str,
        _partition: i32,
    ) -> Result<Option<i64>, SourceError> {
        Ok(None)
    }
}

#[derive(Debug, Clone, Copy)]
struct SlowFactory {
    poll: Duration,
}

impl SourceFactory for SlowFactory {
    type Source = SlowSource;

    async fn create(&self, _topic: &str, _partition: i32) -> Result<SlowSource, SourceError> {
        Ok(SlowSource { poll: self.poll })
    }

    async fn partitions(&self, _topic: &str) -> Result<Vec<i32>, SourceError> {
        Ok(vec![0, 1, 2])
    }
}

/// A source that fails every call with one fixed error.
struct FailingSource {
    error: SourceError,
}

impl RecordSource for FailingSource {
    async fn fetch(
        &mut self,
        _topic: &str,
        _partition: i32,
        _after: Option<i64>,
    ) -> Result<SourceBatch, SourceError> {
        Err(self.error.clone())
    }

    async fn live_start(
        &mut self,
        _topic: &str,
        _partition: i32,
    ) -> Result<Option<i64>, SourceError> {
        Err(self.error.clone())
    }
}

#[tokio::test]
async fn live_fanout_reaches_every_subscriber() {
    let log = MemoryLog::new();
    let pump = pump_for(&log, PumpConfig::default());

    let mut a = pump
        .subscribe(Position::Latest, Filter::default())
        .await
        .unwrap();
    let mut b = pump
        .subscribe(Position::Latest, Filter::default())
        .await
        .unwrap();

    log.append(TOPIC, 0, None, b"one", Vec::new());
    log.append(TOPIC, 0, None, b"two", Vec::new());

    assert_eq!(
        collect(&mut a, 2).await,
        vec![(0, "one".into()), (1, "two".into())]
    );
    assert_eq!(
        collect(&mut b, 2).await,
        vec![(0, "one".into()), (1, "two".into())]
    );
}

#[tokio::test]
async fn replay_from_offset_then_live_without_gaps_or_dups() {
    let log = MemoryLog::new();
    for i in 0..7 {
        log.append(TOPIC, 0, None, format!("old-{i}").as_bytes(), Vec::new());
    }
    let pump = pump_for(&log, PumpConfig::default());

    // Start in history (offset 2), read through the tail, then keep
    // getting live appends — one contiguous offset sequence.
    let mut sub = pump
        .subscribe(Position::After(1), Filter::default())
        .await
        .unwrap();
    let history = collect(&mut sub, 5).await;
    assert_eq!(
        history.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
        vec![2, 3, 4, 5, 6]
    );

    log.append(TOPIC, 0, None, b"fresh", Vec::new());
    assert_eq!(collect(&mut sub, 1).await, vec![(7, "fresh".into())]);
}

#[tokio::test]
async fn earliest_replays_everything() {
    let log = MemoryLog::new();
    log.append(TOPIC, 0, None, b"zero", Vec::new());
    log.append(TOPIC, 0, None, b"one", Vec::new());
    let pump = pump_for(&log, PumpConfig::default());

    let mut sub = pump
        .subscribe(Position::Earliest, Filter::default())
        .await
        .unwrap();
    assert_eq!(
        collect(&mut sub, 2).await,
        vec![(0, "zero".into()), (1, "one".into())]
    );
}

#[tokio::test]
async fn filters_apply_to_replay_and_live() {
    let log = MemoryLog::new();
    log.append(TOPIC, 0, Some(b"user:1"), b"keep-a", Vec::new());
    log.append(TOPIC, 0, Some(b"order:9"), b"drop", Vec::new());
    let pump = pump_for(&log, PumpConfig::default());

    // Built the way a downstream crate must: `Filter` is non-exhaustive,
    // so the struct literal this used to be is not available outside the
    // engine. Keeping the test on the public path proves the ergonomics
    // are tolerable rather than assuming it.
    let mut filter = Filter::default();
    filter.key_prefix = Some(Bytes::from_static(b"user:"));
    let mut sub = pump.subscribe(Position::Earliest, filter).await.unwrap();

    log.append(TOPIC, 0, Some(b"user:2"), b"keep-b", Vec::new());
    log.append(TOPIC, 0, Some(b"cart:3"), b"drop", Vec::new());

    assert_eq!(
        collect(&mut sub, 2).await,
        vec![(0, "keep-a".into()), (2, "keep-b".into())]
    );
}

/// The core guarantee: a subscriber slower than the stream still sees
/// every event, in order, exactly once — backpressure demotes it to
/// catch-up instead of dropping.
#[tokio::test]
async fn slow_subscriber_loses_nothing() {
    let log = MemoryLog::new();
    let mut config = PumpConfig::default();
    config.queue_capacity = 2;
    config.ring_capacity = 4; // force catch-up past the ring, from source
    let pump = pump_for(&log, config);
    let mut sub = pump
        .subscribe(Position::Earliest, Filter::default())
        .await
        .unwrap();

    for i in 0..50 {
        log.append(TOPIC, 0, None, format!("v{i}").as_bytes(), Vec::new());
    }
    // Drain slowly: the tiny queue overflows many times over.
    let mut seen = Vec::new();
    for _ in 0..50 {
        let event = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("timed out")
            .expect("pump closed")
            .expect("stream failed");
        seen.push(event.offset);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(seen, (0..50).collect::<Vec<_>>());
}

/// Catch-up takes turns: with several subscribers past the ring, the
/// pump's one-fetch-per-iteration budget rotates, so none of them is
/// starved by the others and each still sees every offset in order.
#[tokio::test]
async fn several_slow_subscribers_all_catch_up() {
    let log = MemoryLog::new();
    let mut config = PumpConfig::default();
    config.queue_capacity = 2;
    config.ring_capacity = 4; // everyone falls out of the ring
    let pump = pump_for(&log, config);

    let mut subs = Vec::new();
    for _ in 0..3 {
        subs.push(
            pump.subscribe(Position::Earliest, Filter::default())
                .await
                .unwrap(),
        );
    }
    for i in 0..40 {
        log.append(TOPIC, 0, None, format!("v{i}").as_bytes(), Vec::new());
    }

    for (which, sub) in subs.iter_mut().enumerate() {
        let seen: Vec<i64> = collect(sub, 40).await.into_iter().map(|(o, _)| o).collect();
        assert_eq!(
            seen,
            (0..40).collect::<Vec<_>>(),
            "subscriber {which} lost or reordered events"
        );
    }
}

#[tokio::test]
async fn hub_runs_one_pump_per_partition() {
    let log = MemoryLog::with_partitions(2);
    let mut hub =
        Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default()).allow_all_topics();

    let mut a = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    let _b = hub
        .subscribe(TOPIC, 1, Position::Latest, Filter::default())
        .await
        .unwrap();
    let _c = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    assert_eq!(hub.active_partitions().count(), 2);

    log.append(TOPIC, 0, None, b"hello", Vec::new());
    assert_eq!(collect(&mut a, 1).await, vec![(0, "hello".into())]);
}

#[tokio::test]
async fn topic_subscribe_merges_partitions_in_partition_order() {
    use odradek_web_core::TopicPosition;
    use std::collections::HashMap;

    let log = MemoryLog::with_partitions(3);
    for i in 0..4 {
        log.append(TOPIC, i % 3, None, format!("v{i}").as_bytes(), Vec::new());
    }
    let mut hub =
        Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default()).allow_all_topics();
    let mut sub = hub
        .subscribe_topic(TOPIC, TopicPosition::Earliest, Filter::default())
        .await
        .unwrap();
    assert_eq!(sub.partitions(), &[0, 1, 2]);

    // All four replayed events arrive; within each partition, offsets
    // ascend.
    let mut per_partition: HashMap<i32, Vec<i64>> = HashMap::new();
    for _ in 0..4 {
        let event = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("timed out")
            .expect("stream closed")
            .expect("stream failed");
        per_partition
            .entry(event.partition)
            .or_default()
            .push(event.offset);
    }
    assert_eq!(per_partition[&0], vec![0, 1]);
    assert_eq!(per_partition[&1], vec![0]);
    assert_eq!(per_partition[&2], vec![0]);

    // Live appends on any partition keep flowing.
    log.append(TOPIC, 2, None, b"live", Vec::new());
    let event = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timed out")
        .expect("stream closed")
        .expect("stream failed");
    assert_eq!((event.partition, event.offset), (2, 1));
}

#[tokio::test]
async fn topic_cursor_resumes_seen_partitions_and_replays_unseen() {
    use odradek_web_core::TopicPosition;
    use std::collections::BTreeMap;

    let log = MemoryLog::with_partitions(2);
    for i in 0..3 {
        log.append(TOPIC, 0, None, format!("p0-{i}").as_bytes(), Vec::new());
        log.append(TOPIC, 1, None, format!("p1-{i}").as_bytes(), Vec::new());
    }
    let mut hub =
        Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default()).allow_all_topics();

    // The cursor names partition 0 only: resume it at 2, replay
    // partition 1 from the start (absent = never seen).
    let mut cursor = BTreeMap::new();
    cursor.insert(0, 1i64);
    let mut sub = hub
        .subscribe_topic(TOPIC, TopicPosition::After(cursor), Filter::default())
        .await
        .unwrap();

    let mut seen: Vec<(i32, i64)> = Vec::new();
    for _ in 0..4 {
        let event = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("timed out")
            .expect("stream closed")
            .expect("stream failed");
        seen.push((event.partition, event.offset));
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![(0, 2), (1, 0), (1, 1), (1, 2)]);
}

/// A permanent error (NotFound/Auth) skips the retry budget entirely:
/// the terminal StreamError arrives long before even one backoff.
#[tokio::test]
async fn permanent_error_fails_subscribers_immediately() {
    let mut config = PumpConfig::default();
    config.error_backoff = Duration::from_secs(5); // one retry would blow the deadline
    config.max_consecutive_errors = 100;
    let source = FailingSource {
        error: SourceError::not_found("unknown topic ghost"),
    };
    let pump = PumpHandle::spawn(source, "ghost", 0, config);

    // Latest needs no source call, so the subscribe itself succeeds.
    let mut sub = pump
        .subscribe(Position::Latest, Filter::default())
        .await
        .unwrap();
    let item = tokio::time::timeout(Duration::from_secs(1), sub.recv())
        .await
        .expect("terminal error should not wait for backoff")
        .expect("expected a terminal error, not a bare close");
    let err = item.expect_err("expected the stream to fail");
    assert_eq!(err.kind, SourceErrorKind::NotFound);
    assert!(err.message.contains("ghost"), "{}", err.message);
    // After the terminal item the stream is closed.
    assert!(sub.recv().await.is_none());
}

/// Transient errors keep the old behavior: retry with backoff, then a
/// terminal StreamError once the budget is spent.
#[tokio::test]
async fn transient_errors_exhaust_budget_then_fail_subscribers() {
    let mut config = PumpConfig::default();
    config.error_backoff = Duration::from_millis(1);
    config.max_consecutive_errors = 3;
    let source = FailingSource {
        error: SourceError::unavailable("broker down"),
    };
    let pump = PumpHandle::spawn(source, TOPIC, 0, config);

    let mut sub = pump
        .subscribe(Position::Latest, Filter::default())
        .await
        .unwrap();
    let err = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timed out")
        .expect("expected a terminal error, not a bare close")
        .expect_err("expected the stream to fail");
    assert_eq!(err.kind, SourceErrorKind::Unavailable);
    assert!(sub.recv().await.is_none());
}

/// On a quiet topic, a dropped subscriber is noticed without any event
/// flowing, and the now-empty pump exits after `idle_shutdown`.
#[tokio::test]
async fn dead_subscribers_are_detected_on_quiet_topics() {
    let log = MemoryLog::new();
    let mut config = PumpConfig::default();
    config.idle_shutdown = Some(Duration::from_millis(50));
    let pump = pump_for(&log, config);

    let sub = pump
        .subscribe(Position::Latest, Filter::default())
        .await
        .unwrap();
    drop(sub); // no event is ever appended: only the per-iteration check can see this
    tokio::time::sleep(Duration::from_millis(400)).await;

    let result = pump.subscribe(Position::Latest, Filter::default()).await;
    assert!(
        matches!(result, Err(HubError::PumpClosed)),
        "the pump should have exited idle"
    );
}

/// The hub respawns a pump that exited idle, replacing (not leaking)
/// its map entry, and the new pump streams.
#[tokio::test]
async fn hub_respawns_idle_exited_pumps() {
    let log = MemoryLog::new();
    let mut config = PumpConfig::default();
    config.idle_shutdown = Some(Duration::from_millis(50));
    let mut hub = Hub::new(MemoryFactory::new(log.clone()), config).allow_all_topics();

    let sub = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    drop(sub);
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Resubscribe through the hub: the dead pump is replaced in place.
    let mut sub = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .expect("hub should respawn an idle-exited pump");
    assert_eq!(hub.active_partitions().count(), 1);

    log.append(TOPIC, 0, None, b"after-respawn", Vec::new());
    assert_eq!(
        collect(&mut sub, 1).await,
        vec![(0, "after-respawn".into())]
    );
}

/// Hub::shutdown ends live streams cleanly (no error item), clears the
/// pump map, and refuses later subscribes.
#[tokio::test]
async fn hub_shutdown_closes_streams_and_refuses_new_subscribes() {
    let log = MemoryLog::new();
    let mut hub =
        Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default()).allow_all_topics();
    let mut sub = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();

    hub.shutdown().await;
    assert_eq!(hub.active_partitions().count(), 0);

    let end = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timed out waiting for the stream to close");
    assert!(end.is_none(), "shutdown should close cleanly, got {end:?}");

    let result = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await;
    assert!(matches!(result, Err(HubError::ShutDown)), "{result:?}");
}

/// The reconnect-storm shape: many clients subscribing at once to a
/// pump that is inside a long poll. The hub's lock covers the pump map,
/// not the round trip, so they all wait *one* poll rather than queueing
/// one poll behind another.
#[tokio::test]
async fn concurrent_subscribes_do_not_queue_behind_a_slow_pump() {
    let poll = Duration::from_millis(200);
    let hub = std::sync::Arc::new(SharedHub::from_hub(
        Hub::new(SlowFactory { poll }, PumpConfig::default()).allow_all_topics(),
    ));
    // The pump exists and is now inside a fetch.
    let _first = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let clients = 10;
    let start = std::time::Instant::now();
    let mut joined = Vec::new();
    for _ in 0..clients {
        let hub = std::sync::Arc::clone(&hub);
        joined.push(tokio::spawn(async move {
            hub.subscribe(TOPIC, 0, Position::Latest, Filter::default())
                .await
        }));
    }
    for task in joined {
        task.await.unwrap().expect("subscribe should succeed");
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < poll * 3,
        "{clients} concurrent subscribes took {elapsed:?}: they serialized behind the pump"
    );
}

/// Topic-level subscribes fan out across partitions at once, so a
/// P-partition topic costs about one pump round trip, not P.
#[tokio::test]
async fn topic_subscribe_runs_partition_round_trips_concurrently() {
    use odradek_web_core::TopicPosition;

    let poll = Duration::from_millis(200);
    let hub = SharedHub::from_hub(
        Hub::new(SlowFactory { poll }, PumpConfig::default()).allow_all_topics(),
    );
    // Warm the pumps so the measured call is round trips, not spawns.
    let _warm = hub
        .subscribe_topic(TOPIC, TopicPosition::Latest, Filter::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let start = std::time::Instant::now();
    let sub = hub
        .subscribe_topic(TOPIC, TopicPosition::Latest, Filter::default())
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert_eq!(sub.partitions(), &[0, 1, 2]);
    assert!(
        elapsed < poll * 2,
        "3 partitions took {elapsed:?}: their round trips ran in series"
    );
}

/// A gated topic fails before any pump or source exists; ungated
/// topics still stream.
#[tokio::test]
async fn topic_gate_denies_before_any_pump_is_created() {
    let log = MemoryLog::new();
    let mut hub = Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default())
        .with_topic_gate(|topic| !topic.starts_with("internal-"));

    let denied = hub
        .subscribe("internal-audit", 0, Position::Latest, Filter::default())
        .await;
    assert!(matches!(denied, Err(HubError::Denied(_))), "{denied:?}");
    let denied_topic = hub
        .subscribe_topic(
            "internal-audit",
            odradek_web_core::TopicPosition::Latest,
            Filter::default(),
        )
        .await;
    assert!(
        matches!(denied_topic, Err(HubError::Denied(_))),
        "{denied_topic:?}"
    );
    assert_eq!(hub.active_partitions().count(), 0);

    let mut sub = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    log.append(TOPIC, 0, None, b"allowed", Vec::new());
    assert_eq!(collect(&mut sub, 1).await, vec![(0, "allowed".into())]);
}

/// A first subscribe to an unknown topic fails NotFound and leaves no
/// dead entry behind in the pump map.
#[tokio::test]
async fn unknown_topic_fails_not_found_without_leaking_a_pump_entry() {
    let log = MemoryLog::new();
    let factory = MemoryFactory::new(log.clone()).known_topics([TOPIC]);
    let mut hub = Hub::new(factory, PumpConfig::default()).allow_all_topics();

    let result = hub
        .subscribe("ghost", 0, Position::Latest, Filter::default())
        .await;
    match result {
        Err(HubError::Source(e)) => assert_eq!(e.kind, SourceErrorKind::NotFound),
        other => panic!("expected NotFound, got {other:?}"),
    }
    let topic_result = hub
        .subscribe_topic(
            "ghost",
            odradek_web_core::TopicPosition::Earliest,
            Filter::default(),
        )
        .await;
    match topic_result {
        Err(HubError::Source(e)) => assert_eq!(e.kind, SourceErrorKind::NotFound),
        other => panic!("expected NotFound, got {other:?}"),
    }
    assert_eq!(hub.active_partitions().count(), 0);

    // The known topic still works.
    let mut sub = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    log.append(TOPIC, 0, None, b"real", Vec::new());
    assert_eq!(collect(&mut sub, 1).await, vec![(0, "real".into())]);
}

/// The pump map is not a place anonymous requests can put things.
///
/// `Position::Latest` — the default — needs no source call, and the
/// partition index is a free `i32` from the request path, so a hub that
/// inserted before validating would retain one entry per made-up
/// partition *of an allowed topic*, forever: the gate cannot see this
/// dimension at all. 1000 such requests here; the ceiling is the
/// topic's real partition count, which is 2.
#[tokio::test]
async fn unknown_partitions_never_reach_the_pump_map() {
    let log = MemoryLog::with_partitions(2);
    let factory = MemoryFactory::new(log.clone()).known_topics([TOPIC]);
    let mut hub = Hub::new(factory, PumpConfig::default()).allow_all_topics();

    for partition in 2..502 {
        let result = hub
            .subscribe(TOPIC, partition, Position::Latest, Filter::default())
            .await;
        match result {
            Err(HubError::Source(e)) => assert_eq!(e.kind, SourceErrorKind::NotFound),
            other => panic!("partition {partition} should be NotFound, got {other:?}"),
        }
    }
    // The same for a topic that does not exist at all, on the default
    // position — the path that used to answer 200 and then die.
    for partition in 0..500 {
        let result = hub
            .subscribe("ghost", partition, Position::Latest, Filter::default())
            .await;
        assert!(result.is_err(), "unknown topic should not subscribe");
    }
    assert_eq!(
        hub.active_partitions().count(),
        0,
        "1000 requests for partitions that do not exist retained pump entries"
    );

    // Real partitions of the same topic still work.
    let mut sub = hub
        .subscribe(TOPIC, 1, Position::Latest, Filter::default())
        .await
        .unwrap();
    log.append(TOPIC, 1, None, b"real", Vec::new());
    assert_eq!(collect(&mut sub, 1).await, vec![(0, "real".into())]);
    assert_eq!(hub.active_partitions().count(), 1);
}

/// A pump that dies does not keep its slot: the entry is evicted, not
/// retained until something happens to subscribe to that partition
/// again.
#[tokio::test]
async fn exited_pumps_are_evicted_from_the_map() {
    #[derive(Debug, Clone, Copy)]
    struct DyingFactory;

    impl SourceFactory for DyingFactory {
        type Source = FailingSource;

        async fn create(
            &self,
            _topic: &str,
            _partition: i32,
        ) -> Result<FailingSource, SourceError> {
            Ok(FailingSource {
                error: SourceError::not_found("the topic went away"),
            })
        }

        async fn partitions(&self, _topic: &str) -> Result<Vec<i32>, SourceError> {
            Ok(vec![0, 1])
        }
    }

    let mut hub = Hub::new(DyingFactory, PumpConfig::default()).allow_all_topics();
    // `Latest` subscribes without a source call, so this succeeds and
    // the pump then dies on its first fetch.
    let mut sub = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    assert_eq!(hub.active_partitions().count(), 1);
    let item = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timed out")
        .expect("expected a terminal error");
    assert!(item.is_err());
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Reaping is what a subscribe does before it creates anything, but
    // it is also callable on its own.
    assert_eq!(hub.reap_exited_pumps(), 1);
    assert_eq!(hub.active_partitions().count(), 0);
}

/// Past the ceiling, new partitions are refused while established ones
/// keep streaming — the bound that holds even when the gate is wide and
/// every partition asked for is real.
#[tokio::test]
async fn pump_ceiling_refuses_new_partitions() {
    let log = MemoryLog::with_partitions(4);
    let mut hub = Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default())
        .allow_all_topics()
        .with_max_pumps(2);

    let mut first = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    let _second = hub
        .subscribe(TOPIC, 1, Position::Latest, Filter::default())
        .await
        .unwrap();
    let refused = hub
        .subscribe(TOPIC, 2, Position::Latest, Filter::default())
        .await;
    assert!(matches!(refused, Err(HubError::AtCapacity)), "{refused:?}");
    assert_eq!(hub.active_partitions().count(), 2);

    // Another subscriber to a partition already pumping is fine: the
    // ceiling counts pumps, not connections.
    let _also_first = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    log.append(TOPIC, 0, None, b"still streaming", Vec::new());
    assert_eq!(
        collect(&mut first, 1).await,
        vec![(0, "still streaming".into())]
    );
}

/// A hub is born serving nothing: the gate is a decision the embedder
/// has to make, not one that defaults to "every topic on the cluster".
#[tokio::test]
async fn a_hub_without_a_gate_denies_everything() {
    let log = MemoryLog::new();
    let mut hub = Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default());

    let denied = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await;
    assert!(matches!(denied, Err(HubError::Denied(_))), "{denied:?}");
    let denied_topic = hub
        .subscribe_topic(
            TOPIC,
            odradek_web_core::TopicPosition::Latest,
            Filter::default(),
        )
        .await;
    assert!(
        matches!(denied_topic, Err(HubError::Denied(_))),
        "{denied_topic:?}"
    );
    assert_eq!(hub.active_partitions().count(), 0);

    // Saying so explicitly is what opens it.
    let mut open =
        Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default()).allow_all_topics();
    let mut sub = open
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    log.append(TOPIC, 0, None, b"opened", Vec::new());
    assert_eq!(collect(&mut sub, 1).await, vec![(0, "opened".into())]);
}

/// A topic subscribe that runs into the pump ceiling must not leave the
/// pumps it managed to start behind.
///
/// It spawns one pump per partition in order, so a topic with more
/// partitions than the hub has room for gets part way and then fails.
/// The pumps it already started stay in the map, counting against the
/// ceiling until they idle out — so one refused request can hold the
/// hub's whole budget against every other topic, and a client that
/// retries holds it indefinitely.
#[tokio::test]
async fn a_refused_topic_subscribe_leaves_no_pumps_behind() {
    let log = MemoryLog::with_partitions(4);
    let hub = SharedHub::from_hub(
        Hub::new(MemoryFactory::new(log.clone()), PumpConfig::default())
            .allow_all_topics()
            .with_max_pumps(2),
    );

    let refused = hub
        .subscribe_topic(
            TOPIC,
            odradek_web_core::TopicPosition::Latest,
            Filter::default(),
        )
        .await;
    assert!(
        matches!(refused, Err(HubError::AtCapacity)),
        "{:?}",
        refused.map(|_| ())
    );
    assert_eq!(
        hub.active_pumps().await,
        0,
        "a subscribe that could not be served should not hold pump slots"
    );

    // The same through the owned hub, which walks the partitions one at
    // a time rather than acquiring them all under one lock.
    let mut owned = Hub::new(MemoryFactory::new(log), PumpConfig::default())
        .allow_all_topics()
        .with_max_pumps(2);
    let refused = owned
        .subscribe_topic(
            TOPIC,
            odradek_web_core::TopicPosition::Latest,
            Filter::default(),
        )
        .await;
    assert!(
        matches!(refused, Err(HubError::AtCapacity)),
        "{:?}",
        refused.map(|_| ())
    );
    assert_eq!(owned.active_partitions().count(), 0);
}

/// A source that refuses everything below `log_start` the way a broker
/// refuses a fetch under the retention horizon: a transient error, not
/// a permanent one, because the topic and the partition are both fine.
struct TruncatedSource {
    log_start: i64,
    fetches: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl RecordSource for TruncatedSource {
    async fn fetch(
        &mut self,
        _topic: &str,
        _partition: i32,
        after: Option<i64>,
    ) -> Result<SourceBatch, SourceError> {
        // A position below the horizon is refused; `None` — start at
        // whatever the source still holds — is always serviceable, which
        // is the point of spelling a full replay that way.
        if after.is_some_and(|position| position < self.log_start) {
            self.fetches
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(SourceError::unavailable("offset out of range"));
        }
        let mut batch = SourceBatch::default();
        batch.next_after = Some(self.log_start);
        batch.high_watermark = self.log_start;
        Ok(batch)
    }

    async fn live_start(
        &mut self,
        _topic: &str,
        _partition: i32,
    ) -> Result<Option<i64>, SourceError> {
        Ok(Some(self.log_start))
    }
}

/// Resuming from an offset the source no longer has must end the
/// stream, not hang it.
///
/// This is the ordinary expired-resume-token case: a browser reconnects
/// with a `Last-Event-ID` older than the topic's retention. The
/// catch-up fetch fails transiently forever, so the subscriber is never
/// served and never told, while the pump spends a source round trip on
/// it every iteration for as long as the client stays connected.
#[tokio::test]
async fn an_offset_below_the_log_start_ends_the_stream() {
    let fetches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = fetches.clone();

    struct Factory {
        fetches: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl SourceFactory for Factory {
        type Source = TruncatedSource;
        async fn create(&self, _topic: &str, _partition: i32) -> Result<Self::Source, SourceError> {
            Ok(TruncatedSource {
                log_start: 1_000,
                fetches: self.fetches.clone(),
            })
        }
        async fn partitions(&self, _topic: &str) -> Result<Vec<i32>, SourceError> {
            Ok(vec![0])
        }
    }

    let mut hub = Hub::new(Factory { fetches: counter }, PumpConfig::default()).allow_all_topics();
    let mut sub = hub
        .subscribe(TOPIC, 0, Position::After(0), Filter::default())
        .await
        .unwrap();

    let outcome = tokio::time::timeout(Duration::from_secs(2), sub.recv()).await;
    let attempts = fetches.load(std::sync::atomic::Ordering::Relaxed);
    match outcome {
        Ok(Some(Err(_)) | None) => {}
        Ok(Some(Ok(event))) => panic!("unexpected event at {}", event.offset),
        Err(_) => panic!(
            "the stream neither delivered nor ended; the pump retried the \
             out-of-range fetch {attempts} times and the client was told nothing"
        ),
    }
}

/// Dropping a topic subscription must let its pumps go idle, even when
/// the partitions are quiet.
///
/// A topic stream is a forwarder task per partition feeding one merged
/// channel. A forwarder parked in `recv()` still *holds* its
/// partition's receiver, so the pump sees a subscriber that is very
/// much alive — and on a quiet partition nothing ever arrives to make
/// the forwarder notice the merged stream is gone. The pump then never
/// idles, which is every disconnected browser on a low-traffic topic.
#[tokio::test]
async fn dropping_a_topic_stream_releases_quiet_pumps() {
    let log = MemoryLog::with_partitions(2);
    let mut config = PumpConfig::default();
    config.idle_shutdown = Some(Duration::from_millis(100));
    let hub =
        SharedHub::from_hub(Hub::new(MemoryFactory::new(log.clone()), config).allow_all_topics());

    let stream = hub
        .subscribe_topic(
            TOPIC,
            odradek_web_core::TopicPosition::Latest,
            Filter::default(),
        )
        .await
        .unwrap();
    assert_eq!(hub.active_pumps().await, 2);

    // The client goes away without another record ever being produced.
    drop(stream);

    // Well past idle_shutdown, with a reap to collect the exited pumps
    // the way a subscribe would.
    tokio::time::sleep(Duration::from_millis(600)).await;
    hub.reap_exited_pumps().await;
    assert_eq!(
        hub.active_pumps().await,
        0,
        "pumps for a departed subscriber should have idled out"
    );
}
