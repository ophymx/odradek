//! The one retry loop behind every retrying operation in this crate.
//!
//! Producer delivery, consumer fetches, offset commits, and group
//! membership all share the same mechanics — bounded attempts, a
//! jittered pause between rounds, and the last error as the loop's
//! verdict — while differing in how an error is classified and which
//! cache it invalidates. The mechanics live here once; classification
//! stays at each call site, where the context (topic metadata vs.
//! discovered coordinator vs. membership state) is at hand.
//!
//! # Why the pause is jittered
//!
//! Failures here arrive in herds: one leadership change fails every
//! in-flight partition of a topic at the same instant. A fixed pause
//! puts all of them back on the wire on the same grid, forever — the
//! same broker, the same millisecond, round after round. [`Backoff`]
//! spreads them with decorrelated jitter instead, so the herd fans out
//! after the first round while the *attempt* budget (which is what the
//! configured `max_attempts` really buys: seconds of tolerance while a
//! fresh topic elects leaders) stays what it was.
//!
//! Shape note: `attempt` is handed the context back each round as a
//! plain `&mut C` and returns a boxed future borrowing it. An
//! `AsyncFnMut` closure would read better, but its future's `Send`ness
//! cannot be proven across the generic boundary today ("implementation
//! of `Send` is not general enough"), and spawned callers need `Send`.

use std::pin::Pin;
use std::time::Duration;

use crate::cluster::Cluster;
use crate::error::ClientError;

/// One attempt's outcome, as classified by the call site.
pub(crate) enum Attempt<T> {
    Done(T),
    /// Worth another round; kept as the loop's last word if rounds run
    /// out. Any invalidation happened during classification.
    Retry(ClientError),
    Fatal(ClientError),
}

/// A single attempt's boxed future, borrowing the loop's context.
pub(crate) type BoxAttempt<'a, T> = Pin<Box<dyn Future<Output = Attempt<T>> + Send + 'a>>;

/// Run `attempt` up to `max_attempts` times against `ctx`, pausing a
/// jittered `backoff` between rounds (see [`Backoff`]). Exhaustion
/// returns the last retried error.
pub(crate) async fn retry_loop<C: ?Sized, T>(
    ctx: &mut C,
    max_attempts: u32,
    backoff: Duration,
    mut attempt: impl for<'a> FnMut(&'a mut C) -> BoxAttempt<'a, T>,
) -> Result<T, ClientError> {
    let mut last = None;
    let mut backoff = Backoff::new(backoff);
    for round in 0..max_attempts {
        if round > 0 {
            tokio::time::sleep(backoff.next_delay()).await;
        }
        match attempt(ctx).await {
            Attempt::Done(v) => return Ok(v),
            Attempt::Retry(e) => last = Some(e),
            Attempt::Fatal(e) => return Err(e),
        }
    }
    Err(last.unwrap_or(ClientError::ConnectionClosed))
}

/// Decorrelated jitter around a configured pause.
///
/// Each round waits a uniform draw from `[base/2, min(3 × previous,
/// 2 × base)]`: the low end keeps a retry prompt, the ceiling keeps the
/// configured pause meaningful (a 20-attempt budget stays seconds, not
/// minutes), and drawing the *next* round's ceiling from *this* round's
/// draw is what decorrelates a herd — two partitions that started
/// together diverge after one round instead of marching in step.
///
/// The randomness is drawn lazily, so the common path (first attempt
/// succeeds) never touches the RNG at all.
pub(crate) struct Backoff {
    base: Duration,
    /// The last delay handed out; seeds the next round's ceiling.
    previous: Duration,
}

impl Backoff {
    /// Growth factor applied to the previous delay for the next
    /// round's ceiling.
    const GROWTH: u32 = 3;
    /// Hard ceiling on any one delay, as a multiple of `base`.
    const CEILING: u32 = 2;

    pub(crate) fn new(base: Duration) -> Backoff {
        Backoff {
            base,
            previous: base,
        }
    }

    /// The next round's pause.
    pub(crate) fn next_delay(&mut self) -> Duration {
        self.next_delay_from(random_u64())
    }

    /// [`Backoff::next_delay`] with the entropy supplied — the seam
    /// tests use to make jitter reproducible.
    fn next_delay_from(&mut self, entropy: u64) -> Duration {
        let base = u64::try_from(self.base.as_nanos()).unwrap_or(u64::MAX);
        let low = base / 2;
        let high = u64::try_from(self.previous.as_nanos())
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::from(Self::GROWTH))
            .min(base.saturating_mul(u64::from(Self::CEILING)));
        // A zero backoff stays zero; every other span is inclusive.
        let delay = low + entropy % (high.saturating_sub(low) + 1);
        self.previous = Duration::from_nanos(delay);
        self.previous
    }
}

/// A xorshift64* draw from a per-thread stream, seeded on first use.
///
/// Retry jitter wants unpredictability across processes, not
/// cryptographic quality, and it must not cost a syscall per delay —
/// so the OS is asked once per thread and the stream runs from there.
fn random_u64() -> u64 {
    use std::cell::Cell;

    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0) };
    }

    STATE.with(|state| {
        let mut x = state.get();
        if x == 0 {
            let mut seed = [0u8; 8];
            // A refused OS draw must not panic a produce; the clock is a
            // poorer seed but a live one.
            if getrandom::fill(&mut seed).is_err() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
                seed = now.to_le_bytes();
            }
            x = u64::from_le_bytes(seed) | 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    })
}

/// Standard partition-scoped classification: a retriable error means
/// leadership may have moved — invalidate that partition's cached
/// leader and try again.
///
/// Scoped to the one partition on purpose. Invalidating the whole
/// topic would make one partition's NOT_LEADER cost every *other*
/// partition of the topic a metadata refresh too, which on a wide topic
/// turns a single leadership blip into a metadata storm through the one
/// control connection.
pub(crate) fn or_mark_stale<T>(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    result: Result<T, ClientError>,
) -> Attempt<T> {
    match result {
        Ok(v) => Attempt::Done(v),
        Err(e) if e.is_retriable() => {
            cluster.mark_partition_stale(topic, partition);
            Attempt::Retry(e)
        }
        Err(e) => Attempt::Fatal(e),
    }
}

/// Standard coordinator-scoped classification: a retriable error means
/// the coordinator may have moved — forget it and rediscover.
pub(crate) fn or_forget_coordinator<T>(
    cluster: &Cluster,
    group: &str,
    result: Result<T, ClientError>,
) -> Attempt<T> {
    match result {
        Ok(v) => Attempt::Done(v),
        Err(e) if e.is_retriable() => {
            cluster.forget_coordinator(group);
            Attempt::Retry(e)
        }
        Err(e) => Attempt::Fatal(e),
    }
}

/// [`or_forget_coordinator`] for the transaction coordinator, which is
/// a separate discovery with its own cache entry.
pub(crate) fn or_forget_txn_coordinator<T>(
    cluster: &Cluster,
    transactional_id: &str,
    result: Result<T, ClientError>,
) -> Attempt<T> {
    match result {
        Ok(v) => Attempt::Done(v),
        Err(e) if e.is_retriable() => {
            cluster.forget_transaction_coordinator(transactional_id);
            Attempt::Retry(e)
        }
        Err(e) => Attempt::Fatal(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: Duration = Duration::from_millis(250);

    #[test]
    fn delays_stay_inside_the_configured_budget() {
        let mut backoff = Backoff::new(BASE);
        for _ in 0..100 {
            let delay = backoff.next_delay();
            assert!(
                delay >= BASE / 2 && delay <= BASE * 2,
                "{delay:?} outside [125ms, 500ms]"
            );
        }
    }

    #[test]
    fn a_herd_does_not_retry_on_one_grid() {
        // Twelve partitions failing at the same instant, each with its
        // own entropy: the first round already fans them out. Fixed
        // draws (golden-ratio stride over the u64 range) keep it
        // reproducible while standing in for real entropy.
        let round_one: std::collections::BTreeSet<Duration> = (0..12u64)
            .map(|i| Backoff::new(BASE).next_delay_from(i.wrapping_mul(0x9E37_79B9_7F4A_7C15)))
            .collect();
        assert_eq!(round_one.len(), 12, "no two of the herd share a delay");
        let spread = *round_one.last().unwrap() - *round_one.first().unwrap();
        assert!(
            spread > BASE / 2,
            "the herd should smear over a good part of the window, got {spread:?}"
        );
    }

    #[test]
    fn each_round_is_drawn_from_the_last() {
        // Decorrelation: a short draw lowers the next round's ceiling,
        // so the sequence wanders instead of locking onto one value.
        let mut backoff = Backoff::new(BASE);
        let short = backoff.next_delay_from(0);
        assert_eq!(short, BASE / 2, "entropy 0 draws the low end");
        // The next ceiling is 3 x 125ms = 375ms, under the 500ms cap.
        let next = backoff.next_delay_from(u64::MAX);
        assert!(
            next <= BASE / 2 * 3 && next >= BASE / 2,
            "{next:?} should sit under the previous-derived ceiling"
        );
    }

    #[test]
    fn a_zero_backoff_stays_zero() {
        let mut backoff = Backoff::new(Duration::ZERO);
        assert_eq!(backoff.next_delay(), Duration::ZERO);
        assert_eq!(backoff.next_delay(), Duration::ZERO);
    }

    #[test]
    fn the_thread_local_stream_does_not_repeat_itself() {
        let draws: std::collections::BTreeSet<u64> = (0..64).map(|_| random_u64()).collect();
        assert_eq!(draws.len(), 64, "xorshift64* must not cycle this fast");
    }
}
