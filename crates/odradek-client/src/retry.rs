//! The one retry loop behind every retrying operation in this crate.
//!
//! Producer delivery, consumer fetches, offset commits, and group
//! membership all share the same mechanics — bounded attempts, a fixed
//! pause between rounds, and the last error as the loop's verdict —
//! while differing in how an error is classified and which cache it
//! invalidates. The mechanics live here once; classification stays at
//! each call site, where the context (topic metadata vs. discovered
//! coordinator vs. membership state) is at hand.
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

/// Run `attempt` up to `max_attempts` times against `ctx`, pausing
/// `backoff` between rounds. Exhaustion returns the last retried error.
pub(crate) async fn retry_loop<C: ?Sized, T>(
    ctx: &mut C,
    max_attempts: u32,
    backoff: Duration,
    mut attempt: impl for<'a> FnMut(&'a mut C) -> BoxAttempt<'a, T>,
) -> Result<T, ClientError> {
    let mut last = None;
    for round in 0..max_attempts {
        if round > 0 {
            tokio::time::sleep(backoff).await;
        }
        match attempt(ctx).await {
            Attempt::Done(v) => return Ok(v),
            Attempt::Retry(e) => last = Some(e),
            Attempt::Fatal(e) => return Err(e),
        }
    }
    Err(last.unwrap_or(ClientError::ConnectionClosed))
}

/// Standard topic-scoped classification: a retriable error means
/// leadership may have moved — invalidate the topic's metadata and try
/// again.
pub(crate) fn or_mark_stale<T>(
    cluster: &Cluster,
    topic: &str,
    result: Result<T, ClientError>,
) -> Attempt<T> {
    match result {
        Ok(v) => Attempt::Done(v),
        Err(e) if e.is_retriable() => {
            cluster.mark_stale(topic);
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
