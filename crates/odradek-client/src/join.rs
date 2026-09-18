//! Running several futures at once on the caller's task.
//!
//! The crate is caller-driven — no background tasks, no spawning — but
//! "on your task" does not have to mean "one at a time": a broker
//! matches responses to requests by correlation id, so N requests can be
//! in flight over one connection at once (see [`crate::conn`]). What was
//! missing was a way to drive N request futures from one `.await`
//! without pulling in a futures-combinator dependency.
//!
//! [`join_all`] is that: it polls every future on each wake and finishes
//! when the last one does. Polling is O(n) per wake, which is the wrong
//! shape for thousands of futures but exactly right for the handful this
//! crate joins (one per partition in a producer flush), where each poll
//! is a pointer chase against a network round trip.

use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

/// Drive every future to completion concurrently, returning their
/// outputs in the order the futures were given — not the order they
/// finished.
pub(crate) async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut running: Vec<Option<Pin<Box<F>>>> =
        futures.into_iter().map(|f| Some(Box::pin(f))).collect();
    let mut done: Vec<Option<F::Output>> = running.iter().map(|_| None).collect();

    std::future::poll_fn(|cx| {
        let mut all_done = true;
        for (slot, out) in running.iter_mut().zip(done.iter_mut()) {
            let Some(future) = slot.as_mut() else {
                continue;
            };
            match future.as_mut().poll(cx) {
                // Taking the future out of its slot is what keeps a
                // completed future from being polled again.
                Poll::Ready(value) => {
                    *out = Some(value);
                    *slot = None;
                }
                Poll::Pending => all_done = false,
            }
        }
        if all_done {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;

    done.into_iter()
        .map(|out| out.expect("join_all returns only once every future is done"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn outputs_keep_input_order_whatever_the_finish_order() {
        // Reverse-ordered sleeps: the last future finishes first.
        let futures: Vec<_> = (0..5u64)
            .map(|i| async move {
                tokio::time::sleep(std::time::Duration::from_millis(50 - i * 10)).await;
                i
            })
            .collect();
        assert_eq!(join_all(futures).await, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn futures_really_overlap() {
        // Five 100ms sleeps serially would be 500ms; concurrently they
        // are one 100ms wait.
        let started = std::time::Instant::now();
        let futures: Vec<_> = (0..5)
            .map(|_| async { tokio::time::sleep(std::time::Duration::from_millis(100)).await })
            .collect();
        join_all(futures).await;
        assert!(
            started.elapsed() < std::time::Duration::from_millis(400),
            "sleeps should overlap, took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_finished_future_is_never_polled_again() {
        let polls = Arc::new(AtomicUsize::new(0));
        let counted = {
            let polls = Arc::clone(&polls);
            async move {
                polls.fetch_add(1, Ordering::SeqCst);
            }
        };
        // A future that wakes repeatedly keeps the join going long after
        // the counted one is done.
        let slow = async {
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
        };
        let futures: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> =
            vec![Box::pin(counted), Box::pin(slow)];
        join_all(futures).await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn joining_nothing_is_not_a_hang() {
        let futures: Vec<std::future::Ready<u8>> = Vec::new();
        assert!(join_all(futures).await.is_empty());
    }
}
