//! `resume()` wakes the waiters sleeping out a pause, and only those.
//!
//! The resume signal is process-wide, so this check lives alone in its own
//! test binary: no other test can resume a limiter while it counts re-checks.

#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use throttle_net::{Decision, Hybrid, Limiter, Throttle, TokioClock};

/// A limiter that is always ten seconds from its next unit, counting how often
/// a waiter looks at it.
struct Refilling {
    looks: AtomicU32,
    ready_at: tokio::time::Instant,
}

impl Limiter for Refilling {
    fn peek(&self, _: u32) -> Decision {
        let _ = self.looks.fetch_add(1, Ordering::Relaxed);
        let left = self
            .ready_at
            .saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            Decision::Acquired
        } else {
            Decision::Retry { after: left }
        }
    }
    fn acquire_cost(&self, cost: u32) -> Decision {
        self.peek(cost)
    }
    fn available(&self) -> u32 {
        0
    }
    fn capacity(&self) -> u32 {
        1
    }
}

#[tokio::test(start_paused = true)]
async fn resume_wakes_paused_waiters_and_leaves_refill_waiters_asleep() {
    let start = tokio::time::Instant::now();
    let refilling = Arc::new(Refilling {
        looks: AtomicU32::new(0),
        ready_at: start + Duration::from_secs(10),
    });
    let refill_waiter = {
        let hybrid = Hybrid::builder().shared(refilling.clone()).build();
        tokio::spawn(async move {
            hybrid.acquire().await.unwrap();
            tokio::time::Instant::now()
        })
    };
    let paused = Arc::new(Throttle::per_second(10).with_clock(TokioClock::new()));
    paused.pause_for(Duration::from_secs(3600));
    let paused_waiter = {
        let paused = Arc::clone(&paused);
        tokio::spawn(async move {
            paused.acquire().await.unwrap();
            tokio::time::Instant::now()
        })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    let looks_before = refilling.looks.load(Ordering::Relaxed);
    paused.resume();
    let resumed = paused_waiter.await.unwrap() - start;
    assert_eq!(
        resumed,
        Duration::from_secs(1),
        "the paused waiter woke at once"
    );
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        refilling.looks.load(Ordering::Relaxed),
        looks_before,
        "the refill waiter was not woken by the resume"
    );
    assert_eq!(
        refill_waiter.await.unwrap() - start,
        Duration::from_secs(10)
    );
}
