//! `loom` concurrency model checks for throttle-net's own slot accounting.
//!
//! `loom` exhaustively explores the legal interleavings of the atomic operations
//! in the lock-free state the crate owns itself — the [`AdaptiveLimiter`]'s
//! slot and limit accounting and the [`Bulkhead`]'s slots (the token bucket
//! lives in `better-bucket`, which is model-checked there with an advancing
//! clock). The invariants under test:
//!
//! - **No over-admission.** The number of in-flight permits never exceeds the
//!   limit, even under arbitrary thread interleavings.
//! - **No lost slots.** Once every permit is settled, the in-flight count is back
//!   to zero — a permit can neither leak a slot nor release one twice.
//! - **No lost limit updates.** Concurrent outcomes each move the limit; none
//!   overwrites another (TN-006).
//!
//! Run with: `RUSTFLAGS="--cfg throttle_loom" cargo test --test loom_throttle \
//!     --no-default-features --features adaptive --release`
//!
//! The whole file is gated on `cfg(throttle_loom)` and the `adaptive` feature, so
//! a plain `cargo test` compiles it to nothing.
#![cfg(all(throttle_loom, feature = "adaptive"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use loom::sync::Arc;
use loom::thread;
use throttle_net::{AdaptiveLimiter, AdaptiveStrategy, Aimd, Bulkhead, Outcome};

/// Two threads contend for a single slot. At most one may hold it at any instant,
/// and the slot is always returned.
#[test]
fn single_slot_is_never_double_admitted() {
    loom::model(|| {
        let limiter = Arc::new(
            AdaptiveLimiter::builder()
                .floor(1)
                .ceiling(1)
                .initial(1)
                .build(Aimd::default()),
        );

        let other = Arc::clone(&limiter);
        let t = thread::spawn(move || {
            if let Some(permit) = other.try_acquire() {
                // The ceiling is the hard cap; it can never be exceeded.
                assert!(other.in_flight() <= other.ceiling());
                permit.success();
            }
        });

        if let Some(permit) = limiter.try_acquire() {
            assert!(limiter.in_flight() <= limiter.ceiling());
            permit.success();
        }

        t.join().unwrap();
        // Every reserved slot was released exactly once.
        assert_eq!(limiter.in_flight(), 0, "a slot leaked");
    });
}

/// With two slots and two threads, both may run concurrently, but the in-flight
/// count never exceeds the ceiling and always drains back to zero — including the
/// drop-as-failure release path on one thread.
#[test]
fn two_slots_bound_in_flight_and_drain() {
    loom::model(|| {
        let limiter = Arc::new(
            AdaptiveLimiter::builder()
                .floor(1)
                .ceiling(2)
                .initial(2)
                .build(Aimd::default()),
        );

        let other = Arc::clone(&limiter);
        let t = thread::spawn(move || {
            if let Some(permit) = other.try_acquire() {
                assert!(other.in_flight() <= 2);
                // Drop unsettled: exercises the Drop -> failure release path.
                drop(permit);
            }
        });

        if let Some(permit) = limiter.try_acquire() {
            assert!(limiter.in_flight() <= 2);
            permit.success();
        }

        t.join().unwrap();
        assert_eq!(limiter.in_flight(), 0, "a slot leaked");
    });
}

/// A strategy whose outcomes add commuting amounts, so any lost update shows
/// in the final limit.
struct Counting;

impl AdaptiveStrategy for Counting {
    fn adjust(&self, current: u32, _in_flight: u32, outcome: Outcome) -> u32 {
        match outcome {
            Outcome::Failure => current + 100,
            _ => current + 1,
        }
    }
}

/// Two threads each settle one outcome on a shared limiter at the same time.
#[test]
fn racing_settles_both_apply() {
    loom::model(|| {
        // Owned permits take a `std` Arc; the atomics inside are loom's.
        let limiter = std::sync::Arc::new(
            AdaptiveLimiter::builder()
                .floor(1)
                .ceiling(10_000)
                .initial(10)
                .build(Counting),
        );
        let first = limiter.try_acquire_owned().unwrap();
        let second = limiter.try_acquire_owned().unwrap();
        let t = thread::spawn(move || first.failure());
        second.success();
        t.join().unwrap();
        assert_eq!(limiter.current_limit(), 10 + 100 + 1);
        assert_eq!(limiter.in_flight(), 0);
    });
}

/// Bulkhead slots: two threads race for one slot; at most one holds it, and it
/// is always returned.
#[test]
fn bulkhead_slot_is_never_double_granted() {
    loom::model(|| {
        let bulkhead = Arc::new(Bulkhead::new(1));
        let other = Arc::clone(&bulkhead);
        let t = thread::spawn(move || {
            if let Some(permit) = other.try_acquire() {
                assert!(other.in_use() <= 1);
                drop(permit);
            }
        });
        if let Some(permit) = bulkhead.try_acquire() {
            assert!(bulkhead.in_use() <= 1);
            drop(permit);
        }
        t.join().unwrap();
        assert_eq!(bulkhead.in_use(), 0, "a slot leaked");
    });
}
