//! `loom` concurrency model checks for throttle-net's own slot accounting.
//!
//! `loom` exhaustively explores the legal interleavings of the atomic operations
//! in the [`AdaptiveLimiter`]'s reserve/release path — the one piece of
//! lock-free state the crate owns itself (the token bucket lives in
//! `better-bucket`, which is model-checked there). The invariants under test:
//!
//! - **No over-admission.** The number of in-flight permits never exceeds the
//!   limit, even under arbitrary thread interleavings.
//! - **No lost slots.** Once every permit is settled, the in-flight count is back
//!   to zero — a permit can neither leak a slot nor release one twice.
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
use throttle_net::{AdaptiveLimiter, Aimd};

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
