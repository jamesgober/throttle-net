//! Integration tests for the circuit breaker: the state-transition invariant as
//! a property, half-open recovery, and async fail-fast load shedding.

#![cfg(feature = "circuit-breaker")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use throttle_net::{BreakerState, CircuitBreaker, ManualClock, Throttle, ThrottleError, Trip};

/// A generously-fast limiter so the breaker, not rate-limiting, is under test.
fn open_limiter() -> Throttle {
    Throttle::per_second(1_000_000)
}

proptest! {
    /// For a consecutive-failure breaker, the final state must be `Open` exactly
    /// when the outcome sequence reaches the threshold of consecutive failures
    /// at some point (a success resets the run; once open, later records are
    /// ignored). A long cooldown keeps it from auto-recovering mid-sequence.
    #[test]
    fn consecutive_trip_matches_reference(
        threshold in 1u32..8,
        seq in proptest::collection::vec(any::<bool>(), 0..40), // true = failure
    ) {
        let clock = Arc::new(ManualClock::new());
        let breaker = CircuitBreaker::builder()
            .trip(Trip::Consecutive(threshold))
            .cooldown(Duration::from_secs(3600))
            .build(open_limiter())
            .with_clock(clock);

        // Reference model: scan the sequence the way a closed breaker would.
        let mut run = 0u32;
        let mut should_trip = false;
        for &fail in &seq {
            if !should_trip {
                run = if fail { run + 1 } else { 0 };
                if run >= threshold {
                    should_trip = true;
                }
            }
        }

        for &fail in &seq {
            if fail {
                breaker.record_failure();
            } else {
                breaker.record_success();
            }
        }

        prop_assert_eq!(breaker.state() == BreakerState::Open, should_trip);
    }
}

#[test]
fn half_open_recovery_closes_on_success_reopens_on_failure() {
    let clock = Arc::new(ManualClock::new());
    let breaker = CircuitBreaker::builder()
        .trip(Trip::Consecutive(1))
        .cooldown(Duration::from_secs(10))
        .half_open(1, 1)
        .build(open_limiter())
        .with_clock(clock.clone());

    // Trip open.
    breaker.record_failure();
    assert_eq!(breaker.state(), BreakerState::Open);

    // Before cooldown: still open, requests shed.
    assert!(matches!(
        breaker.try_acquire(),
        Err(ThrottleError::CircuitOpen { .. })
    ));

    // After cooldown: a trial is admitted (half-open) and its failure reopens.
    clock.advance(Duration::from_secs(10));
    let trial = breaker.try_acquire().unwrap().expect("trial admitted");
    assert_eq!(breaker.state(), BreakerState::HalfOpen);
    trial.failure();
    assert_eq!(breaker.state(), BreakerState::Open);

    // After another cooldown: a successful trial closes it.
    clock.advance(Duration::from_secs(10));
    let trial = breaker.try_acquire().unwrap().expect("trial admitted");
    assert_eq!(breaker.state(), BreakerState::HalfOpen);
    trial.success();
    assert_eq!(breaker.state(), BreakerState::Closed);
}

#[cfg(feature = "runtime")]
#[tokio::test]
async fn open_breaker_fails_fast_without_waiting() {
    let clock = Arc::new(ManualClock::new());
    let breaker = CircuitBreaker::builder()
        .trip(Trip::Consecutive(1))
        .cooldown(Duration::from_secs(60))
        .build(open_limiter())
        .with_clock(clock);

    breaker.record_failure(); // open

    // The async acquire returns immediately with CircuitOpen — no waiting on the
    // cooldown, no consumption of the wrapped limiter.
    let before = breaker.inner().available();
    let result = breaker.acquire().await;
    assert!(matches!(result, Err(ThrottleError::CircuitOpen { .. })));
    assert_eq!(breaker.inner().available(), before);
}
