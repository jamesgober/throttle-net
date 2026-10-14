//! Regression tests for the 2.0.0 re-audit findings (TN2-*).
//!
//! Each test is a reproduction from the audit, turned into an assertion. Every
//! one of them failed (or hung until its timeout) on the 1.1.0 candidate.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[allow(unused_imports)]
use std::sync::Arc;
#[allow(unused_imports)]
use std::time::Duration;

// TN2-001: two releases before the first woken waiter runs must wake two
// waiters (event-listener's `notify(1)` is not additive).
#[cfg(feature = "tokio")]
#[tokio::test(start_paused = true)]
async fn tn2_001_bulkhead_two_releases_wake_two_waiters() {
    use throttle_net::Bulkhead;

    let bulkhead = Arc::new(Bulkhead::new(2));
    let p1 = bulkhead.try_acquire_owned().unwrap();
    let p2 = bulkhead.try_acquire_owned().unwrap();
    let b1 = Arc::clone(&bulkhead);
    let w1 = tokio::spawn(async move {
        let permit = b1.acquire_owned().await.unwrap();
        tokio::time::sleep(Duration::from_secs(3600)).await;
        drop(permit);
    });
    let b2 = Arc::clone(&bulkhead);
    let w2 = tokio::spawn(async move {
        let _permit = b2.acquire_owned().await.unwrap();
    });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    // Both slots free with no yield in between.
    drop(p1);
    drop(p2);
    let done = tokio::time::timeout(Duration::from_secs(5), w2).await;
    assert!(
        done.is_ok(),
        "the second waiter must get the second free slot"
    );
    assert_eq!(bulkhead.in_use(), 1, "only w1 still holds a slot");
    w1.abort();
}

#[cfg(all(feature = "tokio", feature = "adaptive"))]
#[tokio::test(start_paused = true)]
async fn tn2_001_adaptive_two_settles_wake_two_waiters() {
    use throttle_net::{AdaptiveLimiter, Aimd};

    let limiter = Arc::new(
        AdaptiveLimiter::builder()
            .floor(2)
            .ceiling(2)
            .initial(2)
            .build(Aimd::default()),
    );
    let p1 = limiter.try_acquire_owned().unwrap();
    let p2 = limiter.try_acquire_owned().unwrap();
    let a1 = Arc::clone(&limiter);
    let w1 = tokio::spawn(async move {
        let permit = a1.acquire_owned().await;
        tokio::time::sleep(Duration::from_secs(3600)).await;
        permit.success();
    });
    let a2 = Arc::clone(&limiter);
    let w2 = tokio::spawn(async move {
        a2.acquire_owned().await.success();
    });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    p1.success();
    p2.success();
    let done = tokio::time::timeout(Duration::from_secs(5), w2).await;
    assert!(
        done.is_ok(),
        "the second waiter must get the second free slot"
    );
    assert_eq!(limiter.in_flight(), 1);
    w1.abort();
}

// TN2-001 (the `Limiter::refund` path): refunding two slots in one call wakes
// two waiters even after an earlier single wake-up.
#[cfg(all(feature = "tokio", feature = "adaptive"))]
#[tokio::test(start_paused = true)]
async fn tn2_001_adaptive_refund_wakes_additional_waiters() {
    use throttle_net::{AdaptiveLimiter, Aimd, Limiter};

    let limiter = Arc::new(
        AdaptiveLimiter::builder()
            .floor(3)
            .ceiling(3)
            .initial(3)
            .build(Aimd::default()),
    );
    assert!(limiter.acquire_cost(3).is_acquired());
    let mut waiters = Vec::new();
    for _ in 0..3 {
        let a = Arc::clone(&limiter);
        waiters.push(tokio::spawn(async move {
            let permit = a.acquire_owned().await;
            tokio::time::sleep(Duration::from_secs(3600)).await;
            permit.success();
        }));
    }
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    limiter.refund(1);
    limiter.refund(2);
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(limiter.in_flight(), 3, "all three waiters were admitted");
    for w in waiters {
        w.abort();
    }
}

// TN2-002: a tenant with a deep backlog on its own per-key scope must not
// drain the shared global scope ahead of time and starve other tenants.
#[cfg(feature = "tokio")]
#[tokio::test(start_paused = true)]
async fn tn2_002_layered_noisy_tenant_cannot_starve_the_global_scope() {
    use throttle_net::{Layered, PerKey, Throttle, TokioClock};

    let layered = Arc::new(
        Layered::<&'static str>::builder()
            .global(Throttle::per_second(10).with_clock(TokioClock::new()))
            .per_key(
                PerKey::builder()
                    .per_minute(1)
                    .clock(TokioClock::new())
                    .build(),
            )
            .build(),
    );
    let mut noisy = Vec::new();
    for _ in 0..200 {
        let l = Arc::clone(&layered);
        noisy.push(tokio::spawn(async move { l.acquire(&"noisy", &"/").await }));
    }
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    let start = tokio::time::Instant::now();
    let quiet = layered
        .acquire_with_cost_timeout(&"quiet", &"/", 1, Duration::from_secs(10))
        .await;
    assert_eq!(quiet, Ok(()), "the quiet tenant is admitted");
    assert!(layered.try_acquire(&"quiet2", &"/"), "global budget left");
    let quiet3 = layered.acquire(&"quiet3", &"/").await;
    assert_eq!(quiet3, Ok(()));
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "quiet tenants waited {:?}",
        start.elapsed()
    );
    for handle in noisy {
        handle.abort();
    }
}

// TN2-002 with a shared Hybrid constituent: waiters queued on their private
// slow limiter hold nothing in the shared fast one.
#[cfg(feature = "tokio")]
#[tokio::test(start_paused = true)]
async fn tn2_002_hybrid_waiters_hold_nothing_in_a_shared_constituent() {
    use throttle_net::{Hybrid, Limiter, Throttle, TokioClock};

    let shared: Arc<dyn Limiter> = Arc::new(Throttle::per_second(10).with_clock(TokioClock::new()));
    let slow = Arc::new(
        Hybrid::builder()
            .shared(Arc::clone(&shared))
            .limiter(Throttle::per_minute(1).with_clock(TokioClock::new()))
            .build(),
    );
    assert!(slow.try_acquire());
    let mut waiters = Vec::new();
    for _ in 0..50 {
        let h = Arc::clone(&slow);
        waiters.push(tokio::spawn(async move { h.acquire().await }));
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        shared.available(),
        10,
        "queued waiters took nothing from it"
    );
    for handle in waiters {
        handle.abort();
    }
}

// TN2-003: a breaker admitted through the `Limiter` trait releases its
// half-open trial slot when the units are refunded (rollback or cancel).
#[cfg(feature = "circuit-breaker")]
#[test]
fn tn2_003_trait_refund_frees_the_half_open_trial_slot() {
    use throttle_net::{BreakerState, CircuitBreaker, Limiter, ManualClock, Throttle, Trip};

    let clock = Arc::new(ManualClock::new());
    let breaker = CircuitBreaker::builder()
        .trip(Trip::Consecutive(1))
        .cooldown(Duration::from_secs(1))
        .half_open(1, 1)
        .build(Throttle::per_second(100))
        .with_clock(clock.clone());
    breaker.record_failure();
    clock.advance(Duration::from_secs(2));
    assert!(
        breaker.acquire_cost(1).is_acquired(),
        "the trait admits a probe"
    );
    assert_eq!(breaker.state(), BreakerState::HalfOpen);
    breaker.refund(1); // the composite rolled back: no request happened
    let probe = breaker.try_acquire().unwrap();
    assert!(probe.is_some(), "the trial slot is free again");
    probe.unwrap().success();
    assert_eq!(breaker.state(), BreakerState::Closed);

    // The same through `reserve`.
    breaker.record_failure();
    clock.advance(Duration::from_secs(2));
    assert_eq!(breaker.reserve(1, Duration::MAX), Some(Duration::ZERO));
    breaker.refund(1);
    assert!(breaker.try_acquire().unwrap().is_some());
}

// TN2-003: the audit's reproduction — a breaker inside a Hybrid, a timed
// acquire that cannot be served, and a cancelled one.
#[cfg(all(feature = "circuit-breaker", feature = "tokio"))]
#[tokio::test]
async fn tn2_003_breaker_in_a_hybrid_never_wedges_half_open() {
    use throttle_net::{CircuitBreaker, Hybrid, ManualClock, Throttle, ThrottleError, Trip};

    let clock = Arc::new(ManualClock::new());
    let make = || {
        let breaker = Arc::new(
            CircuitBreaker::builder()
                .trip(Trip::Consecutive(1))
                .cooldown(Duration::from_secs(1))
                .half_open(1, 1)
                .build(Throttle::per_second(100))
                .with_clock(clock.clone()),
        );
        breaker.record_failure();
        clock.advance(Duration::from_secs(2));
        let slow = Throttle::per_second(1);
        assert!(slow.try_acquire());
        let hybrid = Hybrid::builder()
            .shared(breaker.clone())
            .limiter(slow)
            .build();
        (breaker, hybrid)
    };

    let (breaker, hybrid) = make();
    let timed = hybrid
        .acquire_with_cost_timeout(1, Duration::from_millis(100))
        .await;
    assert_eq!(timed, Err(ThrottleError::DeadlineExceeded));
    let probe = breaker.try_acquire().unwrap();
    assert!(probe.is_some(), "not wedged after a timeout");
    probe.unwrap().success();

    let (breaker, hybrid) = make();
    let cancelled = tokio::time::timeout(Duration::from_millis(100), hybrid.acquire()).await;
    assert!(cancelled.is_err());
    let probe = breaker.try_acquire().unwrap();
    assert!(probe.is_some(), "not wedged after a cancellation");
    probe.unwrap().success();
}

// TN2-004: the 1.0 call patterns compile again — a shared (`Arc`), boxed, or
// borrowed limiter is itself a `Limiter`.
#[cfg(feature = "provider-headers")]
#[test]
fn tn2_004_shared_limiters_are_limiters() {
    use throttle_net::provider::{RateLimitInfo, Window};
    use throttle_net::{Hybrid, Limiter, Throttle};

    let info = RateLimitInfo {
        requests: Some(Window {
            remaining: Some(3),
            ..Window::default()
        }),
        ..RateLimitInfo::default()
    };
    let arc_throttle = Arc::new(Throttle::per_second(10));
    assert_eq!(info.sync_requests(&arc_throttle), 7);
    assert_eq!(arc_throttle.available(), 3);

    let boxed: Box<dyn Limiter> = Box::new(Throttle::per_second(10));
    assert_eq!(info.sync_tokens(&boxed), 0, "no token window");
    assert_eq!(info.sync_requests(&boxed), 7);

    let borrowed = Throttle::per_second(10);
    let by_ref = &borrowed;
    assert_eq!(info.sync_requests(&by_ref), 7);

    let hybrid = Hybrid::builder()
        .limiter(Arc::clone(&arc_throttle))
        .limiter(boxed)
        .build();
    assert!(hybrid.try_acquire_with_cost(3));
    assert_eq!(arc_throttle.available(), 0);
}

// TN2-006: a timed acquire always resolves by its deadline, even while a
// constituent keeps answering "retry now" (a half-open breaker whose probe is
// in flight).
#[cfg(all(feature = "circuit-breaker", feature = "tokio"))]
#[tokio::test(start_paused = true)]
async fn tn2_006_timeout_fires_while_a_constituent_says_retry_now() {
    use throttle_net::{CircuitBreaker, Hybrid, ManualClock, Throttle, ThrottleError, Trip};

    let clock = Arc::new(ManualClock::new());
    let breaker = Arc::new(
        CircuitBreaker::builder()
            .trip(Trip::Consecutive(1))
            .cooldown(Duration::from_secs(1))
            .half_open(1, 1)
            .build(Throttle::per_second(100))
            .with_clock(clock.clone()),
    );
    breaker.record_failure();
    clock.advance(Duration::from_secs(2));
    let probe = breaker.try_acquire().unwrap().unwrap();
    let hybrid = Hybrid::builder().shared(breaker.clone()).build();
    let start = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        hybrid.acquire_with_cost_timeout(1, Duration::from_millis(50)),
    )
    .await;
    assert_eq!(result, Ok(Err(ThrottleError::DeadlineExceeded)));
    assert!(
        start.elapsed() <= Duration::from_millis(51),
        "{:?}",
        start.elapsed()
    );
    probe.success();
}

#[cfg(feature = "tokio")]
#[tokio::test(start_paused = true)]
async fn tn2_006_zero_timeout_fails_at_once() {
    use throttle_net::{Hybrid, Throttle, ThrottleError};

    let throttle = Throttle::per_second(1);
    assert!(throttle.try_acquire());
    let hybrid = Hybrid::builder().limiter(throttle).build();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        hybrid.acquire_with_cost_timeout(1, Duration::ZERO),
    )
    .await;
    assert_eq!(result, Ok(Err(ThrottleError::DeadlineExceeded)));
}

// TN2-007: idle-TTL eviction must not drop a key that is in reservation debt.
#[test]
fn tn2_007_ttl_eviction_keeps_reservation_debt() {
    use throttle_net::{Eviction, ManualClock, PerKey};

    let clock = Arc::new(ManualClock::new());
    let limiter: PerKey<u32, _> = PerKey::builder()
        .per_second(10)
        .eviction(Eviction::new(2, Duration::from_millis(1)))
        .shards(1)
        .clock(clock.clone())
        .build();
    assert!(limiter.try_acquire_with_cost(&0, 10));
    assert_eq!(
        limiter.reserve(&0, 10, Duration::MAX),
        Some(Duration::from_secs(1))
    );
    assert_eq!(
        limiter.reserve(&0, 10, Duration::MAX),
        Some(Duration::from_secs(2))
    );
    clock.advance(Duration::from_millis(1100));
    assert!(
        limiter.try_acquire(&1),
        "another key's insert sweeps the shard"
    );
    let mut extra = 0;
    while limiter.try_acquire(&0) {
        extra += 1;
    }
    assert_eq!(extra, 0, "key 0's reservations still hold its budget");
}

// TN2-008: server-driven pauses are capped (by default at five minutes).
#[cfg(feature = "provider-headers")]
#[test]
fn tn2_008_hostile_headers_cannot_pause_for_years() {
    use throttle_net::Throttle;
    use throttle_net::provider::HeaderProfile;

    let five_minutes = Duration::from_secs(300);
    let throttle = Throttle::per_second(10);
    let info = HeaderProfile::STRIPE.parse(&[("Retry-After", "999999999")]);
    let _ = info.sync_requests(&throttle);
    assert!(
        throttle.paused_for() <= five_minutes,
        "{:?}",
        throttle.paused_for()
    );
    assert!(
        throttle.paused_for() > Duration::ZERO,
        "a capped pause still applies"
    );

    let throttle = Throttle::per_second(10);
    let info = HeaderProfile::GITHUB.parse(&[
        ("x-ratelimit-remaining", "0"),
        ("x-ratelimit-reset", "9223372036854775807"),
    ]);
    let _ = info.sync_requests(&throttle);
    assert!(
        throttle.paused_for() <= five_minutes,
        "{:?}",
        throttle.paused_for()
    );
}

#[cfg(feature = "provider-headers")]
#[test]
fn tn2_008_the_pause_ceiling_is_configurable() {
    use throttle_net::Throttle;
    use throttle_net::provider::{DEFAULT_MAX_PAUSE, HeaderProfile};

    assert_eq!(DEFAULT_MAX_PAUSE, Duration::from_secs(300));
    let info = HeaderProfile::STRIPE.parse(&[("Retry-After", "120")]);
    let throttle = Throttle::per_second(10);
    let _ = info.sync_requests_capped(&throttle, Duration::from_secs(5));
    assert!(throttle.paused_for() <= Duration::from_secs(5));
    let throttle = Throttle::per_second(10);
    let _ = info.sync_requests_capped(&throttle, Duration::ZERO);
    assert_eq!(
        throttle.paused_for(),
        Duration::ZERO,
        "a zero ceiling never pauses"
    );
}

// TN2-009: a pause set after waiters reserved still holds them.
#[cfg(feature = "tokio")]
#[tokio::test(start_paused = true)]
async fn tn2_009_pause_holds_waiters_that_already_reserved() {
    use throttle_net::{Throttle, TokioClock};

    let throttle = Arc::new(Throttle::per_second(10).with_clock(TokioClock::new()));
    assert!(throttle.try_acquire_with_cost(10));
    let mut waiters = Vec::new();
    for _ in 0..5 {
        let t = Arc::clone(&throttle);
        waiters.push(tokio::spawn(async move {
            t.acquire_with_cost(2).await.unwrap();
            tokio::time::Instant::now()
        }));
    }
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    let start = tokio::time::Instant::now();
    throttle.pause_for(Duration::from_secs(60));
    for waiter in waiters {
        let done = waiter.await.unwrap() - start;
        assert!(done >= Duration::from_secs(60), "released after {done:?}");
    }
}

// TN2-010: `resume` wakes waiters sleeping out a pause.
#[cfg(feature = "tokio")]
#[tokio::test(start_paused = true)]
async fn tn2_010_resume_wakes_paused_waiters() {
    use throttle_net::{Throttle, TokioClock};

    let throttle = Arc::new(Throttle::per_second(10).with_clock(TokioClock::new()));
    throttle.pause_for(Duration::from_secs(3600));
    let start = tokio::time::Instant::now();
    let t = Arc::clone(&throttle);
    let waiter = tokio::spawn(async move {
        t.acquire().await.unwrap();
        tokio::time::Instant::now()
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    throttle.resume();
    let done = waiter.await.unwrap() - start;
    assert!(done < Duration::from_secs(2), "woke after {done:?}");
}

#[cfg(feature = "tokio")]
#[tokio::test(start_paused = true)]
async fn tn2_010_resume_wakes_sliding_waiters() {
    use throttle_net::{SlidingWindowLog, TokioClock};

    let sliding = Arc::new(SlidingWindowLog::per_second(10).with_clock(TokioClock::new()));
    sliding.pause_for(Duration::from_secs(3600));
    let start = tokio::time::Instant::now();
    let s = Arc::clone(&sliding);
    let waiter = tokio::spawn(async move {
        s.acquire().await.unwrap();
        tokio::time::Instant::now()
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    sliding.resume();
    let done = waiter.await.unwrap() - start;
    assert!(done < Duration::from_secs(2), "woke after {done:?}");
}

#[cfg(feature = "tokio")]
#[tokio::test(start_paused = true)]
async fn tn2_010_resume_wakes_composite_waiters() {
    use throttle_net::{Hybrid, Throttle, TokioClock};

    let hybrid = Arc::new(
        Hybrid::builder()
            .limiter(Throttle::per_second(10).with_clock(TokioClock::new()))
            .build(),
    );
    hybrid.pause_for(Duration::from_secs(3600));
    let start = tokio::time::Instant::now();
    let h = Arc::clone(&hybrid);
    let waiter = tokio::spawn(async move {
        h.acquire().await.unwrap();
        tokio::time::Instant::now()
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    hybrid.resume();
    let done = waiter.await.unwrap() - start;
    assert!(done < Duration::from_secs(2), "woke after {done:?}");
}

// TN2-011: a one-shot flood of drained keys no longer locks newcomers out
// (the evicted keys' debts move to the ledger, so they stay limited).
#[test]
fn tn2_011_a_flood_at_the_cap_no_longer_locks_newcomers_out() {
    use throttle_net::{Eviction, ManualClock, PerKey};

    let clock = Arc::new(ManualClock::new());
    let limiter: PerKey<u64, _> = PerKey::builder()
        .per_minute(100)
        .eviction(Eviction::capacity(10_000))
        .shards(1)
        .clock(clock)
        .build();
    for key in 0..10_000u64 {
        assert!(limiter.try_acquire_with_cost(&key, 100), "flood key {key}");
    }
    assert!(limiter.try_acquire(&u64::MAX), "a newcomer is admitted");
    assert!(
        !limiter.try_acquire(&0),
        "a forgotten flood key is still limited"
    );
}

// TN2-011: locking out needs twice the cap; `WhenFull::ForgetDebt` never
// locks out.
#[test]
fn tn2_011_key_flood_lockout_needs_twice_the_cap() {
    use throttle_net::{Eviction, ManualClock, PerKey, WhenFull};

    let flood = |policy: Eviction, keys: u64| {
        let clock = Arc::new(ManualClock::new());
        let limiter: PerKey<u64, _> = PerKey::builder()
            .per_minute(100)
            .eviction(policy)
            .shards(1)
            .clock(clock)
            .build();
        for key in 0..keys {
            assert!(limiter.try_acquire_with_cost(&key, 100), "flood key {key}");
        }
        limiter
    };

    // Only a flood of twice the cap fills keys and ledger alike.
    let limiter = flood(Eviction::capacity(1_000), 2_000);
    assert!(!limiter.try_acquire(&u64::MAX), "secure default: refused");
    let limiter = flood(
        Eviction::capacity(1_000).with_when_full(WhenFull::ForgetDebt),
        2_000,
    );
    assert!(limiter.try_acquire(&u64::MAX), "ForgetDebt admits");
}

// TN2-012: the tower layers apply backpressure in `poll_ready` and do not
// poll (or hold) the inner service's readiness while they wait.
#[cfg(all(feature = "tower", feature = "tokio"))]
#[tokio::test(start_paused = true)]
async fn tn2_012_tower_layer_waits_in_poll_ready() {
    use core::task::{Context, Poll};
    use std::sync::atomic::{AtomicU32, Ordering};
    use throttle_net::tower::ThrottleLayer;
    use throttle_net::{Throttle, TokioClock};
    use tower::{Service, ServiceBuilder, ServiceExt};

    #[derive(Clone)]
    struct Counting(Arc<AtomicU32>);
    impl Service<u32> for Counting {
        type Response = u32;
        type Error = std::convert::Infallible;
        type Future = core::future::Ready<Result<u32, Self::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            let _ = self.0.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, n: u32) -> Self::Future {
            core::future::ready(Ok(n))
        }
    }

    let polls = Arc::new(AtomicU32::new(0));
    let throttle = Arc::new(Throttle::per_second(1).with_clock(TokioClock::new()));
    let mut service = ServiceBuilder::new()
        .layer(ThrottleLayer::shared(throttle.clone()))
        .service(Counting(Arc::clone(&polls)));
    assert_eq!(service.ready().await.unwrap().call(1).await.unwrap(), 1);
    let polled = polls.load(Ordering::Relaxed);
    // The next unit is a second away: readiness itself is pending.
    let pending = tokio::time::timeout(Duration::from_millis(500), service.ready()).await;
    assert!(pending.is_err(), "poll_ready applies backpressure");
    assert_eq!(
        polls.load(Ordering::Relaxed),
        polled,
        "inner not polled while waiting"
    );
    let ready = tokio::time::timeout(Duration::from_secs(1), service.ready()).await;
    assert!(ready.is_ok());
    // Dropping a ready service gives its unit back.
    drop(service);
    assert_eq!(throttle.available(), 1);
}

// TN2-013: a waiter admitted while closed is shed if the breaker opens
// before its queued wait ends.
#[cfg(all(feature = "circuit-breaker", feature = "tokio"))]
#[tokio::test(start_paused = true)]
async fn tn2_013_queued_breaker_waiter_is_shed_when_the_breaker_opens() {
    use throttle_net::{CircuitBreaker, Throttle, ThrottleError, TokioClock, Trip};

    let breaker = Arc::new(
        CircuitBreaker::builder()
            .trip(Trip::Consecutive(1))
            .cooldown(Duration::from_secs(60))
            .build(Throttle::per_second(1).with_clock(TokioClock::new())),
    );
    let first = breaker.try_acquire().unwrap().unwrap();
    let b = Arc::clone(&breaker);
    let waiter = tokio::spawn(async move {
        b.acquire_owned()
            .await
            .map(throttle_net::OwnedPermit::success)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    first.failure(); // trips the breaker while the waiter is queued
    let result = waiter.await.unwrap();
    assert!(
        matches!(result, Err(ThrottleError::CircuitOpen { .. })),
        "{result:?}"
    );
}

// TN2-014: a composite's sliding-log constituent is charged when the request
// goes, not while it waits on a slower constituent.
#[cfg(feature = "tokio")]
#[tokio::test(start_paused = true)]
async fn tn2_014_sliding_constituent_is_charged_at_send_time() {
    use throttle_net::{Hybrid, Limiter, SlidingWindowLog, Throttle, TokioClock};

    let sliding: Arc<dyn Limiter> =
        Arc::new(SlidingWindowLog::per_second(10).with_clock(TokioClock::new()));
    let slow = Throttle::per_second(1).with_clock(TokioClock::new());
    assert!(slow.try_acquire());
    let hybrid = Arc::new(
        Hybrid::builder()
            .shared(Arc::clone(&sliding))
            .limiter(slow)
            .build(),
    );
    let h = Arc::clone(&hybrid);
    let waiter = tokio::spawn(async move { h.acquire().await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(sliding.available(), 10, "nothing recorded while waiting");
    assert_eq!(waiter.await.unwrap(), Ok(()));
    assert_eq!(sliding.available(), 9);
}

// TN2-015: a manual outcome cannot free a trial slot it never held.
#[cfg(feature = "circuit-breaker")]
#[test]
fn tn2_015_manual_outcome_does_not_free_a_permits_trial_slot() {
    use throttle_net::{CircuitBreaker, ManualClock, Throttle, Trip};

    let clock = Arc::new(ManualClock::new());
    let breaker = CircuitBreaker::builder()
        .trip(Trip::Consecutive(1))
        .cooldown(Duration::from_secs(1))
        .half_open(2, 2)
        .build(Throttle::per_second(100))
        .with_clock(clock.clone());
    breaker.record_failure();
    clock.advance(Duration::from_secs(2));
    let first = breaker.try_acquire().unwrap().unwrap(); // one of two trial slots
    breaker.record_success(); // an unrelated call's outcome
    let second = breaker.try_acquire().unwrap().unwrap();
    assert!(breaker.try_acquire().is_err(), "still two trials at a time");
    first.success();
    second.success();
}

// TN2-016: an overrun settled while a dimension is paused is still charged.
#[test]
fn tn2_016_settle_charges_overrun_while_paused() {
    use throttle_net::{ManualClock, MultiLimiter, Throttle};

    let clock = Arc::new(ManualClock::new());
    let limiter = MultiLimiter::builder()
        .dimension(
            "tokens",
            Throttle::per_minute(1_000).with_clock(clock.clone()),
        )
        .build();
    assert!(limiter.try_acquire_costs(&[("tokens", 1_000)]));
    limiter.pause_for(Duration::from_secs(1));
    limiter.settle_costs(&[("tokens", 1_000)], &[("tokens", 1_500)]);
    clock.advance(Duration::from_secs(1));
    // 500 owed at 1000/min: ~29s more before anything is free.
    let wait = match limiter.peek_costs(&[("tokens", 1)]) {
        throttle_net::Decision::Retry { after } => after,
        other => panic!("{other:?}"),
    };
    assert!(wait >= Duration::from_secs(29), "{wait:?}");
}

// TN2-017: every builder takes an owned name.
#[cfg(all(feature = "circuit-breaker", feature = "adaptive", feature = "tokio"))]
#[test]
fn tn2_017_owned_names_everywhere() {
    use throttle_net::{AdaptiveLimiter, Aimd, Backoff, CircuitBreaker, Queue, Retry, Throttle};

    let id = 7;
    let breaker = CircuitBreaker::builder()
        .name(format!("breaker-{id}"))
        .build(Throttle::per_second(1));
    assert_eq!(breaker.name(), Some("breaker-7"));
    let adaptive = AdaptiveLimiter::builder()
        .name(format!("adaptive-{id}"))
        .build(Aimd::default());
    assert_eq!(adaptive.name(), Some("adaptive-7"));
    let queue: Queue<Throttle> = Queue::builder()
        .name(format!("queue-{id}"))
        .build(Throttle::per_second(1));
    assert_eq!(queue.name(), Some("queue-7"));
    let _retry = Retry::new(Backoff::default()).name(format!("retry-{id}"));
}

// TN2-020: a permit can be given back without counting as a failure.
#[cfg(feature = "circuit-breaker")]
#[test]
fn tn2_020_cancelled_permit_records_no_outcome() {
    use throttle_net::{BreakerState, CircuitBreaker, Throttle, Trip};

    let breaker = CircuitBreaker::builder()
        .trip(Trip::Consecutive(1))
        .build(Throttle::per_second(10));
    breaker.try_acquire().unwrap().unwrap().cancel();
    assert_eq!(breaker.state(), BreakerState::Closed);
    assert_eq!(breaker.inner().available(), 10, "the unit was refunded");
}
