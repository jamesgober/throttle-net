//! Real-clock, multi-threaded over-admission tests.
//!
//! These are the tests that would have caught TN-001: better-bucket 1.0 reused
//! a stale clock reading in its compare-and-swap retry loop, so under thread
//! contention a `per_second(1000)` throttle granted ~77,000 tokens in two
//! seconds with four threads (the legal bound is about 3,000). Single-threaded
//! property tests and a frozen-clock loom model cannot see that class of bug;
//! hammering a limiter from many threads on the real clock does.
//!
//! The bound checked everywhere is the token-bucket contract: over a run of
//! `elapsed`, grants never exceed `capacity + rate * elapsed`. A small slack
//! covers the time between the last grant and the elapsed measurement.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use throttle_net::{Bulkhead, Hybrid, Layered, MultiLimiter, PerKey, SlidingWindowLog, Throttle};

/// How long each hammering run lasts.
const RUN: Duration = Duration::from_millis(400);

/// Runs `attempt` on `threads` threads for [`RUN`], returning the grants and the
/// measured elapsed time.
fn hammer<F>(threads: usize, attempt: F) -> (u64, Duration)
where
    F: Fn(usize) -> bool + Send + Sync + 'static,
{
    let attempt = Arc::new(attempt);
    let granted = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(threads + 1));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let attempt = Arc::clone(&attempt);
            let granted = Arc::clone(&granted);
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let _leader = barrier.wait();
                let mut mine = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    if attempt(t) {
                        mine += 1;
                    }
                }
                let _ = granted.fetch_add(mine, Ordering::Relaxed);
            })
        })
        .collect();
    let _leader = barrier.wait();
    let start = Instant::now();
    thread::sleep(RUN);
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().unwrap();
    }
    (granted.load(Ordering::Relaxed), start.elapsed())
}

/// The most a bucket of `capacity` refilling at `rate_per_sec` may grant in
/// `elapsed`, plus `slack` units of measurement tolerance.
fn bound(capacity: u64, rate_per_sec: u64, elapsed: Duration, slack: u64) -> u64 {
    let accrued = (u128::from(rate_per_sec) * elapsed.as_nanos()).div_ceil(1_000_000_000);
    capacity + u64::try_from(accrued).unwrap() + slack
}

fn threads() -> [usize; 3] {
    [2, 8, 16]
}

#[test]
fn throttle_never_over_admits_under_contention() {
    for threads in threads() {
        let throttle = Arc::new(Throttle::per_second(1_000));
        let t = Arc::clone(&throttle);
        let (granted, elapsed) = hammer(threads, move |_| t.try_acquire());
        let limit = bound(1_000, 1_000, elapsed, 5);
        assert!(granted <= limit, "{threads} threads: {granted} > {limit}");
        // And it is not starving either: most of the budget is used.
        assert!(granted >= 1_000, "{threads} threads granted only {granted}");
    }
}

#[test]
fn cost_aware_grants_never_over_admit_under_contention() {
    let throttle = Arc::new(Throttle::per_second(10_000));
    let t = Arc::clone(&throttle);
    let granted_units = Arc::new(AtomicU64::new(0));
    let units = Arc::clone(&granted_units);
    let (_calls, elapsed) = hammer(8, move |thread| {
        let cost = 1 + u32::try_from(thread % 7).unwrap() * 13;
        let ok = t.try_acquire_with_cost(cost);
        if ok {
            let _ = units.fetch_add(u64::from(cost), Ordering::Relaxed);
        }
        ok
    });
    let taken = granted_units.load(Ordering::Relaxed);
    let limit = bound(10_000, 10_000, elapsed, 100);
    assert!(taken <= limit, "{taken} > {limit}");
}

#[test]
fn reservations_never_over_admit_under_contention() {
    // Reservations take units ahead of accrual; every reservation's wait must
    // cover the debt it creates, so grants usable by the end of the run still
    // obey the bound.
    let throttle = Arc::new(Throttle::per_second(1_000));
    let t = Arc::clone(&throttle);
    let usable = Arc::new(AtomicU64::new(0));
    let u = Arc::clone(&usable);
    let start = Instant::now();
    let (_calls, elapsed) = hammer(8, move |_| match t.reserve(1, Duration::from_millis(50)) {
        Some(wait) => {
            // Count only units usable before the run ends.
            if start.elapsed() + wait <= RUN {
                let _ = u.fetch_add(1, Ordering::Relaxed);
            }
            true
        }
        None => false,
    });
    let used = usable.load(Ordering::Relaxed);
    let limit = bound(1_000, 1_000, elapsed, 5);
    assert!(used <= limit, "{used} > {limit}");
}

#[test]
fn per_key_single_key_never_over_admits_under_contention() {
    for threads in threads() {
        let limiter: Arc<PerKey<u64>> = Arc::new(PerKey::per_second(500));
        let l = Arc::clone(&limiter);
        let (granted, elapsed) = hammer(threads, move |_| l.try_acquire(&7));
        let limit = bound(500, 500, elapsed, 5);
        assert!(granted <= limit, "{threads} threads: {granted} > {limit}");
    }
}

#[test]
fn per_key_flood_cannot_reset_a_limited_key() {
    // A small cap and a flood of unique keys from many threads: the drained
    // victim key must stay limited throughout.
    let limiter: Arc<PerKey<u64>> = Arc::new(
        PerKey::per_duration(5, Duration::from_secs(3600))
            .with_eviction(throttle_net::Eviction::capacity(64))
            .with_shards(1),
    );
    assert!(limiter.try_acquire_with_cost(&u64::MAX, 5));
    let l = Arc::clone(&limiter);
    let next = Arc::new(AtomicU64::new(0));
    let n = Arc::clone(&next);
    let victim_grants = Arc::new(AtomicU64::new(0));
    let v = Arc::clone(&victim_grants);
    let (_flood, _elapsed) = hammer(8, move |thread| {
        if thread == 0 {
            let ok = l.try_acquire(&u64::MAX);
            if ok {
                let _ = v.fetch_add(1, Ordering::Relaxed);
            }
            ok
        } else {
            l.try_acquire(&n.fetch_add(1, Ordering::Relaxed))
        }
    });
    assert_eq!(
        victim_grants.load(Ordering::Relaxed),
        0,
        "the flood reset the victim"
    );
    assert!(limiter.len() <= 64);
}

#[test]
fn hybrid_never_over_admits_under_contention() {
    let hybrid = Arc::new(
        Hybrid::builder()
            .limiter(Throttle::per_second(800))
            .limiter(SlidingWindowLog::per_second(1_000))
            .build(),
    );
    let h = Arc::clone(&hybrid);
    let (granted, elapsed) = hammer(8, move |_| h.try_acquire());
    let limit = bound(800, 800, elapsed, 5);
    assert!(granted <= limit, "{granted} > {limit}");
}

#[test]
fn multi_limiter_never_over_admits_under_contention() {
    let limiter = Arc::new(
        MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(300))
            .dimension("tokens", Throttle::per_second(100_000))
            .build(),
    );
    let l = Arc::clone(&limiter);
    let (granted, elapsed) = hammer(8, move |_| {
        l.try_acquire_costs(&[("requests", 1), ("tokens", 10)])
    });
    let limit = bound(300, 300, elapsed, 5);
    assert!(granted <= limit, "{granted} > {limit}");
}

#[test]
fn layered_global_scope_never_over_admits_under_contention() {
    let layered = Arc::new(
        Layered::<u64>::builder()
            .global(Throttle::per_second(600))
            .per_key(PerKey::per_second(10_000))
            .build(),
    );
    let l = Arc::clone(&layered);
    let (granted, elapsed) = hammer(8, move |thread| l.try_acquire(&(thread as u64), &0));
    let limit = bound(600, 600, elapsed, 5);
    assert!(granted <= limit, "{granted} > {limit}");
}

#[test]
fn sliding_window_never_exceeds_its_limit_in_any_window() {
    // A one-second window over a 400ms run holds every grant: the total can
    // never exceed the limit.
    let limiter = Arc::new(SlidingWindowLog::per_second(700));
    let l = Arc::clone(&limiter);
    let (granted, _elapsed) = hammer(8, move |_| l.try_acquire());
    assert_eq!(granted, 700);
}

#[test]
fn bulkhead_never_exceeds_its_limit_under_contention() {
    let bulkhead = Arc::new(Bulkhead::new(4));
    let peak = Arc::new(AtomicU64::new(0));
    let (b, p) = (Arc::clone(&bulkhead), Arc::clone(&peak));
    let (_granted, _elapsed) = hammer(16, move |_| match b.try_acquire() {
        Some(permit) => {
            let _ = p.fetch_max(u64::from(b.in_use()), Ordering::Relaxed);
            drop(permit);
            true
        }
        None => false,
    });
    assert!(peak.load(Ordering::Relaxed) <= 4);
    assert_eq!(bulkhead.in_use(), 0);
}

#[cfg(feature = "tokio")]
#[test]
fn waiting_acquires_never_over_admit_on_a_multi_threaded_runtime() {
    // Many tasks waiting concurrently: every grant (immediate or reserved) must
    // fall within the bucket bound for the time it took.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let throttle = Arc::new(Throttle::per_second(200));
        let start = Instant::now();
        let tasks: Vec<_> = (0..64)
            .map(|_| {
                let t = Arc::clone(&throttle);
                tokio::spawn(async move {
                    let mut granted_at = Vec::with_capacity(5);
                    for _ in 0..5 {
                        t.acquire().await.unwrap();
                        granted_at.push(start.elapsed());
                    }
                    granted_at
                })
            })
            .collect();
        let mut grants = Vec::with_capacity(320);
        for task in tasks {
            grants.extend(task.await.unwrap());
        }
        grants.sort_unstable();
        assert_eq!(grants.len(), 320);
        // Every prefix of the grants, in the order they completed, fits the
        // bucket bound for the time it took: the n-th grant by time t needs
        // n <= capacity + rate * t (plus a little slack for the moment
        // between a waiter's wake-up and its timestamp).
        for (index, at) in grants.iter().enumerate() {
            let granted = index as u64 + 1;
            assert!(
                granted <= bound(200, 200, *at, 5),
                "grant {granted} at {at:?} exceeds the bound"
            );
        }
        // 320 grants at 200/s with a burst of 200 take at least 0.6s.
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(590), "{elapsed:?}");
    });
}

#[cfg(all(feature = "tokio", feature = "circuit-breaker"))]
#[test]
fn cancelled_probes_never_wedge_a_breaker_under_load() {
    use throttle_net::{BreakerState, CircuitBreaker, Trip};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let breaker = Arc::new(
            CircuitBreaker::builder()
                .trip(Trip::Consecutive(1))
                .cooldown(Duration::from_millis(20))
                .half_open(2, 1)
                .build(Throttle::per_second(50)),
        );
        breaker.record_failure();
        tokio::time::sleep(Duration::from_millis(25)).await;
        // Many probes, most cancelled by short timeouts while they wait.
        let tasks: Vec<_> = (0..32)
            .map(|_| {
                let b = Arc::clone(&breaker);
                tokio::spawn(async move {
                    let _ = tokio::time::timeout(Duration::from_millis(1), b.acquire()).await;
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        // The breaker must still admit a probe and recover.
        let permit = loop {
            match breaker.acquire().await {
                Ok(permit) => break permit,
                Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        };
        permit.success();
        assert_eq!(breaker.state(), BreakerState::Closed);
    });
}

#[cfg(feature = "tokio")]
#[test]
fn waiting_composites_never_over_admit_any_scope_on_a_multi_threaded_runtime() {
    // Waiters queue on their bottleneck scope and take the others when it
    // matures: every scope must still hold its bound for every grant, and the
    // per-key scopes must not be bypassed by the bottleneck switching.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let layered = Arc::new(
            Layered::<u64>::builder()
                .global(Throttle::per_second(100))
                .per_key(PerKey::per_second(40))
                .build(),
        );
        let start = Instant::now();
        let tasks: Vec<_> = (0..32u64)
            .map(|task| {
                let l = Arc::clone(&layered);
                tokio::spawn(async move {
                    let key = task % 4;
                    let mut granted_at = Vec::with_capacity(6);
                    for _ in 0..6 {
                        l.acquire(&key, &0).await.unwrap();
                        granted_at.push((key, start.elapsed()));
                    }
                    granted_at
                })
            })
            .collect();
        let mut grants = Vec::with_capacity(192);
        for task in tasks {
            grants.extend(task.await.unwrap());
        }
        grants.sort_unstable_by_key(|&(_, at)| at);
        assert_eq!(grants.len(), 192);
        let mut per_key = [0u64; 4];
        for (index, &(key, at)) in grants.iter().enumerate() {
            let global = index as u64 + 1;
            assert!(
                global <= bound(100, 100, at, 5),
                "global {global} at {at:?}"
            );
            let slot = usize::try_from(key).unwrap();
            per_key[slot] += 1;
            assert!(
                per_key[slot] <= bound(40, 40, at, 3),
                "key {key}: {} at {at:?}",
                per_key[slot]
            );
        }
    });
}
