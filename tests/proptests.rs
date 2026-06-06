//! Property-based tests for the defining invariant: a limiter never admits more
//! than its capacity, and a composite never admits more than its binding scope.
//!
//! Each property drives a `ManualClock` that is never advanced, so no refill
//! happens during the test. With a full bucket and a frozen clock the number of
//! grants is exact and deterministic — the bound is `min(attempts, capacity)` —
//! which turns "never over-admits" into an equality `proptest` can check over a
//! wide input space.

#![allow(clippy::unwrap_used)]
// The limiter surface these properties exercise requires `std`; with it off the
// crate exposes only `VERSION`, so this suite has nothing to test.
#![cfg(feature = "std")]

use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use throttle_net::{
    Backoff, Eviction, Hybrid, Jitter, Layered, ManualClock, MultiLimiter, PerKey,
    SlidingWindowLog, Throttle,
};

proptest! {
    /// A single throttle hands out exactly its capacity from full, never more.
    #[test]
    fn throttle_burst_never_exceeds_capacity(
        capacity in 1u32..1_000,
        attempts in 0u32..2_000,
    ) {
        let clock = Arc::new(ManualClock::new());
        let throttle = Throttle::per_second(capacity).with_clock(clock);

        let mut granted = 0u32;
        for _ in 0..attempts {
            if throttle.try_acquire() {
                granted += 1;
            }
        }
        prop_assert_eq!(granted, attempts.min(capacity));
        prop_assert!(!throttle.try_acquire() || attempts < capacity);
    }

    /// Cost-aware grants also stop at capacity: the total tokens taken across a
    /// sequence of arbitrary-cost acquisitions never exceeds capacity.
    #[test]
    fn throttle_cost_aware_grants_never_exceed_capacity(
        capacity in 1u32..1_000,
        costs in proptest::collection::vec(1u32..50, 0..200),
    ) {
        let clock = Arc::new(ManualClock::new());
        let throttle = Throttle::per_second(capacity).with_clock(clock);

        let mut taken = 0u32;
        for cost in costs {
            if throttle.try_acquire_with_cost(cost) {
                taken += cost;
            }
        }
        prop_assert!(taken <= capacity, "took {taken}, capacity {capacity}");
    }

    /// A hybrid grants the minimum of its constituents' capacities — the tightest
    /// one binds, and never over-admits past it.
    #[test]
    fn hybrid_grants_the_minimum_constituent_capacity(
        a in 1u32..500,
        b in 1u32..500,
        attempts in 0u32..1_500,
    ) {
        let clock = Arc::new(ManualClock::new());
        let hybrid = Hybrid::builder()
            .limiter(Throttle::per_second(a).with_clock(clock.clone()))
            .limiter(Throttle::per_second(b).with_clock(clock.clone()))
            .build();

        let mut granted = 0u32;
        for _ in 0..attempts {
            if hybrid.try_acquire() {
                granted += 1;
            }
        }
        prop_assert_eq!(granted, attempts.min(a.min(b)));
    }

    /// Per-key state is independent: every key gets its own full capacity, and
    /// the totals add up exactly with no cross-key leakage.
    #[test]
    fn perkey_keys_are_independent_and_each_bounded(
        capacity in 1u32..200,
        keys in 1u64..20,
        attempts in 0u32..400,
    ) {
        let clock = Arc::new(ManualClock::new());
        let limiter = PerKey::<u64>::per_second(capacity)
            .with_clock(clock)
            .with_eviction(Eviction::unbounded());

        let mut total = 0u32;
        for key in 0..keys {
            let mut granted = 0u32;
            for _ in 0..attempts {
                if limiter.try_acquire(&key) {
                    granted += 1;
                }
            }
            prop_assert_eq!(granted, attempts.min(capacity));
            total += granted;
        }
        prop_assert_eq!(total, u32::try_from(keys).unwrap() * attempts.min(capacity));
    }

    /// A unique-key flood never grows the store past its eviction bound.
    #[test]
    fn perkey_flood_stays_within_the_eviction_bound(
        max_keys in 8usize..512,
        flood in 100u64..5_000,
    ) {
        let shards = 8;
        let clock = Arc::new(ManualClock::new());
        let limiter = PerKey::<u64>::per_second(10)
            .with_clock(clock)
            .with_shards(shards)
            .with_eviction(Eviction::capacity(max_keys));

        for k in 0..flood {
            let _ = limiter.try_acquire(&k);
        }

        let per_shard_cap = max_keys.div_ceil(shards).max(1);
        let bound = per_shard_cap * shards;
        prop_assert!(limiter.len() <= bound, "grew to {}, bound {bound}", limiter.len());
    }

    /// A layered limiter admits the minimum across its scopes; for a single key
    /// and endpoint that is `min(global, per_key, per_endpoint)`.
    #[test]
    fn layered_grants_the_minimum_across_scopes(
        global in 1u32..300,
        per_key in 1u32..300,
        per_endpoint in 1u32..300,
        attempts in 0u32..900,
    ) {
        let clock = Arc::new(ManualClock::new());
        let layered = Layered::<u64>::builder()
            .global(Throttle::per_second(global).with_clock(clock.clone()))
            .per_key(PerKey::<u64>::per_second(per_key).with_clock(clock.clone()))
            .per_endpoint(PerKey::<u64>::per_second(per_endpoint).with_clock(clock.clone()))
            .build();

        let mut granted = 0u32;
        for _ in 0..attempts {
            if layered.try_acquire(&1, &1) {
                granted += 1;
            }
        }
        let bound = global.min(per_key).min(per_endpoint);
        prop_assert_eq!(granted, attempts.min(bound));
    }

    /// A multi-dimensional limiter is bound by its tightest dimension when each
    /// call costs one unit per dimension.
    #[test]
    fn multi_dimension_binds_on_the_tightest(
        requests in 1u32..150,
        tokens in 1u32..150,
        attempts in 0u32..400,
    ) {
        let clock = Arc::new(ManualClock::new());
        let limiter = MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(requests).with_clock(clock.clone()))
            .dimension("tokens", Throttle::per_second(tokens).with_clock(clock.clone()))
            .build();

        let mut granted = 0u32;
        for _ in 0..attempts {
            if limiter.try_acquire_costs(&[("requests", 1), ("tokens", 1)]) {
                granted += 1;
            }
        }
        prop_assert_eq!(granted, attempts.min(requests.min(tokens)));
    }

    /// The exact sliding-window log admits no more than `limit` units in any
    /// trailing window. With a frozen clock the whole burst falls in one window,
    /// so the count is exactly `min(attempts, limit)` — never more.
    #[test]
    fn sliding_window_admits_at_most_limit_per_window(
        limit in 1u32..500,
        attempts in 0u32..1_000,
    ) {
        let clock = Arc::new(ManualClock::new());
        let limiter = SlidingWindowLog::new(limit, Duration::from_secs(1)).with_clock(clock);

        let mut granted = 0u32;
        for _ in 0..attempts {
            if limiter.try_acquire() {
                granted += 1;
            }
        }
        prop_assert_eq!(granted, attempts.min(limit));
    }

    /// As the window slides, capacity is reclaimed exactly: after the window fully
    /// passes, the limiter admits another `limit` units and never more across the
    /// two windows than `2 * limit`.
    #[test]
    fn sliding_window_reclaims_capacity_after_the_window(
        limit in 1u32..200,
    ) {
        let clock = Arc::new(ManualClock::new());
        let window = Duration::from_secs(1);
        let limiter = SlidingWindowLog::new(limit, window).with_clock(clock.clone());

        // Fill the first window.
        let mut first = 0u32;
        for _ in 0..(limit * 2) {
            if limiter.try_acquire() { first += 1; }
        }
        prop_assert_eq!(first, limit);

        // Slide past the window: the earlier grants age out and capacity returns.
        clock.advance(window + Duration::from_millis(1));
        let mut second = 0u32;
        for _ in 0..(limit * 2) {
            if limiter.try_acquire() { second += 1; }
        }
        prop_assert_eq!(second, limit);
    }

    /// Every backoff delay, under every jitter mode, stays within `[0, max]` — a
    /// jittered curve never overshoots the configured ceiling, and the unjittered
    /// curve is monotonically non-decreasing up to the cap.
    #[test]
    fn backoff_delays_stay_within_the_ceiling(
        base_ms in 1u64..1_000,
        max_ms in 1u64..60_000,
        factor in 1.0f64..4.0,
        seed in any::<u64>(),
        jitter in prop_oneof![
            Just(Jitter::None),
            Just(Jitter::Full),
            Just(Jitter::Equal),
            Just(Jitter::Decorrelated),
        ],
    ) {
        let max = Duration::from_millis(max_ms);
        let backoff = Backoff::exponential(Duration::from_millis(base_ms), factor)
            .with_max(max)
            .with_jitter(jitter);

        let mut delays = backoff.iter_seeded(seed);
        let mut prev = Duration::ZERO;
        for _ in 0..32 {
            let d = delays.next_delay();
            prop_assert!(d <= max, "delay {d:?} exceeded max {max:?} under {jitter:?}");
            if jitter == Jitter::None {
                // The unjittered curve never decreases until it saturates at max.
                prop_assert!(d >= prev || d == max, "unjittered curve went backwards");
                prev = d;
            }
        }
    }
}

/// The adaptive limit, after any sequence of outcomes, is always clamped to
/// `[floor, ceiling]` and never exceeds the hard ceiling.
#[cfg(feature = "adaptive")]
mod adaptive_props {
    use super::{Arc, Duration, ManualClock};
    use proptest::prelude::*;
    use throttle_net::{AdaptiveLimiter, Aimd};

    proptest! {
        #[test]
        fn adaptive_limit_stays_within_floor_and_ceiling(
            floor in 1u32..20,
            span in 0u32..200,
            // A sequence of outcomes: `true` = success, `false` = failure.
            outcomes in proptest::collection::vec(any::<bool>(), 0..200),
        ) {
            let ceiling = floor + span;
            let clock = Arc::new(ManualClock::new());
            let limiter = AdaptiveLimiter::builder()
                .floor(floor)
                .ceiling(ceiling)
                .initial(floor)
                .build(Aimd::new(3, 0.5))
                .with_clock(clock.clone());

            for ok in outcomes {
                if let Some(permit) = limiter.try_acquire() {
                    prop_assert!(limiter.in_flight() <= ceiling, "in-flight exceeded ceiling");
                    if ok {
                        clock.advance(Duration::from_millis(1));
                        permit.success();
                    } else {
                        permit.failure();
                    }
                }
                let limit = limiter.current_limit();
                prop_assert!(limit >= floor && limit <= ceiling,
                    "limit {limit} escaped [{floor}, {ceiling}]");
            }
        }
    }
}
