//! Property tests over the arithmetic edges: arbitrary operation sequences
//! checked against the token-bucket bound, extreme configurations that must
//! never panic, and a reference model for the sliding window.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use throttle_net::{
    Backoff, Decision, Jitter, Limiter, ManualClock, MultiLimiter, PerKey, SlidingWindowLog,
    Throttle,
};

/// One step of a throttle's life.
#[derive(Debug, Clone)]
enum Op {
    Acquire(u32),
    Reserve(u32, u64),
    RefundLast,
    Advance(u64),
}

fn op(capacity: u32) -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..=capacity.saturating_add(2)).prop_map(Op::Acquire),
        (0..=capacity.saturating_add(2), 0u64..5_000).prop_map(|(c, w)| Op::Reserve(c, w)),
        Just(Op::RefundLast),
        (0u64..2_000).prop_map(Op::Advance),
    ]
}

/// Tokens accrued at `amount` per `period_ms` over `ms`, rounded up.
fn accrued(amount: u32, period_ms: u64, ms: u128) -> u128 {
    (u128::from(amount) * ms).div_ceil(u128::from(period_ms))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Whatever the interleaving of grants, reservations, refunds, and time,
    /// the units a throttle has handed out (net of refunds) never exceed its
    /// burst plus what its rate accrued by the time they become usable.
    #[test]
    fn throttle_operations_obey_the_bucket_bound(
        amount in 1u32..500,
        period_ms in 1u64..5_000,
        burst_extra in 0u32..500,
        ops in (1u32..1_000).prop_flat_map(|cap| proptest::collection::vec(op(cap), 0..120)),
    ) {
        let capacity = amount + burst_extra;
        let clock = Arc::new(ManualClock::new());
        let throttle = Throttle::builder()
            .rate(amount, Duration::from_millis(period_ms))
            .burst(capacity)
            .clock(clock.clone())
            .try_build()
            .unwrap();

        let mut now_ms = 0u128;
        let mut taken = 0u128;
        let mut last: Option<u32> = None;
        for op in ops {
            match op {
                Op::Acquire(cost) => {
                    let granted = throttle.try_acquire_with_cost(cost);
                    if cost > capacity {
                        prop_assert!(!granted || cost == 0);
                    }
                    // A zero cost is always granted and changes nothing.
                    if granted && cost > 0 {
                        taken += u128::from(cost);
                        last = Some(cost);
                        prop_assert!(
                            taken <= u128::from(capacity) + accrued(amount, period_ms, now_ms) + 1,
                            "grant overran the bound: {taken} at {now_ms}ms"
                        );
                    }
                }
                Op::Reserve(cost, max_ms) => {
                    match throttle.reserve(cost, Duration::from_millis(max_ms)) {
                        Some(wait) if cost == 0 => prop_assert_eq!(wait, Duration::ZERO),
                        Some(wait) => {
                            prop_assert!(cost <= capacity);
                            prop_assert!(wait <= Duration::from_millis(max_ms));
                            taken += u128::from(cost);
                            last = Some(cost);
                            let usable_at = now_ms + wait.as_millis();
                            prop_assert!(
                                taken <= u128::from(capacity) + accrued(amount, period_ms, usable_at) + 1,
                                "reservation usable too early: {taken} at {usable_at}ms"
                            );
                        }
                        None => prop_assert!(cost > 0),
                    }
                }
                Op::RefundLast => {
                    if let Some(cost) = last.take() {
                        throttle.refund(cost);
                        taken -= u128::from(cost);
                    }
                }
                Op::Advance(ms) => {
                    clock.advance(Duration::from_millis(ms));
                    now_ms += u128::from(ms);
                }
            }
            prop_assert!(throttle.available() <= capacity);
        }
    }

    /// Peek and acquire agree on every throttle, and the wait peek reports is
    /// exactly when acquire starts succeeding (to the millisecond).
    #[test]
    fn throttle_peek_predicts_acquire(
        amount in 1u32..100_000,
        period_ms in 1u64..86_400_000,
        drain in 0u32..100_000,
        cost in 1u32..100_000,
    ) {
        let clock = Arc::new(ManualClock::new());
        let throttle = Throttle::per_duration(amount, Duration::from_millis(period_ms))
            .with_clock(clock.clone());
        let _ = throttle.try_acquire_with_cost(drain.min(amount));
        match throttle.peek(cost) {
            Decision::Acquired => prop_assert!(throttle.try_acquire_with_cost(cost)),
            Decision::Retry { after } => {
                prop_assert!(!throttle.try_acquire_with_cost(cost));
                clock.advance(after);
                prop_assert!(throttle.try_acquire_with_cost(cost), "not ready after {after:?}");
            }
            Decision::Impossible => {
                prop_assert!(cost > amount);
                prop_assert!(!throttle.try_acquire_with_cost(cost));
            }
            _ => prop_assert!(false, "unknown decision"),
        }
    }

    /// No configuration, however extreme, makes a throttle panic or report
    /// more available than its capacity.
    #[test]
    fn extreme_throttle_configurations_never_panic(
        amount in any::<u32>(),
        period_ns in any::<u64>(),
        burst in proptest::option::of(any::<u32>()),
        initial in proptest::option::of(any::<u32>()),
        cost in any::<u32>(),
        advance_ns in any::<u64>(),
    ) {
        let clock = Arc::new(ManualClock::new());
        let mut builder = Throttle::builder()
            .rate(amount, Duration::from_nanos(period_ns))
            .clock(clock.clone());
        if let Some(burst) = burst {
            builder = builder.burst(burst);
        }
        if let Some(initial) = initial {
            builder = builder.initial(initial);
        }
        let throttle = builder.build();
        let _ = throttle.peek(cost);
        let _ = throttle.try_acquire_with_cost(cost);
        let _ = throttle.reserve(cost, Duration::MAX);
        clock.advance(Duration::from_nanos(advance_ns));
        let _ = throttle.peek(cost);
        throttle.refund(cost);
        let _ = throttle.drain_to(cost / 2);
        throttle.pause_for(Duration::from_nanos(advance_ns));
        prop_assert!(throttle.available() <= throttle.capacity());
    }

    /// Every backoff configuration — any base, ceiling, factor (NaN, infinite,
    /// negative), and jitter — yields delays within the ceiling and never
    /// panics (TN-012).
    #[test]
    fn extreme_backoffs_never_panic_or_exceed_the_ceiling(
        base_ns in any::<u64>(),
        max_ns in proptest::option::of(any::<u64>()),
        factor in prop_oneof![
            any::<f64>(),
            Just(f64::NAN),
            Just(f64::INFINITY),
            Just(f64::NEG_INFINITY),
            Just(0.0),
            -10.0f64..10.0,
        ],
        linear in any::<bool>(),
        jitter in prop_oneof![
            Just(Jitter::None),
            Just(Jitter::Full),
            Just(Jitter::Equal),
            Just(Jitter::Decorrelated),
        ],
        seed in any::<u64>(),
    ) {
        let base = Duration::from_nanos(base_ns);
        let max = max_ns.map_or(Duration::MAX, Duration::from_nanos);
        let backoff = if linear {
            Backoff::linear(base, base)
        } else {
            Backoff::exponential(base, factor)
        }
        .with_max(max)
        .with_jitter(jitter);
        let mut delays = backoff.iter_seeded(seed);
        for _ in 0..70 {
            prop_assert!(delays.next_delay() <= max);
        }
    }

    /// An unseen key is judged exactly as a fresh bucket would be, for any
    /// burst and initial fill.
    #[test]
    fn per_key_peek_on_an_unseen_key_matches_its_first_acquire(
        amount in 1u32..10_000,
        burst in 1u32..10_000,
        initial in 0u32..10_000,
        cost in 0u32..12_000,
    ) {
        let clock = Arc::new(ManualClock::new());
        let limiter: PerKey<u8, _> = PerKey::builder()
            .per_second(amount)
            .burst(burst)
            .initial(initial)
            .clock(clock)
            .try_build()
            .unwrap();
        let predicted = limiter.peek(&1, cost);
        prop_assert!(limiter.is_empty());
        prop_assert_eq!(limiter.try_acquire_with_cost(&1, cost), predicted.is_acquired());
    }

    /// Aliased and repeated entries are charged as one summed cost, and a call
    /// is admitted exactly when the sum fits.
    #[test]
    fn multi_limiter_sums_aliases_and_repeats(
        capacity in 1u32..10_000,
        costs in proptest::collection::vec((0usize..3, 0u32..5_000), 0..6),
    ) {
        let clock = Arc::new(ManualClock::new());
        let limiter = MultiLimiter::builder()
            .dimension("tokens", Throttle::per_minute(capacity).with_clock(clock))
            .alias("input_tokens", "tokens")
            .alias("output_tokens", "tokens")
            .build();
        let names = ["tokens", "input_tokens", "output_tokens"];
        let call: Vec<(&str, u32)> = costs.iter().map(|&(n, c)| (names[n], c)).collect();
        let total: u64 = costs.iter().map(|&(_, c)| u64::from(c)).sum();
        let granted = limiter.try_acquire_costs(&call);
        prop_assert_eq!(granted, total <= u64::from(capacity));
        let left = u64::from(limiter.available("tokens").unwrap());
        prop_assert_eq!(left, if granted { u64::from(capacity) - total } else { u64::from(capacity) });
    }

    /// The sliding log matches a reference model: a request is admitted exactly
    /// when the units granted in the trailing window plus its cost fit the
    /// limit, for any window down to a nanosecond.
    #[test]
    fn sliding_window_matches_a_reference_model(
        limit in 1u32..50,
        window_ns in 1u64..5_000_000,
        steps in proptest::collection::vec((0u32..10, 0u64..3_000_000), 0..150),
    ) {
        let clock = Arc::new(ManualClock::new());
        let limiter = SlidingWindowLog::new(limit, Duration::from_nanos(window_ns))
            .with_clock(clock.clone());
        let mut model: VecDeque<(u64, u32)> = VecDeque::new();
        let mut now = 0u64;
        for (cost, advance) in steps {
            clock.advance(Duration::from_nanos(advance));
            now += advance;
            while model.front().is_some_and(|&(at, _)| at + window_ns <= now) {
                let _ = model.pop_front();
            }
            let used: u32 = model.iter().map(|&(_, c)| c).sum();
            let expect = cost <= limit && used + cost <= limit;
            prop_assert_eq!(limiter.try_acquire_with_cost(cost), expect);
            if expect && cost > 0 {
                model.push_back((now, cost));
            }
            prop_assert_eq!(Limiter::available(&limiter), limit - model.iter().map(|&(_, c)| c).sum::<u32>());
        }
    }
}
