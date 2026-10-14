//! Fuzz target: arbitrary operation sequences against a throttle, checked
//! against the token-bucket bound, and a backoff checked against its ceiling.
//!
//! This is the state-machine fuzzing the 1.0 suite lacked: the bound "units
//! handed out (net of refunds) never exceed the burst plus what the rate has
//! accrued by the time they are usable" is asserted after every step, for any
//! rate, burst, cost, wait, and clock movement the fuzzer finds. The backoff
//! half would have found TN-012 (a divide-by-zero on an unbounded ceiling).
#![no_main]

use std::sync::Arc;
use std::time::Duration;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use throttle_net::{Backoff, Jitter, ManualClock, Throttle};

#[derive(Arbitrary, Debug)]
enum Op {
    Acquire(u32),
    Reserve(u32, u32),
    RefundLast,
    AdvanceMicros(u32),
    Pause(u16),
}

#[derive(Arbitrary, Debug)]
struct Input {
    amount: u32,
    period_millis: u32,
    burst: u32,
    ops: Vec<Op>,
    backoff_base_nanos: u64,
    backoff_max_nanos: u64,
    backoff_factor: f64,
    jitter: u8,
    seed: u64,
}

/// Units accrued at `amount` per `period_ms` over `micros`, rounded up.
fn accrued(amount: u32, period_ms: u32, micros: u128) -> u128 {
    (u128::from(amount) * micros).div_ceil(u128::from(period_ms) * 1_000)
}

/// The bound's tolerance in tokens. better-bucket's clock may run up to one
/// unit ahead of exact, and a unit is at most one token below ~4.3 × 10^9
/// tokens per second: one token of slack. Above that rate it counts in
/// power-of-two multi-token units (the clock stays at or below 4.3 units per
/// nanosecond), where a full bucket may also round up by part of a unit: two
/// units plus one there.
fn tolerance(amount: u32, period_ms: u32) -> u128 {
    let tokens_per_ns_x10 = (u128::from(amount) * 10).div_ceil(u128::from(period_ms) * 1_000_000);
    let unit = tokens_per_ns_x10.div_ceil(43).max(1).next_power_of_two();
    if unit == 1 { 1 } else { 2 * unit + 1 }
}

fuzz_target!(|input: Input| {
    let amount = input.amount.max(1);
    let period_ms = input.period_millis.max(1);
    let capacity = input.burst.max(1);
    let clock = Arc::new(ManualClock::new());
    let Ok(throttle) = Throttle::builder()
        .rate(amount, Duration::from_millis(u64::from(period_ms)))
        .burst(capacity)
        .clock(clock.clone())
        .try_build()
    else {
        return;
    };

    let slack = tolerance(amount, period_ms);
    let mut now_us = 0u128;
    let mut taken = 0u128;
    let mut last = None;
    for op in input.ops.iter().take(256) {
        match *op {
            Op::Acquire(cost) => {
                if cost > 0 && throttle.try_acquire_with_cost(cost) {
                    taken += u128::from(cost);
                    last = Some(cost);
                    assert!(
                        taken <= u128::from(capacity) + accrued(amount, period_ms, now_us) + slack
                    );
                }
            }
            Op::Reserve(cost, max_ms) => {
                let max_wait = Duration::from_millis(u64::from(max_ms));
                if let Some(wait) = throttle.reserve(cost, max_wait) {
                    assert!(wait <= max_wait);
                    if cost > 0 {
                        taken += u128::from(cost);
                        last = Some(cost);
                        let usable = now_us + wait.as_micros();
                        assert!(
                            taken
                                <= u128::from(capacity)
                                    + accrued(amount, period_ms, usable)
                                    + slack
                        );
                    }
                }
            }
            Op::RefundLast => {
                if let Some(cost) = last.take() {
                    throttle.refund(cost);
                    taken -= u128::from(cost);
                }
            }
            Op::AdvanceMicros(us) => {
                clock.advance(Duration::from_micros(u64::from(us)));
                now_us += u128::from(us);
            }
            Op::Pause(ms) => throttle.pause_for(Duration::from_millis(u64::from(ms))),
        }
        assert!(throttle.available() <= capacity);
    }

    let jitter = match input.jitter % 4 {
        0 => Jitter::None,
        1 => Jitter::Full,
        2 => Jitter::Equal,
        _ => Jitter::Decorrelated,
    };
    let max = Duration::from_nanos(input.backoff_max_nanos);
    let mut delays = Backoff::exponential(
        Duration::from_nanos(input.backoff_base_nanos),
        input.backoff_factor,
    )
    .with_max(max)
    .with_jitter(jitter)
    .iter_seeded(input.seed);
    for _ in 0..80 {
        assert!(delays.next_delay() <= max);
    }
});
