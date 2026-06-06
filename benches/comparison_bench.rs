//! Comparative benchmark: the uncontended single-acquire floor against
//! [`governor`](https://crates.io/crates/governor), the most common Rust rate
//! limiter, on the same workload.
//!
//! Both limiters are given an effectively infinite rate so the bucket never
//! empties, isolating the cost of the acquire path itself — one atomic
//! compare-and-swap. The v0.9 target is to match or beat `governor` here.
//!
//! The limiter surface requires `std`; with it off there is nothing to compare,
//! so the harness compiles to an empty `main`.
//!
//! Run with: `cargo bench --bench comparison_bench`

#[cfg(not(feature = "std"))]
fn main() {}

#[cfg(feature = "std")]
use std::hint::black_box;
#[cfg(feature = "std")]
use std::num::NonZeroU32;

#[cfg(feature = "std")]
use criterion::{Criterion, criterion_group, criterion_main};
#[cfg(feature = "std")]
use governor::{Quota, RateLimiter};
#[cfg(feature = "std")]
use throttle_net::Throttle;

/// throttle-net `try_acquire` vs governor `check`, both uncontended with a rate
/// large enough that the limiter never refuses during the run.
#[cfg(feature = "std")]
fn bench_uncontended(c: &mut Criterion) {
    let mut group = c.benchmark_group("uncontended_try_acquire");

    let throttle = Throttle::per_second(u32::MAX);
    group.bench_function("throttle_net", |b| {
        b.iter(|| black_box(throttle.try_acquire()));
    });

    let quota = Quota::per_second(NonZeroU32::new(u32::MAX).expect("non-zero rate"));
    let governor = RateLimiter::direct(quota);
    group.bench_function("governor", |b| {
        b.iter(|| black_box(governor.check().is_ok()));
    });

    group.finish();
}

#[cfg(feature = "std")]
criterion_group!(benches, bench_uncontended);
#[cfg(feature = "std")]
criterion_main!(benches);
