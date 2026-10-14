//! Hot-path benchmarks: the per-call cost of each limiter's non-waiting acquire.
//!
//! - `throttle_try_acquire`: the uncontended single-throttle floor — one clock
//!   read and one atomic compare-and-swap.
//! - `throttle_reserve_refund`: a reservation and its cancellation, the pair a
//!   cancelled waiting acquire costs.
//! - `perkey_lookup_10k_existing`: an existing key among ten thousand — hash,
//!   shard read lock, index probe, clock read, bucket CAS.
//! - `hybrid_two_constituents`, `multi_three_dimensions`, `sliding_try_acquire`:
//!   the composites and the exact sliding log.
//!
//! The rates are effectively unlimited so the limiter never refuses and every
//! iteration does the full grant path.
//!
//! The limiter surface requires `std`; with it off there is nothing to benchmark,
//! so the harness compiles to an empty `main`.
//!
//! Run with: `cargo bench --bench throttle_bench`

#[cfg(not(feature = "std"))]
fn main() {}

#[cfg(feature = "std")]
use std::hint::black_box;
#[cfg(feature = "std")]
use std::time::Duration;

#[cfg(feature = "std")]
use criterion::{Criterion, criterion_group, criterion_main};
#[cfg(feature = "std")]
use throttle_net::{Eviction, Hybrid, MultiLimiter, PerKey, SlidingWindowLog, Throttle};

/// Uncontended single-throttle acquire: the lock-free token-bucket floor.
#[cfg(feature = "std")]
fn bench_throttle_try_acquire(c: &mut Criterion) {
    let throttle = Throttle::per_second(u32::MAX);
    c.bench_function("throttle_try_acquire", |b| {
        b.iter(|| black_box(throttle.try_acquire()));
    });
}

/// A reservation and its refund (the cancellation path of a waiting acquire).
#[cfg(feature = "std")]
fn bench_throttle_reserve_refund(c: &mut Criterion) {
    let throttle = Throttle::per_second(u32::MAX);
    c.bench_function("throttle_reserve_refund", |b| {
        b.iter(|| {
            let wait = throttle.reserve(black_box(1), Duration::MAX);
            throttle.refund(1);
            black_box(wait)
        });
    });
}

/// Per-key lookup with 10 000 live keys.
#[cfg(feature = "std")]
fn bench_perkey_lookup_10k(c: &mut Criterion) {
    const KEYS: u64 = 10_000;
    let limiter = PerKey::<u64>::per_second(u32::MAX)
        .with_eviction(Eviction::unbounded())
        .with_shards(64);
    for key in 0..KEYS {
        let _ = limiter.try_acquire(&key);
    }
    assert_eq!(limiter.len(), usize::try_from(KEYS).unwrap_or(usize::MAX));
    let mut next = 0u64;
    c.bench_function("perkey_lookup_10k_existing", |b| {
        b.iter(|| {
            let key = next % KEYS;
            next = next.wrapping_add(1);
            black_box(limiter.try_acquire(black_box(&key)))
        });
    });
}

/// A hybrid of two token buckets: peek both, commit both.
#[cfg(feature = "std")]
fn bench_hybrid(c: &mut Criterion) {
    let hybrid = Hybrid::builder()
        .limiter(Throttle::per_second(u32::MAX))
        .limiter(Throttle::per_second(u32::MAX))
        .build();
    c.bench_function("hybrid_two_constituents", |b| {
        b.iter(|| black_box(hybrid.try_acquire()));
    });
}

/// A three-dimension LLM-style budget charged per call.
#[cfg(feature = "std")]
fn bench_multi(c: &mut Criterion) {
    let limiter = MultiLimiter::builder()
        .dimension("requests", Throttle::per_second(u32::MAX))
        .dimension("input_tokens", Throttle::per_second(u32::MAX))
        .dimension("output_tokens", Throttle::per_second(u32::MAX))
        .build();
    let costs = [
        ("requests", 1),
        ("input_tokens", 1_500),
        ("output_tokens", 200),
    ];
    c.bench_function("multi_three_dimensions", |b| {
        b.iter(|| black_box(limiter.try_acquire_costs(black_box(&costs))));
    });
}

/// The exact sliding log with a short window, so pruning is exercised.
#[cfg(feature = "std")]
fn bench_sliding(c: &mut Criterion) {
    let limiter = SlidingWindowLog::new(u32::MAX, Duration::from_millis(1));
    c.bench_function("sliding_try_acquire", |b| {
        b.iter(|| black_box(limiter.try_acquire()));
    });
}

#[cfg(feature = "std")]
criterion_group!(
    benches,
    bench_throttle_try_acquire,
    bench_throttle_reserve_refund,
    bench_perkey_lookup_10k,
    bench_hybrid,
    bench_multi,
    bench_sliding
);
#[cfg(feature = "std")]
criterion_main!(benches);
