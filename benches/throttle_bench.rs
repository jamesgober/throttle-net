//! Hot-path benchmarks.
//!
//! The headline number is per-key lookup latency with a populated store: the
//! v0.2 target is well under 1µs for an existing key among ten thousand. The
//! single-throttle acquire is the floor — one atomic compare-and-swap with no map
//! lookup at all.
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
use criterion::{Criterion, criterion_group, criterion_main};
#[cfg(feature = "std")]
use throttle_net::{Eviction, PerKey, Throttle};

/// Uncontended single-throttle acquire: the lock-free token-bucket floor.
#[cfg(feature = "std")]
fn bench_throttle_try_acquire(c: &mut Criterion) {
    // A very large rate so the bucket effectively never empties during the run,
    // isolating the cost of the acquire path itself.
    let throttle = Throttle::per_second(u32::MAX);
    c.bench_function("throttle_try_acquire", |b| {
        b.iter(|| black_box(throttle.try_acquire()));
    });
}

/// Per-key lookup with 10 000 live keys: hash, shard read lock, map get, acquire.
#[cfg(feature = "std")]
fn bench_perkey_lookup_10k(c: &mut Criterion) {
    const KEYS: u64 = 10_000;

    let limiter = PerKey::<u64>::per_second(u32::MAX)
        .with_shards(64)
        .with_eviction(Eviction::unbounded());

    // Populate the store so every probed key is an existing-key (read-lock) hit.
    for key in 0..KEYS {
        let _ = limiter.try_acquire(&key);
    }
    assert_eq!(limiter.len(), usize::try_from(KEYS).unwrap());

    let mut next = 0u64;
    c.bench_function("perkey_lookup_10k_existing", |b| {
        b.iter(|| {
            let key = next % KEYS;
            next = next.wrapping_add(1);
            black_box(limiter.try_acquire(black_box(&key)))
        });
    });
}

#[cfg(feature = "std")]
criterion_group!(benches, bench_throttle_try_acquire, bench_perkey_lookup_10k);
#[cfg(feature = "std")]
criterion_main!(benches);
