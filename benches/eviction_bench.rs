//! Adversarial per-key benchmarks: the cost of a *new* key once the store is
//! full, which is what a flood of unique keys pays on every request.
//!
//! In 1.0 this was a full scan of the shard under its write lock — measured at
//! about 1.79 ms per new key beyond the default cap of 1,048,576 keys, and an
//! idle TTL made every insert a full `retain` (26 µs per insert at 100k keys,
//! growing linearly). With bounded CLOCK eviction and an incremental TTL sweep,
//! each case below is a fixed amount of work however large the store.
//!
//! - `flood_at_cap_recovering_keys`: the default policy, full; every key's
//!   bucket recovers at once, so each new key evicts one.
//! - `flood_at_cap_limited_keys`: full of keys that are all still limited, so a
//!   new key is refused after a bounded scan (it must never evict them).
//! - `flood_with_idle_ttl`: a 100k cap with a five-minute idle TTL and nothing
//!   yet expired — the case that was O(n) per insert.
//!
//! Run with: `cargo bench --bench eviction_bench`

#[cfg(not(feature = "std"))]
fn main() {}

#[cfg(feature = "std")]
use std::hint::black_box;
#[cfg(feature = "std")]
use std::time::Duration;

#[cfg(feature = "std")]
use criterion::{Criterion, criterion_group, criterion_main};
#[cfg(feature = "std")]
use throttle_net::{DEFAULT_MAX_KEYS, Eviction, PerKey};

/// Fills `limiter` to its cap with keys `0..keys`, returning the next key.
#[cfg(feature = "std")]
fn fill(limiter: &PerKey<u64>, keys: u64) -> u64 {
    for key in 0..keys {
        let _ = limiter.try_acquire(&key);
    }
    keys
}

#[cfg(feature = "std")]
fn bench_flood(c: &mut Criterion) {
    let mut group = c.benchmark_group("perkey_flood");

    // Recovering keys: an effectively unlimited rate refills a one-token spend
    // within nanoseconds, so every resident is evictable without loss.
    let recovering = PerKey::<u64>::per_second(u32::MAX);
    let mut next = fill(&recovering, DEFAULT_MAX_KEYS as u64);
    group.bench_function("flood_at_cap_recovering_keys", |b| {
        b.iter(|| {
            next += 1;
            black_box(recovering.try_acquire(black_box(&next)))
        });
    });
    drop(recovering);

    // Limited keys: one token per hour, so nothing recovers during the run.
    let limited = PerKey::<u64>::per_duration(1, Duration::from_secs(3600))
        .with_eviction(Eviction::capacity(100_000));
    let mut next = fill(&limited, 100_000);
    group.bench_function("flood_at_cap_limited_keys", |b| {
        b.iter(|| {
            next += 1;
            black_box(limited.try_acquire(black_box(&next)))
        });
    });
    drop(limited);

    // An idle TTL with nothing expired: every insert sweeps a bounded slice.
    let ttl = PerKey::<u64>::per_second(u32::MAX)
        .with_eviction(Eviction::capacity(100_000).with_idle(Duration::from_secs(300)));
    let mut next = fill(&ttl, 50_000);
    group.bench_function("flood_with_idle_ttl", |b| {
        b.iter(|| {
            next += 1;
            black_box(ttl.try_acquire(black_box(&next)))
        });
    });

    group.finish();
}

#[cfg(feature = "std")]
criterion_group!(benches, bench_flood);
#[cfg(feature = "std")]
criterion_main!(benches);
