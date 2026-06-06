//! Contention benchmark: aggregate `try_acquire` throughput as the number of
//! concurrent acquirers grows (1, 4, 16, 64), all hammering a single shared
//! [`Throttle`].
//!
//! Because the token bucket is lock-free (one atomic compare-and-swap per
//! acquire, no lock on the path), throughput should scale with cores rather than
//! collapsing under contention. The rate is set effectively infinite so every
//! attempt does the full acquire-path work instead of short-circuiting on an
//! empty bucket.
//!
//! The limiter surface requires `std`; with it off the harness is an empty `main`.
//!
//! Run with: `cargo bench --bench contention_bench`

#[cfg(not(feature = "std"))]
fn main() {}

#[cfg(feature = "std")]
use std::hint::black_box;
#[cfg(feature = "std")]
use std::sync::Arc;
#[cfg(feature = "std")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "std")]
use std::thread;

#[cfg(feature = "std")]
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[cfg(feature = "std")]
use throttle_net::Throttle;

/// Each acquirer performs this many `try_acquire` calls per measured iteration,
/// amortizing the per-iteration coordination overhead.
#[cfg(feature = "std")]
const PER_THREAD: u64 = 50_000;

#[cfg(feature = "std")]
fn bench_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("throttle_contention");

    for threads in [1usize, 4, 16, 64] {
        let total_ops = (threads as u64) * PER_THREAD;
        group.throughput(Throughput::Elements(total_ops));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let throttle = Arc::new(Throttle::per_second(u32::MAX));
                // A persistent worker pool started once and released on a barrier
                // each iteration, so thread-spawn cost is not in the measurement.
                b.iter_custom(|iters| {
                    use std::sync::Barrier;
                    use std::time::Instant;

                    let start_gate = Arc::new(Barrier::new(threads + 1));
                    let done_gate = Arc::new(Barrier::new(threads + 1));
                    let stop = Arc::new(AtomicBool::new(false));
                    let mut handles = Vec::with_capacity(threads);

                    for _ in 0..threads {
                        let throttle = Arc::clone(&throttle);
                        let start_gate = Arc::clone(&start_gate);
                        let done_gate = Arc::clone(&done_gate);
                        let stop = Arc::clone(&stop);
                        handles.push(thread::spawn(move || {
                            loop {
                                start_gate.wait();
                                if stop.load(Ordering::Acquire) {
                                    break;
                                }
                                for _ in 0..PER_THREAD {
                                    black_box(throttle.try_acquire());
                                }
                                done_gate.wait();
                            }
                        }));
                    }

                    let begin = Instant::now();
                    for _ in 0..iters {
                        start_gate.wait(); // release the workers for one round
                        done_gate.wait(); // wait for all to finish the round
                    }
                    let elapsed = begin.elapsed();

                    // Release the workers one last time to observe the stop flag.
                    stop.store(true, Ordering::Release);
                    start_gate.wait();
                    for h in handles {
                        let _ = h.join();
                    }
                    elapsed
                });
            },
        );
    }

    group.finish();
}

#[cfg(feature = "std")]
criterion_group!(benches, bench_contention);
#[cfg(feature = "std")]
criterion_main!(benches);
