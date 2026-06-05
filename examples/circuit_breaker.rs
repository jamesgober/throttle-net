//! Wrap a flaky downstream in a circuit breaker: trip open after repeated
//! failures, shed load while it cools down, then recover through a half-open
//! trial once the downstream is healthy again.
//!
//! The simulated downstream fails its first five calls, then succeeds. Watch the
//! breaker move Closed → Open → HalfOpen → Closed.
//!
//! Run with: `cargo run --example circuit_breaker --features circuit-breaker`

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use throttle_net::{CircuitBreaker, Throttle, Trip};

#[tokio::main]
async fn main() {
    let breaker = CircuitBreaker::builder()
        .trip(Trip::Consecutive(3)) // open after 3 failures in a row
        .cooldown(Duration::from_millis(300))
        .half_open(1, 1) // one trial; one success closes
        .build(Throttle::per_second(1000));

    // The downstream is unhealthy for its first five calls, then recovers.
    let calls = AtomicU32::new(0);
    let downstream = || calls.fetch_add(1, Ordering::Relaxed) + 1 > 5;

    for attempt in 1..=14 {
        match breaker.acquire().await {
            Ok(permit) => {
                if downstream() {
                    permit.success();
                    println!("attempt {attempt:>2}: ok    -> {:?}", breaker.state());
                } else {
                    permit.failure();
                    println!("attempt {attempt:>2}: FAIL  -> {:?}", breaker.state());
                }
            }
            Err(_shed) => {
                // Fail fast: the downstream is never touched while open.
                println!("attempt {attempt:>2}: shed  -> {:?}", breaker.state());
                tokio::time::sleep(Duration::from_millis(160)).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
}
