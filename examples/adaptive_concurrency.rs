//! Adaptive concurrency: discover the right in-flight limit from outcomes alone.
//!
//! No fixed rate is configured. The limiter starts cautious and grows while the
//! downstream is healthy, collapses when it degrades, and climbs back as it
//! recovers — always bounded by the floor and ceiling.
//!
//! Run with: `cargo run --example adaptive_concurrency --features adaptive`

use throttle_net::{AdaptiveLimiter, Aimd};

#[tokio::main]
async fn main() {
    let limiter = AdaptiveLimiter::builder()
        .floor(2)
        .ceiling(40)
        .initial(8)
        .build(Aimd::new(2, 0.5)); // +2 per saturated success, halve on failure

    for round in 1..=24 {
        // The downstream is unhealthy for rounds 9..=15, healthy otherwise.
        let healthy = !(9..=15).contains(&round);

        // Saturate the current limit, then report each request's outcome.
        let mut held = Vec::new();
        while let Some(permit) = limiter.try_acquire() {
            held.push(permit);
        }
        let admitted = held.len();
        for permit in held {
            if healthy {
                permit.success();
            } else {
                permit.failure();
            }
        }

        println!(
            "round {round:>2}: {}  admitted {admitted:>2}  ->  limit {}",
            if healthy { "ok  " } else { "FAIL" },
            limiter.current_limit(),
        );
    }
}
