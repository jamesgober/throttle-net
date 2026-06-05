//! Retry a flaky downstream call with exponential backoff and decorrelated
//! jitter, honoring a server `Retry-After` when one is present.
//!
//! The simulated downstream fails a few times — once with a `Retry-After: 1`
//! header — before succeeding. The retry policy waits the computed backoff for
//! plain failures and the header delay when the server asks for one.
//!
//! Run with: `cargo run --example retry_backoff`

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use throttle_net::{Backoff, Retry, RetryAction, parse_retry_after};

/// A simulated downstream failure, optionally carrying a `Retry-After` header.
struct Rejected {
    status: u16,
    retry_after: Option<&'static str>,
}

#[tokio::main]
async fn main() {
    let attempts = AtomicU32::new(0);

    // Exponential from 100ms, doubling, capped at 2s, with decorrelated jitter.
    let retry = Retry::new(
        Backoff::exponential(Duration::from_millis(100), 2.0).with_max(Duration::from_secs(2)),
    )
    .max_attempts(6)
    .respect_retry_after(true);

    let started = Instant::now();
    let result: Result<&str, Rejected> = retry
        .run(
            || async {
                let n = attempts.fetch_add(1, Ordering::Relaxed) + 1;
                println!("[{:>6.2}s] attempt {n}", started.elapsed().as_secs_f64());
                match n {
                    1 => Err(Rejected {
                        status: 503,
                        retry_after: None,
                    }),
                    2 => Err(Rejected {
                        status: 429,
                        retry_after: Some("1"),
                    }), // server says wait 1s
                    3 => Err(Rejected {
                        status: 503,
                        retry_after: None,
                    }),
                    _ => Ok("200 OK"),
                }
            },
            |err: &Rejected| match err.retry_after.and_then(parse_retry_after) {
                Some(after) => RetryAction::RetryAfter(after),
                None => RetryAction::Retry,
            },
        )
        .await;

    match result {
        Ok(body) => println!(
            "succeeded with {body} after {} attempts in {:.2}s",
            attempts.load(Ordering::Relaxed),
            started.elapsed().as_secs_f64()
        ),
        Err(e) => println!("gave up (last status {})", e.status),
    }
}
