//! A full outbound resilience stack around one flaky dependency.
//!
//! - a [`Bulkhead`] caps concurrent calls, so a slow dependency cannot tie up
//!   every task;
//! - a [`CircuitBreaker`] over a [`Throttle`] paces calls and sheds them while
//!   the dependency is failing;
//! - [`resilience::timeout`] bounds each call;
//! - a [`Retry`] with a shared [`RetryBudget`] retries transient failures
//!   without letting an outage multiply traffic;
//! - [`resilience::fallback`] serves a degraded answer when all else fails.
//!
//! Run with: `cargo run --example resilience_stack --features circuit-breaker`

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use throttle_net::resilience::{fallback, timeout};
use throttle_net::{
    Backoff, Bulkhead, CallError, CircuitBreaker, Retry, RetryAction, RetryBudget, Throttle,
    ThrottleError, Trip,
};

/// Why one attempt failed.
#[derive(Debug)]
enum Failure {
    Shed(ThrottleError),
    TimedOut,
    Downstream(&'static str),
}

/// A dependency that hangs on its 2nd call and fails its 3rd to 6th.
async fn dependency(call: u32) -> Result<u32, &'static str> {
    match call {
        2 => {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(call)
        }
        3..=6 => Err("503 Service Unavailable"),
        _ => Ok(call * 10),
    }
}

#[tokio::main]
async fn main() {
    let bulkhead = Arc::new(Bulkhead::new(4).with_name("inventory"));
    let breaker = Arc::new(
        CircuitBreaker::builder()
            .trip(Trip::Consecutive(3))
            .cooldown(Duration::from_millis(200))
            .name("inventory")
            .build(Throttle::per_second(50)),
    );
    let budget = RetryBudget::new(0.2, 2);
    let retry = Retry::new(Backoff::exponential(Duration::from_millis(20), 2.0))
        .max_attempts(3)
        .max_elapsed(Duration::from_secs(2))
        .name("inventory");
    let calls = AtomicU32::new(0);

    for request in 1..=6 {
        let answer = fallback(
            retry.run_with_budget(
                &budget,
                || async {
                    let call = calls.fetch_add(1, Ordering::Relaxed) + 1;
                    let _slot = bulkhead.acquire().await.map_err(Failure::Shed)?;
                    let guarded = breaker.call(|| async {
                        timeout(Duration::from_millis(100), dependency(call))
                            .await
                            .map_err(|_| "timed out")?
                    });
                    match guarded.await {
                        Ok(value) => Ok(value),
                        Err(CallError::Rejected(shed)) => Err(Failure::Shed(shed)),
                        Err(CallError::Failed("timed out")) => Err(Failure::TimedOut),
                        Err(CallError::Failed(err)) => Err(Failure::Downstream(err)),
                    }
                },
                |failure: &Failure| match failure {
                    // Shed by an open breaker: retry no sooner than it allows.
                    Failure::Shed(ThrottleError::CircuitOpen { retry_after }) => {
                        RetryAction::RetryAfter(*retry_after)
                    }
                    Failure::Shed(_) => RetryAction::GiveUp,
                    Failure::TimedOut | Failure::Downstream(_) => RetryAction::Retry,
                },
            ),
            |failure| async move {
                let reason = match failure {
                    Failure::Shed(shed) => format!("shed ({shed})"),
                    Failure::TimedOut => "timed out".to_owned(),
                    Failure::Downstream(err) => format!("downstream error ({err})"),
                };
                println!("  request degraded: {reason}");
                0 // the cached answer
            },
        )
        .await;
        println!(
            "request {request}: answer {answer:>3}  breaker {:?}",
            breaker.state()
        );
    }
}
