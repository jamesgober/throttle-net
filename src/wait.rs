//! The waiting engine shared by every async `acquire`.
//!
//! Each limiter supplies one synchronous *step* — "given at most this much
//! waiting budget, what can you do for me right now?" — and this module turns
//! it into a cancel-safe future:
//!
//! - [`Step::Granted`]: the units were **taken** (possibly ahead of their
//!   accrual, as a reservation) and may be used after the returned wait. The
//!   future sleeps that wait and returns. If it is dropped while sleeping, the
//!   limiter's refund runs, so a cancelled reservation gives its units back.
//! - [`Step::Retry`]: nothing was taken; the earliest the request could succeed
//!   is after the returned wait. The future sleeps and asks again — unless the
//!   wait exceeds the remaining budget, in which case it gives up at once
//!   instead of sleeping towards a deadline it knows it will miss.
//! - [`Step::Never`]: the request can never succeed (its cost exceeds every
//!   capacity it must fit), so the future fails immediately.
//!
//! The timeout budget is tracked by subtracting each sleep from it rather than
//! by reading a clock, so a limiter driven by a `ManualClock` and a runtime
//! with paused time (`tokio::time::pause`) both see an exact, deterministic
//! budget.

use core::time::Duration;

/// What a limiter can do for a waiting request right now.
pub(crate) enum Step {
    /// The units were taken; use them after this wait (`ZERO`: now).
    Granted(Duration),
    /// Nothing was taken; ask again after this wait.
    Retry(Duration),
    /// The request can never be granted.
    Never,
}

/// Why a waiting request failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The request can never be granted.
    Never,
    /// The request could not be granted within its timeout.
    TimedOut,
}

/// Consecutive zero-wait retries (lost races in a composite) answered with a
/// cooperative yield before the engine falls back to [`RACE_BACKOFF`] sleeps.
const RACE_YIELDS: u32 = 4;

/// The pause after repeated lost races, so contended composites cannot spin.
const RACE_BACKOFF: Duration = Duration::from_millis(1);

/// Runs `step` until it grants, gives up at `timeout`, or reports the request
/// impossible. `refund` gives back a reservation the future was cancelled
/// while waiting on; it never runs on any other path.
pub(crate) async fn wait_for<S, R>(
    mut step: S,
    refund: R,
    timeout: Option<Duration>,
) -> Result<(), Refusal>
where
    S: FnMut(Duration) -> Step,
    R: FnOnce(),
{
    let mut budget = timeout.unwrap_or(Duration::MAX);
    let mut refund = Some(refund);
    let mut zero_waits = 0u32;
    loop {
        match step(budget) {
            Step::Granted(wait) => {
                if !wait.is_zero() {
                    let guard = RefundOnDrop(refund.take());
                    crate::rt::sleep(wait).await;
                    guard.disarm();
                }
                return Ok(());
            }
            Step::Retry(after) => {
                if after > budget {
                    return Err(Refusal::TimedOut);
                }
                let nap = if after.is_zero() {
                    zero_waits = zero_waits.saturating_add(1);
                    if zero_waits <= RACE_YIELDS {
                        futures_lite::future::yield_now().await;
                        continue;
                    }
                    RACE_BACKOFF.min(budget)
                } else {
                    zero_waits = 0;
                    after
                };
                crate::rt::sleep(nap).await;
                budget = budget.saturating_sub(nap);
            }
            Step::Never => return Err(Refusal::Never),
        }
    }
}

/// Runs the refund unless [`disarm`](Self::disarm)ed: the cancellation path of
/// a reservation.
struct RefundOnDrop<R: FnOnce()>(Option<R>);

impl<R: FnOnce()> RefundOnDrop<R> {
    /// The reservation was honoured; nothing to give back.
    fn disarm(mut self) {
        let _honoured = self.0.take();
    }
}

impl<R: FnOnce()> Drop for RefundOnDrop<R> {
    fn drop(&mut self) {
        if let Some(refund) = self.0.take() {
            refund();
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::{Refusal, Step, wait_for};
    use core::time::Duration;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test(start_paused = true)]
    async fn test_granted_now_returns_without_sleeping() {
        let start = tokio::time::Instant::now();
        let result = wait_for(|_| Step::Granted(Duration::ZERO), || {}, None).await;
        assert_eq!(result, Ok(()));
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn test_granted_later_sleeps_exactly_the_wait() {
        let start = tokio::time::Instant::now();
        let result = wait_for(|_| Step::Granted(Duration::from_secs(3)), || {}, None).await;
        assert_eq!(result, Ok(()));
        assert_eq!(start.elapsed(), Duration::from_secs(3));
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_beyond_budget_times_out_without_sleeping() {
        let start = tokio::time::Instant::now();
        let result = wait_for(
            |_| Step::Retry(Duration::from_secs(10)),
            || {},
            Some(Duration::from_secs(1)),
        )
        .await;
        assert_eq!(result, Err(Refusal::TimedOut));
        assert_eq!(start.elapsed(), Duration::ZERO, "gave up at once");
    }

    #[tokio::test(start_paused = true)]
    async fn test_budget_shrinks_by_each_sleep() {
        // Three 400ms retries fit a 1s budget only twice.
        let calls = AtomicU32::new(0);
        let result = wait_for(
            |_| {
                let _ = calls.fetch_add(1, Ordering::Relaxed);
                Step::Retry(Duration::from_millis(400))
            },
            || {},
            Some(Duration::from_secs(1)),
        )
        .await;
        assert_eq!(result, Err(Refusal::TimedOut));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn test_never_fails_immediately() {
        let result = wait_for(|_| Step::Never, || {}, None).await;
        assert_eq!(result, Err(Refusal::Never));
    }

    #[tokio::test(start_paused = true)]
    async fn test_repeated_zero_waits_back_off_instead_of_spinning() {
        let calls = AtomicU32::new(0);
        let start = tokio::time::Instant::now();
        let result = wait_for(
            |_| {
                let n = calls.fetch_add(1, Ordering::Relaxed);
                if n < 10 {
                    Step::Retry(Duration::ZERO)
                } else {
                    Step::Granted(Duration::ZERO)
                }
            },
            || {},
            None,
        )
        .await;
        assert_eq!(result, Ok(()));
        // Four yields, then six 1ms backoffs before the grant.
        assert_eq!(start.elapsed(), Duration::from_millis(6));
    }

    #[tokio::test(start_paused = true)]
    async fn test_cancelled_reservation_is_refunded() {
        let refunds = Arc::new(AtomicU32::new(0));
        let r = Arc::clone(&refunds);
        let fut = wait_for(
            |_| Step::Granted(Duration::from_secs(60)),
            move || {
                let _ = r.fetch_add(1, Ordering::Relaxed);
            },
            None,
        );
        let timed = tokio::time::timeout(Duration::from_secs(1), fut).await;
        assert!(timed.is_err(), "the outer timeout cancels the wait");
        assert_eq!(refunds.load(Ordering::Relaxed), 1, "refunded exactly once");
    }

    #[tokio::test(start_paused = true)]
    async fn test_honoured_reservation_is_not_refunded() {
        let refunds = AtomicU32::new(0);
        let result = wait_for(
            |_| Step::Granted(Duration::from_millis(5)),
            || {
                let _ = refunds.fetch_add(1, Ordering::Relaxed);
            },
            None,
        )
        .await;
        assert_eq!(result, Ok(()));
        assert_eq!(refunds.load(Ordering::Relaxed), 0);
    }
}
