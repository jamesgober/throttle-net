//! The waiting engine shared by every async `acquire`.
//!
//! Each limiter supplies one synchronous *step* — "given at most this much
//! waiting budget, what can you do for me right now?" — and this module turns
//! it into a cancel-safe future:
//!
//! - [`Step::Granted`]: the units were **taken** (possibly ahead of their
//!   accrual, as a reservation) and may be used after the returned wait. The
//!   future sleeps that wait, then sleeps out any pause set on the limiter in
//!   the meantime (keeping its reservation), and returns.
//! - [`Step::Holding`]: *some* units were taken — a composite reserved its
//!   bottleneck constituent — and the step must run again after the wait.
//! - [`Step::Retry`]: nothing is held; the earliest the request could succeed
//!   is after the returned wait. The future sleeps and asks again — unless the
//!   wait exceeds the remaining budget, in which case it gives up at once
//!   instead of sleeping towards a deadline it knows it will miss.
//! - [`Step::Never`]: the request can never succeed (its cost exceeds every
//!   capacity it must fit), so the future fails immediately.
//!
//! Whenever the future is dropped, or gives up, while it holds units (after a
//! `Granted` or `Holding` step), the limiter's `release` runs, so a cancelled
//! or timed-out reservation gives its units back.
//!
//! The timeout is a real deadline on the runtime's clock (tokio's, which a test
//! can pause, or the OS monotonic clock under smol): it bounds the *total* time
//! spent, including zero-length retries and pauses, so a `*_timeout` always
//! resolves by its deadline (TN2-006). Sleeps that wait out a pause are raced
//! against [`resume`](crate::Throttle::resume), so lifting a pause wakes the
//! waiters it was holding (TN2-010).

use core::time::Duration;

/// What a limiter can do for a waiting request right now.
pub(crate) enum Step {
    /// Every unit was taken; use them after this wait (`ZERO`: now).
    Granted(Duration),
    /// Some units are held; step again after this wait.
    Holding(Duration),
    /// Nothing is held; ask again after this wait.
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

/// The point in runtime time a timed wait must be resolved by.
#[derive(Clone, Copy)]
struct Deadline(Option<crate::rt::Instant>);

impl Deadline {
    /// `timeout` from now; `None` (or a timeout too far out to represent) is
    /// unbounded.
    fn after(timeout: Option<Duration>) -> Self {
        Self(timeout.and_then(|t| crate::rt::Instant::now().checked_add(t)))
    }

    /// The budget left (`Duration::MAX` when unbounded).
    fn remaining(self) -> Duration {
        self.0.map_or(Duration::MAX, |at| {
            at.saturating_duration_since(crate::rt::Instant::now())
        })
    }
}

/// Runs `step` until it grants, gives up at `timeout`, or reports the request
/// impossible.
///
/// `release` gives back whatever the waiter holds; it runs only when the future
/// is dropped or fails while holding units (after a `Granted` or `Holding`
/// step). `paused` reports how much longer the limiter is paused: a granted
/// waiter sleeps that out before returning, so a pause set after it reserved
/// still holds it (TN2-009).
pub(crate) async fn wait_for<S, R, P>(
    mut step: S,
    mut release: R,
    paused: P,
    timeout: Option<Duration>,
) -> Result<(), Refusal>
where
    S: FnMut(Duration) -> Step,
    R: FnMut(),
    P: Fn() -> Duration,
{
    wait_with(
        (),
        |(), budget| step(budget),
        |()| release(),
        paused,
        timeout,
    )
    .await
}

/// [`wait_for`] with per-wait `state` that `step` and `release` share: what a
/// composite has reserved so far. The engine owns it, so the future stays
/// `Send` without a lock.
pub(crate) async fn wait_with<T, S, R, P>(
    state: T,
    mut step: S,
    release: R,
    paused: P,
    timeout: Option<Duration>,
) -> Result<(), Refusal>
where
    S: FnMut(&mut T, Duration) -> Step,
    R: FnMut(&mut T),
    P: Fn() -> Duration,
{
    let deadline = Deadline::after(timeout);
    let mut held = Release {
        state,
        give_back: release,
        armed: false,
    };
    let mut zero_waits = 0u32;
    loop {
        let epoch = crate::rt::resume_epoch();
        let budget = deadline.remaining();
        let after = match step(&mut held.state, budget) {
            Step::Granted(wait) => {
                held.armed = true;
                if !wait.is_zero() {
                    crate::rt::sleep(wait).await;
                }
                // A pause set while the reservation matured holds it too.
                loop {
                    let epoch = crate::rt::resume_epoch();
                    let pause = paused();
                    if pause.is_zero() {
                        break;
                    }
                    if pause > deadline.remaining() {
                        held.fire();
                        return Err(Refusal::TimedOut);
                    }
                    crate::rt::sleep_unless_resumed(pause, epoch).await;
                }
                held.armed = false;
                return Ok(());
            }
            Step::Holding(wait) => {
                held.armed = true;
                if wait > budget {
                    held.fire();
                    return Err(Refusal::TimedOut);
                }
                wait
            }
            Step::Retry(after) => {
                held.armed = false;
                if after > budget {
                    return Err(Refusal::TimedOut);
                }
                after
            }
            Step::Never => {
                held.armed = false;
                return Err(Refusal::Never);
            }
        };
        let nap = if after.is_zero() {
            zero_waits = zero_waits.saturating_add(1);
            if zero_waits <= RACE_YIELDS {
                crate::rt::yield_now().await;
                continue;
            }
            let left = deadline.remaining();
            if left.is_zero() {
                // The deadline passed while the steps kept answering "now":
                // without this a timed wait could poll forever (TN2-006).
                held.fire();
                return Err(Refusal::TimedOut);
            }
            RACE_BACKOFF.min(left)
        } else {
            zero_waits = 0;
            after
        };
        // Only a waiter whose limiter is paused listens for a resume; one
        // sleeping for a refill just sleeps. A resume that landed after the
        // step (it moved the epoch) means "look again now".
        let pause = paused();
        if crate::rt::resume_epoch() != epoch {
            continue;
        }
        if pause.is_zero() {
            crate::rt::sleep(nap).await;
        } else {
            crate::rt::sleep_unless_resumed(nap, epoch).await;
        }
    }
}

/// Runs `release` on drop while armed: the cancellation path of whatever a
/// waiter holds.
struct Release<T, R: FnMut(&mut T)> {
    state: T,
    give_back: R,
    armed: bool,
}

impl<T, R: FnMut(&mut T)> Release<T, R> {
    /// Gives the held units back now (once).
    fn fire(&mut self) {
        if core::mem::take(&mut self.armed) {
            (self.give_back)(&mut self.state);
        }
    }
}

impl<T, R: FnMut(&mut T)> Drop for Release<T, R> {
    fn drop(&mut self) {
        self.fire();
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::{Refusal, Step, wait_for};
    use core::time::Duration;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn unpaused() -> Duration {
        Duration::ZERO
    }

    #[tokio::test(start_paused = true)]
    async fn test_granted_now_returns_without_sleeping() {
        let start = tokio::time::Instant::now();
        let result = wait_for(|_| Step::Granted(Duration::ZERO), || {}, unpaused, None).await;
        assert_eq!(result, Ok(()));
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn test_granted_later_sleeps_exactly_the_wait() {
        let start = tokio::time::Instant::now();
        let result = wait_for(
            |_| Step::Granted(Duration::from_secs(3)),
            || {},
            unpaused,
            None,
        )
        .await;
        assert_eq!(result, Ok(()));
        assert_eq!(start.elapsed(), Duration::from_secs(3));
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_beyond_budget_times_out_without_sleeping() {
        let start = tokio::time::Instant::now();
        let result = wait_for(
            |_| Step::Retry(Duration::from_secs(10)),
            || {},
            unpaused,
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
            unpaused,
            Some(Duration::from_secs(1)),
        )
        .await;
        assert_eq!(result, Err(Refusal::TimedOut));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn test_never_fails_immediately() {
        let result = wait_for(|_| Step::Never, || {}, unpaused, None).await;
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
            unpaused,
            None,
        )
        .await;
        assert_eq!(result, Ok(()));
        // Four yields, then six 1ms backoffs before the grant.
        assert_eq!(start.elapsed(), Duration::from_millis(6));
    }

    /// TN2-006: a step that keeps answering "retry now" cannot outlive the
    /// deadline (the 1.1 candidate polled forever once the budget hit zero).
    #[tokio::test(start_paused = true)]
    async fn test_endless_zero_retries_time_out_at_the_deadline() {
        let start = tokio::time::Instant::now();
        let result = wait_for(
            |_| Step::Retry(Duration::ZERO),
            || {},
            unpaused,
            Some(Duration::from_millis(50)),
        )
        .await;
        assert_eq!(result, Err(Refusal::TimedOut));
        assert_eq!(start.elapsed(), Duration::from_millis(50));
    }

    #[tokio::test(start_paused = true)]
    async fn test_zero_timeout_with_zero_retries_fails_at_once() {
        let start = tokio::time::Instant::now();
        let result = wait_for(
            |_| Step::Retry(Duration::ZERO),
            || {},
            unpaused,
            Some(Duration::ZERO),
        )
        .await;
        assert_eq!(result, Err(Refusal::TimedOut));
        assert_eq!(start.elapsed(), Duration::ZERO);
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
            unpaused,
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
            unpaused,
            None,
        )
        .await;
        assert_eq!(result, Ok(()));
        assert_eq!(refunds.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_holding_steps_again_and_releases_once_on_cancel() {
        let releases = AtomicU32::new(0);
        let steps = AtomicU32::new(0);
        let fut = wait_for(
            |_| {
                let _ = steps.fetch_add(1, Ordering::Relaxed);
                Step::Holding(Duration::from_secs(1))
            },
            || {
                let _ = releases.fetch_add(1, Ordering::Relaxed);
            },
            unpaused,
            None,
        );
        let timed = tokio::time::timeout(Duration::from_millis(2500), fut).await;
        assert!(timed.is_err());
        assert_eq!(steps.load(Ordering::Relaxed), 3, "stepped after each hold");
        assert_eq!(releases.load(Ordering::Relaxed), 1, "released exactly once");
    }

    #[tokio::test(start_paused = true)]
    async fn test_holding_beyond_the_budget_releases_and_times_out() {
        let releases = AtomicU32::new(0);
        let result = wait_for(
            |_| Step::Holding(Duration::from_secs(5)),
            || {
                let _ = releases.fetch_add(1, Ordering::Relaxed);
            },
            unpaused,
            Some(Duration::from_secs(1)),
        )
        .await;
        assert_eq!(result, Err(Refusal::TimedOut));
        assert_eq!(releases.load(Ordering::Relaxed), 1);
    }

    /// TN2-009: a pause that appears while a reservation matures holds the
    /// waiter until it ends.
    #[tokio::test(start_paused = true)]
    async fn test_granted_waiter_sleeps_out_a_pause() {
        let start = tokio::time::Instant::now();
        let ends = start + Duration::from_secs(10);
        let result = wait_for(
            |_| Step::Granted(Duration::from_secs(1)),
            || {},
            || ends.saturating_duration_since(tokio::time::Instant::now()),
            None,
        )
        .await;
        assert_eq!(result, Ok(()));
        assert_eq!(start.elapsed(), Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn test_pause_beyond_the_deadline_releases_and_times_out() {
        let releases = AtomicU32::new(0);
        let start = tokio::time::Instant::now();
        let result = wait_for(
            |_| Step::Granted(Duration::from_secs(1)),
            || {
                let _ = releases.fetch_add(1, Ordering::Relaxed);
            },
            || Duration::from_secs(60),
            Some(Duration::from_secs(5)),
        )
        .await;
        assert_eq!(result, Err(Refusal::TimedOut));
        assert_eq!(releases.load(Ordering::Relaxed), 1);
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }

    /// A resume wakes a waiter sleeping out a pause, but not one sleeping
    /// for a refill (D3-01).
    #[tokio::test(start_paused = true)]
    #[allow(clippy::unwrap_used)]
    async fn test_resume_wakes_only_paused_waiters() {
        let refill_steps = Arc::new(AtomicU32::new(0));
        let paused_steps = Arc::new(AtomicU32::new(0));
        let paused_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let r = Arc::clone(&refill_steps);
        let refill = tokio::spawn(async move {
            wait_for(
                |_| {
                    let n = r.fetch_add(1, Ordering::Relaxed);
                    if n == 0 {
                        Step::Retry(Duration::from_secs(10))
                    } else {
                        Step::Granted(Duration::ZERO)
                    }
                },
                || {},
                unpaused,
                None,
            )
            .await
        });
        let p = Arc::clone(&paused_steps);
        let flag = Arc::clone(&paused_flag);
        let paused = tokio::spawn(async move {
            wait_for(
                |_| {
                    let _ = p.fetch_add(1, Ordering::Relaxed);
                    if flag.load(Ordering::Relaxed) {
                        Step::Retry(Duration::from_secs(3600))
                    } else {
                        Step::Granted(Duration::ZERO)
                    }
                },
                || {},
                || {
                    if flag.load(Ordering::Relaxed) {
                        Duration::from_secs(3600)
                    } else {
                        Duration::ZERO
                    }
                },
                None,
            )
            .await
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let flag = Arc::clone(&paused_flag);
        crate::rt::resume(move || {
            flag.store(false, Ordering::Relaxed);
            true
        });
        assert_eq!(paused.await.unwrap(), Ok(()));
        let start = tokio::time::Instant::now();
        assert_eq!(
            refill_steps.load(Ordering::Relaxed),
            1,
            "not woken by the resume"
        );
        assert_eq!(refill.await.unwrap(), Ok(()));
        assert!(start.elapsed() >= Duration::from_secs(8));
        assert_eq!(paused_steps.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn test_a_resume_after_the_step_is_seen_without_sleeping() {
        // The epoch moves between the step and the nap: the waiter looks
        // again at once instead of sleeping the hour it was told.
        let steps = AtomicU32::new(0);
        let start = tokio::time::Instant::now();
        let result = wait_for(
            |_| {
                let n = steps.fetch_add(1, Ordering::Relaxed);
                if n == 0 {
                    crate::rt::resume(|| false);
                    Step::Retry(Duration::from_secs(3600))
                } else {
                    Step::Granted(Duration::ZERO)
                }
            },
            || {},
            || {
                if steps.load(Ordering::Relaxed) <= 1 {
                    Duration::from_secs(3600)
                } else {
                    Duration::ZERO
                }
            },
            None,
        )
        .await;
        assert_eq!(result, Ok(()));
        assert_eq!(start.elapsed(), Duration::ZERO);
    }
}
