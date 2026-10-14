//! Runtime-agnostic resilience combinators: [`timeout`], [`fallback`], and
//! [`hedge`].
//!
//! They complement the limiters, [`Retry`](crate::Retry),
//! [`CircuitBreaker`](crate::CircuitBreaker) (`circuit-breaker` feature), and
//! [`Bulkhead`](crate::Bulkhead): bound how long a call may take, substitute a
//! value when it fails, and race a second attempt against a slow first one. They
//! run on whichever timer backend is enabled (`tokio` or `smol`), so the same
//! code works on either runtime. Behind the `tokio` or `smol` feature.

use core::future::{Future, IntoFuture};
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use core::time::Duration;

use crate::error::ThrottleError;

/// The error [`timeout`] returns when the deadline passes first.
///
/// It converts into [`ThrottleError::DeadlineExceeded`] with `?` or `.into()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Elapsed {
    after: Duration,
}

impl Elapsed {
    /// The timeout that elapsed.
    #[must_use]
    pub const fn after(&self) -> Duration {
        self.after
    }
}

impl core::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "operation timed out after {:?}", self.after)
    }
}

impl std::error::Error for Elapsed {}

impl From<Elapsed> for ThrottleError {
    fn from(_: Elapsed) -> Self {
        Self::DeadlineExceeded
    }
}

/// Runs `future`, giving up after `duration`.
///
/// Returns the future's output, or [`Elapsed`] if `duration` passes first — in
/// which case the future is dropped (cancelled) at that point.
///
/// # Errors
///
/// [`Elapsed`] when the timeout fires before the future completes.
///
/// # Examples
///
/// ```
/// # async fn run() {
/// use std::time::Duration;
/// use throttle_net::resilience::timeout;
///
/// let fast = timeout(Duration::from_secs(1), async { 7 }).await;
/// assert_eq!(fast, Ok(7));
///
/// let slow = timeout(Duration::from_millis(10), std::future::pending::<()>()).await;
/// assert!(slow.is_err());
/// # }
/// ```
pub async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, Elapsed>
where
    F: IntoFuture,
{
    let work = async { Ok(future.await) };
    let alarm = async {
        crate::rt::sleep(duration).await;
        Err(Elapsed { after: duration })
    };
    // `or` polls `work` first, so a future that is ready when the timer fires
    // still wins.
    crate::rt::or(work, alarm).await
}

/// Runs `primary`; if it fails, runs `fallback` with the error and returns its
/// value instead.
///
/// The degraded-mode pattern: serve a cached answer, a default, or a cheaper
/// downstream when the preferred one fails (or is shed by a breaker).
///
/// # Examples
///
/// ```
/// # async fn run() {
/// use throttle_net::resilience::fallback;
///
/// let price = fallback(
///     async { Err::<u32, &str>("pricing service down") },
///     |_err| async { 100 }, // the last known price
/// )
/// .await;
/// assert_eq!(price, 100);
/// # }
/// ```
pub async fn fallback<P, T, E, F, G>(primary: P, fallback: F) -> T
where
    P: Future<Output = Result<T, E>>,
    F: FnOnce(E) -> G,
    G: Future<Output = T>,
{
    match primary.await {
        Ok(value) => value,
        Err(error) => fallback(error).await,
    }
}

/// Runs `attempt`, and if it has not finished after `delay`, starts a second,
/// identical attempt and returns whichever **succeeds** first.
///
/// Hedging trades a little extra load for lower tail latency on idempotent
/// calls: when the first attempt is stuck behind a slow replica, the second
/// usually is not. The losing attempt is dropped (cancelled) as soon as one
/// succeeds.
///
/// If the first attempt fails before the hedge starts, its error is returned at
/// once — hedging is for slowness, not failures (use [`Retry`](crate::Retry) for
/// those). Once both are running, a failure from one waits for the other, and
/// if both fail the later error is returned.
///
/// **With a circuit breaker:** the losing attempt is dropped, and a breaker
/// [`Permit`](crate::Permit) dropped unsettled counts as a **failure** — so
/// hedging through `CircuitBreaker::call` would trip the breaker on latency
/// alone. Take the permit inside the attempt and give the loser's back with
/// [`Permit::cancel`](crate::Permit::cancel) (for example from a drop guard),
/// or hedge outside the breaker.
///
/// # Errors
///
/// The operation's own error, as described above.
///
/// # Examples
///
/// ```
/// # async fn run() {
/// use std::sync::atomic::{AtomicU32, Ordering};
/// use std::time::Duration;
/// use throttle_net::resilience::hedge;
///
/// let calls = AtomicU32::new(0);
/// let value: Result<&str, ()> = hedge(Duration::from_millis(10), || {
///     let n = calls.fetch_add(1, Ordering::SeqCst);
///     async move {
///         if n == 0 {
///             std::future::pending::<()>().await; // the first attempt hangs
///         }
///         Ok("from the hedge")
///     }
/// })
/// .await;
/// assert_eq!(value, Ok("from the hedge"));
/// assert_eq!(calls.load(Ordering::SeqCst), 2);
/// # }
/// ```
pub async fn hedge<F, Fut, T, E>(delay: Duration, mut attempt: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut primary = pin!(attempt());
    let mut timer = pin!(crate::rt::sleep(delay));
    let mut secondary: Option<Pin<Box<Fut>>> = None;
    let mut primary_done = false;
    let mut secondary_done = false;
    core::future::poll_fn(move |cx: &mut Context<'_>| {
        if !primary_done {
            if let Poll::Ready(result) = primary.as_mut().poll(cx) {
                match result {
                    Ok(value) => return Poll::Ready(Ok(value)),
                    // Failed before the hedge started: report it now.
                    Err(error) if secondary.is_none() => return Poll::Ready(Err(error)),
                    Err(error) if secondary_done => return Poll::Ready(Err(error)),
                    Err(_) => primary_done = true,
                }
            }
        }
        if secondary.is_none() && timer.as_mut().poll(cx).is_ready() {
            secondary = Some(Box::pin(attempt()));
        }
        if let Some(hedged) = secondary.as_mut() {
            if !secondary_done {
                if let Poll::Ready(result) = hedged.as_mut().poll(cx) {
                    match result {
                        Ok(value) => return Poll::Ready(Ok(value)),
                        Err(error) if primary_done => return Poll::Ready(Err(error)),
                        Err(_) => secondary_done = true,
                    }
                }
            }
        }
        Poll::Pending
    })
    .await
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::{Elapsed, fallback, hedge, timeout};
    use crate::error::ThrottleError;
    use core::time::Duration;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test(start_paused = true)]
    async fn test_timeout_returns_output_or_elapsed() {
        assert_eq!(timeout(Duration::from_secs(1), async { 3 }).await, Ok(3));
        let err = timeout(Duration::from_secs(2), std::future::pending::<()>())
            .await
            .unwrap_err();
        assert_eq!(err.after(), Duration::from_secs(2));
        assert!(err.to_string().contains("timed out"));
        assert_eq!(ThrottleError::from(err), ThrottleError::DeadlineExceeded);
        let _: &dyn std::error::Error = &Elapsed {
            after: Duration::ZERO,
        };
    }

    #[tokio::test(start_paused = true)]
    async fn test_fallback_runs_only_on_error() {
        assert_eq!(
            fallback(async { Ok::<_, ()>(1) }, |()| async { 2 }).await,
            1
        );
        assert_eq!(
            fallback(async { Err::<u8, _>(5u8) }, |e| async move { e * 2 }).await,
            10
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_hedge_returns_the_fast_primary_without_a_second_attempt() {
        let calls = AtomicU32::new(0);
        let result: Result<u32, ()> = hedge(Duration::from_millis(50), || {
            let _ = calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(1) }
        })
        .await;
        assert_eq!(result, Ok(1));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_hedge_rescues_a_slow_primary() {
        let calls = AtomicU32::new(0);
        let start = tokio::time::Instant::now();
        let result: Result<u32, ()> = hedge(Duration::from_millis(50), || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                let wait = if n == 0 { 10_000 } else { 5 };
                tokio::time::sleep(Duration::from_millis(wait)).await;
                Ok(n)
            }
        })
        .await;
        assert_eq!(result, Ok(1));
        assert_eq!(start.elapsed(), Duration::from_millis(55));
    }

    #[tokio::test(start_paused = true)]
    async fn test_hedge_error_semantics() {
        // A fast failure is reported without hedging.
        let calls = AtomicU32::new(0);
        let result: Result<(), u32> = hedge(Duration::from_millis(50), || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move { Err(n) }
        })
        .await;
        assert_eq!(result, Err(0));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Both running: the first failure waits for the other's success.
        let calls = AtomicU32::new(0);
        let result: Result<u32, u32> = hedge(Duration::from_millis(10), || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Err(0)
                } else {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    Ok(1)
                }
            }
        })
        .await;
        assert_eq!(result, Ok(1));

        // Both fail: the later error wins.
        let calls = AtomicU32::new(0);
        let result: Result<(), u32> = hedge(Duration::from_millis(10), || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                tokio::time::sleep(Duration::from_millis(if n == 0 { 40 } else { 5 })).await;
                Err(n)
            }
        })
        .await;
        assert_eq!(result, Err(0), "the primary failed last");
    }
}
