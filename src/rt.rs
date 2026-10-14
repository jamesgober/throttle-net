//! Runtime abstraction: one async `sleep` over the selected backend, and a
//! clock that follows tokio's time.
//!
//! Notifications use `event-listener` (runtime-agnostic) and the few
//! combinators the waits need are defined here, so the only runtime-specific
//! operation is the timer. The `tokio` (default) or `smol` feature provides it
//! (smol's timer is `async_io::Timer`); if both are enabled, tokio's timer is
//! used. The rest of the waiting surface is identical on either, and the
//! same async code runs on whichever executor the application drives it with.

use core::time::Duration;

#[cfg(all(feature = "runtime", not(any(feature = "tokio", feature = "smol"))))]
compile_error!(
    "throttle-net: the async `acquire` surface needs a runtime timer — enable the `tokio` (default) or `smol` feature"
);

/// Sleeps for `duration` using tokio's timer.
#[cfg(feature = "tokio")]
#[inline]
pub(crate) async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

/// Sleeps for `duration` using smol's timer.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
#[inline]
pub(crate) async fn sleep(duration: Duration) {
    // The timer resolves to the instant it fired, which nothing here needs.
    let _fired_at = async_io::Timer::after(duration).await;
}

/// The runtime's notion of "now" for waiting deadlines: tokio's (pausable)
/// instant under the `tokio` feature, the OS monotonic clock under smol.
#[cfg(feature = "tokio")]
pub(crate) use tokio::time::Instant;

/// The runtime's notion of "now" for waiting deadlines.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) use std::time::Instant;

/// Woken whenever a pause is lifted (TN2-010). Only waiters whose own limiter
/// is paused listen on it — a waiter sleeping for a refill never registers —
/// so a `resume` wakes exactly the waiters that are sleeping out a pause (on
/// any limiter: one whose own limiter is still paused re-checks and sleeps
/// again). One process-wide event keeps every pause a single atomic word.
static RESUMED: event_listener::Event = event_listener::Event::new();

/// Bumped on every resume, before the pause is cleared, so a waiter that read
/// it before deciding how to sleep can tell that a resume happened since.
static RESUME_EPOCH: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(0);

/// The current resume epoch: read it *before* checking a pause.
#[inline]
pub(crate) fn resume_epoch() -> usize {
    RESUME_EPOCH.load(core::sync::atomic::Ordering::SeqCst)
}

/// Lifts a pause: bumps the epoch, runs `lift` (which clears the pause and
/// reports whether one was set), and wakes the waiters sleeping one out.
///
/// The epoch moves first, so a waiter that sees the pause gone also sees the
/// new epoch and never sleeps through the resume.
pub(crate) fn resume(lift: impl FnOnce() -> bool) {
    let _previous = RESUME_EPOCH.fetch_add(1, core::sync::atomic::Ordering::SeqCst);
    if lift() {
        let _woken = RESUMED.notify(usize::MAX);
    }
}

/// Sleeps for `duration`, or until a pause is lifted after `epoch` was read,
/// whichever comes first. Very short naps are not worth a listener.
pub(crate) async fn sleep_unless_resumed(duration: Duration, epoch: usize) {
    if duration <= Duration::from_millis(1) {
        sleep(duration).await;
        return;
    }
    let listener = RESUMED.listen();
    if resume_epoch() != epoch {
        return;
    }
    or(sleep(duration), listener).await;
}

/// Runs two futures to completion side by side and resolves with whichever
/// finishes first (polling `a` first); the other is dropped.
pub(crate) async fn or<T, A, B>(a: A, b: B) -> T
where
    A: core::future::Future<Output = T>,
    B: core::future::Future<Output = T>,
{
    let mut a = core::pin::pin!(a);
    let mut b = core::pin::pin!(b);
    core::future::poll_fn(move |cx| {
        if let core::task::Poll::Ready(value) = a.as_mut().poll(cx) {
            return core::task::Poll::Ready(value);
        }
        b.as_mut().poll(cx)
    })
    .await
}

/// Yields to the executor once, so other tasks run before this one resumes.
pub(crate) async fn yield_now() {
    let mut yielded = false;
    core::future::poll_fn(move |cx| {
        if yielded {
            core::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        }
    })
    .await;
}

/// A [`Clock`](clock_lib::Clock) that reads tokio's time.
///
/// In production tokio's clock is the OS monotonic clock, so this behaves like
/// [`SystemClock`](clock_lib::SystemClock). In tests run with
/// `#[tokio::test(start_paused = true)]` (or after `tokio::time::pause()`),
/// tokio's clock is virtual and jumps forward whenever every task is waiting on
/// a timer — so a limiter on a `TokioClock` sees exactly the time its waiting
/// `acquire` slept, and refill-dependent tests run instantly and
/// deterministically, with no `ManualClock` to advance by hand.
///
/// Wall-clock readings ([`Clock::wall`](clock_lib::Clock::wall)) come from the
/// system clock, which tokio does not virtualise.
///
/// Behind the `tokio` feature.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() -> Result<(), throttle_net::ThrottleError> {
/// use std::time::Duration;
/// use throttle_net::{Throttle, TokioClock};
///
/// let throttle = Throttle::per_second(10).with_clock(TokioClock::new());
/// assert!(throttle.try_acquire_with_cost(10));
///
/// let start = tokio::time::Instant::now();
/// throttle.acquire_with_cost(5).await?; // sleeps 500ms of virtual time
/// assert_eq!(start.elapsed(), Duration::from_millis(500));
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tokio")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct TokioClock;

#[cfg(feature = "tokio")]
impl TokioClock {
    /// Creates the clock. It is zero-sized and stateless.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[cfg(feature = "tokio")]
impl clock_lib::Clock for TokioClock {
    #[inline]
    fn now(&self) -> clock_lib::Monotonic {
        clock_lib::Monotonic::from(tokio::time::Instant::now().into_std())
    }

    #[inline]
    fn wall(&self) -> clock_lib::Wall {
        clock_lib::Wall::now()
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::TokioClock;
    use clock_lib::Clock;
    use core::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn test_tokio_clock_follows_paused_time() {
        let clock = TokioClock::new();
        let start = clock.now();
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert_eq!(
            clock.now().saturating_duration_since(start),
            Duration::from_secs(3600)
        );
        assert!(clock.wall().unix_seconds() > 0);
        assert_eq!(TokioClock, clock);
    }
}
