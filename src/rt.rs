//! Runtime abstraction: one async `sleep` over the selected backend, and a
//! clock that follows tokio's time.
//!
//! Notifications use `event-listener` and races use `futures-lite`, both
//! runtime-agnostic, so the only runtime-specific operation is the timer. The
//! `tokio` (default) or `smol` feature provides it; if both are enabled, tokio's
//! timer is used. The rest of the waiting surface is identical on either, and the
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
    let _fired_at = smol::Timer::after(duration).await;
}

/// The runtime's notion of "now" for waiting deadlines: tokio's (pausable)
/// instant under the `tokio` feature, the OS monotonic clock under smol.
#[cfg(feature = "tokio")]
pub(crate) use tokio::time::Instant;

/// The runtime's notion of "now" for waiting deadlines.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
pub(crate) use std::time::Instant;

/// Woken whenever any limiter's pause is lifted, so waiters sleeping out a
/// pause re-check at once (TN2-010). One process-wide event keeps every pause
/// a single atomic word; a resume is rare, so the occasional spurious wake-up
/// of an unrelated waiter (it simply re-checks and sleeps again) is cheap.
static RESUMED: event_listener::Event = event_listener::Event::new();

/// Bumped on every resume, so a waiter that read it before deciding to sleep
/// can tell that a resume happened before it started listening.
static RESUME_EPOCH: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(0);

/// The current resume epoch: read it *before* checking a pause.
#[inline]
pub(crate) fn resume_epoch() -> usize {
    RESUME_EPOCH.load(core::sync::atomic::Ordering::Acquire)
}

/// Announces that a pause was lifted: wakes every waiter sleeping on one.
pub(crate) fn announce_resume() {
    let _previous = RESUME_EPOCH.fetch_add(1, core::sync::atomic::Ordering::AcqRel);
    let _woken = RESUMED.notify(usize::MAX);
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
    futures_lite::future::or(sleep(duration), listener).await;
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
