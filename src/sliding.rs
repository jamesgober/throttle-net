//! An exact sliding-window-log limiter.
//!
//! [`Throttle`](crate::Throttle) is a token bucket: smooth and cheap, but it
//! permits a full burst at any instant. A [`SlidingWindowLog`] is the exact
//! alternative — it records the timestamp of every grant and admits a request
//! only if fewer than `limit` units were granted in the trailing `window`. No
//! boundary burst, at the cost of remembering recent grants.
//!
//! It implements [`Limiter`](crate::Limiter), so it composes everywhere the
//! token bucket does (hybrid, per-key, layered, behind a circuit breaker), and it
//! offers the same waiting [`acquire`](SlidingWindowLog::acquire) surface.

use core::time::Duration;
use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, PoisonError};

use clock_lib::{Clock, Monotonic, SystemClock};

use crate::decision::Decision;
#[cfg(feature = "tokio")]
use crate::error::ThrottleError;
use crate::limiter::Limiter;

/// One grant in the log: when it happened and how many units it took. The whole
/// `count` leaves the window together when `at_ms + window` passes.
#[derive(Clone, Copy)]
struct Grant {
    at_ms: u64,
    count: u32,
}

/// The mutable log, guarded by a mutex.
struct Log {
    /// Grants in arrival order (oldest at the front).
    grants: VecDeque<Grant>,
    /// Sum of `count` across `grants`, kept in step to avoid re-summing.
    used: u32,
}

/// An exact sliding-window-log rate limiter: at most `limit` units in any
/// trailing window of `window`.
///
/// Construct with [`new`](Self::new) (or [`per_second`](Self::per_second)), then
/// use the non-blocking [`try_acquire`](Self::try_acquire) / [`peek`](Self::peek)
/// or the waiting [`acquire`](Self::acquire). Time comes from an injectable
/// [`Clock`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use throttle_net::SlidingWindowLog;
///
/// // At most 5 requests in any 1-second window.
/// let limiter = SlidingWindowLog::new(5, Duration::from_secs(1));
/// for _ in 0..5 {
///     assert!(limiter.try_acquire());
/// }
/// assert!(!limiter.try_acquire()); // the 6th in this window is refused
/// ```
pub struct SlidingWindowLog<C = SystemClock>
where
    C: Clock,
{
    limit: u32,
    window: Duration,
    log: Mutex<Log>,
    clock: C,
    epoch: Monotonic,
}

impl SlidingWindowLog<SystemClock> {
    /// Creates a limiter admitting at most `limit` units per trailing `window`.
    ///
    /// A `limit` of `0` admits nothing; a zero `window` makes every grant expire
    /// immediately (so it behaves as a per-instant limit of `limit`).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::SlidingWindowLog;
    ///
    /// let limiter = SlidingWindowLog::new(100, Duration::from_secs(60)); // 100/min
    /// assert_eq!(limiter.capacity(), 100);
    /// ```
    #[must_use]
    pub fn new(limit: u32, window: Duration) -> Self {
        Self::with_clock_inner(limit, window, SystemClock::new())
    }

    /// Creates a limiter admitting at most `rate` units in any one-second window.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::SlidingWindowLog;
    ///
    /// let limiter = SlidingWindowLog::per_second(50);
    /// assert!(limiter.try_acquire());
    /// ```
    #[must_use]
    pub fn per_second(rate: u32) -> Self {
        Self::new(rate, Duration::from_secs(1))
    }
}

impl<C> SlidingWindowLog<C>
where
    C: Clock + Clone,
{
    fn with_clock_inner(limit: u32, window: Duration, clock: C) -> Self {
        let epoch = clock.now();
        Self {
            limit,
            window,
            log: Mutex::new(Log {
                grants: VecDeque::new(),
                used: 0,
            }),
            clock,
            epoch,
        }
    }

    /// Replaces the time source, for deterministic tests with a
    /// [`ManualClock`](clock_lib::ManualClock). The log is reset.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use clock_lib::ManualClock;
    /// use throttle_net::SlidingWindowLog;
    ///
    /// let clock = Arc::new(ManualClock::new());
    /// let limiter = SlidingWindowLog::new(2, Duration::from_secs(1)).with_clock(clock.clone());
    ///
    /// assert!(limiter.try_acquire());
    /// assert!(limiter.try_acquire());
    /// assert!(!limiter.try_acquire());
    /// clock.advance(Duration::from_secs(1)); // the window slides past both grants
    /// assert!(limiter.try_acquire());
    /// ```
    #[must_use]
    pub fn with_clock<C2>(self, clock: C2) -> SlidingWindowLog<C2>
    where
        C2: Clock + Clone,
    {
        SlidingWindowLog::with_clock_inner(self.limit, self.window, clock)
    }

    #[inline]
    fn lock(&self) -> MutexGuard<'_, Log> {
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[inline]
    fn now_ms(&self) -> u64 {
        let elapsed = self.clock.now().saturating_duration_since(self.epoch);
        u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
    }

    #[inline]
    fn window_ms(&self) -> u64 {
        u64::try_from(self.window.as_millis()).unwrap_or(u64::MAX)
    }

    /// Drops grants that have left the trailing window ending at `now_ms`.
    ///
    /// A grant made at `at_ms` occupies the window until `at_ms + window_ms`; once
    /// that moment has arrived it no longer counts. Grants are ordered oldest-first,
    /// so pruning stops at the first one still inside the window.
    fn prune(log: &mut Log, now_ms: u64, window_ms: u64) {
        while let Some(front) = log.grants.front() {
            if front.at_ms.saturating_add(window_ms) <= now_ms {
                log.used = log.used.saturating_sub(front.count);
                let _ = log.grants.pop_front();
            } else {
                break;
            }
        }
    }

    /// Wait until enough of the oldest grants expire to free `needed` units.
    fn wait_for(log: &Log, now_ms: u64, window_ms: u64, needed: u32) -> Duration {
        let mut freed = 0u32;
        for grant in &log.grants {
            freed = freed.saturating_add(grant.count);
            if freed >= needed {
                let ready_at = grant.at_ms.saturating_add(window_ms);
                return Duration::from_millis(ready_at.saturating_sub(now_ms));
            }
        }
        // Should not happen (capacity is checked first), but never wait forever.
        Duration::from_millis(window_ms)
    }

    /// The shared decision core. Records the grant on success.
    fn decide(&self, cost: u32) -> Decision {
        if cost > self.limit {
            return Decision::Impossible;
        }
        if cost == 0 {
            return Decision::Acquired;
        }
        let now_ms = self.now_ms();
        let window_ms = self.window_ms();
        let mut log = self.lock();
        Self::prune(&mut log, now_ms, window_ms);
        if log.used + cost <= self.limit {
            log.used += cost;
            log.grants.push_back(Grant {
                at_ms: now_ms,
                count: cost,
            });
            Decision::Acquired
        } else {
            let needed = log.used + cost - self.limit;
            Decision::Retry {
                after: Self::wait_for(&log, now_ms, window_ms, needed),
            }
        }
    }

    /// Attempts to take one unit without waiting.
    #[inline]
    #[must_use]
    pub fn try_acquire(&self) -> bool {
        self.decide(1).is_acquired()
    }

    /// Attempts to take `cost` units without waiting (all-or-nothing).
    #[inline]
    #[must_use]
    pub fn try_acquire_with_cost(&self, cost: u32) -> bool {
        self.decide(cost).is_acquired()
    }

    /// Reports whether `cost` units would be admitted now, without recording.
    #[must_use]
    pub fn peek(&self, cost: u32) -> Decision {
        if cost > self.limit {
            return Decision::Impossible;
        }
        if cost == 0 {
            return Decision::Acquired;
        }
        let now_ms = self.now_ms();
        let window_ms = self.window_ms();
        let mut log = self.lock();
        Self::prune(&mut log, now_ms, window_ms);
        if log.used + cost <= self.limit {
            Decision::Acquired
        } else {
            let needed = log.used + cost - self.limit;
            Decision::Retry {
                after: Self::wait_for(&log, now_ms, window_ms, needed),
            }
        }
    }

    /// Units still admissible in the current window.
    #[must_use]
    pub fn available(&self) -> u32 {
        let now_ms = self.now_ms();
        let window_ms = self.window_ms();
        let mut log = self.lock();
        Self::prune(&mut log, now_ms, window_ms);
        self.limit.saturating_sub(log.used)
    }

    /// The window limit (the most units admissible at once).
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.limit
    }
}

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
impl<C> SlidingWindowLog<C>
where
    C: Clock + Clone,
{
    /// Takes one unit, waiting until the window has room.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] when the limit is zero.
    pub async fn acquire(&self) -> Result<(), ThrottleError> {
        self.acquire_with_cost(1).await
    }

    /// Takes `cost` units, waiting until the window has room.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] when `cost` exceeds the
    /// limit, so it can never be admitted.
    pub async fn acquire_with_cost(&self, cost: u32) -> Result<(), ThrottleError> {
        loop {
            match self.decide(cost) {
                Decision::Acquired => return Ok(()),
                Decision::Impossible => {
                    return Err(ThrottleError::CostExceedsCapacity {
                        cost,
                        capacity: self.limit,
                    });
                }
                Decision::Retry { after } => tokio::time::sleep(after).await,
            }
        }
    }
}

impl<C> Limiter for SlidingWindowLog<C>
where
    C: Clock + Clone + Send + Sync,
{
    #[inline]
    fn peek(&self, cost: u32) -> Decision {
        SlidingWindowLog::peek(self, cost)
    }

    #[inline]
    fn acquire_cost(&self, cost: u32) -> Decision {
        self.decide(cost)
    }

    #[inline]
    fn available(&self) -> u32 {
        SlidingWindowLog::available(self)
    }

    #[inline]
    fn capacity(&self) -> u32 {
        self.limit
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::SlidingWindowLog;
    use crate::limiter::Limiter;
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_is_send_sync() {
        assert_send_sync::<SlidingWindowLog>();
    }

    #[test]
    fn test_admits_up_to_limit_then_refuses() {
        let limiter = SlidingWindowLog::new(3, Duration::from_secs(1));
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
        assert_eq!(limiter.available(), 0);
    }

    #[test]
    fn test_window_slides_exactly() {
        let clock = Arc::new(ManualClock::new());
        let limiter = SlidingWindowLog::new(2, Duration::from_secs(1)).with_clock(clock.clone());

        assert!(limiter.try_acquire()); // t=0
        clock.advance(Duration::from_millis(600));
        assert!(limiter.try_acquire()); // t=600
        assert!(!limiter.try_acquire()); // 2 in the last 1s

        // At t=1001 the first grant (t=0) has left the window, but the second
        // (t=600) has not — so exactly one slot opens.
        clock.advance(Duration::from_millis(401));
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_no_boundary_burst() {
        // Unlike a fixed window, the sliding log never admits 2x the limit across
        // a boundary: 3 at the end of one window blocks 3 at the start of the next
        // until they age out.
        let clock = Arc::new(ManualClock::new());
        let limiter = SlidingWindowLog::new(3, Duration::from_secs(1)).with_clock(clock.clone());

        clock.advance(Duration::from_millis(900));
        for _ in 0..3 {
            assert!(limiter.try_acquire()); // 3 grants at t=900
        }
        clock.advance(Duration::from_millis(200)); // t=1100, new "fixed" window
        assert!(!limiter.try_acquire()); // still 3 within the trailing 1s
    }

    #[test]
    fn test_cost_aware_and_impossible() {
        let limiter = SlidingWindowLog::new(5, Duration::from_secs(1));
        assert!(limiter.try_acquire_with_cost(4));
        assert!(!limiter.try_acquire_with_cost(4)); // only 1 left
        assert!(limiter.try_acquire_with_cost(1));
        // A cost beyond the limit can never be admitted.
        assert_eq!(
            SlidingWindowLog::new(5, Duration::from_secs(1)).peek(9),
            crate::Decision::Impossible
        );
    }

    #[test]
    fn test_peek_does_not_record() {
        let limiter = SlidingWindowLog::new(2, Duration::from_secs(1));
        assert!(limiter.peek(2).is_acquired());
        assert_eq!(limiter.available(), 2); // peek took nothing
    }

    #[test]
    fn test_retry_after_points_to_oldest_expiry() {
        let clock = Arc::new(ManualClock::new());
        let limiter = SlidingWindowLog::new(1, Duration::from_secs(1)).with_clock(clock.clone());
        assert!(limiter.try_acquire()); // t=0
        let after = limiter
            .peek(1)
            .retry_after()
            .expect("should suggest a wait");
        // The single grant expires at t=1s, so the wait is ~1s.
        assert_eq!(after, Duration::from_secs(1));
    }

    #[test]
    fn test_works_as_a_limiter_trait_object() {
        let limiter = SlidingWindowLog::new(2, Duration::from_secs(1));
        let dyn_limiter: &dyn Limiter = &limiter;
        assert_eq!(dyn_limiter.capacity(), 2);
        assert!(dyn_limiter.acquire_cost(1).is_acquired());
        assert_eq!(dyn_limiter.available(), 1);
    }
}
