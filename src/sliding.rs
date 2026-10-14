//! An exact sliding-window-log limiter.
//!
//! [`Throttle`](crate::Throttle) is a token bucket: smooth and cheap, but it
//! permits a full burst at any instant. A [`SlidingWindowLog`] is the exact
//! alternative — it records the timestamp of every grant and admits a request
//! only if fewer than `limit` units were granted in the trailing `window`. No
//! boundary burst, at the cost of remembering recent grants.
//!
//! It implements [`Limiter`](crate::Limiter), so it composes everywhere the
//! token bucket does (hybrid, multi-dimensional, layered, behind a circuit
//! breaker), and it offers the same waiting
//! [`acquire`](SlidingWindowLog::acquire) surface.

use core::time::Duration;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, PoisonError};

use clock_lib::{Clock, Monotonic, SystemClock};

use crate::decision::Decision;
#[cfg(feature = "runtime")]
use crate::error::ThrottleError;
use crate::hold::nanos_between;
use crate::limiter::Limiter;
use crate::obs::{self, Label};

/// The limiter kind reported in metrics and events.
const KIND: &str = "sliding_window";

/// One grant in the log: when it happened (nanoseconds since the limiter's
/// epoch) and how many units it took. The whole `count` leaves the window
/// together.
#[derive(Clone, Copy)]
struct Grant {
    at_ns: u64,
    count: u32,
}

/// The mutable log, guarded by a mutex.
struct Log {
    /// Grants in arrival order (oldest at the front).
    grants: VecDeque<Grant>,
    /// Sum of `count` across `grants`, kept in step to avoid re-summing. A
    /// `u64`, so a limit near `u32::MAX` plus a cost can never overflow it.
    used: u64,
    /// `0` when not paused; otherwise the first instant (ns since the epoch)
    /// the pause no longer applies.
    hold_until_ns: u64,
}

/// An exact sliding-window-log rate limiter: at most `limit` units in any
/// trailing window of `window`.
///
/// Construct with [`new`](Self::new), [`per_second`](Self::per_second), or
/// [`per_minute`](Self::per_minute), then use the non-blocking
/// [`try_acquire`](Self::try_acquire) / [`peek`](Self::peek) or the waiting
/// [`acquire`](Self::acquire). Time comes from an injectable [`Clock`] and is
/// tracked to the nanosecond, so windows shorter than a millisecond are exact
/// too.
///
/// Memory is proportional to the grants inside one window (at most `limit`
/// entries); every operation is O(1) amortized, plus the expired grants it
/// prunes.
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
    /// `window` in nanoseconds, at least one (see [`new`](SlidingWindowLog::new)).
    window_ns: u64,
    log: Mutex<Log>,
    clock: C,
    epoch: Monotonic,
    label: Label,
}

impl SlidingWindowLog<SystemClock> {
    /// Creates a limiter admitting at most `limit` units per trailing `window`.
    ///
    /// A `limit` of `0` admits nothing. A zero `window` is a per-instant limit:
    /// at most `limit` units carry the same clock reading, and a grant leaves
    /// the window as soon as the clock moves on — with a real clock that is a
    /// limit per clock tick, which is almost never what you want, so prefer a
    /// real window.
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
        Self::with_clock_inner(limit, window, SystemClock::new(), Label::none())
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

    /// Creates a limiter admitting at most `rate` units in any one-minute window.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::SlidingWindowLog;
    ///
    /// let limiter = SlidingWindowLog::per_minute(30);
    /// assert_eq!(limiter.capacity(), 30);
    /// ```
    #[must_use]
    pub fn per_minute(rate: u32) -> Self {
        Self::new(rate, Duration::from_secs(60))
    }
}

impl<C> SlidingWindowLog<C>
where
    C: Clock,
{
    fn with_clock_inner(limit: u32, window: Duration, clock: C, label: Label) -> Self {
        let epoch = clock.now();
        Self {
            limit,
            window,
            window_ns: u64::try_from(window.as_nanos()).unwrap_or(u64::MAX).max(1),
            log: Mutex::new(Log {
                grants: VecDeque::new(),
                used: 0,
                hold_until_ns: 0,
            }),
            clock,
            epoch,
            label,
        }
    }

    /// Replaces the time source, for deterministic tests with a
    /// [`ManualClock`](clock_lib::ManualClock). The log is reset; the name is
    /// kept.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use throttle_net::{ManualClock, SlidingWindowLog};
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
        C2: Clock,
    {
        SlidingWindowLog::with_clock_inner(self.limit, self.window, clock, self.label)
    }

    /// Names this instance for metrics and events.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::SlidingWindowLog;
    ///
    /// let limiter = SlidingWindowLog::per_second(5).with_name("search");
    /// assert_eq!(limiter.name(), Some("search"));
    /// ```
    #[must_use]
    pub fn with_name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
        self
    }

    /// The instance name, if one was set.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.label.get()
    }

    /// The trailing window length.
    #[must_use]
    pub fn window(&self) -> Duration {
        self.window
    }

    #[inline]
    fn lock(&self) -> MutexGuard<'_, Log> {
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[inline]
    fn now_ns(&self) -> u64 {
        nanos_between(self.epoch, self.clock.now())
    }

    /// Drops grants that have left the trailing window ending at `now_ns`.
    ///
    /// A grant made at `at_ns` occupies the window until `at_ns + window_ns`;
    /// once that moment has arrived it no longer counts. Grants are ordered
    /// oldest-first, so pruning stops at the first one still inside.
    fn prune(&self, log: &mut Log, now_ns: u64) {
        while let Some(front) = log.grants.front() {
            if front.at_ns.saturating_add(self.window_ns) <= now_ns {
                log.used = log.used.saturating_sub(u64::from(front.count));
                let _expired = log.grants.pop_front();
            } else {
                break;
            }
        }
    }

    /// The wait until enough of the oldest grants expire to free `needed` units.
    fn wait_for(&self, log: &Log, now_ns: u64, needed: u64) -> Duration {
        let mut freed = 0u64;
        for grant in &log.grants {
            freed = freed.saturating_add(u64::from(grant.count));
            if freed >= needed {
                let ready_at = grant.at_ns.saturating_add(self.window_ns);
                return Duration::from_nanos(ready_at.saturating_sub(now_ns));
            }
        }
        // Unreachable while `cost <= limit` (checked first): the whole log
        // frees `used`, and `needed <= used`. Never report "now" for it.
        Duration::from_nanos(self.window_ns)
    }

    /// The shared decision core, under the lock. Records the grant when
    /// `record` is set and the units fit.
    fn evaluate(&self, cost: u32, record: bool) -> Decision {
        if cost > self.limit {
            return Decision::Impossible;
        }
        if cost == 0 {
            return Decision::Acquired;
        }
        let now_ns = self.now_ns();
        let mut log = self.lock();
        let held = log.hold_until_ns.saturating_sub(now_ns);
        if held > 0 {
            return Decision::Retry {
                after: Duration::from_nanos(held),
            };
        }
        self.prune(&mut log, now_ns);
        let wanted = log.used + u64::from(cost);
        if wanted <= u64::from(self.limit) {
            if record {
                // Concurrent callers read the clock before taking the lock, so
                // a later holder can carry an earlier reading. Stamping it no
                // earlier than the newest grant keeps the log sorted (pruning
                // and waits rely on it); the grant then expires a hair later,
                // which only ever makes the limiter stricter.
                let at_ns = log
                    .grants
                    .back()
                    .map_or(now_ns, |last| now_ns.max(last.at_ns));
                log.used = wanted;
                log.grants.push_back(Grant { at_ns, count: cost });
            }
            Decision::Acquired
        } else {
            let needed = wanted - u64::from(self.limit);
            Decision::Retry {
                after: self.wait_for(&log, now_ns, needed),
            }
        }
    }

    /// Attempts to take one unit without waiting.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::SlidingWindowLog;
    ///
    /// let limiter = SlidingWindowLog::per_second(1);
    /// assert!(limiter.try_acquire());
    /// assert!(!limiter.try_acquire());
    /// ```
    #[inline]
    #[must_use]
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_with_cost(1)
    }

    /// Attempts to take `cost` units without waiting (all-or-nothing).
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::SlidingWindowLog;
    ///
    /// let limiter = SlidingWindowLog::per_second(10);
    /// assert!(limiter.try_acquire_with_cost(8));
    /// assert!(!limiter.try_acquire_with_cost(3));
    /// ```
    #[inline]
    #[must_use]
    pub fn try_acquire_with_cost(&self, cost: u32) -> bool {
        let granted = self.evaluate(cost, true).is_acquired();
        obs::attempt(KIND, &self.label, cost, granted);
        granted
    }

    /// Reports whether `cost` units would be admitted now, without recording.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Decision, SlidingWindowLog};
    ///
    /// let limiter = SlidingWindowLog::per_second(2);
    /// assert_eq!(limiter.peek(2), Decision::Acquired);
    /// assert_eq!(limiter.peek(3), Decision::Impossible);
    /// ```
    #[must_use]
    pub fn peek(&self, cost: u32) -> Decision {
        self.evaluate(cost, false)
    }

    /// Units still admissible in the current window (`0` while paused).
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::SlidingWindowLog;
    ///
    /// let limiter = SlidingWindowLog::per_second(5);
    /// assert!(limiter.try_acquire_with_cost(2));
    /// assert_eq!(limiter.available(), 3);
    /// ```
    #[must_use]
    pub fn available(&self) -> u32 {
        let now_ns = self.now_ns();
        let mut log = self.lock();
        if log.hold_until_ns > now_ns {
            return 0;
        }
        self.prune(&mut log, now_ns);
        u32::try_from(u64::from(self.limit).saturating_sub(log.used)).unwrap_or(0)
    }

    /// The window limit (the most units admissible at once).
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.limit
    }

    /// Gives back `cost` units granted for work that never used them.
    ///
    /// The log does not know which grant a refund belongs to, so it removes the
    /// units from the **oldest** grants in the window. Those expire no later than
    /// the refunded grant would have, so the limiter is never more permissive
    /// than if the right grant had been removed — it may briefly be stricter.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::SlidingWindowLog;
    ///
    /// let limiter = SlidingWindowLog::per_second(3);
    /// assert!(limiter.try_acquire_with_cost(3));
    /// limiter.refund(2);
    /// assert_eq!(limiter.available(), 2);
    /// ```
    pub fn refund(&self, cost: u32) {
        let mut remaining = cost;
        let mut log = self.lock();
        while remaining > 0 {
            let Some(front) = log.grants.front_mut() else {
                break;
            };
            let take = front.count.min(remaining);
            front.count -= take;
            remaining -= take;
            if front.count == 0 {
                let _emptied = log.grants.pop_front();
            }
            log.used = log.used.saturating_sub(u64::from(take));
        }
    }

    /// Grants nothing for `duration` from now. Grants already in the window
    /// keep ageing out meanwhile. A shorter pause never shortens a longer one;
    /// [`resume`](Self::resume) lifts it early.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::SlidingWindowLog;
    ///
    /// let limiter = SlidingWindowLog::per_second(5);
    /// limiter.pause_for(Duration::from_secs(10));
    /// assert!(!limiter.try_acquire());
    /// limiter.resume();
    /// assert!(limiter.try_acquire());
    /// ```
    pub fn pause_for(&self, duration: Duration) {
        if duration.is_zero() {
            return;
        }
        let now_ns = self.now_ns();
        let until = now_ns
            .saturating_add(u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
            .max(1);
        let mut log = self.lock();
        log.hold_until_ns = log.hold_until_ns.max(until);
    }

    /// Lifts any pause immediately, waking waiters that were sleeping it out.
    pub fn resume(&self) {
        #[cfg(feature = "runtime")]
        crate::rt::resume(|| core::mem::take(&mut self.lock().hold_until_ns) != 0);
        #[cfg(not(feature = "runtime"))]
        {
            self.lock().hold_until_ns = 0;
        }
    }

    /// Records `cost` units that were already spent, whether or not they fit
    /// and even while paused: they occupy the window like any grant, so the
    /// log can run over its limit until they age out.
    fn charge_spent(&self, cost: u32) {
        if cost == 0 {
            return;
        }
        let now_ns = self.now_ns();
        let mut log = self.lock();
        self.prune(&mut log, now_ns);
        let at_ns = log
            .grants
            .back()
            .map_or(now_ns, |last| now_ns.max(last.at_ns));
        log.used = log.used.saturating_add(u64::from(cost));
        log.grants.push_back(Grant { at_ns, count: cost });
    }

    /// How much longer the limiter stays paused (`Duration::ZERO` when it is
    /// not paused).
    #[must_use]
    pub fn paused_for(&self) -> Duration {
        let now_ns = self.now_ns();
        Duration::from_nanos(self.lock().hold_until_ns.saturating_sub(now_ns))
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl<C> SlidingWindowLog<C>
where
    C: Clock,
{
    /// Takes one unit, waiting until the window has room.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] when the limit is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use throttle_net::SlidingWindowLog;
    ///
    /// let limiter = SlidingWindowLog::per_second(10);
    /// limiter.acquire().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire(&self) -> Result<(), ThrottleError> {
        self.acquire_with_cost(1).await
    }

    /// Takes `cost` units, waiting until the window has room.
    ///
    /// The waiter sleeps until the oldest grants leave the window and tries
    /// again. Unlike [`Throttle`](crate::Throttle) it does not reserve, so under
    /// sustained contention a large cost can wait behind smaller ones; use a
    /// token bucket when strict arrival-order fairness matters. Cancel-safe:
    /// nothing is held while waiting.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] when `cost` exceeds the
    /// limit, so it can never be admitted.
    pub async fn acquire_with_cost(&self, cost: u32) -> Result<(), ThrottleError> {
        self.wait(cost, None).await
    }

    /// Takes `cost` units, waiting at most `timeout`. Gives up at once if the
    /// window will not have room within the timeout.
    ///
    /// # Errors
    ///
    /// [`ThrottleError::DeadlineExceeded`] if the units cannot be had in time;
    /// [`ThrottleError::CostExceedsCapacity`] if `cost` exceeds the limit.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// use std::time::Duration;
    /// use throttle_net::{SlidingWindowLog, ThrottleError};
    ///
    /// let limiter = SlidingWindowLog::new(1, Duration::from_secs(60));
    /// assert!(limiter.try_acquire());
    /// let err = limiter
    ///     .acquire_with_cost_timeout(1, Duration::from_millis(10))
    ///     .await
    ///     .unwrap_err();
    /// assert_eq!(err, ThrottleError::DeadlineExceeded);
    /// # }
    /// ```
    pub async fn acquire_with_cost_timeout(
        &self,
        cost: u32,
        timeout: Duration,
    ) -> Result<(), ThrottleError> {
        self.wait(cost, Some(timeout)).await
    }

    async fn wait(&self, cost: u32, timeout: Option<Duration>) -> Result<(), ThrottleError> {
        use crate::wait::{Refusal, Step, wait_for};
        let timer = obs::Timer::start();
        let result = wait_for(
            |_budget| match self.evaluate(cost, true) {
                Decision::Acquired => Step::Granted(Duration::ZERO),
                Decision::Retry { after } => Step::Retry(after),
                _ => Step::Never,
            },
            || self.refund(cost),
            || self.paused_for(),
            timeout,
        )
        .await
        .map_err(|refusal| match refusal {
            Refusal::Never => ThrottleError::CostExceedsCapacity {
                cost,
                capacity: self.limit,
            },
            Refusal::TimedOut => ThrottleError::DeadlineExceeded,
        });
        obs::waited(KIND, &self.label, &timer, cost, obs::Waited::of(&result));
        result
    }
}

impl<C> Limiter for SlidingWindowLog<C>
where
    C: Clock,
{
    #[inline]
    fn peek(&self, cost: u32) -> Decision {
        self.evaluate(cost, false)
    }

    #[inline]
    fn acquire_cost(&self, cost: u32) -> Decision {
        self.evaluate(cost, true)
    }

    #[inline]
    fn available(&self) -> u32 {
        SlidingWindowLog::available(self)
    }

    #[inline]
    fn capacity(&self) -> u32 {
        self.limit
    }

    fn refund(&self, cost: u32) {
        SlidingWindowLog::refund(self, cost);
    }

    fn pause_for(&self, duration: Duration) {
        SlidingWindowLog::pause_for(self, duration);
    }

    fn paused_for(&self) -> Duration {
        SlidingWindowLog::paused_for(self)
    }

    fn resume(&self) {
        SlidingWindowLog::resume(self);
    }

    /// Records the units in the window even if they exceed the limit or the
    /// log is paused; later grants wait until they age out.
    fn charge(&self, cost: u32) -> u32 {
        self.charge_spent(cost);
        0
    }
}

impl<C: Clock> core::fmt::Debug for SlidingWindowLog<C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SlidingWindowLog")
            .field("name", &self.label)
            .field("limit", &self.limit)
            .field("window", &self.window)
            .field("available", &self.available())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::SlidingWindowLog;
    use crate::decision::Decision;
    use crate::limiter::Limiter;
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    fn manual(
        limit: u32,
        window: Duration,
    ) -> (Arc<ManualClock>, SlidingWindowLog<Arc<ManualClock>>) {
        let clock = Arc::new(ManualClock::new());
        let limiter = SlidingWindowLog::new(limit, window).with_clock(Arc::clone(&clock));
        (clock, limiter)
    }

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
        let (clock, limiter) = manual(2, Duration::from_secs(1));
        assert!(limiter.try_acquire()); // t=0
        clock.advance(Duration::from_millis(600));
        assert!(limiter.try_acquire()); // t=600
        assert!(!limiter.try_acquire());
        clock.advance(Duration::from_millis(400)); // t=1000: the t=0 grant leaves
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_no_boundary_burst() {
        let (clock, limiter) = manual(3, Duration::from_secs(1));
        clock.advance(Duration::from_millis(900));
        for _ in 0..3 {
            assert!(limiter.try_acquire());
        }
        clock.advance(Duration::from_millis(200));
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_sub_millisecond_window_is_exact() {
        // Regression (TN-009): any window under 1ms was unlimited.
        let (clock, limiter) = manual(5, Duration::from_micros(500));
        for _ in 0..5 {
            assert!(limiter.try_acquire());
        }
        assert!(!limiter.try_acquire(), "the sixth in 500us is refused");
        assert_eq!(
            limiter.peek(1),
            Decision::Retry {
                after: Duration::from_micros(500)
            }
        );
        clock.advance(Duration::from_micros(500));
        assert!(limiter.try_acquire());
    }

    #[test]
    fn test_zero_window_is_a_per_instant_limit() {
        // Regression (TN-009): a zero window admitted everything.
        let (clock, limiter) = manual(5, Duration::ZERO);
        let granted = (0..1000).filter(|_| limiter.try_acquire()).count();
        assert_eq!(granted, 5, "5 per clock reading");
        clock.advance(Duration::from_nanos(1));
        assert!(limiter.try_acquire());
    }

    #[test]
    fn test_limit_near_u32_max_does_not_overflow() {
        // Regression (TN-019): `used + cost` wrapped in u32.
        let limiter = SlidingWindowLog::new(u32::MAX, Duration::from_secs(60));
        assert!(limiter.try_acquire_with_cost(u32::MAX));
        assert!(!limiter.try_acquire_with_cost(2));
        assert!(!limiter.try_acquire());
        assert_eq!(limiter.available(), 0);
    }

    #[test]
    fn test_cost_aware_and_impossible() {
        let limiter = SlidingWindowLog::new(5, Duration::from_secs(1));
        assert!(limiter.try_acquire_with_cost(4));
        assert!(!limiter.try_acquire_with_cost(4));
        assert!(limiter.try_acquire_with_cost(1));
        assert_eq!(limiter.peek(9), Decision::Impossible);
        assert!(limiter.try_acquire_with_cost(0));
        assert!(!SlidingWindowLog::new(0, Duration::from_secs(1)).try_acquire());
    }

    #[test]
    fn test_peek_does_not_record() {
        let limiter = SlidingWindowLog::new(2, Duration::from_secs(1));
        assert!(limiter.peek(2).is_acquired());
        assert_eq!(limiter.available(), 2);
    }

    #[test]
    fn test_retry_after_points_to_oldest_expiry() {
        let (_clock, limiter) = manual(1, Duration::from_secs(1));
        assert!(limiter.try_acquire());
        let after = limiter
            .peek(1)
            .retry_after()
            .expect("should suggest a wait");
        assert_eq!(after, Duration::from_secs(1));
    }

    #[test]
    fn test_refund_removes_the_oldest_units() {
        let (clock, limiter) = manual(4, Duration::from_secs(1));
        assert!(limiter.try_acquire_with_cost(2)); // t=0
        clock.advance(Duration::from_millis(500));
        assert!(limiter.try_acquire_with_cost(2)); // t=500
        limiter.refund(3); // removes both t=0 units and one t=500 unit
        assert_eq!(limiter.available(), 3);
        limiter.refund(10); // more than recorded: drains the log, no underflow
        assert_eq!(limiter.available(), 4);
    }

    #[test]
    fn test_pause_and_resume() {
        let (clock, limiter) = manual(3, Duration::from_secs(1));
        limiter.pause_for(Duration::from_secs(2));
        assert!(!limiter.try_acquire());
        assert_eq!(limiter.available(), 0);
        assert_eq!(limiter.paused_for(), Duration::from_secs(2));
        assert_eq!(
            limiter.peek(1),
            Decision::Retry {
                after: Duration::from_secs(2)
            }
        );
        limiter.pause_for(Duration::from_secs(1)); // does not shorten
        assert_eq!(limiter.paused_for(), Duration::from_secs(2));
        limiter.pause_for(Duration::ZERO);
        clock.advance(Duration::from_secs(2));
        assert!(limiter.try_acquire());
        limiter.pause_for(Duration::MAX);
        assert!(!limiter.try_acquire());
        limiter.resume();
        assert!(limiter.try_acquire());
    }

    #[test]
    fn test_works_as_a_limiter_trait_object() {
        let limiter = SlidingWindowLog::new(2, Duration::from_secs(1)).with_name("w");
        let dyn_limiter: &dyn Limiter = &limiter;
        assert_eq!(dyn_limiter.capacity(), 2);
        assert!(dyn_limiter.acquire_cost(1).is_acquired());
        assert_eq!(dyn_limiter.available(), 1);
        assert_eq!(dyn_limiter.peek(1), Decision::Acquired);
        // The provided reserve takes what is there now, never queues.
        assert_eq!(dyn_limiter.reserve(1, Duration::MAX), Some(Duration::ZERO));
        assert_eq!(dyn_limiter.reserve(1, Duration::MAX), None);
        dyn_limiter.refund(1);
        assert_eq!(dyn_limiter.available(), 1);
        dyn_limiter.pause_for(Duration::from_secs(1));
        assert_eq!(dyn_limiter.available(), 0);
        assert_eq!(limiter.name(), Some("w"));
        assert_eq!(limiter.window(), Duration::from_secs(1));
        assert!(format!("{limiter:?}").contains("SlidingWindowLog"));
        assert_eq!(SlidingWindowLog::per_minute(7).capacity(), 7);
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_waits_for_the_window_to_slide() {
        let limiter = SlidingWindowLog::new(2, Duration::from_millis(20));
        assert!(limiter.try_acquire_with_cost(2));
        assert!(limiter.acquire().await.is_ok());
        let err = limiter.acquire_with_cost(3).await.unwrap_err();
        assert!(matches!(
            err,
            crate::error::ThrottleError::CostExceedsCapacity {
                cost: 3,
                capacity: 2
            }
        ));
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_timeout_gives_up_when_the_window_is_too_long() {
        let limiter = SlidingWindowLog::new(1, Duration::from_secs(60));
        assert!(limiter.try_acquire());
        let err = limiter
            .acquire_with_cost_timeout(1, Duration::from_millis(5))
            .await
            .unwrap_err();
        assert_eq!(err, crate::error::ThrottleError::DeadlineExceeded);
    }
}
