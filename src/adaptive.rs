//! Adaptive concurrency limiting: find the right in-flight limit without being
//! told it.
//!
//! A token bucket needs you to know the downstream's capacity up front. An
//! [`AdaptiveLimiter`] *discovers* it: it caps the number of in-flight requests
//! at a limit it adjusts from observed outcomes. When requests succeed (and stay
//! fast) it lets more through; when they fail or slow down it pulls back. The
//! limit is bounded by a floor and a ceiling, and **never exceeds the ceiling**,
//! so the adaptation can only ever be more conservative than your hard cap.
//!
//! Two strategies ship: [`Aimd`] (additive increase, multiplicative decrease) and
//! [`Vegas`] (latency-based, after TCP Vegas), and you can supply your own via
//! [`AdaptiveStrategy`]. Outcomes are fed back through a [`AdaptivePermit`] — settle it
//! with [`success`](AdaptivePermit::success) or [`failure`](AdaptivePermit::failure), or let it
//! drop (counted as a failure).
//!
//! Behind the `adaptive` feature. Unlike the rate limiters, the waiting
//! [`acquire`](AdaptiveLimiter::acquire) blocks on a *slot* freeing, not on a
//! timer, so its clock is only used to measure round-trip time.

use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use core::time::Duration;

use clock_lib::{Clock, Monotonic, SystemClock};
use tokio::sync::Notify;

/// The observed result of one completed request, fed back to the strategy.
///
/// `#[non_exhaustive]`: more signals may be added.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The request succeeded, with this round-trip time. Latency-based strategies
    /// use the timing; count-based ones treat it as a plain success.
    Success {
        /// Measured round-trip time of the request.
        rtt: Duration,
    },
    /// The request failed (an error, a timeout, or a downstream rejection) — a
    /// signal to back off.
    Failure,
}

/// How an [`AdaptiveLimiter`] moves its concurrency limit in response to an
/// [`Outcome`].
///
/// The limiter clamps the returned value to `[floor, ceiling]`, so a strategy
/// need not enforce the bounds itself.
pub trait AdaptiveStrategy: Send + Sync {
    /// Returns the proposed new limit given the `current` limit, the `in_flight`
    /// count at the time of the observation (including the just-finished request),
    /// and the `outcome`.
    fn adjust(&self, current: u32, in_flight: u32, outcome: Outcome) -> u32;
}

/// Additive-increase, multiplicative-decrease.
///
/// On success — but only while the limit is actually being used — the limit grows
/// by a fixed step; on failure it is cut by a multiplier. The classic congestion
/// response: probe upward gently, retreat sharply.
///
/// # Examples
///
/// ```
/// use throttle_net::{Aimd, AdaptiveLimiter};
///
/// // Grow by 1 on a saturated success, halve on failure; limit in [4, 200].
/// let limiter = AdaptiveLimiter::builder()
///     .floor(4)
///     .ceiling(200)
///     .build(Aimd::new(1, 0.5));
/// # let _ = limiter;
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Aimd {
    increase: u32,
    decrease: f64,
}

impl Aimd {
    /// Increase the limit by `increase` per saturated success; multiply by
    /// `decrease` (in `(0.0, 1.0]`) on failure.
    #[must_use]
    pub fn new(increase: u32, decrease: f64) -> Self {
        Self {
            increase: increase.max(1),
            decrease: decrease.clamp(0.0, 1.0),
        }
    }
}

impl Default for Aimd {
    /// Increase by 1, halve on failure.
    fn default() -> Self {
        Self::new(1, 0.5)
    }
}

impl AdaptiveStrategy for Aimd {
    fn adjust(&self, current: u32, in_flight: u32, outcome: Outcome) -> u32 {
        match outcome {
            // Only grow when the limit is saturated; inflating an idle limit
            // would let a later burst overwhelm the downstream.
            Outcome::Success { .. } if in_flight >= current => {
                current.saturating_add(self.increase)
            }
            Outcome::Success { .. } => current,
            Outcome::Failure => {
                let cut = (f64::from(current) * self.decrease) as u32;
                cut.max(1)
            }
        }
    }
}

/// Latency-based adaptation, after TCP Vegas.
///
/// From the round-trip time it estimates the queue depth at the downstream —
/// `limit * (rtt - min_rtt) / rtt`, where `min_rtt` is the best (no-load) latency
/// seen so far. A small estimated queue means there is headroom, so the limit
/// grows; a large one means a queue is forming, so it shrinks. Failures halve the
/// limit outright.
///
/// # Examples
///
/// ```
/// use throttle_net::{AdaptiveLimiter, Vegas};
///
/// // Grow while the estimated queue is below 3, shrink above 6.
/// let limiter = AdaptiveLimiter::builder()
///     .floor(2)
///     .ceiling(100)
///     .build(Vegas::new(3, 6));
/// # let _ = limiter;
/// ```
#[derive(Debug)]
pub struct Vegas {
    alpha: u32,
    beta: u32,
    /// Best round-trip time seen, in nanoseconds; the no-load latency estimate.
    min_rtt_ns: AtomicU64,
}

impl Vegas {
    /// Grow while the estimated queue is below `alpha`, shrink above `beta`.
    /// `beta` is raised to at least `alpha` to keep a stable band between them.
    #[must_use]
    pub fn new(alpha: u32, beta: u32) -> Self {
        Self {
            alpha,
            beta: beta.max(alpha),
            min_rtt_ns: AtomicU64::new(u64::MAX),
        }
    }
}

impl Default for Vegas {
    /// Grow below an estimated queue of 3, shrink above 6.
    fn default() -> Self {
        Self::new(3, 6)
    }
}

impl AdaptiveStrategy for Vegas {
    fn adjust(&self, current: u32, _in_flight: u32, outcome: Outcome) -> u32 {
        let rtt = match outcome {
            Outcome::Failure => return (current / 2).max(1),
            Outcome::Success { rtt } => rtt,
        };
        let rtt_ns = u64::try_from(rtt.as_nanos()).unwrap_or(u64::MAX).max(1);
        // Track the best (no-load) latency seen.
        let min_ns = self
            .min_rtt_ns
            .fetch_min(rtt_ns, Ordering::AcqRel)
            .min(rtt_ns);

        // Estimated queue depth = current * (rtt - min_rtt) / rtt.
        let queue = u64::from(current).saturating_mul(rtt_ns.saturating_sub(min_ns)) / rtt_ns;
        if queue < u64::from(self.alpha) {
            current.saturating_add(1)
        } else if queue > u64::from(self.beta) {
            current.saturating_sub(1)
        } else {
            current
        }
    }
}

/// A concurrency limiter whose in-flight limit adapts to observed outcomes.
///
/// Build one with [`AdaptiveLimiter::builder`]. Behind the `adaptive` feature.
///
/// # Examples
///
/// ```
/// # async fn run() {
/// use throttle_net::{Aimd, AdaptiveLimiter};
///
/// let limiter = AdaptiveLimiter::builder()
///     .floor(2)
///     .ceiling(50)
///     .initial(10)
///     .build(Aimd::default());
///
/// if let Some(permit) = limiter.try_acquire() {
///     // ... call the downstream, then report how it went ...
///     let ok = true;
///     if ok { permit.success() } else { permit.failure() }
/// }
/// # }
/// ```
pub struct AdaptiveLimiter<S, C = SystemClock>
where
    C: Clock,
{
    strategy: S,
    limit: AtomicU32,
    in_flight: AtomicU32,
    floor: u32,
    ceiling: u32,
    notify: Notify,
    clock: C,
}

impl AdaptiveLimiter<core::convert::Infallible> {
    /// Starts building an adaptive limiter.
    #[must_use]
    pub fn builder() -> AdaptiveLimiterBuilder {
        AdaptiveLimiterBuilder::new()
    }
}

impl<S, C> AdaptiveLimiter<S, C>
where
    S: AdaptiveStrategy,
    C: Clock + Clone,
{
    fn new(strategy: S, floor: u32, ceiling: u32, initial: u32, clock: C) -> Self {
        let floor = floor.max(1);
        let ceiling = ceiling.max(floor);
        Self {
            strategy,
            limit: AtomicU32::new(initial.clamp(floor, ceiling)),
            in_flight: AtomicU32::new(0),
            floor,
            ceiling,
            notify: Notify::new(),
            clock,
        }
    }

    /// Replaces the time source (used to measure round-trip time), for
    /// deterministic tests. Resets the limiter.
    #[must_use]
    pub fn with_clock<C2>(self, clock: C2) -> AdaptiveLimiter<S, C2>
    where
        C2: Clock + Clone,
    {
        AdaptiveLimiter::new(
            self.strategy,
            self.floor,
            self.ceiling,
            self.limit.load(Ordering::Acquire),
            clock,
        )
    }

    /// The current concurrency limit.
    #[must_use]
    pub fn current_limit(&self) -> u32 {
        self.limit.load(Ordering::Acquire)
    }

    /// The number of requests currently in flight.
    #[must_use]
    pub fn in_flight(&self) -> u32 {
        self.in_flight.load(Ordering::Acquire)
    }

    /// The hard ceiling the adapting limit can never exceed.
    #[must_use]
    pub fn ceiling(&self) -> u32 {
        self.ceiling
    }

    /// Attempts to reserve a slot without waiting.
    fn try_reserve(&self) -> bool {
        loop {
            let in_flight = self.in_flight.load(Ordering::Acquire);
            if in_flight >= self.limit.load(Ordering::Acquire) {
                return false;
            }
            if self
                .in_flight
                .compare_exchange_weak(
                    in_flight,
                    in_flight + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Attempts to admit a request without waiting, returning a [`AdaptivePermit`] when a
    /// slot is free.
    #[must_use]
    pub fn try_acquire(&self) -> Option<AdaptivePermit<'_, S, C>> {
        self.try_reserve().then(|| AdaptivePermit::new(self))
    }

    /// Applies an [`Outcome`] to the limit and releases the slot.
    fn settle(&self, outcome: Outcome) {
        // `in_flight` still counts this request, so the strategy sees whether the
        // limit was saturated.
        let in_flight = self.in_flight.load(Ordering::Acquire);
        let current = self.limit.load(Ordering::Acquire);
        let proposed = self.strategy.adjust(current, in_flight, outcome);
        self.limit
            .store(proposed.clamp(self.floor, self.ceiling), Ordering::Release);
        let _ = self.in_flight.fetch_sub(1, Ordering::AcqRel);
        // A slot freed (and the limit may have grown): wake a waiter.
        self.notify.notify_waiters();
    }

    /// Round-trip time since `started`, per this limiter's clock.
    fn rtt_since(&self, started: Monotonic) -> Duration {
        self.clock.now().saturating_duration_since(started)
    }

    #[inline]
    fn now(&self) -> Monotonic {
        self.clock.now()
    }
}

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
impl<S, C> AdaptiveLimiter<S, C>
where
    S: AdaptiveStrategy,
    C: Clock + Clone,
{
    /// Admits a request, waiting until a slot is free.
    ///
    /// Unlike the rate limiters, this waits on a slot being released (or the
    /// limit growing), not on a timer. Returns a [`AdaptivePermit`] to settle with the
    /// request's outcome.
    pub async fn acquire(&self) -> AdaptivePermit<'_, S, C> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            let _ = notified.as_mut().enable();
            if self.try_reserve() {
                return AdaptivePermit::new(self);
            }
            notified.await;
        }
    }
}

/// A reserved concurrency slot. Settle it with [`success`](Self::success) or
/// [`failure`](Self::failure) after the request completes; dropping it unsettled
/// records a **failure** and frees the slot.
#[must_use = "settle the permit with `.success()` or `.failure()`; dropping it counts as a failure"]
pub struct AdaptivePermit<'a, S, C>
where
    S: AdaptiveStrategy,
    C: Clock + Clone,
{
    limiter: &'a AdaptiveLimiter<S, C>,
    started: Monotonic,
    settled: bool,
}

impl<'a, S, C> AdaptivePermit<'a, S, C>
where
    S: AdaptiveStrategy,
    C: Clock + Clone,
{
    fn new(limiter: &'a AdaptiveLimiter<S, C>) -> Self {
        Self {
            started: limiter.now(),
            limiter,
            settled: false,
        }
    }

    /// Records a successful request; its round-trip time is measured from when the
    /// permit was acquired.
    pub fn success(mut self) {
        let rtt = self.limiter.rtt_since(self.started);
        self.limiter.settle(Outcome::Success { rtt });
        self.settled = true;
    }

    /// Records a failed request.
    pub fn failure(mut self) {
        self.limiter.settle(Outcome::Failure);
        self.settled = true;
    }
}

impl<S, C> Drop for AdaptivePermit<'_, S, C>
where
    S: AdaptiveStrategy,
    C: Clock + Clone,
{
    fn drop(&mut self) {
        if !self.settled {
            self.limiter.settle(Outcome::Failure);
        }
    }
}

/// Builder for an [`AdaptiveLimiter`].
#[derive(Debug, Clone, Copy)]
pub struct AdaptiveLimiterBuilder {
    floor: u32,
    ceiling: u32,
    initial: Option<u32>,
}

impl Default for AdaptiveLimiterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl AdaptiveLimiterBuilder {
    /// Creates a builder with a floor of 1 and a ceiling of 100.
    #[must_use]
    pub fn new() -> Self {
        Self {
            floor: 1,
            ceiling: 100,
            initial: None,
        }
    }

    /// Sets the minimum concurrency limit (clamped to at least 1).
    #[must_use]
    pub fn floor(mut self, floor: u32) -> Self {
        self.floor = floor.max(1);
        self
    }

    /// Sets the maximum concurrency limit — the hard ceiling the adapting limit
    /// never exceeds.
    #[must_use]
    pub fn ceiling(mut self, ceiling: u32) -> Self {
        self.ceiling = ceiling;
        self
    }

    /// Sets the starting limit. Defaults to the floor (probe up from cautious).
    #[must_use]
    pub fn initial(mut self, initial: u32) -> Self {
        self.initial = Some(initial);
        self
    }

    /// Builds the limiter with the given adaptation `strategy`, driven by the
    /// system clock.
    #[must_use]
    pub fn build<S>(self, strategy: S) -> AdaptiveLimiter<S, SystemClock>
    where
        S: AdaptiveStrategy,
    {
        let initial = self.initial.unwrap_or(self.floor);
        AdaptiveLimiter::new(
            strategy,
            self.floor,
            self.ceiling,
            initial,
            SystemClock::new(),
        )
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::{AdaptiveLimiter, AdaptiveStrategy, Aimd, Outcome, Vegas};
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_adaptive_is_send_sync() {
        assert_send_sync::<AdaptiveLimiter<Aimd>>();
        assert_send_sync::<AdaptiveLimiter<Vegas>>();
    }

    #[test]
    fn test_aimd_adjust_rules() {
        let aimd = Aimd::new(2, 0.5);
        // Saturated success grows by the increase.
        assert_eq!(
            aimd.adjust(
                10,
                10,
                Outcome::Success {
                    rtt: Duration::ZERO
                }
            ),
            12
        );
        // Unsaturated success holds.
        assert_eq!(
            aimd.adjust(
                10,
                3,
                Outcome::Success {
                    rtt: Duration::ZERO
                }
            ),
            10
        );
        // Failure halves.
        assert_eq!(aimd.adjust(10, 10, Outcome::Failure), 5);
    }

    #[test]
    fn test_degradation_drives_limit_to_floor() {
        let limiter = AdaptiveLimiter::builder()
            .floor(4)
            .ceiling(100)
            .initial(64)
            .build(Aimd::new(4, 0.5));

        // A run of failures collapses the limit to the floor, never below.
        for _ in 0..10 {
            let permit = limiter.try_acquire().expect("a slot under the limit");
            permit.failure();
        }
        assert_eq!(limiter.current_limit(), 4);
    }

    #[test]
    fn test_recovery_drives_limit_up_bounded_by_ceiling() {
        let limiter = AdaptiveLimiter::builder()
            .floor(1)
            .ceiling(8)
            .initial(1)
            .build(Aimd::new(1, 0.5));

        // Saturated successes grow the limit one step at a time up to the ceiling,
        // and never past it. Hold `permit`s so the limit stays saturated.
        for _ in 0..50 {
            let mut held = Vec::new();
            while let Some(p) = limiter.try_acquire() {
                held.push(p);
            }
            // Settle one as a saturated success to nudge the limit up.
            if let Some(p) = held.pop() {
                p.success();
            }
            for p in held {
                p.success();
            }
        }
        assert_eq!(limiter.current_limit(), 8, "grows to the ceiling");
        // Many more successes can never push it past the ceiling.
        for _ in 0..20 {
            let p = limiter.try_acquire().expect("slot");
            p.success();
        }
        assert_eq!(limiter.current_limit(), 8, "never exceeds the ceiling");
    }

    #[test]
    fn test_never_admits_more_than_the_limit() {
        let limiter = AdaptiveLimiter::builder()
            .floor(3)
            .ceiling(3)
            .initial(3)
            .build(Aimd::default());

        let p1 = limiter.try_acquire().expect("1");
        let p2 = limiter.try_acquire().expect("2");
        let p3 = limiter.try_acquire().expect("3");
        assert_eq!(limiter.in_flight(), 3);
        // The limit (and ceiling) is 3, so a fourth is refused.
        assert!(limiter.try_acquire().is_none());
        drop((p1, p2, p3));
    }

    #[test]
    fn test_dropping_permit_counts_as_failure() {
        let limiter = AdaptiveLimiter::builder()
            .floor(1)
            .ceiling(100)
            .initial(10)
            .build(Aimd::new(1, 0.5));
        drop(limiter.try_acquire().expect("slot")); // unsettled -> failure
        assert_eq!(limiter.current_limit(), 5);
        assert_eq!(limiter.in_flight(), 0, "the slot is released");
    }

    #[test]
    fn test_vegas_grows_on_low_latency_shrinks_on_high() {
        let clock = Arc::new(ManualClock::new());
        let limiter = AdaptiveLimiter::builder()
            .floor(1)
            .ceiling(100)
            .initial(20)
            .build(Vegas::new(3, 6))
            .with_clock(clock.clone());

        // First success establishes the min RTT at 10ms (queue estimate 0 -> grow).
        let p = limiter.try_acquire().expect("slot");
        clock.advance(Duration::from_millis(10));
        p.success();
        assert_eq!(limiter.current_limit(), 21);

        // A much slower request (200ms) implies a deep queue -> shrink.
        let p = limiter.try_acquire().expect("slot");
        clock.advance(Duration::from_millis(200));
        p.success();
        assert!(
            limiter.current_limit() < 21,
            "high latency shrinks the limit"
        );
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_async_acquire_waits_for_a_freed_slot() {
        let limiter = Arc::new(
            AdaptiveLimiter::builder()
                .floor(1)
                .ceiling(1)
                .initial(1)
                .build(Aimd::default()),
        );

        let held = limiter.try_acquire().expect("the one slot");
        assert!(limiter.try_acquire().is_none());

        let l = Arc::clone(&limiter);
        let waiter = tokio::spawn(async move { l.acquire().await.success() });
        // Give the waiter a moment to park, then free the slot.
        tokio::task::yield_now().await;
        held.success();
        waiter.await.unwrap();
    }
}
