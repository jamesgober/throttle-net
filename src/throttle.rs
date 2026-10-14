//! The core throttle: one token bucket with a waiting, cost-aware acquire.

use std::borrow::Cow;

use better_bucket::{Bucket, BucketConfig, Decision as BucketDecision};
use clock_lib::{Clock, Monotonic, SystemClock};
use core::time::Duration;

use crate::decision::Decision;
use crate::error::ThrottleError;
use crate::hold::{Hold, nanos_between};
use crate::limiter::Limiter;
use crate::obs::{self, Label};

/// The limiter kind reported in metrics and events.
const KIND: &str = "throttle";

/// A single outbound throttle backed by a token bucket.
///
/// Construct one with [`per_second`](Self::per_second),
/// [`per_minute`](Self::per_minute), [`per_duration`](Self::per_duration), or
/// [`builder`](Self::builder) (for a burst different from the rate, a partial
/// initial fill, or an injected clock), then pace outbound work with
/// [`acquire`](Self::acquire). Because throttle-net protects *downstreams*, the
/// headline operation **waits** for a token rather than rejecting the caller —
/// you are slowing your own requests, not dropping someone else's. When you
/// would rather not wait, [`try_acquire`](Self::try_acquire) reports the outcome
/// immediately.
///
/// The bucket refills smoothly; by default it starts full and its capacity
/// equals the per-period amount, so a burst up to the capacity is admitted at
/// once and the sustained rate is the refill rate. Accounting is lock-free (one
/// atomic compare-and-swap per grant, in
/// [`better-bucket`](https://crates.io/crates/better-bucket)) and holds under
/// any number of concurrent callers: over any window the units granted never
/// exceed the capacity plus what the rate accrues. Time comes from an
/// injectable [`Clock`] — [`SystemClock`] in production, a
/// [`ManualClock`](clock_lib::ManualClock) in tests.
///
/// Waiting acquires are **fair**: each one reserves its units at once and
/// sleeps until they have accrued, so later callers queue behind it and a large
/// request is never starved by a stream of small ones. They are **cancel-safe**:
/// dropping the future (a `select!` branch losing, a `tokio::time::timeout`
/// firing) returns any reserved units.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "runtime")]
/// # async fn run() -> Result<(), throttle_net::ThrottleError> {
/// use throttle_net::Throttle;
///
/// // 100 requests per second, bursting up to 100.
/// let throttle = Throttle::per_second(100);
/// throttle.acquire().await?; // returns as soon as a token is free
/// # Ok(())
/// # }
/// ```
pub struct Throttle<C: Clock = SystemClock> {
    bucket: Bucket<C>,
    /// The origin of [`hold`](Self::hold)'s nanosecond timeline.
    epoch: Monotonic,
    hold: Hold,
    label: Label,
}

impl Throttle<SystemClock> {
    /// Creates a throttle that admits `rate` units per second, bursting up to
    /// `rate`, driven by the OS monotonic clock.
    ///
    /// A `rate` of `0` yields a throttle that grants nothing; an
    /// [`acquire`](Self::acquire) on it returns
    /// [`ThrottleError::CostExceedsCapacity`]. Use [`builder`](Self::builder)
    /// and [`try_build`](ThrottleBuilder::try_build) to reject such a
    /// configuration as an error instead.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::per_second(50);
    /// assert_eq!(throttle.capacity(), 50);
    /// assert!(throttle.try_acquire());
    /// ```
    #[must_use]
    pub fn per_second(rate: u32) -> Self {
        Self::from_bucket(Bucket::per_second(rate), Label::none())
    }

    /// Creates a throttle that admits `rate` units per minute, bursting up to
    /// `rate`, driven by the OS monotonic clock.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Throttle;
    ///
    /// // 600 requests per minute: one every 100ms on average, burst of 600.
    /// let throttle = Throttle::per_minute(600);
    /// assert_eq!(throttle.capacity(), 600);
    /// ```
    #[must_use]
    pub fn per_minute(rate: u32) -> Self {
        Self::per_duration(rate, Duration::from_secs(60))
    }

    /// Creates a throttle that admits `amount` units every `period`, bursting up
    /// to `amount`, driven by the OS monotonic clock.
    ///
    /// Use this when the natural window is neither a second nor a minute — five
    /// per hundred milliseconds, or ten thousand per day. An `amount` of `0` or
    /// a zero `period` yields a throttle that grants nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Throttle;
    ///
    /// // 5 requests per 100 milliseconds.
    /// let throttle = Throttle::per_duration(5, Duration::from_millis(100));
    /// assert_eq!(throttle.capacity(), 5);
    /// ```
    #[must_use]
    pub fn per_duration(amount: u32, period: Duration) -> Self {
        Self::from_bucket(Bucket::per_duration(amount, period), Label::none())
    }

    /// Creates a throttle that paces `amount` units per `period` **without
    /// bursts**: a capacity of one, so grants are spaced `period / amount`
    /// apart — the generic cell rate algorithm (GCRA) / leaky-bucket-as-meter
    /// behaviour, as `governor` provides by default.
    ///
    /// With a capacity of one, every request must cost `1`. For cost-aware
    /// smooth pacing, use [`builder`](Self::builder) with a
    /// [`burst`](ThrottleBuilder::burst) equal to the largest single cost.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Throttle;
    ///
    /// // 10 per second, one every 100ms, never two at once.
    /// let throttle = Throttle::smooth(10, Duration::from_secs(1));
    /// assert_eq!(throttle.capacity(), 1);
    /// assert!(throttle.try_acquire());
    /// assert!(!throttle.try_acquire());
    /// ```
    #[must_use]
    pub fn smooth(amount: u32, period: Duration) -> Self {
        Self::builder().rate(amount, period).burst(1).build()
    }

    /// Starts building a throttle with full control over the rate, the burst,
    /// the initial fill, the clock, and the instance name.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Throttle;
    ///
    /// // 10 per second sustained, bursts up to 50, starting with 5 available.
    /// let throttle = Throttle::builder()
    ///     .per_second(10)
    ///     .burst(50)
    ///     .initial(5)
    ///     .name("search-api")
    ///     .try_build()?;
    /// assert_eq!(throttle.capacity(), 50);
    /// assert_eq!(throttle.available(), 5);
    /// # Ok::<(), throttle_net::ThrottleError>(())
    /// ```
    pub fn builder() -> ThrottleBuilder {
        ThrottleBuilder::new()
    }
}

impl<C: Clock> Throttle<C> {
    fn from_bucket(bucket: Bucket<C>, label: Label) -> Self {
        let epoch = bucket.clock().now();
        Self {
            bucket,
            epoch,
            hold: Hold::new(),
            label,
        }
    }

    /// Replaces the time source, returning a throttle driven by `clock`.
    ///
    /// The common use is deterministic testing: inject a
    /// [`ManualClock`](clock_lib::ManualClock) (shared via an
    /// [`Arc`](std::sync::Arc)) and drive refills by advancing it, with no real
    /// sleeping. The bucket is rebuilt around the new clock with its initial
    /// fill; the name is kept and any pause is dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use throttle_net::{ManualClock, Throttle};
    ///
    /// let clock = Arc::new(ManualClock::new());
    /// let throttle = Throttle::per_second(2).with_clock(clock.clone());
    ///
    /// assert!(throttle.try_acquire());
    /// assert!(throttle.try_acquire());
    /// assert!(!throttle.try_acquire()); // drained
    ///
    /// clock.advance(Duration::from_secs(1)); // a full period refills it
    /// assert!(throttle.try_acquire());
    /// ```
    #[must_use]
    pub fn with_clock<C2: Clock>(self, clock: C2) -> Throttle<C2> {
        Throttle::from_bucket(self.bucket.with_clock(clock), self.label)
    }

    /// Names this instance. The name labels every metric and event it emits
    /// (with the `metrics` / `tracing` features), so two throttles do not share
    /// one series.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::per_second(10).with_name("github");
    /// assert_eq!(throttle.name(), Some("github"));
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

    /// The maximum number of tokens the throttle can hold (its burst size).
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.bucket.capacity()
    }

    /// The number of whole tokens available right now (`0` while paused).
    ///
    /// A point-in-time read for observability and tests, not a reservation.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::per_second(5);
    /// assert!(throttle.try_acquire_with_cost(2));
    /// assert_eq!(throttle.available(), 3);
    /// ```
    #[inline]
    #[must_use]
    pub fn available(&self) -> u32 {
        let now = self.now();
        if !self.held(now).is_zero() {
            return 0;
        }
        self.bucket.available_at(now)
    }

    /// Attempts to take one token without waiting, returning whether it was
    /// granted.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::per_second(1);
    /// assert!(throttle.try_acquire());  // the one token
    /// assert!(!throttle.try_acquire()); // none left this instant
    /// ```
    #[inline]
    #[must_use]
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_with_cost(1)
    }

    /// Attempts to take `cost` tokens without waiting, returning whether they
    /// were granted.
    ///
    /// Granting is all-or-nothing: either every token is deducted or none is.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::per_second(10);
    /// assert!(throttle.try_acquire_with_cost(7));
    /// assert!(!throttle.try_acquire_with_cost(7)); // only 3 left
    /// ```
    #[inline]
    #[must_use]
    pub fn try_acquire_with_cost(&self, cost: u32) -> bool {
        let granted = self.decide(cost).is_acquired();
        obs::attempt(KIND, &self.label, cost, granted);
        granted
    }

    /// Reports whether `cost` tokens would be granted now, without taking them.
    ///
    /// The non-consuming counterpart to
    /// [`try_acquire_with_cost`](Self::try_acquire_with_cost), used by composite
    /// limiters to poll a constituent before committing. It uses the same
    /// arithmetic as a grant, so [`Decision::Acquired`] here means a grant would
    /// succeed this instant, [`Decision::Retry`] carries the exact wait (rounded
    /// up to the millisecond, and at least the remaining pause), and
    /// [`Decision::Impossible`] means no wait would ever be enough.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Decision, Throttle};
    ///
    /// let throttle = Throttle::per_second(4);
    /// assert_eq!(throttle.peek(3), Decision::Acquired); // would grant, took nothing
    /// assert!(throttle.try_acquire_with_cost(4));        // still full
    /// assert_eq!(throttle.peek(5), Decision::Impossible); // over capacity
    /// ```
    #[inline]
    #[must_use]
    pub fn peek(&self, cost: u32) -> Decision {
        if cost > self.capacity() {
            return Decision::Impossible;
        }
        let now = self.now();
        let held = self.held(now);
        match self.bucket.time_until_at(cost, now) {
            None => Decision::Impossible,
            Some(wait) => {
                let after = wait.max(held);
                if after.is_zero() {
                    Decision::Acquired
                } else {
                    Decision::Retry { after }
                }
            }
        }
    }

    /// Takes `cost` tokens now, even if they have not accrued yet, provided they
    /// will have accrued within `max_wait`, and returns how long to wait before
    /// using them (`Some(Duration::ZERO)` if they were already there). Returns
    /// `None`, taking nothing, if the wait would exceed `max_wait`, the cost
    /// exceeds the capacity, or the throttle is paused.
    ///
    /// A reservation queues later callers behind it, which is what keeps waiting
    /// fair. If the work is abandoned before the wait ends, give the tokens back
    /// with [`refund`](Self::refund). The waiting [`acquire`](Self::acquire)
    /// does exactly this for you.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use throttle_net::{ManualClock, Throttle};
    ///
    /// let clock = Arc::new(ManualClock::new());
    /// let throttle = Throttle::per_second(10).with_clock(clock.clone());
    /// assert!(throttle.try_acquire_with_cost(10)); // empty
    ///
    /// // Five tokens accrue in half a second: reserve them now.
    /// let wait = throttle.reserve(5, Duration::from_secs(1));
    /// assert_eq!(wait, Some(Duration::from_millis(500)));
    /// // The next caller queues behind that reservation.
    /// assert_eq!(throttle.reserve(5, Duration::from_secs(1)), Some(Duration::from_secs(1)));
    /// // And a reservation over the limit takes nothing.
    /// assert_eq!(throttle.reserve(10, Duration::from_secs(1)), None);
    /// ```
    #[must_use = "the returned wait must be honoured before the tokens are used"]
    pub fn reserve(&self, cost: u32, max_wait: Duration) -> Option<Duration> {
        let now = self.now();
        if !self.held(now).is_zero() {
            return None;
        }
        self.bucket.reserve_at(cost, max_wait, now)
    }

    /// Returns `cost` previously granted or reserved tokens to the throttle, for
    /// work that was cancelled or failed before it used them. The balance never
    /// exceeds the capacity, and a refund also pays down an outstanding
    /// reservation.
    ///
    /// Only refund what was taken: refunding tokens that were never granted lets
    /// the throttle exceed its configured rate.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::per_second(10);
    /// assert!(throttle.try_acquire_with_cost(10));
    /// throttle.refund(4); // the request for 4 of them never went out
    /// assert!(throttle.available() >= 4);
    /// ```
    pub fn refund(&self, cost: u32) {
        self.bucket.refund(cost);
    }

    /// Takes tokens until at most `remaining` are available, and returns how
    /// many were taken. Never adds tokens.
    ///
    /// This reconciles the local view with a downstream that reports fewer
    /// units left than the throttle believes (see
    /// [`provider::RateLimitInfo`](crate::provider::RateLimitInfo) with the
    /// `provider-headers` feature).
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::per_second(100);
    /// assert_eq!(throttle.drain_to(10), 90);
    /// assert_eq!(throttle.available(), 10);
    /// assert_eq!(throttle.drain_to(50), 0); // never adds
    /// ```
    pub fn drain_to(&self, remaining: u32) -> u32 {
        let now = self.now();
        let available = self.bucket.available_at(now);
        if available <= remaining {
            return 0;
        }
        self.bucket.acquire_up_to_at(available - remaining, now)
    }

    /// Grants nothing for `duration` from now. Tokens keep accruing (up to the
    /// capacity) while paused; when the pause ends the throttle resumes with
    /// whatever has accrued. A shorter pause never shortens a longer one already
    /// in place; [`resume`](Self::resume) lifts it early. `Duration::MAX`
    /// pauses until resumed.
    ///
    /// Use it when a downstream says its budget is exhausted until a reset, or
    /// answers `429` with a `Retry-After`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use throttle_net::{ManualClock, Throttle};
    ///
    /// let clock = Arc::new(ManualClock::new());
    /// let throttle = Throttle::per_second(10).with_clock(clock.clone());
    ///
    /// throttle.pause_for(Duration::from_secs(30)); // server: "come back in 30s"
    /// assert!(!throttle.try_acquire());
    /// assert_eq!(throttle.paused_for(), Duration::from_secs(30));
    ///
    /// clock.advance(Duration::from_secs(30));
    /// assert!(throttle.try_acquire());
    /// ```
    pub fn pause_for(&self, duration: Duration) {
        let now = self.now();
        self.hold.extend(nanos_between(self.epoch, now), duration);
    }

    /// Lifts any pause immediately, waking waiters that were sleeping it out.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::per_second(10);
    /// throttle.pause_for(Duration::MAX);
    /// assert!(!throttle.try_acquire());
    /// throttle.resume();
    /// assert!(throttle.try_acquire());
    /// ```
    pub fn resume(&self) {
        self.hold.clear();
    }

    /// How much longer the throttle stays paused (`Duration::ZERO` when it is
    /// not paused).
    #[must_use]
    pub fn paused_for(&self) -> Duration {
        self.held(self.now())
    }

    /// The current reading of the throttle's clock.
    #[inline]
    fn now(&self) -> Monotonic {
        self.bucket.clock().now()
    }

    /// The remaining pause at `now`, skipping the arithmetic when no pause was
    /// ever set.
    #[inline]
    fn held(&self, now: Monotonic) -> Duration {
        if self.hold.is_set() {
            self.hold.remaining(nanos_between(self.epoch, now))
        } else {
            Duration::ZERO
        }
    }

    /// The synchronous, consuming core shared by the trait impl and the public
    /// non-waiting methods. Deducts `cost` on success; emits nothing.
    #[inline]
    fn decide(&self, cost: u32) -> Decision {
        if cost > self.capacity() {
            return Decision::Impossible;
        }
        let now = self.now();
        let held = self.held(now);
        if !held.is_zero() {
            return Decision::Retry { after: held };
        }
        match self.bucket.acquire_at(cost, now) {
            BucketDecision::Allowed => Decision::Acquired,
            BucketDecision::Denied { retry_after } if retry_after == Duration::MAX => {
                Decision::Impossible
            }
            BucketDecision::Denied { retry_after } => Decision::Retry { after: retry_after },
            // `better_bucket::Decision` is `#[non_exhaustive]`. An outcome this
            // version does not understand is treated as un-grantable rather
            // than risk over-sending to a downstream.
            _ => Decision::Impossible,
        }
    }

    /// One step of a waiting acquire: reserve, or report how long to wait.
    #[cfg(feature = "runtime")]
    fn step(&self, cost: u32, budget: Duration) -> crate::wait::Step {
        use crate::wait::Step;
        if cost > self.capacity() {
            return Step::Never;
        }
        let now = self.now();
        let held = self.held(now);
        if !held.is_zero() {
            // While paused, reserving would release a burst the moment the
            // pause ends; wait the pause out first instead.
            return Step::Retry(held);
        }
        match self.bucket.reserve_at(cost, budget, now) {
            Some(wait) => Step::Granted(wait),
            None => match self.bucket.time_until_at(cost, now) {
                None => Step::Never,
                Some(wait) => Step::Retry(wait),
            },
        }
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl<C: Clock> Throttle<C> {
    /// Takes one token, waiting until one is available.
    ///
    /// This is the marquee outbound operation: it paces the caller instead of
    /// rejecting it. See [`acquire_with_cost`](Self::acquire_with_cost) for the
    /// waiting and cancellation semantics.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] when the capacity is `0`,
    /// because a single token can never be granted.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::per_second(100);
    /// throttle.acquire().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire(&self) -> Result<(), ThrottleError> {
        self.acquire_with_cost(1).await
    }

    /// Takes `cost` tokens, waiting until they are available.
    ///
    /// The cost lets one request weigh more than another — a batch of ten, or an
    /// LLM call billed by token count. The waiter reserves its tokens at once
    /// and sleeps exactly until they have accrued, so waiters are served in
    /// arrival order and a large cost is never starved. While the throttle is
    /// [paused](Self::pause_for), waiters sleep out the pause — including
    /// waiters that had already reserved when the pause began (they keep their
    /// reservation and go when it ends) — and [`resume`](Self::resume) wakes
    /// them at once.
    ///
    /// **Cancel-safe:** dropping the future before it completes returns the
    /// reserved tokens, so wrapping it in `tokio::time::timeout` or a `select!`
    /// never leaks budget.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] when `cost` exceeds the
    /// throttle's capacity; that request can never be granted, so it fails fast
    /// rather than waiting forever.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::per_second(1000);
    /// throttle.acquire_with_cost(250).await?; // a heavier request
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire_with_cost(&self, cost: u32) -> Result<(), ThrottleError> {
        self.wait(cost, None).await
    }

    /// Takes one token, waiting at most `timeout`.
    ///
    /// See [`acquire_with_cost_timeout`](Self::acquire_with_cost_timeout).
    ///
    /// # Errors
    ///
    /// [`ThrottleError::DeadlineExceeded`] if no token can be had within
    /// `timeout`; [`ThrottleError::CostExceedsCapacity`] if the capacity is `0`.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use std::time::Duration;
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::per_second(100);
    /// throttle.acquire_timeout(Duration::from_millis(250)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire_timeout(&self, timeout: Duration) -> Result<(), ThrottleError> {
        self.acquire_with_cost_timeout(1, timeout).await
    }

    /// Takes `cost` tokens, waiting at most `timeout`.
    ///
    /// Because waits are reservations with a known end, the throttle knows up
    /// front whether the tokens can be had in time: if not, it returns
    /// [`ThrottleError::DeadlineExceeded`] **immediately**, reserving nothing,
    /// instead of sleeping until the deadline only to fail. Cancel-safe, as
    /// [`acquire_with_cost`](Self::acquire_with_cost).
    ///
    /// # Errors
    ///
    /// [`ThrottleError::DeadlineExceeded`] if the tokens cannot be had within
    /// `timeout`; [`ThrottleError::CostExceedsCapacity`] if `cost` exceeds the
    /// capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// use std::time::Duration;
    /// use throttle_net::{Throttle, ThrottleError};
    ///
    /// // 1 per hour, already spent: no token within a second.
    /// let throttle = Throttle::per_duration(1, Duration::from_secs(3600));
    /// assert!(throttle.try_acquire());
    /// let err = throttle
    ///     .acquire_with_cost_timeout(1, Duration::from_secs(1))
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

    /// The waiting core: the shared engine plus this throttle's errors and
    /// observability.
    async fn wait(&self, cost: u32, timeout: Option<Duration>) -> Result<(), ThrottleError> {
        let timer = obs::Timer::start();
        let result = crate::wait::wait_for(
            |budget| self.step(cost, budget),
            || self.bucket.refund(cost),
            || self.paused_for(),
            timeout,
        )
        .await
        .map_err(|refusal| match refusal {
            crate::wait::Refusal::Never => ThrottleError::CostExceedsCapacity {
                cost,
                capacity: self.capacity(),
            },
            crate::wait::Refusal::TimedOut => ThrottleError::DeadlineExceeded,
        });
        obs::waited(KIND, &self.label, &timer, cost, obs::Waited::of(&result));
        result
    }
}

impl<C: Clock> Limiter for Throttle<C> {
    #[inline]
    fn peek(&self, cost: u32) -> Decision {
        Throttle::peek(self, cost)
    }

    #[inline]
    fn acquire_cost(&self, cost: u32) -> Decision {
        self.decide(cost)
    }

    #[inline]
    fn available(&self) -> u32 {
        Throttle::available(self)
    }

    #[inline]
    fn capacity(&self) -> u32 {
        self.bucket.capacity()
    }

    fn refund(&self, cost: u32) {
        Throttle::refund(self, cost);
    }

    fn reserve(&self, cost: u32, max_wait: Duration) -> Option<Duration> {
        Throttle::reserve(self, cost, max_wait)
    }

    fn pause_for(&self, duration: Duration) {
        Throttle::pause_for(self, duration);
    }

    fn paused_for(&self) -> Duration {
        Throttle::paused_for(self)
    }

    fn resume(&self) {
        Throttle::resume(self);
    }

    /// Records the units as debt, even while paused: they were spent
    /// downstream, so later callers wait behind them. Only a closed throttle
    /// (capacity `0`) leaves units uncharged.
    fn charge(&self, cost: u32) -> u32 {
        let capacity = self.capacity();
        if capacity == 0 {
            return cost;
        }
        let now = self.now();
        let mut left = cost;
        while left > 0 {
            let chunk = left.min(capacity);
            if self.bucket.reserve_at(chunk, Duration::MAX, now).is_none() {
                return left;
            }
            left -= chunk;
        }
        0
    }
}

impl<C: Clock> core::fmt::Debug for Throttle<C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let config = self.bucket.config();
        f.debug_struct("Throttle")
            .field("name", &self.label)
            .field("capacity", &config.capacity())
            .field("refill_amount", &config.refill_amount())
            .field("refill_period", &config.refill_period())
            .field("available", &self.available())
            .field("paused_for", &self.paused_for())
            .finish_non_exhaustive()
    }
}

/// Builder for a [`Throttle`]: the rate, an independent burst (capacity), the
/// initial fill, the clock, and the instance name.
///
/// [`build`](Self::build) never fails — an unusable configuration yields a
/// throttle that grants nothing, like [`Throttle::per_second(0)`](Throttle::per_second)
/// — while [`try_build`](Self::try_build) reports it as
/// [`ThrottleError::InvalidConfig`].
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
/// use throttle_net::{ManualClock, Throttle};
///
/// let clock = Arc::new(ManualClock::new());
/// let throttle = Throttle::builder()
///     .rate(1_000, Duration::from_secs(60)) // 1000 per minute sustained
///     .burst(100)                           // but never more than 100 at once
///     .initial(0)                           // start empty
///     .clock(clock.clone())
///     .try_build()?;
///
/// assert_eq!(throttle.available(), 0);
/// clock.advance(Duration::from_secs(6)); // 6s accrue 100 tokens
/// assert_eq!(throttle.available(), 100);
/// # Ok::<(), throttle_net::ThrottleError>(())
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct ThrottleBuilder<C = SystemClock> {
    amount: u32,
    period: Duration,
    burst: Option<u32>,
    initial: Option<u32>,
    clock: C,
    label: Label,
}

impl ThrottleBuilder<SystemClock> {
    /// A builder with no rate set (it must be set before building), the burst
    /// defaulting to the per-period amount, starting full, on the system clock.
    pub fn new() -> Self {
        Self {
            amount: 0,
            period: Duration::from_secs(1),
            burst: None,
            initial: None,
            clock: SystemClock::new(),
            label: Label::none(),
        }
    }
}

impl Default for ThrottleBuilder<SystemClock> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Clock> ThrottleBuilder<C> {
    /// Refills `amount` tokens every `period`. Unless [`burst`](Self::burst) is
    /// set, the capacity is also `amount`.
    ///
    /// Rates above about 4.3 × 10^9 tokens per second (only reachable with a
    /// sub-second period) are effectively unlimited, and better-bucket counts
    /// them in multi-token units: a grant is rounded up to a whole unit, and a
    /// bucket may hand out up to one unit more than its exact bound.
    pub fn rate(mut self, amount: u32, period: Duration) -> Self {
        self.amount = amount;
        self.period = period;
        self
    }

    /// Refills `amount` tokens per second.
    pub fn per_second(self, amount: u32) -> Self {
        self.rate(amount, Duration::from_secs(1))
    }

    /// Refills `amount` tokens per minute.
    pub fn per_minute(self, amount: u32) -> Self {
        self.rate(amount, Duration::from_secs(60))
    }

    /// Sets the capacity — the most tokens that can be taken at once —
    /// independently of the rate. A burst of `1` gives smooth, GCRA-style
    /// pacing (see [`Throttle::smooth`]).
    pub fn burst(mut self, burst: u32) -> Self {
        self.burst = Some(burst);
        self
    }

    /// Sets how many tokens the throttle starts with (clamped to the capacity).
    /// Defaults to full. `0` starts empty, so the first burst cannot hit a
    /// downstream that was just restarted.
    pub fn initial(mut self, initial: u32) -> Self {
        self.initial = Some(initial);
        self
    }

    /// Names the throttle for metrics and events (see [`Throttle::with_name`]).
    pub fn name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
        self
    }

    /// Drives the throttle with `clock` instead of the system clock.
    pub fn clock<C2: Clock>(self, clock: C2) -> ThrottleBuilder<C2> {
        ThrottleBuilder {
            amount: self.amount,
            period: self.period,
            burst: self.burst,
            initial: self.initial,
            clock,
            label: self.label,
        }
    }

    /// Validates the configuration into a bucket configuration.
    fn config(&self) -> Result<BucketConfig, ThrottleError> {
        let invalid = |reason| ThrottleError::InvalidConfig { reason };
        if self.amount == 0 {
            return Err(invalid(
                "rate amount must be non-zero; set it with rate/per_second/per_minute",
            ));
        }
        if self.period.is_zero() {
            return Err(invalid("rate period must be non-zero"));
        }
        let capacity = self.burst.unwrap_or(self.amount);
        if capacity == 0 {
            return Err(invalid("burst must be non-zero"));
        }
        let initial = self.initial.unwrap_or(capacity).min(capacity);
        BucketConfig::new(capacity, self.amount, self.period, initial)
            .map_err(|_| invalid("the token bucket rejected the configuration"))
    }

    /// Builds the throttle, or reports why the configuration cannot work.
    ///
    /// # Errors
    ///
    /// [`ThrottleError::InvalidConfig`] when the rate was never set or is zero,
    /// the period is zero, or the burst is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Throttle, ThrottleError};
    ///
    /// let err = Throttle::builder().per_second(10).burst(0).try_build().unwrap_err();
    /// assert!(matches!(err, ThrottleError::InvalidConfig { .. }));
    /// ```
    pub fn try_build(self) -> Result<Throttle<C>, ThrottleError> {
        let config = self.config()?;
        Ok(Throttle::from_bucket(
            Bucket::with_config_and_clock(config, self.clock),
            self.label,
        ))
    }

    /// Builds the throttle. An unusable configuration (see
    /// [`try_build`](Self::try_build)) yields a throttle that grants nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Throttle;
    ///
    /// let throttle = Throttle::builder().per_minute(60).burst(5).build();
    /// assert_eq!(throttle.capacity(), 5);
    ///
    /// let closed = Throttle::builder().build(); // no rate set
    /// assert_eq!(closed.capacity(), 0);
    /// assert!(!closed.try_acquire());
    /// ```
    #[must_use]
    pub fn build(self) -> Throttle<C> {
        let bucket = match self.config() {
            Ok(config) => Bucket::with_config_and_clock(config, self.clock),
            Err(_) => Bucket::per_second(0).with_clock(self.clock),
        };
        Throttle::from_bucket(bucket, self.label)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::Throttle;
    use crate::decision::Decision;
    use crate::error::ThrottleError;
    use crate::limiter::Limiter;
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    fn manual(rate: u32) -> (Arc<ManualClock>, Throttle<Arc<ManualClock>>) {
        let clock = Arc::new(ManualClock::new());
        let throttle = Throttle::per_second(rate).with_clock(Arc::clone(&clock));
        (clock, throttle)
    }

    #[test]
    fn test_public_types_are_send_sync() {
        assert_send_sync::<Throttle>();
        assert_send_sync::<Decision>();
        assert_send_sync::<ThrottleError>();
    }

    #[test]
    fn test_try_acquire_grants_up_to_capacity_then_refuses() {
        let throttle = Throttle::per_second(3);
        assert!(throttle.try_acquire());
        assert!(throttle.try_acquire());
        assert!(throttle.try_acquire());
        assert!(!throttle.try_acquire());
    }

    #[test]
    fn test_try_acquire_with_cost_is_all_or_nothing() {
        let throttle = Throttle::per_second(10);
        assert!(throttle.try_acquire_with_cost(7));
        assert!(!throttle.try_acquire_with_cost(7));
        assert!(throttle.try_acquire_with_cost(3));
    }

    #[test]
    fn test_refill_after_a_full_period_under_manual_clock() {
        let (clock, throttle) = manual(4);
        for _ in 0..4 {
            assert!(throttle.try_acquire());
        }
        assert!(!throttle.try_acquire());
        clock.advance(Duration::from_secs(1));
        assert!(throttle.try_acquire());
    }

    #[test]
    fn test_acquire_cost_reports_retry_then_impossible() {
        let throttle = Throttle::per_second(2);
        assert_eq!(throttle.acquire_cost(2), Decision::Acquired);
        assert!(matches!(throttle.acquire_cost(1), Decision::Retry { .. }));
        assert_eq!(throttle.acquire_cost(3), Decision::Impossible);
    }

    #[test]
    fn test_peek_agrees_with_acquire_exactly() {
        let (clock, throttle) = manual(10);
        assert!(throttle.try_acquire_with_cost(10));
        assert_eq!(
            throttle.peek(5),
            Decision::Retry {
                after: Duration::from_millis(500)
            }
        );
        clock.advance(Duration::from_millis(499));
        assert!(!throttle.peek(5).is_acquired());
        assert!(!throttle.try_acquire_with_cost(5));
        clock.advance(Duration::from_millis(1));
        assert!(throttle.peek(5).is_acquired());
        assert!(throttle.try_acquire_with_cost(5));
    }

    #[test]
    fn test_large_capacity_is_not_clamped() {
        // Regression (TN-010): better-bucket 1.0 clamped capacities to
        // 4,294,967 tokens; a 10M-TPM budget refused a 5M-token call.
        let throttle = Throttle::per_duration(10_000_000, Duration::from_secs(60))
            .with_clock(Arc::new(ManualClock::new()));
        assert_eq!(throttle.capacity(), 10_000_000);
        assert!(throttle.try_acquire_with_cost(5_000_000));
        assert_eq!(throttle.available(), 5_000_000);
    }

    #[test]
    fn test_very_slow_rate_peek_and_acquire_agree() {
        // Regression (TN-020): 1 token per 60 days refilled never in 1.0, and
        // peek said Retry while acquire said Impossible.
        let clock = Arc::new(ManualClock::new());
        let period = Duration::from_secs(60 * 86_400);
        let throttle = Throttle::per_duration(1, period).with_clock(Arc::clone(&clock));
        assert!(throttle.try_acquire());
        assert_eq!(throttle.peek(1), throttle.acquire_cost(1));
        assert!(matches!(throttle.peek(1), Decision::Retry { .. }));
        clock.advance(period);
        assert!(throttle.try_acquire(), "refills after one period");
    }

    #[test]
    fn test_per_minute_and_smooth_constructors() {
        let throttle = Throttle::per_minute(120);
        assert_eq!(throttle.capacity(), 120);

        let clock = Arc::new(ManualClock::new());
        let smooth = Throttle::smooth(10, Duration::from_secs(1)).with_clock(Arc::clone(&clock));
        assert_eq!(smooth.capacity(), 1);
        assert!(smooth.try_acquire());
        assert!(!smooth.try_acquire());
        clock.advance(Duration::from_millis(100));
        assert!(smooth.try_acquire(), "one token every 100ms");
        assert_eq!(smooth.peek(2), Decision::Impossible);
    }

    #[test]
    fn test_builder_burst_initial_and_name() {
        let clock = Arc::new(ManualClock::new());
        let throttle = Throttle::builder()
            .per_second(10)
            .burst(50)
            .initial(5)
            .name(String::from("owned-name"))
            .clock(Arc::clone(&clock))
            .try_build()
            .unwrap();
        assert_eq!(throttle.capacity(), 50);
        assert_eq!(throttle.available(), 5);
        assert_eq!(throttle.name(), Some("owned-name"));
        clock.advance(Duration::from_secs(10));
        assert_eq!(
            throttle.available(),
            50,
            "accrues to the burst, not the rate"
        );
        // Initial above the burst is clamped.
        let clamped = Throttle::builder().per_second(4).initial(99).build();
        assert_eq!(clamped.available(), 4);
    }

    #[test]
    fn test_builder_rejects_unusable_configurations() {
        let reason = |err: ThrottleError| match err {
            ThrottleError::InvalidConfig { reason } => reason,
            other => panic!("unexpected {other:?}"),
        };
        assert!(reason(Throttle::builder().try_build().unwrap_err()).contains("rate"));
        assert!(
            reason(
                Throttle::builder()
                    .rate(5, Duration::ZERO)
                    .try_build()
                    .unwrap_err()
            )
            .contains("period")
        );
        assert!(
            reason(
                Throttle::builder()
                    .per_second(5)
                    .burst(0)
                    .try_build()
                    .unwrap_err()
            )
            .contains("burst")
        );
        // `build` turns the same configurations into a closed throttle.
        let closed = Throttle::builder().per_second(5).burst(0).build();
        assert_eq!(closed.capacity(), 0);
        assert_eq!(closed.peek(1), Decision::Impossible);
        assert_eq!(super::ThrottleBuilder::default().build().capacity(), 0);
    }

    #[test]
    fn test_zero_rate_grants_nothing() {
        let throttle = Throttle::per_second(0);
        assert_eq!(throttle.capacity(), 0);
        assert!(!throttle.try_acquire());
        assert!(
            throttle.try_acquire_with_cost(0),
            "zero cost is always granted"
        );
        assert_eq!(throttle.peek(1), Decision::Impossible);
    }

    #[test]
    fn test_reserve_queues_and_refund_pays_down() {
        let (clock, throttle) = manual(10);
        assert!(throttle.try_acquire_with_cost(10));
        assert_eq!(
            throttle.reserve(5, Duration::from_secs(2)),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            throttle.reserve(5, Duration::from_secs(2)),
            Some(Duration::from_secs(1))
        );
        // Cancel the second reservation: the next one waits only 1s again.
        throttle.refund(5);
        assert_eq!(
            throttle.reserve(5, Duration::from_secs(2)),
            Some(Duration::from_secs(1))
        );
        clock.advance(Duration::from_secs(1));
        assert_eq!(throttle.available(), 0, "both reservations are consumed");
        assert_eq!(throttle.reserve(11, Duration::MAX), None);
    }

    #[test]
    fn test_drain_to_never_adds() {
        let throttle = Throttle::per_second(100);
        assert_eq!(throttle.drain_to(10), 90);
        assert_eq!(throttle.available(), 10);
        assert_eq!(throttle.drain_to(50), 0);
        assert_eq!(throttle.available(), 10);
        assert_eq!(throttle.drain_to(0), 10);
        assert_eq!(throttle.available(), 0);
    }

    #[test]
    fn test_pause_blocks_grants_until_it_ends() {
        let (clock, throttle) = manual(10);
        throttle.pause_for(Duration::from_secs(30));
        assert!(!throttle.try_acquire());
        assert_eq!(throttle.available(), 0);
        assert_eq!(
            throttle.peek(1),
            Decision::Retry {
                after: Duration::from_secs(30)
            }
        );
        assert_eq!(throttle.reserve(1, Duration::MAX), None);
        assert_eq!(throttle.paused_for(), Duration::from_secs(30));
        // A shorter pause does not shorten it.
        throttle.pause_for(Duration::from_secs(1));
        assert_eq!(throttle.paused_for(), Duration::from_secs(30));

        clock.advance(Duration::from_secs(30));
        assert_eq!(throttle.paused_for(), Duration::ZERO);
        assert!(throttle.try_acquire());
        // Tokens kept accruing (to capacity) during the pause.
        assert_eq!(throttle.available(), 9);
    }

    #[test]
    fn test_resume_lifts_an_indefinite_pause() {
        let throttle = Throttle::per_second(3);
        throttle.pause_for(Duration::MAX);
        assert!(!throttle.try_acquire());
        throttle.resume();
        assert!(throttle.try_acquire());
    }

    #[test]
    fn test_trait_surface_forwards() {
        let (clock, throttle) = manual(4);
        let limiter: &dyn Limiter = &throttle;
        assert_eq!(limiter.capacity(), 4);
        assert_eq!(limiter.available(), 4);
        assert_eq!(limiter.reserve(4, Duration::MAX), Some(Duration::ZERO));
        limiter.refund(2);
        assert_eq!(limiter.available(), 2);
        limiter.pause_for(Duration::from_secs(5));
        assert_eq!(limiter.available(), 0);
        clock.advance(Duration::from_secs(5));
        assert_eq!(limiter.available(), 4);
    }

    #[test]
    fn test_with_clock_keeps_the_name_and_debug_shows_state() {
        let throttle = Throttle::per_second(3)
            .with_name("api")
            .with_clock(Arc::new(ManualClock::new()));
        assert_eq!(throttle.name(), Some("api"));
        let text = format!("{throttle:?}");
        assert!(text.contains("\"api\""));
        assert!(text.contains("capacity: 3"));
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_returns_immediately_when_a_token_is_free() {
        let throttle = Throttle::per_second(1);
        assert!(throttle.acquire().await.is_ok());
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_with_cost_errors_when_cost_exceeds_capacity() {
        let throttle = Throttle::per_second(5);
        let err = throttle.acquire_with_cost(9).await.unwrap_err();
        assert_eq!(
            err,
            ThrottleError::CostExceedsCapacity {
                cost: 9,
                capacity: 5,
            }
        );
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_waits_for_refill_then_succeeds() {
        // One token every 20ms: drained, the waiter needs a real refill.
        let throttle = Throttle::per_duration(1, Duration::from_millis(20));
        assert!(throttle.try_acquire());
        assert!(!throttle.try_acquire());
        assert!(throttle.acquire().await.is_ok());
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_timeout_fails_fast_when_the_wait_is_known_to_be_too_long() {
        let throttle = Throttle::per_duration(1, Duration::from_secs(3600));
        assert!(throttle.try_acquire());
        let start = std::time::Instant::now();
        let err = throttle
            .acquire_timeout(Duration::from_secs(5))
            .await
            .unwrap_err();
        assert_eq!(err, ThrottleError::DeadlineExceeded);
        assert!(start.elapsed() < Duration::from_secs(1), "did not sleep");
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(start_paused = true)]
    async fn test_cancelled_acquire_returns_its_reservation() {
        // Real-clock throttle, paused tokio time: the reservation wait is 1s.
        let throttle = Throttle::per_second(10);
        assert!(throttle.try_acquire_with_cost(10));
        let cancelled =
            tokio::time::timeout(Duration::from_millis(10), throttle.acquire_with_cost(10)).await;
        assert!(cancelled.is_err(), "the outer timeout cancelled the wait");
        // The reservation was refunded: a new 10-token reservation waits ~1s
        // (the original deficit), not ~2s (behind a leaked reservation).
        let wait = throttle.reserve(10, Duration::MAX).unwrap();
        assert!(wait <= Duration::from_secs(1), "leaked: {wait:?}");
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(start_paused = true)]
    async fn test_waiters_are_served_in_reservation_order() {
        // A large request reserved first is not starved by later small ones.
        let throttle = Arc::new(Throttle::per_second(10));
        assert!(throttle.try_acquire_with_cost(10));
        let big = {
            let t = Arc::clone(&throttle);
            tokio::spawn(async move { t.acquire_with_cost(10).await })
        };
        tokio::task::yield_now().await;
        // The big reservation holds the next second's tokens; a small request
        // now must queue behind it.
        assert!(!throttle.try_acquire());
        assert!(big.await.unwrap().is_ok());
    }
}
