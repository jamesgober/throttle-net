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
//! [`AdaptiveStrategy`]. Outcomes are fed back through an [`AdaptivePermit`]
//! (or an [`OwnedAdaptivePermit`] that can move into a spawned task) — settle it
//! with `success` or `failure`, or let it drop (counted as a failure).
//!
//! Two control-loop safeguards apply to every strategy:
//!
//! - **Atomic updates.** Each outcome moves the limit with a compare-and-swap,
//!   so concurrent outcomes never overwrite each other.
//! - **One decrease per congestion event.** A failure only lowers the limit if
//!   its request was admitted *after* the most recent decrease. When a burst of
//!   in-flight requests fails together (one downstream blip), the first failure
//!   backs off and the rest — evidence of the same event — do not compound it,
//!   the way TCP reacts at most once per round trip.
//!
//! Behind the `adaptive` feature. Unlike the rate limiters, the waiting
//! [`acquire`](AdaptiveLimiter::acquire) blocks on a *slot* freeing, not on a
//! timer, so its clock is only used to measure round-trip time.

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::borrow::Cow;
use std::sync::Arc;

use clock_lib::{Clock, Monotonic, SystemClock};
use event_listener::Event;

use crate::decision::Decision;
#[cfg(feature = "runtime")]
use crate::error::ThrottleError;
use crate::limiter::Limiter;
use crate::obs::{self, Label};
// `in_flight` and `limit` route through the loom-aware indirection so the
// concurrency model check can explore their interleavings.
use crate::sync::AtomicU32;

/// How long a composite polling this limiter through the [`Limiter`] trait
/// waits before asking again for a slot (it cannot be notified).
const TRAIT_POLL: Duration = Duration::from_millis(1);

/// The observed result of one completed request, fed back to the strategy.
///
/// `#[non_exhaustive]`: more signals may be added.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
/// need not enforce the bounds itself. `adjust` may be called again for the same
/// outcome if a concurrent update wins the race to store the limit, so it must
/// not count calls (a strategy that keeps state, like [`Vegas`]'s minimum
/// latency, should treat repeats as harmless).
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
/// assert_eq!(limiter.current_limit(), 4);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Aimd {
    increase: u32,
    decrease: f64,
}

impl Aimd {
    /// Increase the limit by `increase` (at least 1) per saturated success;
    /// multiply by `decrease` (clamped to `[0.0, 1.0]`; NaN counts as `0.5`) on
    /// failure.
    #[must_use]
    pub fn new(increase: u32, decrease: f64) -> Self {
        Self {
            increase: increase.max(1),
            decrease: if decrease.is_nan() {
                0.5
            } else {
                decrease.clamp(0.0, 1.0)
            },
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
                // `decrease` is in [0, 1], so the product is in [0, current] and
                // converts back to u32 exactly (fractions truncate down).
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
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
/// seen. A small estimated queue means there is headroom, so the limit grows; a
/// large one means a queue is forming, so it shrinks. Failures halve the limit.
///
/// The no-load latency is **re-probed** every
/// [`probe_interval`](Self::with_probe_interval) samples (default 1000): the
/// minimum restarts from the next sample. Without that, one anomalously fast
/// response — or a permanent shift in the downstream's baseline latency — would
/// pin the queue estimate high and hold the limit at the floor forever.
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
    /// Best round-trip time seen since the last re-probe, in nanoseconds.
    min_rtt_ns: AtomicU64,
    /// Samples observed; every `probe_interval`-th restarts the minimum.
    samples: AtomicU64,
    probe_interval: u64,
}

/// The default number of samples between no-load latency re-probes.
const DEFAULT_PROBE_INTERVAL: u64 = 1_000;

impl Vegas {
    /// Grow while the estimated queue is below `alpha`, shrink above `beta`.
    /// `beta` is raised to at least `alpha` to keep a stable band between them.
    #[must_use]
    pub fn new(alpha: u32, beta: u32) -> Self {
        Self {
            alpha,
            beta: beta.max(alpha),
            min_rtt_ns: AtomicU64::new(u64::MAX),
            samples: AtomicU64::new(0),
            probe_interval: DEFAULT_PROBE_INTERVAL,
        }
    }

    /// Re-probes the no-load latency every `samples` successful samples (at
    /// least 1).
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Vegas;
    ///
    /// let vegas = Vegas::new(3, 6).with_probe_interval(500);
    /// # let _ = vegas;
    /// ```
    #[must_use]
    pub fn with_probe_interval(mut self, samples: u64) -> Self {
        self.probe_interval = samples.max(1);
        self
    }
}

impl Default for Vegas {
    /// Grow below an estimated queue of 3, shrink above 6, re-probe every 1000
    /// samples.
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
        let sample = self.samples.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        let min_ns = if sample % self.probe_interval == 0 {
            // Re-probe: forget the old minimum and start from this sample.
            self.min_rtt_ns.store(rtt_ns, Ordering::Relaxed);
            rtt_ns
        } else {
            self.min_rtt_ns
                .fetch_min(rtt_ns, Ordering::AcqRel)
                .min(rtt_ns)
        };
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
/// It is also a [`Limiter`], so it composes inside a [`Hybrid`](crate::Hybrid)
/// (a request then needs a slot *and* the other constituents' tokens): a unit
/// acquired through the trait is a slot, released with
/// [`release`](Self::release) (with an outcome) or [`Limiter::refund`]
/// (without one).
///
/// # Examples
///
/// ```
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
/// ```
pub struct AdaptiveLimiter<S, C = SystemClock>
where
    C: Clock,
{
    strategy: S,
    limit: AtomicU32,
    in_flight: AtomicU32,
    /// Incremented each time a failure lowers the limit; permits record the
    /// value they were admitted under (see the module docs).
    decrease_epoch: AtomicU64,
    floor: u32,
    ceiling: u32,
    notify: Event,
    clock: C,
    label: Label,
}

impl AdaptiveLimiter<core::convert::Infallible> {
    /// Starts building an adaptive limiter.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{AdaptiveLimiter, Aimd};
    ///
    /// let limiter = AdaptiveLimiter::builder().ceiling(10).build(Aimd::default());
    /// assert_eq!(limiter.ceiling(), 10);
    /// ```
    pub fn builder() -> AdaptiveLimiterBuilder {
        AdaptiveLimiterBuilder::new()
    }
}

impl<S, C> AdaptiveLimiter<S, C>
where
    S: AdaptiveStrategy,
    C: Clock,
{
    fn new(strategy: S, floor: u32, ceiling: u32, initial: u32, clock: C, label: Label) -> Self {
        let floor = floor.max(1);
        let ceiling = ceiling.max(floor);
        let initial = initial.clamp(floor, ceiling);
        obs::adaptive_limit(&label, initial, initial);
        Self {
            strategy,
            limit: AtomicU32::new(initial),
            in_flight: AtomicU32::new(0),
            decrease_epoch: AtomicU64::new(0),
            floor,
            ceiling,
            notify: Event::new(),
            clock,
            label,
        }
    }

    /// Replaces the time source (used to measure round-trip time), for
    /// deterministic tests. Resets the in-flight count; keeps the limit.
    #[must_use]
    pub fn with_clock<C2>(self, clock: C2) -> AdaptiveLimiter<S, C2>
    where
        C2: Clock,
    {
        let limit = self.limit.load(Ordering::Acquire);
        AdaptiveLimiter::new(
            self.strategy,
            self.floor,
            self.ceiling,
            limit,
            clock,
            self.label,
        )
    }

    /// Names this instance for metrics and events.
    #[must_use]
    pub fn with_name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
        let limit = self.current_limit();
        obs::adaptive_limit(&self.label, limit, limit);
        self
    }

    /// The instance name, if one was set.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.label.get()
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

    /// The floor the adapting limit never drops below.
    #[must_use]
    pub fn floor(&self) -> u32 {
        self.floor
    }

    /// The hard ceiling the adapting limit can never exceed.
    #[must_use]
    pub fn ceiling(&self) -> u32 {
        self.ceiling
    }

    /// Attempts to reserve `slots` slots without waiting.
    fn try_reserve(&self, slots: u32) -> bool {
        loop {
            let in_flight = self.in_flight.load(Ordering::Acquire);
            let Some(wanted) = in_flight.checked_add(slots) else {
                return false;
            };
            if wanted > self.limit.load(Ordering::Acquire) {
                return false;
            }
            if self
                .in_flight
                .compare_exchange_weak(in_flight, wanted, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// The decrease epoch a newly admitted request is stamped with.
    #[inline]
    fn epoch(&self) -> u64 {
        self.decrease_epoch.load(Ordering::Acquire)
    }

    /// Attempts to admit a request without waiting, returning an
    /// [`AdaptivePermit`] when a slot is free.
    #[must_use]
    pub fn try_acquire(&self) -> Option<AdaptivePermit<'_, S, C>> {
        let granted = self.try_reserve(1);
        obs::attempt(KIND, &self.label, 1, granted);
        granted.then(|| AdaptivePermit::new(self))
    }

    /// Releases one slot taken through the [`Limiter`] trait, applying
    /// `outcome` to the limit as a settled permit would.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{AdaptiveLimiter, Aimd, Limiter, Outcome};
    ///
    /// let limiter = AdaptiveLimiter::builder().initial(8).ceiling(8).build(Aimd::default());
    /// assert!(limiter.acquire_cost(1).is_acquired());
    /// limiter.release(Outcome::Failure);
    /// assert_eq!(limiter.in_flight(), 0);
    /// assert_eq!(limiter.current_limit(), 4);
    /// ```
    pub fn release(&self, outcome: Outcome) {
        self.settle(outcome, self.epoch());
    }

    /// Applies an [`Outcome`] to the limit and releases the slot. `epoch` is the
    /// decrease epoch the request was admitted under.
    fn settle(&self, outcome: Outcome, epoch: u64) {
        // `in_flight` still counts this request, so the strategy sees whether
        // the limit was saturated.
        let in_flight = self.in_flight.load(Ordering::Acquire);
        let is_failure = matches!(outcome, Outcome::Failure);
        // A failure admitted before the latest decrease belongs to a congestion
        // event that has already been answered: release the slot only.
        if !(is_failure && epoch < self.epoch()) {
            self.adjust_limit(in_flight, outcome, is_failure);
        }
        let _released = self
            .in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_sub(1))
            });
        // A slot freed (and the limit may have grown): wake one waiter. If it
        // was cancelled meanwhile, event-listener passes the wake-up on.
        let _woken = self.notify.notify(1);
    }

    /// Moves the limit by the strategy with a CAS loop, so concurrent outcomes
    /// never overwrite each other (TN-006).
    fn adjust_limit(&self, in_flight: u32, outcome: Outcome, is_failure: bool) {
        let mut current = self.limit.load(Ordering::Acquire);
        loop {
            let new = self
                .strategy
                .adjust(current, in_flight, outcome)
                .clamp(self.floor, self.ceiling);
            if new == current {
                return;
            }
            match self.limit.compare_exchange_weak(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if is_failure && new < current {
                        let _bumped = self.decrease_epoch.fetch_add(1, Ordering::AcqRel);
                    }
                    obs::adaptive_limit(&self.label, current, new);
                    if new > current {
                        // More slots: wake as many waiters as were added.
                        let _woken = self.notify.notify((new - current) as usize);
                    }
                    return;
                }
                Err(actual) => current = actual,
            }
        }
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

/// The limiter kind reported in metrics and events.
const KIND: &str = "adaptive";

impl<S, C> AdaptiveLimiter<S, C>
where
    S: AdaptiveStrategy + 'static,
    C: Clock + 'static,
{
    /// Like [`try_acquire`](Self::try_acquire), returning an
    /// [`OwnedAdaptivePermit`] that can move into a spawned task.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use throttle_net::{AdaptiveLimiter, Aimd};
    ///
    /// let limiter = Arc::new(AdaptiveLimiter::builder().initial(2).build(Aimd::default()));
    /// let permit = limiter.try_acquire_owned().expect("a slot");
    /// std::thread::spawn(move || permit.success()).join().unwrap();
    /// assert_eq!(limiter.in_flight(), 0);
    /// ```
    #[must_use]
    pub fn try_acquire_owned(self: &Arc<Self>) -> Option<OwnedAdaptivePermit<S, C>> {
        let granted = self.try_reserve(1);
        obs::attempt(KIND, &self.label, 1, granted);
        granted.then(|| OwnedAdaptivePermit::new(Arc::clone(self)))
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl<S, C> AdaptiveLimiter<S, C>
where
    S: AdaptiveStrategy,
    C: Clock,
{
    /// Admits a request, waiting until a slot is free.
    ///
    /// Unlike the rate limiters, this waits on a slot being released (or the
    /// limit growing), not on a timer. **Cancel-safe:** dropping the future
    /// before it returns holds no slot.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// use throttle_net::{AdaptiveLimiter, Aimd};
    ///
    /// let limiter = AdaptiveLimiter::builder().initial(4).build(Aimd::default());
    /// let permit = limiter.acquire().await;
    /// permit.success();
    /// # }
    /// ```
    pub async fn acquire(&self) -> AdaptivePermit<'_, S, C> {
        let timer = obs::Timer::start();
        self.wait_for_slot().await;
        obs::waited(KIND, &self.label, &timer, 1, obs::Waited::Granted);
        AdaptivePermit::new(self)
    }

    /// Admits a request, waiting at most `timeout` for a slot.
    ///
    /// # Errors
    ///
    /// [`ThrottleError::DeadlineExceeded`] if no slot frees within `timeout`.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// use std::time::Duration;
    /// use throttle_net::{AdaptiveLimiter, Aimd, ThrottleError};
    ///
    /// let limiter = AdaptiveLimiter::builder().initial(1).ceiling(1).build(Aimd::default());
    /// let held = limiter.try_acquire().unwrap();
    /// let err = limiter.acquire_timeout(Duration::from_millis(5)).await.unwrap_err();
    /// assert_eq!(err, ThrottleError::DeadlineExceeded);
    /// held.success();
    /// # }
    /// ```
    pub async fn acquire_timeout(
        &self,
        timeout: Duration,
    ) -> Result<AdaptivePermit<'_, S, C>, ThrottleError> {
        let timer = obs::Timer::start();
        let result = crate::resilience::timeout(timeout, self.wait_for_slot())
            .await
            .map(|()| AdaptivePermit::new(self))
            .map_err(ThrottleError::from);
        obs::waited(KIND, &self.label, &timer, 1, obs::Waited::of(&result));
        result
    }

    /// Waits until a slot is reserved. Cancel-safe: a slot is only ever taken
    /// by the synchronous `try_reserve`, never held across an await.
    async fn wait_for_slot(&self) {
        loop {
            if self.try_reserve(1) {
                return;
            }
            // `listen` registers immediately, so a slot freed between the check
            // above and the await is still delivered.
            let listener = self.notify.listen();
            if self.try_reserve(1) {
                return;
            }
            listener.await;
        }
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl<S, C> AdaptiveLimiter<S, C>
where
    S: AdaptiveStrategy + 'static,
    C: Clock + 'static,
{
    /// Like [`acquire`](Self::acquire), returning an [`OwnedAdaptivePermit`]
    /// that can move into a spawned task.
    pub async fn acquire_owned(self: &Arc<Self>) -> OwnedAdaptivePermit<S, C> {
        self.wait_for_slot().await;
        OwnedAdaptivePermit::new(Arc::clone(self))
    }
}

impl<S, C> Limiter for AdaptiveLimiter<S, C>
where
    S: AdaptiveStrategy,
    C: Clock,
{
    /// Whether `cost` slots are free now. A wait cannot be predicted (it ends
    /// when a request completes), so a refusal suggests polling again shortly.
    fn peek(&self, cost: u32) -> Decision {
        if cost > self.ceiling {
            return Decision::Impossible;
        }
        let free = self.current_limit().saturating_sub(self.in_flight());
        if cost <= free {
            Decision::Acquired
        } else {
            Decision::Retry { after: TRAIT_POLL }
        }
    }

    /// Takes `cost` slots if they are free. Release them with
    /// [`release`](AdaptiveLimiter::release) or [`refund`](Limiter::refund).
    fn acquire_cost(&self, cost: u32) -> Decision {
        if cost > self.ceiling {
            return Decision::Impossible;
        }
        if self.try_reserve(cost) {
            Decision::Acquired
        } else {
            Decision::Retry { after: TRAIT_POLL }
        }
    }

    fn available(&self) -> u32 {
        self.current_limit().saturating_sub(self.in_flight())
    }

    /// The ceiling: no request for more slots can ever be admitted.
    fn capacity(&self) -> u32 {
        self.ceiling
    }

    /// Releases `cost` slots without feeding an outcome to the strategy.
    fn refund(&self, cost: u32) {
        let _released = self
            .in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_sub(cost))
            });
        let _woken = self.notify.notify(cost as usize);
    }
}

impl<S, C> core::fmt::Debug for AdaptiveLimiter<S, C>
where
    C: Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AdaptiveLimiter")
            .field("name", &self.label)
            .field("limit", &self.limit.load(Ordering::Relaxed))
            .field("in_flight", &self.in_flight.load(Ordering::Relaxed))
            .field("floor", &self.floor)
            .field("ceiling", &self.ceiling)
            .finish_non_exhaustive()
    }
}

/// A reserved concurrency slot. Settle it with [`success`](Self::success) or
/// [`failure`](Self::failure) after the request completes; dropping it unsettled
/// records a **failure** and frees the slot.
#[must_use = "settle the permit with `.success()` or `.failure()`; dropping it counts as a failure"]
pub struct AdaptivePermit<'a, S, C>
where
    S: AdaptiveStrategy,
    C: Clock,
{
    limiter: &'a AdaptiveLimiter<S, C>,
    started: Monotonic,
    epoch: u64,
    settled: bool,
}

impl<'a, S, C> AdaptivePermit<'a, S, C>
where
    S: AdaptiveStrategy,
    C: Clock,
{
    fn new(limiter: &'a AdaptiveLimiter<S, C>) -> Self {
        Self {
            started: limiter.now(),
            epoch: limiter.epoch(),
            limiter,
            settled: false,
        }
    }

    /// Records a successful request; its round-trip time is measured from when
    /// the permit was acquired.
    pub fn success(mut self) {
        self.settled = true;
        let rtt = self.limiter.rtt_since(self.started);
        self.limiter.settle(Outcome::Success { rtt }, self.epoch);
    }

    /// Records a failed request.
    pub fn failure(mut self) {
        self.settled = true;
        self.limiter.settle(Outcome::Failure, self.epoch);
    }
}

impl<S, C> Drop for AdaptivePermit<'_, S, C>
where
    S: AdaptiveStrategy,
    C: Clock,
{
    fn drop(&mut self) {
        if !self.settled {
            self.limiter.settle(Outcome::Failure, self.epoch);
        }
    }
}

impl<S, C> core::fmt::Debug for AdaptivePermit<'_, S, C>
where
    S: AdaptiveStrategy,
    C: Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AdaptivePermit")
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

/// An owned [`AdaptivePermit`]: it keeps the limiter alive through an [`Arc`],
/// so it can move into a spawned task. Same settling rules.
#[must_use = "settle the permit with `.success()` or `.failure()`; dropping it counts as a failure"]
pub struct OwnedAdaptivePermit<S, C = SystemClock>
where
    S: AdaptiveStrategy,
    C: Clock,
{
    limiter: Arc<AdaptiveLimiter<S, C>>,
    started: Monotonic,
    epoch: u64,
    settled: bool,
}

impl<S, C> OwnedAdaptivePermit<S, C>
where
    S: AdaptiveStrategy,
    C: Clock,
{
    fn new(limiter: Arc<AdaptiveLimiter<S, C>>) -> Self {
        Self {
            started: limiter.now(),
            epoch: limiter.epoch(),
            limiter,
            settled: false,
        }
    }

    /// Records a successful request; its round-trip time is measured from when
    /// the permit was acquired.
    pub fn success(mut self) {
        self.settled = true;
        let rtt = self.limiter.rtt_since(self.started);
        self.limiter.settle(Outcome::Success { rtt }, self.epoch);
    }

    /// Records a failed request.
    pub fn failure(mut self) {
        self.settled = true;
        self.limiter.settle(Outcome::Failure, self.epoch);
    }
}

impl<S, C> Drop for OwnedAdaptivePermit<S, C>
where
    S: AdaptiveStrategy,
    C: Clock,
{
    fn drop(&mut self) {
        if !self.settled {
            self.limiter.settle(Outcome::Failure, self.epoch);
        }
    }
}

impl<S, C> core::fmt::Debug for OwnedAdaptivePermit<S, C>
where
    S: AdaptiveStrategy,
    C: Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("OwnedAdaptivePermit")
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

/// Builder for an [`AdaptiveLimiter`].
///
/// # Examples
///
/// ```
/// use throttle_net::{AdaptiveLimiter, Vegas};
///
/// let limiter = AdaptiveLimiter::builder()
///     .floor(2)
///     .ceiling(64)
///     .initial(8)
///     .name("search")
///     .build(Vegas::default());
/// assert_eq!(limiter.current_limit(), 8);
/// assert_eq!(limiter.name(), Some("search"));
/// ```
#[derive(Debug, Clone, Copy)]
#[must_use]
pub struct AdaptiveLimiterBuilder {
    floor: u32,
    ceiling: u32,
    initial: Option<u32>,
    /// A static name, so the builder stays `Copy`; name an instance with an
    /// owned string through its `with_name`.
    label: Option<&'static str>,
}

impl Default for AdaptiveLimiterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl AdaptiveLimiterBuilder {
    /// Creates a builder with a floor of 1 and a ceiling of 100.
    pub fn new() -> Self {
        Self {
            floor: 1,
            ceiling: 100,
            initial: None,
            label: None,
        }
    }

    /// Sets the minimum concurrency limit (clamped to at least 1).
    pub fn floor(mut self, floor: u32) -> Self {
        self.floor = floor.max(1);
        self
    }

    /// Sets the maximum concurrency limit — the hard ceiling the adapting limit
    /// never exceeds (raised to the floor if below it).
    pub fn ceiling(mut self, ceiling: u32) -> Self {
        self.ceiling = ceiling;
        self
    }

    /// Sets the starting limit (clamped to `[floor, ceiling]`). Defaults to the
    /// floor: probe up from cautious.
    pub fn initial(mut self, initial: u32) -> Self {
        self.initial = Some(initial);
        self
    }

    /// Names the limiter for metrics and events.
    pub fn name(mut self, name: &'static str) -> Self {
        self.label = Some(name);
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
            self.label.map_or_else(Label::none, Label::new),
        )
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::{AdaptiveLimiter, AdaptiveStrategy, Aimd, Outcome, Vegas};
    use crate::decision::Decision;
    use crate::limiter::Limiter;
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    fn success() -> Outcome {
        Outcome::Success {
            rtt: Duration::ZERO,
        }
    }

    #[test]
    fn test_adaptive_is_send_sync() {
        assert_send_sync::<AdaptiveLimiter<Aimd>>();
        assert_send_sync::<AdaptiveLimiter<Vegas>>();
        assert_send_sync::<super::OwnedAdaptivePermit<Aimd>>();
    }

    #[test]
    fn test_aimd_adjust_rules() {
        let aimd = Aimd::new(2, 0.5);
        assert_eq!(aimd.adjust(10, 10, success()), 12);
        assert_eq!(aimd.adjust(10, 3, success()), 10);
        assert_eq!(aimd.adjust(10, 10, Outcome::Failure), 5);
        assert_eq!(aimd.adjust(1, 1, Outcome::Failure), 1);
        // Out-of-range parameters are sanitised.
        assert_eq!(Aimd::new(0, f64::NAN).adjust(10, 10, Outcome::Failure), 5);
        assert_eq!(Aimd::new(0, 9.0).adjust(10, 10, success()), 11);
    }

    #[test]
    fn test_degradation_drives_limit_to_floor() {
        let limiter = AdaptiveLimiter::builder()
            .floor(4)
            .ceiling(100)
            .initial(64)
            .build(Aimd::new(4, 0.5));
        for _ in 0..10 {
            let permit = limiter.try_acquire().expect("a slot under the limit");
            permit.failure();
        }
        assert_eq!(limiter.current_limit(), 4);
    }

    #[test]
    fn test_correlated_failures_decrease_once() {
        // Regression (TN-007): N in-flight requests failing together cut the
        // limit by decrease^N, collapsing it to the floor.
        let limiter = AdaptiveLimiter::builder()
            .floor(1)
            .ceiling(100)
            .initial(64)
            .build(Aimd::new(1, 0.5));
        let permits: Vec<_> = (0..20).map(|_| limiter.try_acquire().unwrap()).collect();
        for permit in permits {
            permit.failure();
        }
        assert_eq!(limiter.current_limit(), 32, "one blip, one halving");
        assert_eq!(limiter.in_flight(), 0);
        // A failure from a request admitted after that decrease counts again.
        limiter.try_acquire().unwrap().failure();
        assert_eq!(limiter.current_limit(), 16);
    }

    /// A strategy whose every outcome adds a fixed amount: concurrent updates
    /// commute, so the final limit is exact unless an update is lost.
    struct Counting;

    impl AdaptiveStrategy for Counting {
        fn adjust(&self, current: u32, _in_flight: u32, outcome: Outcome) -> u32 {
            match outcome {
                Outcome::Failure => current.saturating_add(1_000),
                _ => current.saturating_add(1),
            }
        }
    }

    #[test]
    fn test_concurrent_settles_never_lose_an_update() {
        // Regression (TN-006): `settle` did load -> adjust -> store, so
        // concurrent outcomes overwrote each other (a failure's decrease could
        // be replaced by a concurrent success's increase).
        const THREADS: u32 = 8;
        const EACH: u32 = 500;
        let limiter = Arc::new(
            AdaptiveLimiter::builder()
                .floor(1)
                .ceiling(u32::MAX)
                .initial(1_000)
                .build(Counting),
        );
        let barrier = Arc::new(std::sync::Barrier::new(THREADS as usize));
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let limiter = Arc::clone(&limiter);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let _leader = barrier.wait();
                    for _ in 0..EACH {
                        let permit = limiter.try_acquire().unwrap();
                        if t % 2 == 0 {
                            permit.success()
                        } else {
                            permit.failure()
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let successes = THREADS / 2 * EACH;
        let failures = THREADS / 2 * EACH;
        assert_eq!(
            limiter.current_limit(),
            1_000 + successes + 1_000 * failures
        );
        assert_eq!(limiter.in_flight(), 0);
    }

    #[test]
    fn test_recovery_drives_limit_up_bounded_by_ceiling() {
        let limiter = AdaptiveLimiter::builder()
            .floor(1)
            .ceiling(8)
            .initial(1)
            .build(Aimd::new(1, 0.5));
        for _ in 0..50 {
            let mut held = Vec::new();
            while let Some(p) = limiter.try_acquire() {
                held.push(p);
            }
            for p in held {
                p.success();
            }
        }
        assert_eq!(limiter.current_limit(), 8);
        for _ in 0..20 {
            limiter.try_acquire().expect("slot").success();
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
        drop(limiter.try_acquire().expect("slot"));
        assert_eq!(limiter.current_limit(), 5);
        assert_eq!(limiter.in_flight(), 0);
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
        let p = limiter.try_acquire().expect("slot");
        clock.advance(Duration::from_millis(10));
        p.success();
        assert_eq!(limiter.current_limit(), 21);
        let p = limiter.try_acquire().expect("slot");
        clock.advance(Duration::from_millis(200));
        p.success();
        assert!(limiter.current_limit() < 21);
    }

    #[test]
    fn test_vegas_reprobes_the_no_load_latency() {
        // Regression (TN-007): one anomalously fast sample pinned min_rtt and
        // held the limit at the floor with no way back.
        let vegas = Vegas::new(3, 6).with_probe_interval(4);
        let rtt = |ms| Outcome::Success {
            rtt: Duration::from_millis(ms),
        };
        assert_eq!(vegas.adjust(50, 0, rtt(1)), 51); // the outlier: min = 1ms
        // A steady 100ms baseline reads as a deep queue until the re-probe.
        assert_eq!(vegas.adjust(50, 0, rtt(100)), 49);
        assert_eq!(vegas.adjust(50, 0, rtt(100)), 49);
        // Fourth sample re-probes: the minimum restarts at 100ms.
        assert_eq!(vegas.adjust(50, 0, rtt(100)), 51);
        assert_eq!(vegas.adjust(50, 0, rtt(100)), 51, "recovered");
        assert_eq!(vegas.adjust(9, 0, Outcome::Failure), 4);
    }

    #[test]
    fn test_limiter_trait_surface_and_release() {
        let limiter = AdaptiveLimiter::builder()
            .floor(1)
            .ceiling(4)
            .initial(4)
            .name("pool")
            .build(Aimd::new(1, 0.5));
        assert_eq!(limiter.peek(4), Decision::Acquired);
        assert_eq!(limiter.peek(5), Decision::Impossible);
        assert!(limiter.acquire_cost(3).is_acquired());
        assert_eq!(limiter.available(), 1);
        assert!(matches!(limiter.peek(2), Decision::Retry { .. }));
        assert!(matches!(limiter.acquire_cost(2), Decision::Retry { .. }));
        assert_eq!(limiter.acquire_cost(9), Decision::Impossible);
        limiter.refund(2);
        assert_eq!(limiter.in_flight(), 1);
        limiter.release(success());
        assert_eq!(limiter.in_flight(), 0);
        assert_eq!(Limiter::capacity(&limiter), 4);
        assert_eq!(limiter.floor(), 1);
        assert_eq!(limiter.name(), Some("pool"));
        assert!(format!("{limiter:?}").contains("AdaptiveLimiter"));
        let permit = limiter.try_acquire().unwrap();
        assert!(format!("{permit:?}").contains("AdaptivePermit"));
        permit.success();
    }

    #[test]
    fn test_owned_permits() {
        let limiter = Arc::new(
            AdaptiveLimiter::builder()
                .initial(2)
                .ceiling(2)
                .build(Aimd::new(1, 0.5)),
        );
        let a = limiter.try_acquire_owned().unwrap();
        let b = limiter.try_acquire_owned().unwrap();
        assert!(limiter.try_acquire_owned().is_none());
        assert!(format!("{a:?}").contains("OwnedAdaptivePermit"));
        std::thread::spawn(move || a.success()).join().unwrap();
        b.failure();
        assert_eq!(limiter.in_flight(), 0);
        drop(limiter.try_acquire_owned().unwrap());
        assert_eq!(limiter.in_flight(), 0);
    }

    #[cfg(feature = "runtime")]
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
        tokio::task::yield_now().await;
        held.success();
        waiter.await.unwrap();
        let owned = limiter.acquire_owned().await;
        owned.success();
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(start_paused = true)]
    async fn test_acquire_timeout_and_cancellation_hold_no_slot() {
        let limiter = AdaptiveLimiter::builder()
            .floor(1)
            .ceiling(1)
            .initial(1)
            .build(Aimd::default());
        let held = limiter.try_acquire().unwrap();
        assert!(
            limiter
                .acquire_timeout(Duration::from_millis(10))
                .await
                .is_err()
        );
        let cancelled = tokio::time::timeout(Duration::from_millis(10), limiter.acquire()).await;
        assert!(cancelled.is_err());
        assert_eq!(limiter.in_flight(), 1, "only the held permit");
        held.success();
        let permit = limiter
            .acquire_timeout(Duration::from_millis(10))
            .await
            .unwrap();
        permit.success();
    }
}
