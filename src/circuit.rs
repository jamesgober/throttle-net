//! A circuit breaker that wraps any [`Limiter`] and fails fast when a downstream
//! is unhealthy.
//!
//! A limiter paces requests; a breaker *stops* them. When the protected
//! downstream produces enough failures, the breaker trips **open** and sheds
//! requests immediately — without consuming the wrapped limiter's tokens — so a
//! struggling dependency is given room to recover instead of being hammered.
//! After a cooldown it goes **half-open**, admitting a few trial requests; if
//! they succeed it **closes** and normal pacing resumes, otherwise it opens
//! again.
//!
//! Outcomes are reported back through a [`Permit`]: [`acquire`](CircuitBreaker::acquire)
//! hands you one, and you call [`success`](Permit::success) or
//! [`failure`](Permit::failure) after the call. Dropping a permit without
//! settling it counts as a failure, so an early return or panic is treated
//! conservatively.

use core::time::Duration;
use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, PoisonError};

use clock_lib::{Clock, Monotonic, SystemClock};

use crate::decision::Decision;
use crate::error::ThrottleError;
use crate::limiter::Limiter;

/// The condition under which a closed breaker trips open.
///
/// `#[non_exhaustive]`: more trip conditions may be added.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Trip {
    /// Trip after this many consecutive failures (a success resets the count).
    Consecutive(u32),
    /// Trip when the failure ratio over the last `window` calls reaches `ratio`,
    /// once at least `min_calls` have been observed.
    Ratio {
        /// How many recent calls to consider.
        window: u32,
        /// The failure fraction in `[0.0, 1.0]` that trips the breaker.
        ratio: f64,
        /// The minimum calls before the ratio is evaluated.
        min_calls: u32,
    },
    /// Trip when at least `failures` failures occur within a rolling `period`.
    Windowed {
        /// The failure count that trips the breaker.
        failures: u32,
        /// The rolling time window the failures are counted in.
        period: Duration,
    },
}

/// The breaker's current state, as a snapshot.
///
/// `#[non_exhaustive]`: matching should include a wildcard.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    /// Requests flow to the wrapped limiter; failures are being counted.
    Closed,
    /// Requests are shed immediately; the downstream is being given time.
    Open,
    /// A few trial requests are allowed to test whether the downstream recovered.
    HalfOpen,
}

/// The mutable state machine, guarded by a single mutex.
struct Shared {
    state: BreakerState,
    /// Consecutive failures (for [`Trip::Consecutive`]).
    consecutive: u32,
    /// Recent outcomes, `true` = failure (for [`Trip::Ratio`]).
    outcomes: VecDeque<bool>,
    /// Failure timestamps in milliseconds (for [`Trip::Windowed`]).
    failure_times: VecDeque<u64>,
    /// Trial requests in flight while half-open.
    half_open_inflight: u32,
    /// Successful trials accumulated while half-open.
    half_open_successes: u32,
    /// Milliseconds (store epoch) at which an open breaker may go half-open.
    open_until_ms: u64,
}

impl Shared {
    fn new() -> Self {
        Self {
            state: BreakerState::Closed,
            consecutive: 0,
            outcomes: VecDeque::new(),
            failure_times: VecDeque::new(),
            half_open_inflight: 0,
            half_open_successes: 0,
            open_until_ms: 0,
        }
    }

    /// Resets the failure bookkeeping when the breaker closes.
    fn reset_counters(&mut self) {
        self.consecutive = 0;
        self.outcomes.clear();
        self.failure_times.clear();
        self.half_open_inflight = 0;
        self.half_open_successes = 0;
    }
}

/// Whether the breaker admits a request right now.
enum Admit {
    Allow,
    Reject(Duration),
}

/// A circuit breaker wrapping a limiter `L`, timed by clock `C`.
///
/// Construct one with [`CircuitBreaker::builder`]. Build requires the
/// `circuit-breaker` feature.
///
/// # Examples
///
/// ```
/// # async fn run() {
/// use std::time::Duration;
/// use throttle_net::{CircuitBreaker, Throttle, Trip};
///
/// let breaker = CircuitBreaker::builder()
///     .trip(Trip::Consecutive(5))
///     .cooldown(Duration::from_secs(10))
///     .build(Throttle::per_second(100));
///
/// match breaker.acquire().await {
///     Ok(permit) => {
///         // ... call the downstream ...
///         let ok = true;
///         if ok { permit.success() } else { permit.failure() }
///     }
///     Err(_shed) => {
///         // breaker open (or limiter exhausted): fail fast
///     }
/// }
/// # }
/// ```
pub struct CircuitBreaker<L, C = SystemClock>
where
    C: Clock,
{
    inner: L,
    config: Config,
    shared: Mutex<Shared>,
    clock: C,
    epoch: Monotonic,
}

/// Validated breaker configuration.
#[derive(Debug, Clone, Copy)]
struct Config {
    trip: Trip,
    cooldown: Duration,
    half_open_trials: u32,
    half_open_required: u32,
}

// Anchored on a concrete, limiter-free type so `CircuitBreaker::builder()` needs
// no type annotation; the wrapped limiter type is fixed later by
// [`CircuitBreakerBuilder::build`].
impl CircuitBreaker<core::convert::Infallible> {
    /// Starts building a breaker. Defaults: [`Trip::Consecutive(5)`](Trip::Consecutive),
    /// a 30-second cooldown, and a single trial that must succeed to close.
    #[must_use]
    pub fn builder() -> CircuitBreakerBuilder {
        CircuitBreakerBuilder::new()
    }
}

impl<L, C> CircuitBreaker<L, C>
where
    L: Limiter,
    C: Clock + Clone,
{
    fn new(inner: L, config: Config, clock: C) -> Self {
        let epoch = clock.now();
        Self {
            inner,
            config,
            shared: Mutex::new(Shared::new()),
            clock,
            epoch,
        }
    }

    /// Replaces the time source (the cooldown clock), for deterministic tests.
    /// The breaker is reset to closed around the new clock.
    #[must_use]
    pub fn with_clock<C2>(self, clock: C2) -> CircuitBreaker<L, C2>
    where
        C2: Clock + Clone,
    {
        CircuitBreaker::new(self.inner, self.config, clock)
    }

    /// The current state (a momentary snapshot).
    #[must_use]
    pub fn state(&self) -> BreakerState {
        self.lock().state
    }

    /// A shared reference to the wrapped limiter.
    pub fn inner(&self) -> &L {
        &self.inner
    }

    #[inline]
    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[inline]
    fn now_ms(&self) -> u64 {
        let elapsed = self.clock.now().saturating_duration_since(self.epoch);
        u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
    }

    /// Decides admission and performs any state transition the clock has earned
    /// (open → half-open). Reserves a half-open trial slot when it admits one.
    fn admit(&self, now_ms: u64) -> Admit {
        let mut shared = self.lock();
        match shared.state {
            BreakerState::Closed => Admit::Allow,
            BreakerState::Open => {
                if now_ms >= shared.open_until_ms {
                    shared.state = BreakerState::HalfOpen;
                    shared.half_open_inflight = 1;
                    shared.half_open_successes = 0;
                    crate::obs::circuit_transition("Open", "HalfOpen", 1);
                    Admit::Allow
                } else {
                    Admit::Reject(Duration::from_millis(shared.open_until_ms - now_ms))
                }
            }
            BreakerState::HalfOpen => {
                if shared.half_open_inflight < self.config.half_open_trials {
                    shared.half_open_inflight += 1;
                    Admit::Allow
                } else {
                    // Trials are already in flight; shed extra probes.
                    Admit::Reject(Duration::ZERO)
                }
            }
        }
    }

    /// Releases a reserved half-open slot when an admitted request never reaches
    /// the downstream (e.g. the wrapped limiter says the cost is impossible).
    fn abort(&self) {
        let mut shared = self.lock();
        if shared.state == BreakerState::HalfOpen {
            shared.half_open_inflight = shared.half_open_inflight.saturating_sub(1);
        }
    }

    /// Records the outcome of a settled request and transitions as needed.
    fn record(&self, success: bool) {
        let now_ms = self.now_ms();
        let mut shared = self.lock();
        match shared.state {
            BreakerState::HalfOpen => {
                shared.half_open_inflight = shared.half_open_inflight.saturating_sub(1);
                if success {
                    shared.half_open_successes += 1;
                    if shared.half_open_successes >= self.config.half_open_required {
                        shared.state = BreakerState::Closed;
                        shared.reset_counters();
                        crate::obs::circuit_transition("HalfOpen", "Closed", 0);
                    }
                } else {
                    self.open(&mut shared, now_ms);
                }
            }
            BreakerState::Closed => {
                if success {
                    shared.consecutive = 0;
                    record_outcome(&mut shared, false, now_ms, self.config.trip);
                } else {
                    shared.consecutive += 1;
                    record_outcome(&mut shared, true, now_ms, self.config.trip);
                    if tripped(&shared, now_ms, self.config.trip) {
                        self.open(&mut shared, now_ms);
                    }
                }
            }
            // A record while fully open is unusual (nothing was admitted); ignore.
            BreakerState::Open => {}
        }
    }

    /// Moves the breaker to open and arms the cooldown.
    fn open(&self, shared: &mut Shared, now_ms: u64) {
        let from = if shared.state == BreakerState::HalfOpen {
            "HalfOpen"
        } else {
            "Closed"
        };
        shared.state = BreakerState::Open;
        shared.open_until_ms = now_ms
            .saturating_add(u64::try_from(self.config.cooldown.as_millis()).unwrap_or(u64::MAX));
        shared.half_open_inflight = 0;
        shared.half_open_successes = 0;
        crate::obs::circuit_transition(from, "Open", 2);
    }

    /// Reports a successful protected call. Prefer settling a [`Permit`].
    pub fn record_success(&self) {
        self.record(true);
    }

    /// Reports a failed protected call. Prefer settling a [`Permit`].
    pub fn record_failure(&self) {
        self.record(false);
    }

    /// Attempts to admit a request without waiting, returning a [`Permit`] on
    /// success.
    ///
    /// # Errors
    ///
    /// - [`ThrottleError::CircuitOpen`] when the breaker is open (or its
    ///   half-open trials are full): the request is shed without touching the
    ///   wrapped limiter.
    /// - [`ThrottleError::CostExceedsCapacity`] when the wrapped limiter can
    ///   never grant a single unit.
    ///
    /// Returns `Ok(None)` when the breaker would admit but the wrapped limiter
    /// has no token available right now (normal rate-limiting, not a breaker
    /// fault).
    pub fn try_acquire(&self) -> Result<Option<Permit<'_, L, C>>, ThrottleError> {
        let now_ms = self.now_ms();
        match self.admit(now_ms) {
            Admit::Reject(retry_after) => Err(ThrottleError::CircuitOpen { retry_after }),
            Admit::Allow => match self.inner.acquire_cost(1) {
                Decision::Acquired => Ok(Some(Permit::new(self))),
                Decision::Retry { .. } => {
                    self.abort();
                    Ok(None)
                }
                Decision::Impossible => {
                    self.abort();
                    Err(ThrottleError::CostExceedsCapacity {
                        cost: 1,
                        capacity: self.inner.capacity(),
                    })
                }
            },
        }
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl<L, C> CircuitBreaker<L, C>
where
    L: Limiter,
    C: Clock + Clone,
{
    /// Admits a request, failing fast if the breaker is open and otherwise
    /// pacing on the wrapped limiter until a token is free.
    ///
    /// A circuit-open condition returns immediately (load shedding); a plain
    /// rate-limit waits. Returns a [`Permit`] to settle with the call's outcome.
    ///
    /// # Errors
    ///
    /// - [`ThrottleError::CircuitOpen`] when the breaker is open or its trials
    ///   are full — returned without waiting.
    /// - [`ThrottleError::CostExceedsCapacity`] when the wrapped limiter can
    ///   never grant the request.
    pub async fn acquire(&self) -> Result<Permit<'_, L, C>, ThrottleError> {
        // Breaker admission is checked once, up front: an open breaker fails fast
        // rather than waiting. The reserved slot (if half-open) is held across the
        // rate-limit wait and released by the permit or on an impossible cost.
        match self.admit(self.now_ms()) {
            Admit::Reject(retry_after) => return Err(ThrottleError::CircuitOpen { retry_after }),
            Admit::Allow => {}
        }
        loop {
            match self.inner.acquire_cost(1) {
                Decision::Acquired => return Ok(Permit::new(self)),
                Decision::Retry { after } => crate::rt::sleep(after).await,
                Decision::Impossible => {
                    self.abort();
                    return Err(ThrottleError::CostExceedsCapacity {
                        cost: 1,
                        capacity: self.inner.capacity(),
                    });
                }
            }
        }
    }
}

/// Appends an outcome to the structures the configured [`Trip`] needs.
fn record_outcome(shared: &mut Shared, failure: bool, now_ms: u64, trip: Trip) {
    match trip {
        Trip::Consecutive(_) => {}
        Trip::Ratio { window, .. } => {
            shared.outcomes.push_back(failure);
            while shared.outcomes.len() > window as usize {
                let _ = shared.outcomes.pop_front();
            }
        }
        Trip::Windowed { period, .. } => {
            if failure {
                shared.failure_times.push_back(now_ms);
            }
            let cutoff =
                now_ms.saturating_sub(u64::try_from(period.as_millis()).unwrap_or(u64::MAX));
            while shared.failure_times.front().is_some_and(|&t| t < cutoff) {
                let _ = shared.failure_times.pop_front();
            }
        }
    }
}

/// Whether the closed breaker's failure state has reached its trip condition.
fn tripped(shared: &Shared, now_ms: u64, trip: Trip) -> bool {
    match trip {
        Trip::Consecutive(n) => shared.consecutive >= n,
        Trip::Ratio {
            ratio, min_calls, ..
        } => {
            let total = shared.outcomes.len() as u32;
            if total < min_calls || total == 0 {
                return false;
            }
            let failures = shared.outcomes.iter().filter(|&&f| f).count() as u32;
            f64::from(failures) / f64::from(total) >= ratio
        }
        Trip::Windowed { failures, period } => {
            let cutoff =
                now_ms.saturating_sub(u64::try_from(period.as_millis()).unwrap_or(u64::MAX));
            let recent = shared
                .failure_times
                .iter()
                .filter(|&&t| t >= cutoff)
                .count() as u32;
            recent >= failures
        }
    }
}

/// A reserved permission to make one protected call.
///
/// Settle it with [`success`](Self::success) or [`failure`](Self::failure) after
/// the call returns. If dropped unsettled — an early return, a `?`, or a panic —
/// it records a **failure**, so the breaker errs toward protecting the
/// downstream.
#[must_use = "settle the permit with `.success()` or `.failure()`; dropping it counts as a failure"]
pub struct Permit<'a, L, C>
where
    L: Limiter,
    C: Clock + Clone,
{
    breaker: &'a CircuitBreaker<L, C>,
    settled: bool,
}

impl<'a, L, C> Permit<'a, L, C>
where
    L: Limiter,
    C: Clock + Clone,
{
    fn new(breaker: &'a CircuitBreaker<L, C>) -> Self {
        Self {
            breaker,
            settled: false,
        }
    }

    /// Records that the protected call succeeded.
    pub fn success(mut self) {
        self.breaker.record(true);
        self.settled = true;
    }

    /// Records that the protected call failed.
    pub fn failure(mut self) {
        self.breaker.record(false);
        self.settled = true;
    }
}

impl<L, C> Drop for Permit<'_, L, C>
where
    L: Limiter,
    C: Clock + Clone,
{
    fn drop(&mut self) {
        if !self.settled {
            self.breaker.record(false);
        }
    }
}

/// Builder for a [`CircuitBreaker`].
#[derive(Debug, Clone, Copy)]
pub struct CircuitBreakerBuilder {
    trip: Trip,
    cooldown: Duration,
    half_open_trials: u32,
    half_open_required: u32,
}

impl Default for CircuitBreakerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CircuitBreakerBuilder {
    /// Creates a builder with the default policy: [`Trip::Consecutive(5)`](Trip::Consecutive),
    /// a 30-second cooldown, and a single trial that must succeed to close.
    #[must_use]
    pub fn new() -> Self {
        Self {
            trip: Trip::Consecutive(5),
            cooldown: Duration::from_secs(30),
            half_open_trials: 1,
            half_open_required: 1,
        }
    }

    /// Sets the condition under which the breaker trips open.
    #[must_use]
    pub fn trip(mut self, trip: Trip) -> Self {
        self.trip = trip;
        self
    }

    /// Sets how long the breaker stays open before admitting a trial request.
    #[must_use]
    pub fn cooldown(mut self, cooldown: Duration) -> Self {
        self.cooldown = cooldown;
        self
    }

    /// Sets how many trial requests may run concurrently while half-open, and
    /// how many must succeed to close. `trials` and `required` are clamped to at
    /// least one; `required` is clamped to at most `trials`.
    #[must_use]
    pub fn half_open(mut self, trials: u32, required: u32) -> Self {
        self.half_open_trials = trials.max(1);
        self.half_open_required = required.max(1).min(self.half_open_trials);
        self
    }

    /// Wraps `limiter`, producing a breaker driven by the system clock.
    #[must_use]
    pub fn build<L>(self, limiter: L) -> CircuitBreaker<L, SystemClock>
    where
        L: Limiter,
    {
        CircuitBreaker::new(
            limiter,
            Config {
                trip: self.trip,
                cooldown: self.cooldown,
                half_open_trials: self.half_open_trials,
                half_open_required: self.half_open_required,
            },
            SystemClock::new(),
        )
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::{BreakerState, CircuitBreaker, Trip};
    use crate::throttle::Throttle;
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_breaker_is_send_sync() {
        assert_send_sync::<CircuitBreaker<Throttle>>();
    }

    fn breaker(
        trip: Trip,
        cooldown: Duration,
        clock: Arc<ManualClock>,
    ) -> CircuitBreaker<Throttle, Arc<ManualClock>> {
        CircuitBreaker::builder()
            .trip(trip)
            .cooldown(cooldown)
            .half_open(1, 1)
            .build(Throttle::per_second(1_000_000))
            .with_clock(clock)
    }

    #[test]
    fn test_consecutive_failures_trip_open() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(3), Duration::from_secs(10), clock);

        assert_eq!(cb.state(), BreakerState::Closed);
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), BreakerState::Closed);
        cb.record_failure(); // third in a row
        assert_eq!(cb.state(), BreakerState::Open);
    }

    #[test]
    fn test_success_resets_consecutive_count() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(3), Duration::from_secs(10), clock);

        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // resets
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), BreakerState::Closed); // only two since reset
    }

    #[test]
    fn test_open_sheds_requests_without_touching_limiter() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(1), Duration::from_secs(10), clock);

        cb.record_failure(); // trips open
        assert_eq!(cb.state(), BreakerState::Open);

        let before = cb.inner().available();
        let result = cb.try_acquire();
        assert!(matches!(
            result,
            Err(crate::ThrottleError::CircuitOpen { .. })
        ));
        // The wrapped limiter was not consumed.
        assert_eq!(cb.inner().available(), before);
    }

    #[test]
    fn test_half_open_after_cooldown_then_close_on_success() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(1), Duration::from_secs(10), clock.clone());

        cb.record_failure(); // open
        assert_eq!(cb.state(), BreakerState::Open);

        clock.advance(Duration::from_secs(10)); // cooldown elapsed
        let permit = cb.try_acquire().unwrap().expect("a trial is admitted");
        assert_eq!(cb.state(), BreakerState::HalfOpen);
        permit.success();
        assert_eq!(cb.state(), BreakerState::Closed);
    }

    #[test]
    fn test_half_open_failure_reopens() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(1), Duration::from_secs(10), clock.clone());

        cb.record_failure(); // open
        clock.advance(Duration::from_secs(10));
        let permit = cb.try_acquire().unwrap().expect("a trial is admitted");
        assert_eq!(cb.state(), BreakerState::HalfOpen);
        permit.failure(); // trial failed
        assert_eq!(cb.state(), BreakerState::Open);
    }

    #[test]
    fn test_open_rejects_until_cooldown_elapses() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(1), Duration::from_secs(10), clock.clone());

        cb.record_failure(); // open
        clock.advance(Duration::from_secs(9)); // not yet
        assert!(matches!(
            cb.try_acquire(),
            Err(crate::ThrottleError::CircuitOpen { .. })
        ));
        clock.advance(Duration::from_secs(1)); // now cooled down
        assert!(cb.try_acquire().unwrap().is_some());
    }

    #[test]
    fn test_dropping_permit_counts_as_failure() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(2), Duration::from_secs(10), clock);

        // Two acquired-but-dropped permits count as two failures and trip it.
        drop(cb.try_acquire().unwrap());
        assert_eq!(cb.state(), BreakerState::Closed);
        drop(cb.try_acquire().unwrap());
        assert_eq!(cb.state(), BreakerState::Open);
    }

    #[test]
    fn test_ratio_trip() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(
            Trip::Ratio {
                window: 10,
                ratio: 0.5,
                min_calls: 4,
            },
            Duration::from_secs(10),
            clock,
        );

        cb.record_success();
        cb.record_success();
        assert_eq!(cb.state(), BreakerState::Closed);
        cb.record_failure();
        cb.record_failure(); // 2/4 = 0.5 with 4 calls
        assert_eq!(cb.state(), BreakerState::Open);
    }

    #[test]
    fn test_windowed_trip_prunes_old_failures() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(
            Trip::Windowed {
                failures: 3,
                period: Duration::from_secs(5),
            },
            Duration::from_secs(10),
            clock.clone(),
        );

        cb.record_failure();
        clock.advance(Duration::from_secs(6)); // first failure ages out of the window
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), BreakerState::Closed); // only 2 within 5s
        cb.record_failure();
        assert_eq!(cb.state(), BreakerState::Open); // 3 within 5s
    }
}
