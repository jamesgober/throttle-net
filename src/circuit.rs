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
//! Outcomes are reported back through a [`Permit`] (or an [`OwnedPermit`] that
//! can move into a spawned task): [`acquire`](CircuitBreaker::acquire) hands you
//! one, and you call [`success`](Permit::success) or [`failure`](Permit::failure)
//! after the call — or let [`call`](CircuitBreaker::call) do all of it. Dropping
//! a permit without settling it counts as a failure, so an early return, a
//! timeout, or a panic is treated conservatively.
//!
//! Every permit is stamped with the breaker's state *generation*, so the outcome
//! of a slow call admitted before a transition never counts toward the state
//! after it: a call admitted while closed that fails during a half-open probe
//! neither reopens the breaker nor frees a probe slot it never held.

use core::time::Duration;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use clock_lib::{Clock, Monotonic, SystemClock};

use crate::decision::Decision;
use crate::error::ThrottleError;
use crate::limiter::Limiter;
use crate::obs::{self, Label};

/// The limiter kind reported in metrics and events.
const KIND: &str = "circuit_breaker";

/// The condition under which a closed breaker trips open.
///
/// Out-of-range parameters are normalised when the breaker is built (see each
/// variant), so no configuration can make it panic or never trip by accident.
///
/// `#[non_exhaustive]`: more trip conditions may be added.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Trip {
    /// Trip after this many consecutive failures (a success resets the count).
    /// `0` is treated as `1`.
    Consecutive(u32),
    /// Trip when the failure ratio over the last `window` calls reaches `ratio`,
    /// once at least `min_calls` have been observed.
    ///
    /// A `window` of `0` is treated as `1`; `ratio` is clamped to
    /// `[0.0, 1.0]`, and NaN is treated as `1.0` (trip only when every recent
    /// call failed). `min_calls` is clamped to `window`, since no more calls
    /// than that are ever counted.
    Ratio {
        /// How many recent calls to consider.
        window: u32,
        /// The failure fraction in `[0.0, 1.0]` that trips the breaker.
        ratio: f64,
        /// The minimum calls before the ratio is evaluated.
        min_calls: u32,
    },
    /// Trip when at least `failures` failures occur within a rolling `period`.
    /// `0` failures is treated as `1`.
    Windowed {
        /// The failure count that trips the breaker.
        failures: u32,
        /// The rolling time window the failures are counted in.
        period: Duration,
    },
}

impl Trip {
    /// The trip condition with out-of-range parameters normalised.
    fn normalised(self) -> Self {
        match self {
            Self::Consecutive(n) => Self::Consecutive(n.max(1)),
            Self::Ratio {
                window,
                ratio,
                min_calls,
            } => {
                let window = window.max(1);
                Self::Ratio {
                    window,
                    ratio: if ratio.is_nan() {
                        1.0
                    } else {
                        ratio.clamp(0.0, 1.0)
                    },
                    min_calls: min_calls.min(window),
                }
            }
            Self::Windowed { failures, period } => Self::Windowed {
                failures: failures.max(1),
                period,
            },
        }
    }
}

/// The breaker's current state, as a snapshot.
///
/// `#[non_exhaustive]`: matching should include a wildcard.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BreakerState {
    /// Requests flow to the wrapped limiter; failures are being counted.
    Closed,
    /// Requests are shed immediately; the downstream is being given time.
    Open,
    /// A few trial requests are allowed to test whether the downstream recovered.
    HalfOpen,
}

impl BreakerState {
    /// The state's name and gauge code.
    const fn describe(self) -> (&'static str, u8) {
        match self {
            Self::Closed => ("Closed", 0),
            Self::HalfOpen => ("HalfOpen", 1),
            Self::Open => ("Open", 2),
        }
    }
}

/// The mutable state machine, guarded by a single mutex.
struct Shared {
    state: BreakerState,
    /// Incremented on every transition; permits carry the value they were
    /// admitted under, and outcomes from an older generation are ignored.
    generation: u64,
    /// Consecutive failures (for [`Trip::Consecutive`]).
    consecutive: u32,
    /// Recent outcomes, `true` = failure (for [`Trip::Ratio`]).
    outcomes: VecDeque<bool>,
    /// Failure timestamps in milliseconds (for [`Trip::Windowed`]).
    failure_times: VecDeque<u64>,
    /// Trial requests in flight while half-open.
    half_open_inflight: u32,
    /// How many of those trials were admitted through the [`Limiter`] trait
    /// (`acquire_cost` / `reserve`), which issues no permit: a trait
    /// [`refund`](Limiter::refund) or a manual `record_*` releases one of them
    /// (TN2-003, TN2-015).
    trait_probes: u32,
    /// Successful trials accumulated while half-open.
    half_open_successes: u32,
    /// Milliseconds (breaker epoch) at which an open breaker may go half-open.
    open_until_ms: u64,
}

impl Shared {
    fn new() -> Self {
        Self {
            state: BreakerState::Closed,
            generation: 0,
            consecutive: 0,
            outcomes: VecDeque::new(),
            failure_times: VecDeque::new(),
            half_open_inflight: 0,
            trait_probes: 0,
            half_open_successes: 0,
            open_until_ms: 0,
        }
    }

    /// Clears every counter (on closing, and on a manual reset).
    fn reset_counters(&mut self) {
        self.consecutive = 0;
        self.outcomes.clear();
        self.failure_times.clear();
        self.half_open_inflight = 0;
        self.trait_probes = 0;
        self.half_open_successes = 0;
    }
}

/// A breaker admission: the generation it was made under and whether it holds
/// a half-open trial slot.
#[derive(Debug, Clone, Copy)]
struct Ticket {
    generation: u64,
    probe: bool,
}

/// A circuit breaker wrapping a limiter `L`, timed by clock `C`.
///
/// Construct one with [`CircuitBreaker::builder`]. Behind the `circuit-breaker`
/// feature.
///
/// The breaker is itself a [`Limiter`], so it composes inside a
/// [`Hybrid`](crate::Hybrid) or as a [`Layered`](crate::Layered) global scope.
/// Requests admitted through the trait carry no permit, so report their
/// outcomes with [`record_success`](Self::record_success) /
/// [`record_failure`](Self::record_failure).
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "runtime")]
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
    label: Label,
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
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{BreakerState, CircuitBreaker, Throttle};
    ///
    /// let breaker = CircuitBreaker::builder().build(Throttle::per_second(10));
    /// assert_eq!(breaker.state(), BreakerState::Closed);
    /// ```
    pub fn builder() -> CircuitBreakerBuilder {
        CircuitBreakerBuilder::new()
    }
}

impl<L, C> CircuitBreaker<L, C>
where
    L: Limiter,
    C: Clock,
{
    fn new(inner: L, config: Config, clock: C, label: Label) -> Self {
        let epoch = clock.now();
        obs::circuit_state(&label, None, "Closed", 0);
        Self {
            inner,
            config,
            shared: Mutex::new(Shared::new()),
            clock,
            epoch,
            label,
        }
    }

    /// Replaces the time source (the cooldown clock), for deterministic tests.
    /// The breaker is reset to closed around the new clock; the name is kept.
    #[must_use]
    pub fn with_clock<C2>(self, clock: C2) -> CircuitBreaker<L, C2>
    where
        C2: Clock,
    {
        CircuitBreaker::new(self.inner, self.config, clock, self.label)
    }

    /// Names this instance for metrics and events.
    #[must_use]
    pub fn with_name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
        obs::circuit_state(&self.label, None, "Closed", self.lock().state.describe().1);
        self
    }

    /// The instance name, if one was set.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.label.get()
    }

    /// The current state (a momentary snapshot). An open breaker whose cooldown
    /// has elapsed still reports `Open` until a request is admitted as a trial.
    #[must_use]
    pub fn state(&self) -> BreakerState {
        self.lock().state
    }

    /// A shared reference to the wrapped limiter.
    pub fn inner(&self) -> &L {
        &self.inner
    }

    /// Forces the breaker closed and clears its failure history, for example
    /// after an operator has confirmed the downstream is healthy.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{BreakerState, CircuitBreaker, Throttle, Trip};
    ///
    /// let breaker = CircuitBreaker::builder().trip(Trip::Consecutive(1)).build(Throttle::per_second(5));
    /// breaker.record_failure();
    /// assert_eq!(breaker.state(), BreakerState::Open);
    /// breaker.reset();
    /// assert_eq!(breaker.state(), BreakerState::Closed);
    /// ```
    pub fn reset(&self) {
        let mut shared = self.lock();
        let from = shared.state;
        shared.reset_counters();
        self.transition(&mut shared, from, BreakerState::Closed);
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

    /// Moves to `to`, bumping the generation and reporting the change.
    fn transition(&self, shared: &mut Shared, from: BreakerState, to: BreakerState) {
        shared.state = to;
        shared.generation = shared.generation.wrapping_add(1);
        let (to_name, code) = to.describe();
        obs::circuit_state(&self.label, Some(from.describe().0), to_name, code);
    }

    /// Decides admission and performs any transition the clock has earned
    /// (open → half-open). Reserves a half-open trial slot when it admits one.
    fn admit(&self) -> Result<Ticket, Duration> {
        self.admit_as(false)
    }

    /// [`admit`](Self::admit), counting a half-open trial taken through the
    /// [`Limiter`] trait (which issues no permit) when `via_trait` is set.
    fn admit_as(&self, via_trait: bool) -> Result<Ticket, Duration> {
        let now_ms = self.now_ms();
        let mut shared = self.lock();
        let ticket = self.admit_locked(&mut shared, now_ms)?;
        if ticket.probe && via_trait {
            shared.trait_probes = shared.trait_probes.saturating_add(1);
        }
        Ok(ticket)
    }

    /// The admission decision, under the lock.
    fn admit_locked(&self, shared: &mut Shared, now_ms: u64) -> Result<Ticket, Duration> {
        match shared.state {
            BreakerState::Closed => Ok(Ticket {
                generation: shared.generation,
                probe: false,
            }),
            BreakerState::Open if now_ms >= shared.open_until_ms => {
                self.transition(shared, BreakerState::Open, BreakerState::HalfOpen);
                shared.half_open_inflight = 1;
                shared.trait_probes = 0;
                shared.half_open_successes = 0;
                Ok(Ticket {
                    generation: shared.generation,
                    probe: true,
                })
            }
            BreakerState::Open => Err(Duration::from_millis(shared.open_until_ms - now_ms)),
            BreakerState::HalfOpen if shared.half_open_inflight < self.config.half_open_trials => {
                shared.half_open_inflight += 1;
                Ok(Ticket {
                    generation: shared.generation,
                    probe: true,
                })
            }
            // Trials are already in flight; shed extra probes.
            BreakerState::HalfOpen => Err(Duration::ZERO),
        }
    }

    /// [`admit`](Self::admit), with a rejection reported as an error and
    /// counted as shed.
    fn admit_or_shed(&self) -> Result<Ticket, ThrottleError> {
        self.admit().map_err(|retry_after| {
            obs::shed(KIND, &self.label);
            ThrottleError::CircuitOpen { retry_after }
        })
    }

    /// Releases a half-open trial slot that never reached the downstream (the
    /// wrapped limiter refused, or the waiting acquire was cancelled).
    fn abort(&self, ticket: Ticket) {
        self.abort_as(ticket, false);
    }

    /// [`abort`](Self::abort) for a trial admitted through the [`Limiter`]
    /// trait when `via_trait` is set.
    fn abort_as(&self, ticket: Ticket, via_trait: bool) {
        if !ticket.probe {
            return;
        }
        let mut shared = self.lock();
        if shared.generation == ticket.generation && shared.state == BreakerState::HalfOpen {
            shared.half_open_inflight = shared.half_open_inflight.saturating_sub(1);
            if via_trait {
                shared.trait_probes = shared.trait_probes.saturating_sub(1);
            }
        }
    }

    /// Releases one half-open trial taken through the [`Limiter`] trait whose
    /// request never happened (a composite rolled back or was cancelled).
    fn release_trait_probe(&self) {
        let mut shared = self.lock();
        if shared.state == BreakerState::HalfOpen && shared.trait_probes > 0 {
            shared.trait_probes -= 1;
            shared.half_open_inflight = shared.half_open_inflight.saturating_sub(1);
        }
    }

    /// Whether a waiter admitted with `ticket` may still go after its wait: not
    /// if the breaker has since moved to a state that would shed it (TN2-013).
    #[cfg(feature = "runtime")]
    fn still_admits(&self, ticket: Ticket) -> Result<(), Duration> {
        let now_ms = self.now_ms();
        let shared = self.lock();
        if shared.generation == ticket.generation || shared.state == BreakerState::Closed {
            return Ok(());
        }
        Err(match shared.state {
            BreakerState::Open => {
                Duration::from_millis(shared.open_until_ms.saturating_sub(now_ms))
            }
            _ => Duration::ZERO,
        })
    }

    /// Records the outcome of a settled permit.
    fn settle(&self, ticket: Ticket, success: bool) {
        let now_ms = self.now_ms();
        let mut shared = self.lock();
        if shared.generation != ticket.generation {
            // Admitted under an earlier state: says nothing about this one.
            return;
        }
        self.apply(&mut shared, success, now_ms, ticket.probe);
    }

    /// Applies an outcome to the current state. `probe` says whether it holds a
    /// half-open trial slot to release.
    fn apply(&self, shared: &mut Shared, success: bool, now_ms: u64, probe: bool) {
        match shared.state {
            BreakerState::HalfOpen => {
                if probe {
                    shared.half_open_inflight = shared.half_open_inflight.saturating_sub(1);
                }
                if success {
                    shared.half_open_successes += 1;
                    if shared.half_open_successes >= self.config.half_open_required {
                        shared.reset_counters();
                        self.transition(shared, BreakerState::HalfOpen, BreakerState::Closed);
                    }
                } else {
                    self.open(shared, now_ms);
                }
            }
            BreakerState::Closed => {
                if success {
                    shared.consecutive = 0;
                } else {
                    shared.consecutive = shared.consecutive.saturating_add(1);
                }
                record_outcome(shared, !success, now_ms, self.config.trip);
                if !success && tripped(shared, now_ms, self.config.trip) {
                    self.open(shared, now_ms);
                }
            }
            // Nothing is admitted while fully open; an outcome now is stale.
            BreakerState::Open => {}
        }
    }

    /// Moves the breaker to open and arms the cooldown.
    fn open(&self, shared: &mut Shared, now_ms: u64) {
        let from = shared.state;
        shared.open_until_ms = now_ms
            .saturating_add(u64::try_from(self.config.cooldown.as_millis()).unwrap_or(u64::MAX));
        shared.half_open_inflight = 0;
        shared.trait_probes = 0;
        shared.half_open_successes = 0;
        self.transition(shared, from, BreakerState::Open);
    }

    /// Reports a successful protected call made without a permit (for example
    /// through the [`Limiter`] trait). Prefer settling a [`Permit`].
    ///
    /// While half-open, the outcome counts as a trial result. It frees a trial
    /// slot only if one is held by a trial admitted through the [`Limiter`]
    /// trait: a call that never held a slot cannot free another's.
    pub fn record_success(&self) {
        self.record_manual(true);
    }

    /// Reports a failed protected call made without a permit. Prefer settling
    /// a [`Permit`].
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{BreakerState, CircuitBreaker, Throttle, Trip};
    ///
    /// let breaker = CircuitBreaker::builder().trip(Trip::Consecutive(2)).build(Throttle::per_second(5));
    /// breaker.record_failure();
    /// breaker.record_failure();
    /// assert_eq!(breaker.state(), BreakerState::Open);
    /// ```
    pub fn record_failure(&self) {
        self.record_manual(false);
    }

    fn record_manual(&self, success: bool) {
        let now_ms = self.now_ms();
        let mut shared = self.lock();
        let probe = shared.state == BreakerState::HalfOpen && shared.trait_probes > 0;
        if probe {
            shared.trait_probes -= 1;
        }
        self.apply(&mut shared, success, now_ms, probe);
    }

    /// Attempts to admit a request without waiting, returning a [`Permit`] on
    /// success.
    ///
    /// Returns `Ok(None)` when the breaker would admit but the wrapped limiter
    /// has no token available right now (normal rate limiting, not a breaker
    /// fault).
    ///
    /// # Errors
    ///
    /// - [`ThrottleError::CircuitOpen`] when the breaker is open (or its
    ///   half-open trials are full): the request is shed without touching the
    ///   wrapped limiter.
    /// - [`ThrottleError::CostExceedsCapacity`] when the wrapped limiter can
    ///   never grant a single unit.
    pub fn try_acquire(&self) -> Result<Option<Permit<'_, L, C>>, ThrottleError> {
        self.try_admit()
            .map(|ticket| ticket.map(|ticket| Permit::new(self, ticket)))
    }

    /// The non-waiting admission shared by the borrowed and owned permits.
    fn try_admit(&self) -> Result<Option<Ticket>, ThrottleError> {
        let ticket = self.admit_or_shed()?;
        match self.inner.acquire_cost(1) {
            Decision::Acquired => {
                obs::granted(KIND, &self.label, 1);
                Ok(Some(ticket))
            }
            Decision::Retry { .. } => {
                self.abort(ticket);
                obs::denied(KIND, &self.label);
                Ok(None)
            }
            _ => {
                self.abort(ticket);
                Err(ThrottleError::CostExceedsCapacity {
                    cost: 1,
                    capacity: self.inner.capacity(),
                })
            }
        }
    }
}

impl<L, C> CircuitBreaker<L, C>
where
    L: Limiter + 'static,
    C: Clock + 'static,
{
    /// Like [`try_acquire`](Self::try_acquire), returning an [`OwnedPermit`]
    /// that holds a reference count on the breaker, so it can move into a
    /// spawned task.
    ///
    /// # Errors
    ///
    /// As [`try_acquire`](Self::try_acquire).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use throttle_net::{CircuitBreaker, Throttle};
    ///
    /// let breaker = Arc::new(CircuitBreaker::builder().build(Throttle::per_second(10)));
    /// let permit = breaker.try_acquire_owned()?.expect("a token is free");
    /// std::thread::spawn(move || permit.success()).join().unwrap();
    /// # Ok::<(), throttle_net::ThrottleError>(())
    /// ```
    pub fn try_acquire_owned(self: &Arc<Self>) -> Result<Option<OwnedPermit<L, C>>, ThrottleError> {
        self.try_admit()
            .map(|ticket| ticket.map(|ticket| OwnedPermit::new(Arc::clone(self), ticket)))
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl<L, C> CircuitBreaker<L, C>
where
    L: Limiter,
    C: Clock,
{
    /// Admits a request, failing fast if the breaker is open and otherwise
    /// pacing on the wrapped limiter until a token is free.
    ///
    /// A circuit-open condition returns immediately (load shedding); a plain
    /// rate limit waits. Returns a [`Permit`] to settle with the call's outcome.
    ///
    /// **Cancel-safe:** dropping the future while it waits on the limiter
    /// releases the half-open trial slot it holds (if any) and returns any
    /// reserved token, so a timed-out probe cannot leave the breaker stuck
    /// half-open.
    ///
    /// # Errors
    ///
    /// - [`ThrottleError::CircuitOpen`] when the breaker is open or its trials
    ///   are full — returned without waiting.
    /// - [`ThrottleError::CostExceedsCapacity`] when the wrapped limiter can
    ///   never grant the request.
    pub async fn acquire(&self) -> Result<Permit<'_, L, C>, ThrottleError> {
        let ticket = self.admit_waiting(None).await?;
        Ok(Permit::new(self, ticket))
    }

    /// Like [`acquire`](Self::acquire), but waits at most `timeout` on the
    /// wrapped limiter (giving up at once if its token is known not to come in
    /// time).
    ///
    /// # Errors
    ///
    /// As [`acquire`](Self::acquire), plus [`ThrottleError::DeadlineExceeded`].
    pub async fn acquire_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Permit<'_, L, C>, ThrottleError> {
        let ticket = self.admit_waiting(Some(timeout)).await?;
        Ok(Permit::new(self, ticket))
    }

    /// Runs `operation` under the breaker: admits (failing fast while open),
    /// awaits the operation, and settles the permit from its result — `Ok` as a
    /// success, `Err` as a failure. If the returned future is dropped mid-call,
    /// the call counts as a failure (a timed-out call is a failed one) — so do
    /// not race `call` futures against each other (a hedge, a `select!`): the
    /// losers would trip the breaker on latency alone. Settle those with
    /// [`Permit::cancel`] instead.
    ///
    /// # Errors
    ///
    /// [`CallError::Rejected`] when the breaker (or the wrapped limiter) refused
    /// the call — the operation never ran; [`CallError::Failed`] with the
    /// operation's own error otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// use throttle_net::{CallError, CircuitBreaker, Throttle, Trip};
    ///
    /// let breaker = CircuitBreaker::builder()
    ///     .trip(Trip::Consecutive(1))
    ///     .build(Throttle::per_second(10));
    ///
    /// let first: Result<(), CallError<&str>> = breaker.call(|| async { Err("boom") }).await;
    /// assert!(matches!(first, Err(CallError::Failed("boom"))));
    ///
    /// // The failure tripped the breaker: the next call is shed unrun.
    /// let second: Result<(), CallError<&str>> = breaker.call(|| async { Ok(()) }).await;
    /// assert!(matches!(second, Err(CallError::Rejected(_))));
    /// # }
    /// ```
    pub async fn call<F, Fut, T, E>(&self, operation: F) -> Result<T, CallError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: core::future::Future<Output = Result<T, E>>,
    {
        let permit = self.acquire().await.map_err(CallError::Rejected)?;
        Self::settle_call(permit, operation).await
    }

    /// Like [`call`](Self::call), but waits at most `timeout` to be admitted
    /// (on the wrapped limiter). The operation itself is not timed: wrap it in
    /// [`resilience::timeout`](crate::resilience::timeout) for that (a call
    /// that times out then counts as a failure).
    ///
    /// # Errors
    ///
    /// [`CallError::Rejected`] when the breaker or the wrapped limiter refused
    /// the call, including [`ThrottleError::DeadlineExceeded`] when it could
    /// not be admitted within `timeout` — the operation never ran;
    /// [`CallError::Failed`] with the operation's own error otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// use std::time::Duration;
    /// use throttle_net::{CallError, CircuitBreaker, Throttle, ThrottleError};
    ///
    /// let breaker = CircuitBreaker::builder().build(Throttle::per_duration(1, Duration::from_secs(60)));
    /// let first: Result<u8, CallError<()>> =
    ///     breaker.call_timeout(Duration::from_millis(10), || async { Ok(1) }).await;
    /// assert!(matches!(first, Ok(1)));
    /// // The next unit is a minute away: refused at once, unrun.
    /// let second: Result<u8, CallError<()>> =
    ///     breaker.call_timeout(Duration::from_millis(10), || async { Ok(2) }).await;
    /// assert!(matches!(second, Err(CallError::Rejected(ThrottleError::DeadlineExceeded))));
    /// # }
    /// ```
    pub async fn call_timeout<F, Fut, T, E>(
        &self,
        timeout: Duration,
        operation: F,
    ) -> Result<T, CallError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: core::future::Future<Output = Result<T, E>>,
    {
        let permit = self
            .acquire_timeout(timeout)
            .await
            .map_err(CallError::Rejected)?;
        Self::settle_call(permit, operation).await
    }

    /// Runs `operation` under `permit` and settles it from the result.
    async fn settle_call<F, Fut, T, E>(
        permit: Permit<'_, L, C>,
        operation: F,
    ) -> Result<T, CallError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: core::future::Future<Output = Result<T, E>>,
    {
        match operation().await {
            Ok(value) => {
                permit.success();
                Ok(value)
            }
            Err(error) => {
                permit.failure();
                Err(CallError::Failed(error))
            }
        }
    }

    /// Admission plus a cancel-safe wait on the wrapped limiter.
    async fn admit_waiting(&self, timeout: Option<Duration>) -> Result<Ticket, ThrottleError> {
        use crate::wait::{Refusal, Step, wait_for};
        let timer = obs::Timer::start();
        let ticket = match self.admit_or_shed() {
            Ok(ticket) => ticket,
            Err(err) => return Err(err),
        };
        // Releases the trial slot unless the wait succeeds, including when this
        // future is dropped mid-wait (TN-002).
        let slot = SlotGuard {
            breaker: self,
            ticket: Some(ticket),
        };
        let result = wait_for(
            |budget| match self.inner.reserve(1, budget) {
                Some(wait) => Step::Granted(wait),
                None => match self.inner.peek(1) {
                    Decision::Retry { after } => Step::Retry(after),
                    Decision::Acquired => Step::Retry(Duration::ZERO),
                    _ => Step::Never,
                },
            },
            || self.inner.refund(1),
            || self.inner.paused_for(),
            timeout,
        )
        .await
        .map_err(|refusal| match refusal {
            Refusal::Never => ThrottleError::CostExceedsCapacity {
                cost: 1,
                capacity: self.inner.capacity(),
            },
            Refusal::TimedOut => ThrottleError::DeadlineExceeded,
        })
        .and_then(|()| {
            // A long (queued) wait may outlive the state it was admitted in:
            // if the breaker has opened meanwhile, shed instead of sending to
            // the downstream it opened on (TN2-013).
            let ticket = slot.disarm();
            match self.still_admits(ticket) {
                Ok(()) => Ok(ticket),
                Err(retry_after) => {
                    self.inner.refund(1);
                    self.abort(ticket);
                    obs::shed(KIND, &self.label);
                    Err(ThrottleError::CircuitOpen { retry_after })
                }
            }
        });
        obs::waited(KIND, &self.label, &timer, 1, obs::Waited::of(&result));
        result
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl<L, C> CircuitBreaker<L, C>
where
    L: Limiter + 'static,
    C: Clock + 'static,
{
    /// Like [`acquire`](Self::acquire), returning an [`OwnedPermit`] that can
    /// move into a spawned task.
    ///
    /// # Errors
    ///
    /// As [`acquire`](Self::acquire).
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use std::sync::Arc;
    /// use throttle_net::{CircuitBreaker, Throttle};
    ///
    /// let breaker = Arc::new(CircuitBreaker::builder().build(Throttle::per_second(10)));
    /// let permit = breaker.acquire_owned().await?;
    /// tokio::spawn(async move {
    ///     // ... call the downstream ...
    ///     permit.success();
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire_owned(self: &Arc<Self>) -> Result<OwnedPermit<L, C>, ThrottleError> {
        let ticket = self.admit_waiting(None).await?;
        Ok(OwnedPermit::new(Arc::clone(self), ticket))
    }

    /// Like [`acquire_timeout`](Self::acquire_timeout), returning an
    /// [`OwnedPermit`].
    ///
    /// # Errors
    ///
    /// As [`acquire_timeout`](Self::acquire_timeout).
    pub async fn acquire_owned_timeout(
        self: &Arc<Self>,
        timeout: Duration,
    ) -> Result<OwnedPermit<L, C>, ThrottleError> {
        let ticket = self.admit_waiting(Some(timeout)).await?;
        Ok(OwnedPermit::new(Arc::clone(self), ticket))
    }
}

/// Releases a half-open trial slot unless disarmed: the cancellation path of a
/// waiting admission.
#[cfg(feature = "runtime")]
struct SlotGuard<'a, L, C>
where
    L: Limiter,
    C: Clock,
{
    breaker: &'a CircuitBreaker<L, C>,
    ticket: Option<Ticket>,
}

#[cfg(feature = "runtime")]
impl<L, C> SlotGuard<'_, L, C>
where
    L: Limiter,
    C: Clock,
{
    /// The wait succeeded; the slot passes to a permit.
    fn disarm(mut self) -> Ticket {
        self.ticket.take().unwrap_or(Ticket {
            generation: u64::MAX,
            probe: false,
        })
    }
}

#[cfg(feature = "runtime")]
impl<L, C> Drop for SlotGuard<'_, L, C>
where
    L: Limiter,
    C: Clock,
{
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.breaker.abort(ticket);
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
                let _evicted = shared.outcomes.pop_front();
            }
        }
        Trip::Windowed { period, .. } => {
            if failure {
                shared.failure_times.push_back(now_ms);
            }
            let cutoff =
                now_ms.saturating_sub(u64::try_from(period.as_millis()).unwrap_or(u64::MAX));
            while shared.failure_times.front().is_some_and(|&t| t < cutoff) {
                let _evicted = shared.failure_times.pop_front();
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
            let total = shared.outcomes.len();
            if total == 0 || total < min_calls as usize {
                return false;
            }
            let failures = shared.outcomes.iter().filter(|&&f| f).count();
            // Counts are bounded by the window (a u32), so both convert exactly.
            #[allow(clippy::cast_precision_loss)]
            let observed = failures as f64 / total as f64;
            observed >= ratio
        }
        Trip::Windowed { failures, period } => {
            let cutoff =
                now_ms.saturating_sub(u64::try_from(period.as_millis()).unwrap_or(u64::MAX));
            let recent = shared
                .failure_times
                .iter()
                .filter(|&&t| t >= cutoff)
                .count();
            recent >= failures as usize
        }
    }
}

/// Why [`CircuitBreaker::call`] did not return the operation's value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError<E> {
    /// The breaker or the wrapped limiter refused the call; it never ran.
    Rejected(ThrottleError),
    /// The call ran and failed with its own error (recorded as a failure).
    Failed(E),
}

impl<E: core::fmt::Display> core::fmt::Display for CallError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Rejected(err) => write!(f, "call rejected: {err}"),
            Self::Failed(err) => write!(f, "call failed: {err}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CallError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rejected(err) => Some(err),
            Self::Failed(err) => Some(err),
        }
    }
}

/// A reserved permission to make one protected call.
///
/// Settle it with [`success`](Self::success) or [`failure`](Self::failure) after
/// the call returns. If dropped unsettled — an early return, a `?`, a timeout,
/// or a panic — it records a **failure**, so the breaker errs toward protecting
/// the downstream. Its outcome only counts toward the state it was admitted in.
#[must_use = "settle the permit with `.success()` or `.failure()`; dropping it counts as a failure"]
pub struct Permit<'a, L, C>
where
    L: Limiter,
    C: Clock,
{
    breaker: &'a CircuitBreaker<L, C>,
    ticket: Ticket,
    settled: bool,
}

impl<'a, L, C> Permit<'a, L, C>
where
    L: Limiter,
    C: Clock,
{
    fn new(breaker: &'a CircuitBreaker<L, C>, ticket: Ticket) -> Self {
        Self {
            breaker,
            ticket,
            settled: false,
        }
    }

    /// Records that the protected call succeeded.
    pub fn success(mut self) {
        self.settled = true;
        self.breaker.settle(self.ticket, true);
    }

    /// Records that the protected call failed.
    pub fn failure(mut self) {
        self.settled = true;
        self.breaker.settle(self.ticket, false);
    }

    /// Gives the admission back **without an outcome**: the call was never
    /// made, or was abandoned before it reached the downstream (a hedge that
    /// lost, a request cancelled before sending). Frees a half-open trial slot
    /// and refunds the wrapped limiter's unit; the breaker records nothing.
    pub fn cancel(mut self) {
        self.settled = true;
        self.breaker.abort(self.ticket);
        self.breaker.inner.refund(1);
    }
}

impl<L, C> Drop for Permit<'_, L, C>
where
    L: Limiter,
    C: Clock,
{
    fn drop(&mut self) {
        if !self.settled {
            self.breaker.settle(self.ticket, false);
        }
    }
}

impl<L, C> core::fmt::Debug for Permit<'_, L, C>
where
    L: Limiter,
    C: Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Permit")
            .field("trial", &self.ticket.probe)
            .finish_non_exhaustive()
    }
}

/// An owned [`Permit`]: it keeps the breaker alive through an [`Arc`], so it
/// can be moved into a spawned task. Same settling rules — dropping it unsettled
/// records a failure.
#[must_use = "settle the permit with `.success()` or `.failure()`; dropping it counts as a failure"]
pub struct OwnedPermit<L, C = SystemClock>
where
    L: Limiter,
    C: Clock,
{
    breaker: Arc<CircuitBreaker<L, C>>,
    ticket: Ticket,
    settled: bool,
}

impl<L, C> OwnedPermit<L, C>
where
    L: Limiter,
    C: Clock,
{
    fn new(breaker: Arc<CircuitBreaker<L, C>>, ticket: Ticket) -> Self {
        Self {
            breaker,
            ticket,
            settled: false,
        }
    }

    /// Records that the protected call succeeded.
    pub fn success(mut self) {
        self.settled = true;
        self.breaker.settle(self.ticket, true);
    }

    /// Records that the protected call failed.
    pub fn failure(mut self) {
        self.settled = true;
        self.breaker.settle(self.ticket, false);
    }

    /// Gives the admission back without an outcome. See [`Permit::cancel`].
    pub fn cancel(mut self) {
        self.settled = true;
        self.breaker.abort(self.ticket);
        self.breaker.inner.refund(1);
    }
}

impl<L, C> Drop for OwnedPermit<L, C>
where
    L: Limiter,
    C: Clock,
{
    fn drop(&mut self) {
        if !self.settled {
            self.breaker.settle(self.ticket, false);
        }
    }
}

impl<L, C> core::fmt::Debug for OwnedPermit<L, C>
where
    L: Limiter,
    C: Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("OwnedPermit")
            .field("trial", &self.ticket.probe)
            .finish_non_exhaustive()
    }
}

impl<L, C> Limiter for CircuitBreaker<L, C>
where
    L: Limiter,
    C: Clock,
{
    /// The breaker's verdict without any transition: an open breaker reports
    /// the remaining cooldown, a full half-open one a zero wait; otherwise the
    /// wrapped limiter's peek.
    fn peek(&self, cost: u32) -> Decision {
        let now_ms = self.now_ms();
        {
            let shared = self.lock();
            match shared.state {
                BreakerState::Open if now_ms < shared.open_until_ms => {
                    return Decision::Retry {
                        after: Duration::from_millis(shared.open_until_ms - now_ms),
                    };
                }
                BreakerState::HalfOpen
                    if shared.half_open_inflight >= self.config.half_open_trials =>
                {
                    return Decision::Retry {
                        after: Duration::ZERO,
                    };
                }
                _ => {}
            }
        }
        self.inner.peek(cost)
    }

    /// Admits through the breaker, then takes `cost` from the wrapped limiter.
    /// No permit is issued: report the outcome with
    /// [`record_success`](CircuitBreaker::record_success) /
    /// [`record_failure`](CircuitBreaker::record_failure).
    fn acquire_cost(&self, cost: u32) -> Decision {
        let ticket = match self.admit_as(true) {
            Ok(ticket) => ticket,
            Err(after) => {
                obs::shed(KIND, &self.label);
                return Decision::Retry { after };
            }
        };
        let decision = self.inner.acquire_cost(cost);
        if !decision.is_acquired() {
            self.abort_as(ticket, true);
        }
        decision
    }

    /// `0` while the breaker sheds; otherwise the wrapped limiter's count.
    fn available(&self) -> u32 {
        if self.peek(0).is_acquired() {
            self.inner.available()
        } else {
            0
        }
    }

    fn capacity(&self) -> u32 {
        self.inner.capacity()
    }

    /// Returns the units to the wrapped limiter and, while half-open, frees
    /// the trial slot the trait admission took: the request never happened (a
    /// composite rolled back, or its waiting acquire was cancelled), so it must
    /// not hold the breaker half-open (TN2-003).
    ///
    /// A refund carries no identity, so the breaker frees *a* trait-admitted
    /// trial slot of the current half-open period if one is held. A refund of
    /// units admitted earlier, while the breaker was closed, that arrives
    /// during a later half-open period (an estimate settled after a trip, say)
    /// therefore frees a live trial's slot, and one more trial than
    /// `half_open_trials` can run until that trial settles. Over-counting the
    /// other way could wedge the breaker half-open, so the approximation errs
    /// towards admitting. Permits ([`acquire`](CircuitBreaker::acquire)) carry
    /// their admission and are exact.
    fn refund(&self, cost: u32) {
        self.inner.refund(cost);
        self.release_trait_probe();
    }

    fn reserve(&self, cost: u32, max_wait: Duration) -> Option<Duration> {
        let ticket = self.admit_as(true).ok()?;
        let reserved = self.inner.reserve(cost, max_wait);
        if reserved.is_none() {
            self.abort_as(ticket, true);
        }
        reserved
    }

    fn pause_for(&self, duration: Duration) {
        self.inner.pause_for(duration);
    }

    fn paused_for(&self) -> Duration {
        self.inner.paused_for()
    }

    fn resume(&self) {
        self.inner.resume();
    }

    /// Charges the wrapped limiter; the breaker itself has nothing to charge.
    fn charge(&self, cost: u32) -> u32 {
        self.inner.charge(cost)
    }
}

impl<L, C> core::fmt::Debug for CircuitBreaker<L, C>
where
    L: Limiter,
    C: Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CircuitBreaker")
            .field("name", &self.label)
            .field("state", &self.state())
            .field("trip", &self.config.trip)
            .field("cooldown", &self.config.cooldown)
            .finish_non_exhaustive()
    }
}

/// Builder for a [`CircuitBreaker`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use throttle_net::{CircuitBreaker, Throttle, Trip};
///
/// let breaker = CircuitBreaker::builder()
///     .trip(Trip::Ratio { window: 20, ratio: 0.5, min_calls: 10 })
///     .cooldown(Duration::from_secs(15))
///     .half_open(3, 2) // up to 3 trials; 2 successes close it
///     .name("payments")
///     .build(Throttle::per_second(50));
/// assert_eq!(breaker.name(), Some("payments"));
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct CircuitBreakerBuilder {
    trip: Trip,
    cooldown: Duration,
    half_open_trials: u32,
    half_open_required: u32,
    label: Label,
}

impl Default for CircuitBreakerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CircuitBreakerBuilder {
    /// Creates a builder with the default policy: [`Trip::Consecutive(5)`](Trip::Consecutive),
    /// a 30-second cooldown, and a single trial that must succeed to close.
    pub fn new() -> Self {
        Self {
            trip: Trip::Consecutive(5),
            cooldown: Duration::from_secs(30),
            half_open_trials: 1,
            half_open_required: 1,
            label: Label::none(),
        }
    }

    /// Sets the condition under which the breaker trips open.
    pub fn trip(mut self, trip: Trip) -> Self {
        self.trip = trip;
        self
    }

    /// Sets how long the breaker stays open before admitting a trial request.
    pub fn cooldown(mut self, cooldown: Duration) -> Self {
        self.cooldown = cooldown;
        self
    }

    /// Sets how many trial requests may run concurrently while half-open, and
    /// how many must succeed to close. `trials` and `required` are clamped to at
    /// least one; `required` is clamped to at most `trials`.
    pub fn half_open(mut self, trials: u32, required: u32) -> Self {
        self.half_open_trials = trials.max(1);
        self.half_open_required = required.max(1).min(self.half_open_trials);
        self
    }

    /// Names the breaker for metrics and events.
    pub fn name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
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
                trip: self.trip.normalised(),
                cooldown: self.cooldown,
                half_open_trials: self.half_open_trials,
                half_open_required: self.half_open_required,
            },
            SystemClock::new(),
            self.label,
        )
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::{BreakerState, CallError, CircuitBreaker, Trip};
    use crate::decision::Decision;
    use crate::error::ThrottleError;
    use crate::limiter::Limiter;
    use crate::throttle::Throttle;
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    type Breaker = CircuitBreaker<Throttle, Arc<ManualClock>>;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_breaker_is_send_sync() {
        assert_send_sync::<CircuitBreaker<Throttle>>();
        assert_send_sync::<super::OwnedPermit<Throttle>>();
    }

    fn breaker(trip: Trip, cooldown: Duration, clock: Arc<ManualClock>) -> Breaker {
        CircuitBreaker::builder()
            .trip(trip)
            .cooldown(cooldown)
            .half_open(1, 1)
            .build(Throttle::per_second(1_000_000))
            .with_clock(clock)
    }

    fn tripped_breaker(clock: &Arc<ManualClock>) -> Breaker {
        let cb = breaker(
            Trip::Consecutive(1),
            Duration::from_secs(10),
            Arc::clone(clock),
        );
        cb.record_failure();
        assert_eq!(cb.state(), BreakerState::Open);
        cb
    }

    #[test]
    fn test_consecutive_failures_trip_open() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(3), Duration::from_secs(10), clock);
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), BreakerState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), BreakerState::Open);
    }

    #[test]
    fn test_success_resets_consecutive_count() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(3), Duration::from_secs(10), clock);
        cb.record_failure();
        cb.record_failure();
        cb.record_success();
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), BreakerState::Closed);
    }

    #[test]
    fn test_open_sheds_requests_without_touching_limiter() {
        let clock = Arc::new(ManualClock::new());
        let cb = tripped_breaker(&clock);
        let before = cb.inner().available();
        assert!(matches!(
            cb.try_acquire(),
            Err(ThrottleError::CircuitOpen { .. })
        ));
        assert_eq!(cb.inner().available(), before);
    }

    #[test]
    fn test_half_open_after_cooldown_then_close_on_success() {
        let clock = Arc::new(ManualClock::new());
        let cb = tripped_breaker(&clock);
        clock.advance(Duration::from_secs(10));
        let permit = cb.try_acquire().unwrap().expect("a trial is admitted");
        assert_eq!(cb.state(), BreakerState::HalfOpen);
        permit.success();
        assert_eq!(cb.state(), BreakerState::Closed);
    }

    #[test]
    fn test_half_open_failure_reopens() {
        let clock = Arc::new(ManualClock::new());
        let cb = tripped_breaker(&clock);
        clock.advance(Duration::from_secs(10));
        let permit = cb.try_acquire().unwrap().expect("a trial is admitted");
        permit.failure();
        assert_eq!(cb.state(), BreakerState::Open);
    }

    #[test]
    fn test_open_rejects_until_cooldown_elapses() {
        let clock = Arc::new(ManualClock::new());
        let cb = tripped_breaker(&clock);
        clock.advance(Duration::from_secs(9));
        match cb.try_acquire() {
            Err(ThrottleError::CircuitOpen { retry_after }) => {
                assert_eq!(retry_after, Duration::from_secs(1));
            }
            other => panic!("expected CircuitOpen, got {:?}", other.map(|p| p.is_some())),
        }
        clock.advance(Duration::from_secs(1));
        assert!(cb.try_acquire().unwrap().is_some());
    }

    #[test]
    fn test_extra_probes_are_shed_while_trials_are_in_flight() {
        let clock = Arc::new(ManualClock::new());
        let cb = tripped_breaker(&clock);
        clock.advance(Duration::from_secs(10));
        let trial = cb.try_acquire().unwrap().unwrap();
        assert_eq!(
            cb.try_acquire().err(),
            Some(ThrottleError::CircuitOpen {
                retry_after: Duration::ZERO
            })
        );
        trial.success();
    }

    #[test]
    fn test_dropping_permit_counts_as_failure() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(2), Duration::from_secs(10), clock);
        drop(cb.try_acquire().unwrap());
        assert_eq!(cb.state(), BreakerState::Closed);
        drop(cb.try_acquire().unwrap());
        assert_eq!(cb.state(), BreakerState::Open);
    }

    #[test]
    fn test_late_outcome_from_a_closed_epoch_is_ignored_in_half_open() {
        // Regression (TN-008): a slow call admitted while closed that settles
        // during half-open counted as the probe's outcome and freed a probe
        // slot it never held.
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(1), Duration::from_secs(10), clock.clone());
        let slow = cb.try_acquire().unwrap().unwrap(); // admitted while closed
        cb.record_failure(); // trips open
        clock.advance(Duration::from_secs(10));
        let probe = cb.try_acquire().unwrap().unwrap(); // half-open trial
        slow.failure(); // stale: must not reopen or free the trial slot
        assert_eq!(cb.state(), BreakerState::HalfOpen);
        assert!(
            cb.try_acquire().is_err(),
            "the single trial slot is still held"
        );
        probe.success();
        assert_eq!(cb.state(), BreakerState::Closed);
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
        cb.record_failure();
        assert_eq!(cb.state(), BreakerState::Open);
    }

    #[test]
    fn test_ratio_parameters_are_normalised() {
        assert_eq!(
            Trip::Ratio {
                window: 0,
                ratio: f64::NAN,
                min_calls: 9
            }
            .normalised(),
            Trip::Ratio {
                window: 1,
                ratio: 1.0,
                min_calls: 1
            }
        );
        assert_eq!(
            Trip::Ratio {
                window: 5,
                ratio: 7.0,
                min_calls: 0
            }
            .normalised(),
            Trip::Ratio {
                window: 5,
                ratio: 1.0,
                min_calls: 0
            }
        );
        assert_eq!(Trip::Consecutive(0).normalised(), Trip::Consecutive(1));
        // A zero window with a NaN ratio trips on a single failure.
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(
            Trip::Ratio {
                window: 0,
                ratio: f64::NAN,
                min_calls: 0,
            },
            Duration::from_secs(1),
            clock,
        );
        cb.record_success();
        assert_eq!(cb.state(), BreakerState::Closed);
        cb.record_failure();
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
        clock.advance(Duration::from_secs(6));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), BreakerState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), BreakerState::Open);
    }

    #[test]
    fn test_concurrent_half_open_trials() {
        let clock = Arc::new(ManualClock::new());
        let cb = CircuitBreaker::builder()
            .trip(Trip::Consecutive(1))
            .cooldown(Duration::from_secs(1))
            .half_open(3, 2)
            .build(Throttle::per_second(100))
            .with_clock(clock.clone());
        cb.record_failure();
        clock.advance(Duration::from_secs(1));
        let a = cb.try_acquire().unwrap().unwrap();
        let b = cb.try_acquire().unwrap().unwrap();
        let c = cb.try_acquire().unwrap().unwrap();
        assert!(cb.try_acquire().is_err(), "three trials at most");
        a.success();
        assert_eq!(cb.state(), BreakerState::HalfOpen);
        b.success();
        assert_eq!(cb.state(), BreakerState::Closed, "two successes close it");
        c.failure(); // stale: from the half-open generation
        assert_eq!(cb.state(), BreakerState::Closed);
    }

    #[test]
    fn test_inner_limiter_refusal_releases_the_trial_slot() {
        let clock = Arc::new(ManualClock::new());
        let cb = CircuitBreaker::builder()
            .trip(Trip::Consecutive(1))
            .cooldown(Duration::from_secs(1))
            .build(Throttle::per_duration(1, Duration::from_secs(3600)).with_clock(clock.clone()))
            .with_clock(clock.clone());
        assert!(cb.inner().try_acquire()); // drain the limiter
        cb.record_failure();
        clock.advance(Duration::from_secs(1));
        assert!(cb.try_acquire().unwrap().is_none(), "limiter empty");
        assert_eq!(cb.state(), BreakerState::HalfOpen);
        clock.advance(Duration::from_secs(3600));
        assert!(cb.try_acquire().unwrap().is_some(), "the slot was released");
    }

    #[test]
    fn test_impossible_inner_limiter_is_reported() {
        let cb = CircuitBreaker::builder().build(Throttle::per_second(0));
        assert_eq!(
            cb.try_acquire().err(),
            Some(ThrottleError::CostExceedsCapacity {
                cost: 1,
                capacity: 0
            })
        );
    }

    #[test]
    fn test_owned_permit_crosses_threads() {
        let cb = Arc::new(
            CircuitBreaker::builder()
                .trip(Trip::Consecutive(1))
                .build(Throttle::per_second(10)),
        );
        let permit = cb.try_acquire_owned().unwrap().unwrap();
        std::thread::spawn(move || permit.failure()).join().unwrap();
        assert_eq!(cb.state(), BreakerState::Open);
        let cb = Arc::new(
            CircuitBreaker::builder()
                .trip(Trip::Consecutive(1))
                .build(Throttle::per_second(10)),
        );
        drop(cb.try_acquire_owned().unwrap().unwrap()); // unsettled: a failure
        assert_eq!(cb.state(), BreakerState::Open);
        assert!(cb.try_acquire_owned().is_err());
        let cb = Arc::new(CircuitBreaker::builder().build(Throttle::per_second(10)));
        let ok = cb.try_acquire_owned().unwrap().unwrap();
        assert!(format!("{ok:?}").contains("OwnedPermit"));
        ok.success();
        assert_eq!(cb.state(), BreakerState::Closed);
    }

    #[test]
    fn test_limiter_trait_surface() {
        let clock = Arc::new(ManualClock::new());
        let cb = breaker(Trip::Consecutive(1), Duration::from_secs(10), clock.clone());
        let limiter: &dyn Limiter = &cb;
        assert_eq!(limiter.capacity(), 1_000_000);
        assert!(limiter.acquire_cost(1).is_acquired());
        limiter.refund(1);
        assert_eq!(limiter.available(), 1_000_000);
        assert_eq!(limiter.reserve(1, Duration::ZERO), Some(Duration::ZERO));
        cb.record_failure(); // trips
        assert_eq!(
            limiter.peek(1),
            Decision::Retry {
                after: Duration::from_secs(10)
            }
        );
        assert_eq!(limiter.available(), 0);
        assert!(!limiter.acquire_cost(1).is_acquired());
        assert_eq!(limiter.reserve(1, Duration::MAX), None);
        clock.advance(Duration::from_secs(10));
        // The trait admission takes the trial slot; a manual record settles it.
        assert!(limiter.acquire_cost(1).is_acquired());
        assert_eq!(
            limiter.peek(1),
            Decision::Retry {
                after: Duration::ZERO
            }
        );
        cb.record_success();
        assert_eq!(cb.state(), BreakerState::Closed);
        limiter.pause_for(Duration::from_secs(1));
        let paused = cb.inner().paused_for();
        assert!(paused > Duration::from_millis(900) && paused <= Duration::from_secs(1));
    }

    #[test]
    fn test_reset_names_and_debug() {
        let clock = Arc::new(ManualClock::new());
        let cb = tripped_breaker(&clock).with_name("db");
        assert_eq!(cb.name(), Some("db"));
        cb.reset();
        assert_eq!(cb.state(), BreakerState::Closed);
        assert!(format!("{cb:?}").contains("\"db\""));
        let permit = cb.try_acquire().unwrap().unwrap();
        assert!(format!("{permit:?}").contains("Permit"));
        permit.success();
    }

    #[test]
    fn test_call_error_display_and_source() {
        let rejected: CallError<std::io::Error> = CallError::Rejected(ThrottleError::QueueFull);
        assert!(rejected.to_string().contains("rejected"));
        let failed = CallError::Failed(std::io::Error::other("x"));
        assert!(failed.to_string().contains("failed"));
        assert!(std::error::Error::source(&failed).is_some());
        assert!(std::error::Error::source(&rejected).is_some());
    }

    #[cfg(feature = "tokio")]
    mod waiting {
        use super::*;

        #[tokio::test(start_paused = true)]
        async fn test_cancelled_half_open_acquire_does_not_wedge_the_breaker() {
            // Regression (TN-002): a probe cancelled while waiting on the inner
            // limiter leaked its trial slot, and the breaker stayed half-open
            // forever, shedding everything with `retry_after: 0`.
            let clock = Arc::new(ManualClock::new());
            let cb = CircuitBreaker::builder()
                .trip(Trip::Consecutive(1))
                .cooldown(Duration::from_secs(1))
                .build(Throttle::per_duration(1, Duration::from_secs(3600)))
                .with_clock(clock.clone());
            assert!(cb.inner().try_acquire()); // drain the 1/hour limiter
            cb.record_failure();
            clock.advance(Duration::from_secs(1));
            let cancelled = tokio::time::timeout(Duration::from_millis(50), cb.acquire()).await;
            assert!(cancelled.is_err(), "the probe was cancelled mid-wait");
            assert_eq!(cb.state(), BreakerState::HalfOpen);
            // The slot is free again: another probe is admitted (not shed).
            let next = tokio::time::timeout(Duration::from_millis(50), cb.acquire()).await;
            assert!(
                next.is_err(),
                "admitted and waiting on the limiter, not shed"
            );
            cb.inner().refund(1);
            let permit = cb.acquire().await.unwrap();
            permit.success();
            assert_eq!(cb.state(), BreakerState::Closed);
        }

        #[tokio::test(start_paused = true)]
        async fn test_acquire_timeout_and_call() {
            let cb = CircuitBreaker::builder()
                .trip(Trip::Consecutive(1))
                .build(Throttle::per_duration(1, Duration::from_secs(3600)));
            let permit = cb.acquire_timeout(Duration::from_secs(1)).await.unwrap();
            permit.success();
            assert_eq!(
                cb.acquire_timeout(Duration::from_secs(1)).await.err(),
                Some(ThrottleError::DeadlineExceeded)
            );
            assert_eq!(
                cb.state(),
                BreakerState::Closed,
                "a timeout is not a failure"
            );

            let cb = CircuitBreaker::builder()
                .trip(Trip::Consecutive(1))
                .build(Throttle::per_second(10));
            let ok: Result<u8, CallError<&str>> = cb.call(|| async { Ok(7) }).await;
            assert_eq!(ok, Ok(7));
            let failed: Result<u8, CallError<&str>> = cb.call(|| async { Err("no") }).await;
            assert_eq!(failed, Err(CallError::Failed("no")));
            let shed: Result<u8, CallError<&str>> = cb.call(|| async { Ok(1) }).await;
            assert!(matches!(
                shed,
                Err(CallError::Rejected(ThrottleError::CircuitOpen { .. }))
            ));
        }

        #[tokio::test(start_paused = true)]
        async fn test_acquire_owned_and_impossible() {
            let cb = Arc::new(CircuitBreaker::builder().build(Throttle::per_second(10)));
            let permit = cb.acquire_owned().await.unwrap();
            tokio::spawn(async move { permit.success() }).await.unwrap();
            let none = CircuitBreaker::builder().build(Throttle::per_second(0));
            assert_eq!(
                none.acquire().await.err(),
                Some(ThrottleError::CostExceedsCapacity {
                    cost: 1,
                    capacity: 0
                })
            );
        }
    }
}
