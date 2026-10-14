//! A hybrid limiter: a request must clear every constituent.

use core::time::Duration;
use std::borrow::Cow;
use std::sync::Arc;

use crate::decision::Decision;
#[cfg(feature = "runtime")]
use crate::error::ThrottleError;
use crate::limiter::{Limiter, acquire_all, peek_all, refund_all, reserve_all};
use crate::obs::{self, Label};

/// The limiter kind reported in metrics and events.
const KIND: &str = "hybrid";

/// Several limiters combined so a request must satisfy **all** of them.
///
/// The classic use is layering windows on one resource — say "at most 10 per
/// second *and* at most 100 per minute" — where either ceiling can bind. A
/// hybrid is itself a [`Limiter`], so hybrids nest and slot anywhere a single
/// limiter does.
///
/// Acquisition is two-phase to stay correct: every constituent is first
/// [`peek`](Limiter::peek)ed, and units are only taken once all of them would
/// grant. If a concurrent caller wins a race between the peek and the commit,
/// the constituents already charged are [refunded](Limiter::refund), so a
/// refused request costs nothing in limiters that support refunds (every
/// built-in one does). The waiting [`acquire`](Self::acquire) reserves on every
/// constituent at once, so waiters queue fairly on token-bucket constituents.
///
/// Build one with [`Hybrid::builder`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use throttle_net::{Hybrid, Throttle};
///
/// // 10 per second, and no more than 100 per minute.
/// let hybrid = Hybrid::builder()
///     .limiter(Throttle::per_second(10))
///     .limiter(Throttle::per_duration(100, Duration::from_secs(60)))
///     .build();
///
/// assert!(hybrid.try_acquire());
/// ```
#[derive(Clone)]
pub struct Hybrid {
    constituents: Arc<[Arc<dyn Limiter>]>,
    label: Label,
}

impl Hybrid {
    /// Starts building a hybrid limiter.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Hybrid, Throttle};
    ///
    /// let hybrid = Hybrid::builder().limiter(Throttle::per_second(5)).build();
    /// assert_eq!(hybrid.len(), 1);
    /// ```
    pub fn builder() -> HybridBuilder {
        HybridBuilder::default()
    }

    #[inline]
    fn pairs(&self, cost: u32) -> impl Iterator<Item = (&dyn Limiter, u32)> + Clone {
        self.constituents.iter().map(move |l| (l.as_ref(), cost))
    }

    /// The number of constituents.
    #[must_use]
    pub fn len(&self) -> usize {
        self.constituents.len()
    }

    /// Returns `true` if the hybrid has no constituents (it then admits
    /// everything).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.constituents.is_empty()
    }

    /// The instance name, if one was set with [`HybridBuilder::name`].
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.label.get()
    }

    /// Attempts to take one unit from every constituent without waiting,
    /// returning whether all granted.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Hybrid, Throttle};
    ///
    /// let hybrid = Hybrid::builder().limiter(Throttle::per_second(1)).build();
    /// assert!(hybrid.try_acquire());
    /// assert!(!hybrid.try_acquire());
    /// ```
    #[inline]
    #[must_use]
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_with_cost(1)
    }

    /// Attempts to take `cost` units from every constituent without waiting.
    /// All-or-nothing across constituents.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Hybrid, Throttle};
    ///
    /// let hybrid = Hybrid::builder()
    ///     .limiter(Throttle::per_second(10))
    ///     .limiter(Throttle::per_second(4))
    ///     .build();
    /// assert!(hybrid.try_acquire_with_cost(4));
    /// assert!(!hybrid.try_acquire_with_cost(1)); // the 4/s constituent is empty
    /// ```
    #[inline]
    #[must_use]
    pub fn try_acquire_with_cost(&self, cost: u32) -> bool {
        let granted = acquire_all(self.pairs(cost)).is_acquired();
        obs::attempt(KIND, &self.label, cost, granted);
        granted
    }

    /// Reports whether `cost` units would be granted by every constituent now,
    /// without taking anything.
    #[must_use]
    pub fn peek(&self, cost: u32) -> Decision {
        peek_all(self.pairs(cost))
    }

    /// Pauses every constituent for `duration` (see [`Limiter::pause_for`]).
    pub fn pause_for(&self, duration: Duration) {
        for limiter in self.constituents.iter() {
            limiter.pause_for(duration);
        }
    }

    /// Lifts every constituent's pause (see [`Limiter::resume`]).
    pub fn resume(&self) {
        for limiter in self.constituents.iter() {
            limiter.resume();
        }
    }

    /// The longest remaining pause among the constituents (`Duration::ZERO`
    /// when none is paused).
    #[must_use]
    pub fn paused_for(&self) -> Duration {
        self.constituents
            .iter()
            .map(Limiter::paused_for)
            .max()
            .unwrap_or(Duration::ZERO)
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl Hybrid {
    /// Takes one unit from every constituent, waiting until all are free.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] if some constituent's
    /// capacity is too small to ever grant the request.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use throttle_net::{Hybrid, Throttle};
    ///
    /// let hybrid = Hybrid::builder().limiter(Throttle::per_second(100)).build();
    /// hybrid.acquire().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire(&self) -> Result<(), ThrottleError> {
        self.acquire_with_cost(1).await
    }

    /// Takes `cost` units from every constituent, waiting until all are free.
    ///
    /// The waiter queues on the *bottleneck* — the constituent with the longest
    /// wait — and takes the others only when that reservation matures, so a
    /// fast constituent (perhaps shared with other callers) is never charged
    /// long before the request can go. While any constituent is
    /// [paused](Self::pause_for), the waiter sleeps the pause out, and a
    /// [`resume`](Self::resume) wakes it. **Cancel-safe:** dropping the future
    /// returns whatever it held.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] if some constituent can
    /// never grant `cost`.
    pub async fn acquire_with_cost(&self, cost: u32) -> Result<(), ThrottleError> {
        self.wait(cost, None).await
    }

    /// Takes `cost` units from every constituent, waiting at most `timeout`.
    ///
    /// # Errors
    ///
    /// [`ThrottleError::DeadlineExceeded`] if every constituent cannot grant
    /// within `timeout`; [`ThrottleError::CostExceedsCapacity`] if some
    /// constituent can never grant `cost`.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use std::time::Duration;
    /// use throttle_net::{Hybrid, Throttle};
    ///
    /// let hybrid = Hybrid::builder().limiter(Throttle::per_second(100)).build();
    /// hybrid.acquire_with_cost_timeout(5, Duration::from_millis(200)).await?;
    /// # Ok(())
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
        use crate::limiter::{Progress, paused_all, release_all, step_all};
        use crate::wait::{Refusal, wait_with};
        let timer = obs::Timer::start();
        let result = wait_with(
            Progress::new(),
            |progress, budget| step_all(self.pairs(cost), budget, progress),
            |progress| release_all(self.pairs(cost), progress),
            || paused_all(self.pairs(cost)),
            timeout,
        )
        .await
        .map_err(|refusal| match refusal {
            Refusal::Never => ThrottleError::CostExceedsCapacity {
                cost,
                capacity: Limiter::capacity(self),
            },
            Refusal::TimedOut => ThrottleError::DeadlineExceeded,
        });
        obs::waited(KIND, &self.label, &timer, cost, obs::Waited::of(&result));
        result
    }
}

impl Limiter for Hybrid {
    #[inline]
    fn peek(&self, cost: u32) -> Decision {
        peek_all(self.pairs(cost))
    }

    #[inline]
    fn acquire_cost(&self, cost: u32) -> Decision {
        acquire_all(self.pairs(cost))
    }

    /// The headroom of the *binding* constituent: the fewest units any one of
    /// them has available. An empty hybrid is unbounded ([`u32::MAX`]).
    #[inline]
    fn available(&self) -> u32 {
        self.constituents
            .iter()
            .map(Limiter::available)
            .min()
            .unwrap_or(u32::MAX)
    }

    /// The capacity of the *binding* constituent: the smallest capacity among
    /// them, since that is the first ceiling a request hits. An empty hybrid is
    /// unbounded ([`u32::MAX`]).
    #[inline]
    fn capacity(&self) -> u32 {
        self.constituents
            .iter()
            .map(Limiter::capacity)
            .min()
            .unwrap_or(u32::MAX)
    }

    fn refund(&self, cost: u32) {
        refund_all(self.pairs(cost));
    }

    fn reserve(&self, cost: u32, max_wait: Duration) -> Option<Duration> {
        reserve_all(self.pairs(cost), max_wait)
    }

    fn pause_for(&self, duration: Duration) {
        Hybrid::pause_for(self, duration);
    }

    fn paused_for(&self) -> Duration {
        Hybrid::paused_for(self)
    }

    fn resume(&self) {
        Hybrid::resume(self);
    }
}

impl core::fmt::Debug for Hybrid {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Hybrid")
            .field("name", &self.label)
            .field("constituents", &self.constituents.len())
            .field("capacity", &Limiter::capacity(self))
            .field("available", &Limiter::available(self))
            .finish()
    }
}

/// Builder for a [`Hybrid`] limiter.
///
/// Add constituents with [`limiter`](Self::limiter); each must outlive the
/// hybrid, so it is stored behind an [`Arc`]. Finish with [`build`](Self::build).
///
/// # Examples
///
/// ```
/// use throttle_net::{Hybrid, Throttle};
///
/// let hybrid = Hybrid::builder()
///     .limiter(Throttle::per_second(5))
///     .name("billing")
///     .build();
/// assert_eq!(hybrid.name(), Some("billing"));
/// ```
#[derive(Default)]
#[must_use]
pub struct HybridBuilder {
    constituents: Vec<Arc<dyn Limiter>>,
    label: Label,
}

impl HybridBuilder {
    /// Adds a constituent the hybrid must satisfy.
    pub fn limiter(mut self, limiter: impl Limiter + 'static) -> Self {
        self.constituents.push(Arc::new(limiter));
        self
    }

    /// Adds an already-shared constituent (for reusing one limiter across
    /// several composites).
    pub fn shared(mut self, limiter: Arc<dyn Limiter>) -> Self {
        self.constituents.push(limiter);
        self
    }

    /// Names the hybrid for metrics and events.
    pub fn name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
        self
    }

    /// Builds the [`Hybrid`].
    #[must_use]
    pub fn build(self) -> Hybrid {
        Hybrid {
            constituents: self.constituents.into(),
            label: self.label,
        }
    }
}

impl core::fmt::Debug for HybridBuilder {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HybridBuilder")
            .field("name", &self.label)
            .field("constituents", &self.constituents.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::Hybrid;
    use crate::decision::Decision;
    use crate::limiter::Limiter;
    use crate::sliding::SlidingWindowLog;
    use crate::throttle::Throttle;
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_hybrid_is_send_sync() {
        assert_send_sync::<Hybrid>();
    }

    #[test]
    fn test_request_must_clear_every_constituent() {
        let hybrid = Hybrid::builder()
            .limiter(Throttle::per_second(5))
            .limiter(Throttle::per_duration(3, Duration::from_secs(60)))
            .build();
        assert!(hybrid.try_acquire());
        assert!(hybrid.try_acquire());
        assert!(hybrid.try_acquire());
        assert!(!hybrid.try_acquire());
    }

    #[test]
    fn test_peek_does_not_consume() {
        let hybrid = Hybrid::builder().limiter(Throttle::per_second(2)).build();
        assert!(hybrid.peek(2).is_acquired());
        assert!(hybrid.try_acquire_with_cost(2));
    }

    #[test]
    fn test_no_token_lost_when_a_later_constituent_refuses() {
        let plenty: Arc<dyn Limiter> = Arc::new(Throttle::per_second(100));
        let exhausted = Throttle::per_second(1);
        assert!(exhausted.try_acquire());
        let hybrid = Hybrid::builder()
            .shared(plenty.clone())
            .limiter(exhausted)
            .build();
        let before = plenty.available();
        assert!(!hybrid.try_acquire());
        assert_eq!(plenty.available(), before);
    }

    #[test]
    fn test_capacity_and_available_report_the_binding_constituent() {
        let hybrid = Hybrid::builder()
            .limiter(Throttle::per_second(10))
            .limiter(Throttle::per_second(4))
            .build();
        assert_eq!(hybrid.capacity(), 4);
        assert_eq!(hybrid.available(), 4);
        assert_eq!(hybrid.len(), 2);
        assert!(!hybrid.is_empty());
    }

    #[test]
    fn test_empty_hybrid_admits_everything() {
        let hybrid = Hybrid::builder().build();
        assert!(hybrid.is_empty());
        assert!(hybrid.try_acquire_with_cost(u32::MAX));
        assert_eq!(hybrid.capacity(), u32::MAX);
        assert_eq!(hybrid.available(), u32::MAX);
    }

    #[test]
    fn test_refill_recovers_the_hybrid_under_manual_clock() {
        let clock = Arc::new(ManualClock::new());
        let hybrid = Hybrid::builder()
            .limiter(Throttle::per_second(2).with_clock(clock.clone()))
            .limiter(SlidingWindowLog::per_second(2).with_clock(clock.clone()))
            .build();
        assert!(hybrid.try_acquire());
        assert!(hybrid.try_acquire());
        assert!(!hybrid.try_acquire());
        clock.advance(Duration::from_secs(1));
        assert!(hybrid.try_acquire());
    }

    #[test]
    fn test_trait_reserve_refund_and_pause_reach_every_constituent() {
        let clock = Arc::new(ManualClock::new());
        let a = Arc::new(Throttle::per_second(10).with_clock(clock.clone()));
        let b = Arc::new(Throttle::per_second(10).with_clock(clock.clone()));
        let hybrid = Hybrid::builder()
            .shared(a.clone())
            .shared(b.clone())
            .name("pair")
            .build();
        assert_eq!(
            Limiter::reserve(&hybrid, 4, Duration::MAX),
            Some(Duration::ZERO)
        );
        assert_eq!((a.available(), b.available()), (6, 6));
        Limiter::refund(&hybrid, 4);
        assert_eq!((a.available(), b.available()), (10, 10));
        hybrid.pause_for(Duration::from_secs(3));
        assert_eq!(
            hybrid.peek(1),
            Decision::Retry {
                after: Duration::from_secs(3)
            }
        );
        Limiter::pause_for(&hybrid, Duration::from_secs(1));
        assert_eq!(a.paused_for(), Duration::from_secs(3));
        assert!(format!("{hybrid:?}").contains("\"pair\""));
        assert!(format!("{:?}", Hybrid::builder()).contains("HybridBuilder"));
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_errors_when_a_constituent_can_never_grant() {
        use crate::error::ThrottleError;

        let hybrid = Hybrid::builder()
            .limiter(Throttle::per_second(10))
            .limiter(Throttle::per_second(2))
            .build();
        let err = hybrid.acquire_with_cost(5).await.unwrap_err();
        assert_eq!(
            err,
            ThrottleError::CostExceedsCapacity {
                cost: 5,
                capacity: 2
            }
        );
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(start_paused = true)]
    async fn test_acquire_waits_on_a_mixed_hybrid_and_times_out() {
        use crate::error::ThrottleError;

        // A token bucket (native reservations) beside a sliding log (none).
        let hybrid = Hybrid::builder()
            .limiter(Throttle::per_second(100))
            .limiter(SlidingWindowLog::new(1, Duration::from_millis(50)))
            .build();
        assert!(hybrid.try_acquire());
        assert!(hybrid.acquire().await.is_ok(), "waits for the log to slide");
        let err = hybrid
            .acquire_with_cost_timeout(1, Duration::from_millis(1))
            .await
            .unwrap_err();
        assert_eq!(err, ThrottleError::DeadlineExceeded);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(start_paused = true)]
    async fn test_cancelled_acquire_refunds_every_constituent() {
        let a = Arc::new(Throttle::per_second(10));
        let b = Arc::new(Throttle::per_second(10));
        let hybrid = Hybrid::builder()
            .shared(a.clone())
            .shared(b.clone())
            .build();
        assert!(hybrid.try_acquire_with_cost(10));
        let cancelled =
            tokio::time::timeout(Duration::from_millis(5), hybrid.acquire_with_cost(10)).await;
        assert!(cancelled.is_err());
        // Both reservations were refunded: a fresh one waits ~1s, not ~2s.
        assert!(a.reserve(10, Duration::MAX).unwrap() <= Duration::from_secs(1));
        assert!(b.reserve(10, Duration::MAX).unwrap() <= Duration::from_secs(1));
    }
}
