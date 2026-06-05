//! A hybrid limiter: a request must clear every constituent.

use std::sync::Arc;

use crate::decision::Decision;
#[cfg(feature = "tokio")]
use crate::error::ThrottleError;
use crate::limiter::{Limiter, acquire_all, peek_all};

/// Several limiters combined so a request must satisfy **all** of them.
///
/// The classic use is layering windows on one resource — say "at most 10 per
/// second *and* at most 100 per minute" — where either ceiling can bind. A
/// hybrid is itself a [`Limiter`], so hybrids nest and slot anywhere a single
/// limiter does.
///
/// Acquisition is two-phase to stay correct: every constituent is first
/// [`peek`](Limiter::peek)ed, and tokens are only taken once all of them would
/// grant. Without that, an early constituent could spend a token for a request a
/// later one refuses. See [`Limiter`] for the full rationale.
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
}

impl Hybrid {
    /// Starts building a hybrid limiter.
    #[must_use]
    pub fn builder() -> HybridBuilder {
        HybridBuilder {
            constituents: Vec::new(),
        }
    }

    #[inline]
    fn pairs(&self, cost: u32) -> impl Iterator<Item = (&dyn Limiter, u32)> + Clone {
        self.constituents.iter().map(move |l| (l.as_ref(), cost))
    }

    /// Attempts to take one token from every constituent without waiting,
    /// returning whether all granted.
    ///
    /// All-or-nothing across constituents: either every one is debited or, on a
    /// refusal, the call reports failure.
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

    /// Attempts to take `cost` tokens from every constituent without waiting.
    #[inline]
    #[must_use]
    pub fn try_acquire_with_cost(&self, cost: u32) -> bool {
        acquire_all(self.pairs(cost)).is_acquired()
    }
}

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
impl Hybrid {
    /// Takes one token from every constituent, waiting until all are free.
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

    /// Takes `cost` tokens from every constituent, waiting until all are free.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] if some constituent can
    /// never grant `cost`.
    pub async fn acquire_with_cost(&self, cost: u32) -> Result<(), ThrottleError> {
        loop {
            match acquire_all(self.pairs(cost)) {
                Decision::Acquired => return Ok(()),
                Decision::Impossible => {
                    return Err(ThrottleError::CostExceedsCapacity {
                        cost,
                        capacity: self.capacity(),
                    });
                }
                Decision::Retry { after } => tokio::time::sleep(after).await,
            }
        }
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

    /// The headroom of the *binding* constituent: the fewest tokens any one of
    /// them has available. An empty hybrid is unbounded ([`u32::MAX`]).
    #[inline]
    fn available(&self) -> u32 {
        self.constituents
            .iter()
            .map(|l| l.available())
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
            .map(|l| l.capacity())
            .min()
            .unwrap_or(u32::MAX)
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
///     .build();
/// # let _ = hybrid;
/// ```
#[derive(Default)]
pub struct HybridBuilder {
    constituents: Vec<Arc<dyn Limiter>>,
}

impl HybridBuilder {
    /// Adds a constituent the hybrid must satisfy.
    #[must_use]
    pub fn limiter(mut self, limiter: impl Limiter + 'static) -> Self {
        self.constituents.push(Arc::new(limiter));
        self
    }

    /// Adds an already-shared constituent (for reusing one limiter across
    /// several composites).
    #[must_use]
    pub fn shared(mut self, limiter: Arc<dyn Limiter>) -> Self {
        self.constituents.push(limiter);
        self
    }

    /// Builds the [`Hybrid`].
    #[must_use]
    pub fn build(self) -> Hybrid {
        Hybrid {
            constituents: self.constituents.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::Hybrid;
    use crate::limiter::Limiter;
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
        // 5 per second AND 3 per minute: the tighter (3) binds.
        let hybrid = Hybrid::builder()
            .limiter(Throttle::per_second(5))
            .limiter(Throttle::per_duration(3, Duration::from_secs(60)))
            .build();

        assert!(hybrid.try_acquire());
        assert!(hybrid.try_acquire());
        assert!(hybrid.try_acquire());
        // The per-minute limiter is now empty even though the per-second one is
        // not, so the hybrid refuses.
        assert!(!hybrid.try_acquire());
    }

    #[test]
    fn test_peek_does_not_consume() {
        let hybrid = Hybrid::builder().limiter(Throttle::per_second(2)).build();
        assert!(hybrid.peek(2).is_acquired());
        // peek took nothing, so both tokens are still there.
        assert!(hybrid.try_acquire_with_cost(2));
    }

    #[test]
    fn test_no_token_lost_when_a_later_constituent_refuses() {
        // First limiter has plenty; second is exhausted. The peek-then-commit
        // contract means the first limiter must NOT lose a token to a request
        // the second one blocks.
        let plenty: Arc<dyn Limiter> = Arc::new(Throttle::per_second(100));
        let exhausted = Throttle::per_second(1);
        assert!(exhausted.try_acquire()); // drain it to zero

        let hybrid = Hybrid::builder()
            .shared(plenty.clone())
            .limiter(exhausted)
            .build();

        let before = plenty.available();
        assert!(!hybrid.try_acquire());
        // The plentiful limiter is untouched: the hybrid peeked, saw the second
        // constituent could not grant, and never committed to the first.
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
    }

    #[test]
    fn test_refill_recovers_the_hybrid_under_manual_clock() {
        let clock = Arc::new(ManualClock::new());
        let hybrid = Hybrid::builder()
            .limiter(Throttle::per_second(2).with_clock(clock.clone()))
            .limiter(Throttle::per_second(2).with_clock(clock.clone()))
            .build();

        assert!(hybrid.try_acquire());
        assert!(hybrid.try_acquire());
        assert!(!hybrid.try_acquire());

        clock.advance(Duration::from_secs(1));
        assert!(hybrid.try_acquire());
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_acquire_errors_when_a_constituent_can_never_grant() {
        use crate::error::ThrottleError;

        let hybrid = Hybrid::builder()
            .limiter(Throttle::per_second(10))
            .limiter(Throttle::per_second(2))
            .build();
        let err = hybrid.acquire_with_cost(5).await.unwrap_err();
        assert!(matches!(
            err,
            ThrottleError::CostExceedsCapacity {
                cost: 5,
                capacity: 2
            }
        ));
    }
}
