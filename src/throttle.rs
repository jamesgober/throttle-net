//! The Tier-1 throttle: one token bucket with a waiting acquire.

use core::time::Duration;

use better_bucket::{Bucket, Decision as BucketDecision};
use clock_lib::{Clock, SystemClock};

use crate::decision::Decision;
#[cfg(feature = "tokio")]
use crate::error::ThrottleError;
use crate::limiter::Limiter;

/// A single outbound throttle backed by a token bucket.
///
/// This is the Tier-1 surface: construct one with [`per_second`](Self::per_second)
/// or [`per_duration`](Self::per_duration), then pace your outbound work with
/// [`acquire`](Self::acquire). Because throttle-net protects *downstreams*, the
/// headline operation **waits** for a token rather than rejecting the caller —
/// you are slowing your own requests, not dropping someone else's. When you would
/// rather not wait, [`try_acquire`](Self::try_acquire) reports the outcome
/// immediately.
///
/// The bucket refills smoothly and starts full, so a burst up to the capacity is
/// admitted at once and the sustained rate is the refill rate. Token accounting
/// is lock-free (a single atomic compare-and-swap per acquire), and time is read
/// from an injectable [`Clock`] — [`SystemClock`] in production, or a
/// `ManualClock` in tests via [`with_clock`](Self::with_clock).
///
/// # Examples
///
/// ```
/// # async fn run() -> Result<(), throttle_net::ThrottleError> {
/// use throttle_net::Throttle;
///
/// // 100 requests per second, bursting up to 100.
/// let throttle = Throttle::per_second(100);
/// throttle.acquire().await?; // returns as soon as a token is free
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Throttle<C: Clock = SystemClock> {
    bucket: Bucket<C>,
}

impl Throttle<SystemClock> {
    /// Creates a throttle that admits `rate` units per second, bursting up to
    /// `rate`, driven by the OS monotonic clock.
    ///
    /// A `rate` of `0` yields a throttle that grants nothing; an
    /// [`acquire`](Self::acquire) on it returns
    /// [`ThrottleError::CostExceedsCapacity`].
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
        Self {
            bucket: Bucket::per_second(rate),
        }
    }

    /// Creates a throttle that admits `amount` units every `period`, bursting up
    /// to `amount`, driven by the OS monotonic clock.
    ///
    /// Use this when the natural window is not one second — for example, sixty
    /// calls per minute, or five per hundred milliseconds.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Throttle;
    ///
    /// // 60 requests per minute.
    /// let throttle = Throttle::per_duration(60, Duration::from_secs(60));
    /// assert_eq!(throttle.capacity(), 60);
    /// ```
    #[must_use]
    pub fn per_duration(amount: u32, period: Duration) -> Self {
        Self {
            bucket: Bucket::per_duration(amount, period),
        }
    }
}

impl<C: Clock> Throttle<C> {
    /// Replaces the time source, returning a throttle driven by `clock`.
    ///
    /// The common use is deterministic testing: inject a
    /// [`ManualClock`](clock_lib::ManualClock) (shared via an
    /// [`Arc`](std::sync::Arc)) and drive refills by advancing it, with no real
    /// sleeping. The bucket is re-anchored to the new clock and starts full.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use clock_lib::ManualClock;
    /// use throttle_net::Throttle;
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
        Throttle {
            bucket: self.bucket.with_clock(clock),
        }
    }

    /// The maximum number of tokens the throttle can hold (its burst size).
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.bucket.capacity()
    }

    /// The number of whole tokens available right now.
    ///
    /// A point-in-time read for observability and tests, not a reservation.
    #[inline]
    #[must_use]
    pub fn available(&self) -> u32 {
        self.bucket.available()
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
        self.bucket.try_acquire(1)
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
        self.bucket.try_acquire(cost)
    }

    /// Reports whether `cost` tokens would be granted now, without taking them.
    ///
    /// This is the non-consuming counterpart to [`try_acquire_with_cost`](Self::try_acquire_with_cost),
    /// used by composite limiters to poll a constituent before committing. The
    /// [`Decision::Retry`] wait is estimated from the refill rate, so it is a
    /// close guide rather than an exact promise.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Decision, Throttle};
    ///
    /// let throttle = Throttle::per_second(4);
    /// assert_eq!(throttle.peek(3), Decision::Acquired); // would grant, took nothing
    /// assert!(throttle.try_acquire_with_cost(4));        // still full
    /// ```
    #[inline]
    #[must_use]
    pub fn peek(&self, cost: u32) -> Decision {
        let capacity = self.bucket.capacity();
        if cost > capacity {
            return Decision::Impossible;
        }
        let available = self.bucket.available();
        if available >= cost {
            return Decision::Acquired;
        }
        let config = self.bucket.config();
        let refill_amount = config.refill_amount();
        let period = config.refill_period();
        if refill_amount == 0 || period.is_zero() {
            // No refill, and not enough on hand: it will never accrue.
            return Decision::Impossible;
        }
        // `cost <= capacity` and `available < cost`, so the deficit is positive
        // and bounded by capacity; no underflow.
        let deficit = cost - available;
        Decision::Retry {
            after: estimate_refill_wait(period, deficit, refill_amount),
        }
    }

    /// The synchronous, consuming core shared by the trait impl and the waiting
    /// surface. Deducts `cost` on success.
    #[inline]
    fn decide(&self, cost: u32) -> Decision {
        match self.bucket.acquire(cost) {
            BucketDecision::Allowed => Decision::Acquired,
            BucketDecision::Denied { retry_after } if retry_after == Duration::MAX => {
                Decision::Impossible
            }
            BucketDecision::Denied { retry_after } => Decision::Retry { after: retry_after },
            // `better_bucket::Decision` is `#[non_exhaustive]`. An outcome this
            // version does not understand is treated as un-grantable rather than
            // risk over-sending to a downstream.
            _ => Decision::Impossible,
        }
    }
}

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
impl<C: Clock> Throttle<C> {
    /// Takes one token, waiting until one is available.
    ///
    /// This is the marquee outbound operation: it paces the caller instead of
    /// rejecting it. It returns once a token has been deducted, or
    /// [`ThrottleError::CostExceedsCapacity`] if the throttle's capacity is zero.
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
    /// LLM call billed by token count. The waiter sleeps for the bucket's own
    /// estimate of the refill time and retries, so it converges without busy
    /// spinning even under contention.
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
        let timer = crate::obs::Timer::start();
        let result = loop {
            match self.decide(cost) {
                Decision::Acquired => break Ok(()),
                Decision::Impossible => {
                    break Err(ThrottleError::CostExceedsCapacity {
                        cost,
                        capacity: self.capacity(),
                    });
                }
                Decision::Retry { after } => tokio::time::sleep(after).await,
            }
        };
        if result.is_ok() {
            crate::obs::acquired("throttle");
        }
        crate::obs::wait("throttle", &timer);
        crate::obs::trace_acquire("throttle", cost, result.is_ok(), &timer);
        result
    }
}

/// Estimates the wait until `deficit` tokens accrue at `refill_amount` per
/// `period`, rounded up so the caller never wakes a touch too early.
///
/// Computed in integer nanoseconds (`u128`) to stay deterministic and avoid
/// floating point; the result is clamped to the `Duration::from_nanos` range.
#[inline]
fn estimate_refill_wait(period: Duration, deficit: u32, refill_amount: u32) -> Duration {
    let numerator = period.as_nanos().saturating_mul(u128::from(deficit));
    let nanos = numerator.div_ceil(u128::from(refill_amount));
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
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
        self.bucket.available()
    }

    #[inline]
    fn capacity(&self) -> u32 {
        self.bucket.capacity()
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
        // Only 3 remain, so a cost of 7 takes nothing.
        assert!(!throttle.try_acquire_with_cost(7));
        assert!(throttle.try_acquire_with_cost(3));
    }

    #[test]
    fn test_refill_after_a_full_period_under_manual_clock() {
        let clock = Arc::new(ManualClock::new());
        let throttle = Throttle::per_second(4).with_clock(clock.clone());

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
        // Drained: another unit must wait.
        assert!(matches!(throttle.acquire_cost(1), Decision::Retry { .. }));
        // A cost beyond capacity can never be granted.
        assert_eq!(throttle.acquire_cost(3), Decision::Impossible);
    }

    #[test]
    fn test_available_tracks_consumption() {
        let throttle = Throttle::per_second(5);
        assert_eq!(throttle.available(), 5);
        assert!(throttle.try_acquire_with_cost(2));
        assert_eq!(throttle.available(), 3);
    }

    #[tokio::test]
    async fn test_acquire_returns_immediately_when_a_token_is_free() {
        let throttle = Throttle::per_second(1);
        assert!(throttle.acquire().await.is_ok());
    }

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

    #[tokio::test]
    async fn test_acquire_waits_for_refill_then_succeeds() {
        // Capacity 1000 refilling at 1 token/ms: after draining, one token
        // returns in about a millisecond, so the waiter completes promptly.
        let throttle = Throttle::per_second(1000);
        for _ in 0..1000 {
            assert!(throttle.try_acquire());
        }
        assert!(!throttle.try_acquire());
        assert!(throttle.acquire().await.is_ok());
    }
}
