//! Multi-dimensional limits: one request, several independent budgets.

use std::sync::Arc;

use crate::decision::Decision;
#[cfg(feature = "tokio")]
use crate::error::ThrottleError;
use crate::limiter::{Limiter, acquire_all, peek_all};

/// One named, independently-metered dimension: its label and its limiter.
type Dimension = (Box<str>, Arc<dyn Limiter>);

/// A limiter with several named dimensions, each metered independently.
///
/// One outbound call often spends against more than one budget at once. An LLM
/// request, for instance, counts as one *request*, some number of *input
/// tokens*, and some number of *output tokens* — each with its own ceiling. A
/// `MultiLimiter` holds one limiter per dimension and admits a call only when
/// **every** dimension can afford its share, applying the per-dimension costs
/// atomically (peek-all-then-commit, like [`Hybrid`](crate::Hybrid)).
///
/// Costs are supplied per call as `(dimension, cost)` pairs. A dimension not
/// named in a call is charged nothing; a name with no matching dimension is
/// ignored.
///
/// Build one with [`MultiLimiter::builder`].
///
/// # Examples
///
/// ```
/// # async fn run() -> Result<(), throttle_net::ThrottleError> {
/// use std::time::Duration;
/// use throttle_net::{MultiLimiter, Throttle};
///
/// let minute = Duration::from_secs(60);
/// let limiter = MultiLimiter::builder()
///     .dimension("requests", Throttle::per_duration(60, minute))
///     .dimension("input_tokens", Throttle::per_duration(100_000, minute))
///     .dimension("output_tokens", Throttle::per_duration(20_000, minute))
///     .build();
///
/// // A call billed at 1 request, 1500 input tokens, 200 output tokens.
/// limiter
///     .acquire_costs(&[
///         ("requests", 1),
///         ("input_tokens", 1500),
///         ("output_tokens", 200),
///     ])
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MultiLimiter {
    dimensions: Arc<[Dimension]>,
}

/// Resolves the cost charged to a dimension for one call: the matching entry, or
/// zero when the dimension is not mentioned.
#[inline]
fn cost_for(name: &str, costs: &[(&str, u32)]) -> u32 {
    costs
        .iter()
        .copied()
        .find(|(n, _)| *n == name)
        .map_or(0, |(_, c)| c)
}

impl MultiLimiter {
    /// Starts building a multi-dimensional limiter.
    #[must_use]
    pub fn builder() -> MultiLimiterBuilder {
        MultiLimiterBuilder {
            dimensions: Vec::new(),
        }
    }

    #[inline]
    fn pairs<'a>(
        &'a self,
        costs: &'a [(&'a str, u32)],
    ) -> impl Iterator<Item = (&'a dyn Limiter, u32)> + Clone {
        self.dimensions
            .iter()
            .map(move |(name, limiter)| (limiter.as_ref(), cost_for(name, costs)))
    }

    /// Reports whether the call's per-dimension costs would all be granted now,
    /// without taking anything.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{MultiLimiter, Throttle};
    ///
    /// let limiter = MultiLimiter::builder()
    ///     .dimension("requests", Throttle::per_second(10))
    ///     .dimension("tokens", Throttle::per_second(1000))
    ///     .build();
    ///
    /// assert!(limiter.peek_costs(&[("requests", 1), ("tokens", 500)]).is_acquired());
    /// ```
    #[inline]
    #[must_use]
    pub fn peek_costs(&self, costs: &[(&str, u32)]) -> Decision {
        peek_all(self.pairs(costs))
    }

    /// Attempts to charge the call's per-dimension costs without waiting,
    /// returning whether every dimension granted.
    ///
    /// All-or-nothing across dimensions.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{MultiLimiter, Throttle};
    ///
    /// let limiter = MultiLimiter::builder()
    ///     .dimension("requests", Throttle::per_second(2))
    ///     .build();
    ///
    /// assert!(limiter.try_acquire_costs(&[("requests", 2)]));
    /// assert!(!limiter.try_acquire_costs(&[("requests", 1)]));
    /// ```
    #[inline]
    #[must_use]
    pub fn try_acquire_costs(&self, costs: &[(&str, u32)]) -> bool {
        acquire_all(self.pairs(costs)).is_acquired()
    }

    /// Returns the tokens available in `dimension` right now, or `None` if there
    /// is no such dimension.
    #[must_use]
    pub fn available(&self, dimension: &str) -> Option<u32> {
        self.dimensions
            .iter()
            .find(|(name, _)| name.as_ref() == dimension)
            .map(|(_, limiter)| limiter.available())
    }
}

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
impl MultiLimiter {
    /// Charges the call's per-dimension costs, waiting until every dimension can
    /// afford its share.
    ///
    /// This is the headline multi-dimensional operation: it paces the caller
    /// until all budgets allow the call, then commits all of them together.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] when some dimension's cost
    /// exceeds that dimension's capacity, naming that dimension's figures; such a
    /// call can never succeed.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use throttle_net::{MultiLimiter, Throttle};
    ///
    /// let limiter = MultiLimiter::builder()
    ///     .dimension("requests", Throttle::per_second(100))
    ///     .dimension("tokens", Throttle::per_second(100_000))
    ///     .build();
    ///
    /// limiter.acquire_costs(&[("requests", 1), ("tokens", 1500)]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire_costs(&self, costs: &[(&str, u32)]) -> Result<(), ThrottleError> {
        loop {
            match acquire_all(self.pairs(costs)) {
                Decision::Acquired => return Ok(()),
                Decision::Impossible => return Err(self.capacity_error(costs)),
                Decision::Retry { after } => tokio::time::sleep(after).await,
            }
        }
    }

    /// Builds the precise [`ThrottleError::CostExceedsCapacity`] for an
    /// un-grantable call by finding the dimension that cannot afford its cost.
    fn capacity_error(&self, costs: &[(&str, u32)]) -> ThrottleError {
        for (name, limiter) in self.dimensions.iter() {
            let cost = cost_for(name, costs);
            if cost > limiter.capacity() {
                return ThrottleError::CostExceedsCapacity {
                    cost,
                    capacity: limiter.capacity(),
                };
            }
        }
        // No dimension over capacity (a static, never-refilling dimension drained
        // below the cost) — report the first dimension that cannot grant.
        for (name, limiter) in self.dimensions.iter() {
            let cost = cost_for(name, costs);
            if limiter.peek(cost) == Decision::Impossible {
                return ThrottleError::CostExceedsCapacity {
                    cost,
                    capacity: limiter.capacity(),
                };
            }
        }
        ThrottleError::CostExceedsCapacity {
            cost: 0,
            capacity: 0,
        }
    }
}

/// Builder for a [`MultiLimiter`].
///
/// Name each dimension with [`dimension`](Self::dimension), then
/// [`build`](Self::build).
///
/// # Examples
///
/// ```
/// use throttle_net::{MultiLimiter, Throttle};
///
/// let limiter = MultiLimiter::builder()
///     .dimension("requests", Throttle::per_second(10))
///     .build();
/// # let _ = limiter;
/// ```
#[derive(Default)]
pub struct MultiLimiterBuilder {
    dimensions: Vec<Dimension>,
}

impl MultiLimiterBuilder {
    /// Adds a named dimension backed by `limiter`.
    ///
    /// Adding the same name twice keeps both; each is charged independently, so
    /// prefer distinct names.
    #[must_use]
    pub fn dimension(mut self, name: impl Into<Box<str>>, limiter: impl Limiter + 'static) -> Self {
        self.dimensions.push((name.into(), Arc::new(limiter)));
        self
    }

    /// Adds a named dimension backed by an already-shared limiter.
    #[must_use]
    pub fn shared(mut self, name: impl Into<Box<str>>, limiter: Arc<dyn Limiter>) -> Self {
        self.dimensions.push((name.into(), limiter));
        self
    }

    /// Builds the [`MultiLimiter`].
    #[must_use]
    pub fn build(self) -> MultiLimiter {
        MultiLimiter {
            dimensions: self.dimensions.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::MultiLimiter;
    use crate::throttle::Throttle;
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_multi_limiter_is_send_sync() {
        assert_send_sync::<MultiLimiter>();
    }

    #[test]
    fn test_all_dimensions_must_afford_their_share() {
        let limiter = MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(10))
            .dimension("tokens", Throttle::per_second(1000))
            .build();

        // Plenty of request headroom, but the token dimension only holds 1000.
        assert!(limiter.try_acquire_costs(&[("requests", 1), ("tokens", 1000)]));
        // Tokens are now spent; another token-heavy call is refused even though
        // requests are fine.
        assert!(!limiter.try_acquire_costs(&[("requests", 1), ("tokens", 1)]));
        // ...and the request dimension was not charged for the refused call.
        assert_eq!(limiter.available("requests"), Some(9));
    }

    #[test]
    fn test_unmentioned_dimension_is_not_charged() {
        let limiter = MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(2))
            .dimension("tokens", Throttle::per_second(100))
            .build();

        // Charge only the request dimension.
        assert!(limiter.try_acquire_costs(&[("requests", 1)]));
        assert_eq!(limiter.available("tokens"), Some(100));
        assert_eq!(limiter.available("requests"), Some(1));
    }

    #[test]
    fn test_unknown_dimension_name_is_ignored() {
        let limiter = MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(1))
            .build();
        assert!(limiter.try_acquire_costs(&[("requests", 1), ("nonexistent", 999)]));
    }

    #[test]
    fn test_available_is_none_for_unknown_dimension() {
        let limiter = MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(1))
            .build();
        assert_eq!(limiter.available("missing"), None);
    }

    #[test]
    fn test_peek_costs_does_not_consume() {
        let limiter = MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(5))
            .build();
        assert!(limiter.peek_costs(&[("requests", 5)]).is_acquired());
        assert_eq!(limiter.available("requests"), Some(5));
    }

    #[test]
    fn test_refill_recovers_each_dimension_under_manual_clock() {
        let clock = Arc::new(ManualClock::new());
        let limiter = MultiLimiter::builder()
            .dimension(
                "requests",
                Throttle::per_second(2).with_clock(clock.clone()),
            )
            .dimension("tokens", Throttle::per_second(10).with_clock(clock.clone()))
            .build();

        assert!(limiter.try_acquire_costs(&[("requests", 2), ("tokens", 10)]));
        assert!(!limiter.try_acquire_costs(&[("requests", 1), ("tokens", 1)]));

        clock.advance(Duration::from_secs(1));
        assert!(limiter.try_acquire_costs(&[("requests", 2), ("tokens", 10)]));
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_acquire_costs_errors_and_names_the_overspent_dimension() {
        use crate::error::ThrottleError;

        let limiter = MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(100))
            .dimension("tokens", Throttle::per_second(1000))
            .build();
        // 2000 tokens against a 1000 capacity can never succeed.
        let err = limiter
            .acquire_costs(&[("requests", 1), ("tokens", 2000)])
            .await
            .unwrap_err();
        assert_eq!(
            err,
            ThrottleError::CostExceedsCapacity {
                cost: 2000,
                capacity: 1000,
            }
        );
    }
}
