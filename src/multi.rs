//! Multi-dimensional limits: one request, several independent budgets.

use core::time::Duration;
use std::borrow::Cow;
use std::sync::Arc;

use crate::decision::Decision;
use crate::error::ThrottleError;
use crate::limiter::{Limiter, acquire_all, peek_all, refund_all};
use crate::obs::{self, Label};

/// The limiter kind reported in metrics and events.
const KIND: &str = "multi";

/// One named, independently metered dimension.
struct Dimension {
    name: Box<str>,
    limiter: Arc<dyn Limiter>,
}

/// An extra name that charges an existing dimension.
struct Alias {
    name: Box<str>,
    target: Box<str>,
}

/// The shared, immutable dimension table.
struct Table {
    dimensions: Box<[Dimension]>,
    aliases: Box<[Alias]>,
}

impl Table {
    /// The dimension `name` charges: the dimension itself, or an alias's target.
    #[inline]
    fn target<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        if self.dimensions.iter().any(|d| &*d.name == name) {
            return Some(name);
        }
        self.aliases
            .iter()
            .find(|a| &*a.name == name)
            .map(|a| &*a.target)
    }

    /// The total cost charged to `dimension` by one call: every entry naming it
    /// or one of its aliases, summed (saturating).
    #[inline]
    fn cost_for(&self, dimension: &str, costs: &[(&str, u32)]) -> u32 {
        costs
            .iter()
            .filter(|(name, _)| self.target(name) == Some(dimension))
            .fold(0u32, |sum, (_, cost)| sum.saturating_add(*cost))
    }

    /// The index of the first entry naming neither a dimension nor an alias.
    fn first_unknown(&self, costs: &[(&str, u32)]) -> Option<usize> {
        costs
            .iter()
            .position(|(name, _)| self.target(name).is_none())
    }
}

/// A limiter with several named dimensions, each metered independently.
///
/// One outbound call often spends against more than one budget at once. An LLM
/// request, for instance, counts as one *request* and some number of *tokens*,
/// each with its own ceiling. A `MultiLimiter` holds one limiter per dimension
/// and admits a call only when **every** dimension can afford its share,
/// applying the per-dimension costs atomically (peek-all-then-commit with
/// rollback, like [`Hybrid`](crate::Hybrid)).
///
/// Costs are supplied per call as `(dimension, cost)` pairs. Several entries
/// for the same dimension are summed; a dimension not named in a call is
/// charged nothing. An **alias** ([`MultiLimiterBuilder::alias`]) lets several
/// names charge one budget — for a provider that meters input and output
/// tokens against one combined limit.
///
/// A name that matches no dimension or alias is refused by default — a typo
/// must not charge nothing and admit the call: [`Decision::Impossible`] from
/// the non-waiting methods and [`ThrottleError::UnknownDimension`] from the
/// waiting ones. A **lenient** limiter
/// ([`MultiLimiterBuilder::strict`]`(false)`, the 1.x default) ignores unknown
/// names instead, for callers that deliberately pass one cost list to
/// limiters with different dimensions.
///
/// Build one with [`MultiLimiter::builder`].
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "runtime")]
/// # async fn run() -> Result<(), throttle_net::ThrottleError> {
/// use throttle_net::{MultiLimiter, Throttle};
///
/// let limiter = MultiLimiter::builder()
///     .dimension("requests", Throttle::per_minute(60))
///     .dimension("input_tokens", Throttle::per_minute(100_000))
///     .dimension("output_tokens", Throttle::per_minute(20_000))
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
    table: Arc<Table>,
    strict: bool,
    label: Label,
}

impl MultiLimiter {
    /// Starts building a multi-dimensional limiter.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{MultiLimiter, Throttle};
    ///
    /// let limiter = MultiLimiter::builder()
    ///     .dimension("requests", Throttle::per_second(10))
    ///     .build();
    /// assert_eq!(limiter.dimensions().collect::<Vec<_>>(), ["requests"]);
    /// ```
    pub fn builder() -> MultiLimiterBuilder {
        MultiLimiterBuilder::default()
    }

    #[inline]
    fn pairs<'a>(
        &'a self,
        costs: &'a [(&'a str, u32)],
    ) -> impl Iterator<Item = (&'a dyn Limiter, u32)> + Clone {
        self.table
            .dimensions
            .iter()
            .map(move |d| (d.limiter.as_ref(), self.table.cost_for(&d.name, costs)))
    }

    /// In strict mode, the index of the first unknown name in `costs`.
    #[inline]
    fn unknown(&self, costs: &[(&str, u32)]) -> Option<usize> {
        if self.strict {
            self.table.first_unknown(costs)
        } else {
            None
        }
    }

    /// Returns a copy that is strict (or lenient) about unknown dimension names.
    /// Useful for the presets, which are built lenient.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Decision, MultiLimiter, Throttle};
    ///
    /// let limiter = MultiLimiter::builder()
    ///     .dimension("requests", Throttle::per_second(10))
    ///     .build()
    ///     .with_strict(true);
    /// assert!(limiter.is_strict());
    /// assert_eq!(limiter.peek_costs(&[("reqeusts", 1)]), Decision::Impossible);
    /// ```
    #[must_use]
    pub fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Whether unknown dimension names are refused.
    #[must_use]
    pub fn is_strict(&self) -> bool {
        self.strict
    }

    /// Names this instance for metrics and events.
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

    /// The dimension names, in the order they were added (aliases excluded).
    pub fn dimensions(&self) -> impl Iterator<Item = &str> {
        self.table.dimensions.iter().map(|d| &*d.name)
    }

    /// The limiter behind `name` (a dimension or an alias), for inspection or
    /// direct reconciliation — `available`, `pause_for`, and so on.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{MultiLimiter, Throttle};
    ///
    /// let limiter = MultiLimiter::builder()
    ///     .dimension("tokens", Throttle::per_minute(1000))
    ///     .alias("input_tokens", "tokens")
    ///     .build();
    /// assert_eq!(limiter.dimension("input_tokens").map(|d| d.capacity()), Some(1000));
    /// assert!(limiter.dimension("missing").is_none());
    /// ```
    #[must_use]
    pub fn dimension(&self, name: &str) -> Option<&dyn Limiter> {
        let target = self.table.target(name)?;
        self.table
            .dimensions
            .iter()
            .find(|d| &*d.name == target)
            .map(|d| d.limiter.as_ref())
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
        if self.unknown(costs).is_some() {
            return Decision::Impossible;
        }
        peek_all(self.pairs(costs))
    }

    /// Attempts to charge the call's per-dimension costs without waiting,
    /// returning whether every dimension granted. All-or-nothing across
    /// dimensions; a strict limiter refuses unknown names.
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
        let granted = self.unknown(costs).is_none() && acquire_all(self.pairs(costs)).is_acquired();
        // One call, whatever its per-dimension shares.
        obs::attempt(KIND, &self.label, 1, granted);
        granted
    }

    /// Gives back a call's per-dimension costs, for a call that was charged but
    /// never sent (see [`Limiter::refund`]). Unknown names are ignored.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{MultiLimiter, Throttle};
    ///
    /// let limiter = MultiLimiter::builder()
    ///     .dimension("tokens", Throttle::per_second(100))
    ///     .build();
    /// assert!(limiter.try_acquire_costs(&[("tokens", 60)]));
    /// limiter.refund_costs(&[("tokens", 60)]);
    /// assert_eq!(limiter.available("tokens"), Some(100));
    /// ```
    pub fn refund_costs(&self, costs: &[(&str, u32)]) {
        refund_all(self.pairs(costs).filter(|(_, cost)| *cost > 0));
    }

    /// Settles a call that was admitted on an **estimate**: for each dimension,
    /// gives back what the call did not use, or charges the overspend.
    ///
    /// LLM calls are the motivating case: the output token count is unknown
    /// until the response arrives, so admit the call on an estimate (or the
    /// `max_tokens` ceiling), then settle with the usage the provider reports.
    /// An underspend is [refunded](Limiter::refund). An overspend has already
    /// happened at the provider, so it is charged even if it does not fit: on a
    /// token-bucket dimension it becomes debt (a reservation that later calls
    /// wait behind); a dimension without reservations is drained of what it
    /// has. Names that match no dimension are ignored.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use throttle_net::{ManualClock, MultiLimiter, Throttle};
    ///
    /// let clock = Arc::new(ManualClock::new());
    /// let limiter = MultiLimiter::builder()
    ///     .dimension("output_tokens", Throttle::per_minute(1_000).with_clock(clock.clone()))
    ///     .build();
    ///
    /// // Admit on the 500-token ceiling; the response used only 120.
    /// let estimate = [("output_tokens", 500)];
    /// assert!(limiter.try_acquire_costs(&estimate));
    /// limiter.settle_costs(&estimate, &[("output_tokens", 120)]);
    /// assert_eq!(limiter.available("output_tokens"), Some(880));
    ///
    /// // Another call overspent its estimate: the excess is owed.
    /// assert!(limiter.try_acquire_costs(&[("output_tokens", 800)]));
    /// limiter.settle_costs(&[("output_tokens", 800)], &[("output_tokens", 1_000)]);
    /// assert_eq!(limiter.available("output_tokens"), Some(0));
    /// assert!(!limiter.try_acquire_costs(&[("output_tokens", 1)]), "in debt");
    /// ```
    pub fn settle_costs(&self, estimated: &[(&str, u32)], actual: &[(&str, u32)]) {
        for dimension in &*self.table.dimensions {
            let reserved = self.table.cost_for(&dimension.name, estimated);
            let used = self.table.cost_for(&dimension.name, actual);
            let limiter = dimension.limiter.as_ref();
            match used.cmp(&reserved) {
                core::cmp::Ordering::Less => limiter.refund(reserved - used),
                core::cmp::Ordering::Greater => {
                    let _uncharged = limiter.charge(used - reserved);
                }
                core::cmp::Ordering::Equal => {}
            }
        }
    }

    /// Returns the units available in `dimension` (or the dimension an alias
    /// charges) right now, or `None` if there is no such name.
    #[must_use]
    pub fn available(&self, dimension: &str) -> Option<u32> {
        self.dimension(dimension).map(Limiter::available)
    }

    /// Pauses every dimension for `duration` (see [`Limiter::pause_for`]).
    pub fn pause_for(&self, duration: Duration) {
        for dimension in &*self.table.dimensions {
            dimension.limiter.pause_for(duration);
        }
    }

    /// Lifts every dimension's pause (see [`Limiter::resume`]).
    pub fn resume(&self) {
        for dimension in &*self.table.dimensions {
            dimension.limiter.resume();
        }
    }

    /// The longest remaining pause among the dimensions (`Duration::ZERO` when
    /// none is paused).
    #[must_use]
    pub fn paused_for(&self) -> Duration {
        self.table
            .dimensions
            .iter()
            .map(|d| d.limiter.paused_for())
            .max()
            .unwrap_or(Duration::ZERO)
    }

    /// Takes units from `dimension` until at most `remaining` are available,
    /// returning how many were taken, or `None` if there is no such name.
    /// Never adds units. Used to reconcile with a downstream's reported budget.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{MultiLimiter, Throttle};
    ///
    /// let limiter = MultiLimiter::builder()
    ///     .dimension("requests", Throttle::per_minute(100))
    ///     .build();
    /// assert_eq!(limiter.drain_to("requests", 10), Some(90));
    /// assert_eq!(limiter.available("requests"), Some(10));
    /// assert_eq!(limiter.drain_to("nope", 0), None);
    /// ```
    // Callers usually drain for the side effect; the count is informative.
    #[allow(clippy::must_use_candidate)]
    pub fn drain_to(&self, dimension: &str, remaining: u32) -> Option<u32> {
        self.dimension(dimension)
            .map(|limiter| crate::limiter::drain_to(limiter, remaining))
    }

    /// The precise error for a call that can never be granted: the first
    /// dimension whose share exceeds what it can ever grant.
    #[cfg(feature = "runtime")]
    fn never_error(&self, costs: &[(&str, u32)]) -> ThrottleError {
        let mut fallback = None;
        for dimension in &*self.table.dimensions {
            let cost = self.table.cost_for(&dimension.name, costs);
            let capacity = dimension.limiter.capacity();
            if cost > capacity {
                return ThrottleError::CostExceedsCapacity { cost, capacity };
            }
            if fallback.is_none() && dimension.limiter.peek(cost) == Decision::Impossible {
                fallback = Some(ThrottleError::CostExceedsCapacity { cost, capacity });
            }
        }
        fallback.unwrap_or(ThrottleError::CostExceedsCapacity {
            cost: 0,
            capacity: 0,
        })
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl MultiLimiter {
    /// Charges the call's per-dimension costs, waiting until every dimension
    /// can afford its share.
    ///
    /// This is the headline multi-dimensional operation: it paces the caller
    /// until all budgets allow the call, then commits all of them together.
    /// Each attempt reserves on every dimension at once (fair queueing on token
    /// buckets); if any cannot, the others are refunded and the waiter sleeps
    /// for the longest wait. **Cancel-safe:** dropping the future returns any
    /// reservation.
    ///
    /// # Errors
    ///
    /// - [`ThrottleError::CostExceedsCapacity`] when some dimension's share
    ///   exceeds that dimension's capacity (it names that dimension's
    ///   figures); such a call can never succeed.
    /// - [`ThrottleError::UnknownDimension`] when the limiter is strict and a
    ///   name matches no dimension or alias.
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
        self.wait(costs, None).await
    }

    /// Charges the call's per-dimension costs, waiting at most `timeout`.
    ///
    /// # Errors
    ///
    /// As [`acquire_costs`](Self::acquire_costs), plus
    /// [`ThrottleError::DeadlineExceeded`] if the dimensions cannot all grant
    /// within `timeout` (detected up front where the dimensions reserve).
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use std::time::Duration;
    /// use throttle_net::{MultiLimiter, Throttle};
    ///
    /// let limiter = MultiLimiter::builder()
    ///     .dimension("requests", Throttle::per_second(100))
    ///     .build();
    /// limiter
    ///     .acquire_costs_timeout(&[("requests", 1)], Duration::from_secs(2))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire_costs_timeout(
        &self,
        costs: &[(&str, u32)],
        timeout: Duration,
    ) -> Result<(), ThrottleError> {
        self.wait(costs, Some(timeout)).await
    }

    async fn wait(
        &self,
        costs: &[(&str, u32)],
        timeout: Option<Duration>,
    ) -> Result<(), ThrottleError> {
        use crate::limiter::{Progress, paused_all, release_all, step_all};
        use crate::wait::{Refusal, wait_with};
        let timer = obs::Timer::start();
        let result = match self.unknown(costs) {
            Some(index) => Err(ThrottleError::UnknownDimension { index }),
            None => wait_with(
                Progress::new(),
                |progress, budget| step_all(self.pairs(costs), budget, progress),
                |progress| release_all(self.pairs(costs), progress),
                || paused_all(self.pairs(costs)),
                timeout,
            )
            .await
            .map_err(|refusal| match refusal {
                Refusal::Never => self.never_error(costs),
                Refusal::TimedOut => ThrottleError::DeadlineExceeded,
            }),
        };
        obs::waited(KIND, &self.label, &timer, 1, obs::Waited::of(&result));
        result
    }
}

impl core::fmt::Debug for MultiLimiter {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        /// The dimensions as a `name => available` map.
        struct Dimensions<'a>(&'a Table);
        impl core::fmt::Debug for Dimensions<'_> {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.debug_map()
                    .entries(
                        self.0
                            .dimensions
                            .iter()
                            .map(|d| (&*d.name, d.limiter.available())),
                    )
                    .finish()
            }
        }
        f.debug_struct("MultiLimiter")
            .field("name", &self.label)
            .field("strict", &self.strict)
            .field("available", &Dimensions(&self.table))
            .finish()
    }
}

/// Builder for a [`MultiLimiter`].
///
/// Name each dimension with [`dimension`](Self::dimension), optionally add
/// [aliases](Self::alias) or relax [strict](Self::strict) name checking (on by
/// default), then [`build`](Self::build) (lenient about configuration
/// mistakes) or [`try_build`](Self::try_build) (reports them).
///
/// # Examples
///
/// ```
/// use throttle_net::{MultiLimiter, Throttle};
///
/// // OpenAI-style: one combined token budget, charged as input or output.
/// let limiter = MultiLimiter::builder()
///     .dimension("requests", Throttle::per_minute(500))
///     .dimension("tokens", Throttle::per_minute(30_000))
///     .alias("input_tokens", "tokens")
///     .alias("output_tokens", "tokens")
///     .try_build()?;
///
/// assert!(limiter.try_acquire_costs(&[("requests", 1), ("input_tokens", 1000), ("output_tokens", 500)]));
/// assert_eq!(limiter.available("tokens"), Some(28_500));
/// # Ok::<(), throttle_net::ThrottleError>(())
/// ```
#[must_use]
pub struct MultiLimiterBuilder {
    dimensions: Vec<Dimension>,
    aliases: Vec<Alias>,
    strict: bool,
    label: Label,
}

impl Default for MultiLimiterBuilder {
    /// No dimensions, strict about unknown names, unnamed.
    fn default() -> Self {
        Self {
            dimensions: Vec::new(),
            aliases: Vec::new(),
            strict: true,
            label: Label::none(),
        }
    }
}

impl MultiLimiterBuilder {
    /// Adds a named dimension backed by `limiter`.
    ///
    /// Adding the same name twice keeps both (each is charged the full cost);
    /// [`try_build`](Self::try_build) rejects it.
    pub fn dimension(self, name: impl Into<Box<str>>, limiter: impl Limiter + 'static) -> Self {
        self.shared(name, Arc::new(limiter))
    }

    /// Adds a named dimension backed by an already-shared limiter.
    pub fn shared(mut self, name: impl Into<Box<str>>, limiter: Arc<dyn Limiter>) -> Self {
        self.dimensions.push(Dimension {
            name: name.into(),
            limiter,
        });
        self
    }

    /// Makes `alias` charge the dimension `target`. Costs named by either are
    /// summed into that one budget.
    pub fn alias(mut self, alias: impl Into<Box<str>>, target: impl Into<Box<str>>) -> Self {
        self.aliases.push(Alias {
            name: alias.into(),
            target: target.into(),
        });
        self
    }

    /// Whether calls naming an unknown dimension are refused (`true`, the
    /// default) or the unknown names ignored (`false`). See [`MultiLimiter`].
    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Names the limiter for metrics and events.
    pub fn name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
        self
    }

    /// Builds the [`MultiLimiter`], keeping any configuration mistakes as they
    /// are: duplicate names are charged independently, and an alias to a
    /// missing dimension never matches.
    #[must_use]
    pub fn build(self) -> MultiLimiter {
        MultiLimiter {
            table: Arc::new(Table {
                dimensions: self.dimensions.into(),
                aliases: self.aliases.into(),
            }),
            strict: self.strict,
            label: self.label,
        }
    }

    /// Builds the [`MultiLimiter`], rejecting configuration mistakes.
    ///
    /// # Errors
    ///
    /// [`ThrottleError::InvalidConfig`] when two dimensions share a name, an
    /// alias reuses a dimension's or another alias's name, or an alias targets
    /// a dimension that does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{MultiLimiter, Throttle, ThrottleError};
    ///
    /// let err = MultiLimiter::builder()
    ///     .dimension("tokens", Throttle::per_second(10))
    ///     .alias("input", "token") // typo in the target
    ///     .try_build()
    ///     .unwrap_err();
    /// assert!(matches!(err, ThrottleError::InvalidConfig { .. }));
    /// ```
    pub fn try_build(self) -> Result<MultiLimiter, ThrottleError> {
        let invalid = |reason| Err(ThrottleError::InvalidConfig { reason });
        for (i, dimension) in self.dimensions.iter().enumerate() {
            if self.dimensions[..i]
                .iter()
                .any(|d| d.name == dimension.name)
            {
                return invalid("two dimensions share a name");
            }
        }
        for (i, alias) in self.aliases.iter().enumerate() {
            if self.dimensions.iter().any(|d| d.name == alias.name)
                || self.aliases[..i].iter().any(|a| a.name == alias.name)
            {
                return invalid("an alias reuses an existing dimension or alias name");
            }
            if !self.dimensions.iter().any(|d| d.name == alias.target) {
                return invalid("an alias targets a dimension that does not exist");
            }
        }
        Ok(self.build())
    }
}

impl core::fmt::Debug for MultiLimiterBuilder {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MultiLimiterBuilder")
            .field(
                "dimensions",
                &self.dimensions.iter().map(|d| &*d.name).collect::<Vec<_>>(),
            )
            .field("aliases", &self.aliases.len())
            .field("strict", &self.strict)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::MultiLimiter;
    use crate::decision::Decision;
    use crate::error::ThrottleError;
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
            .dimension("tokens", Throttle::per_minute(1000))
            .build();
        assert!(limiter.try_acquire_costs(&[("requests", 1), ("tokens", 1000)]));
        assert!(!limiter.try_acquire_costs(&[("requests", 1), ("tokens", 1)]));
        assert_eq!(limiter.available("requests"), Some(9));
    }

    #[test]
    fn test_unmentioned_dimension_is_not_charged() {
        let limiter = MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(2))
            .dimension("tokens", Throttle::per_second(100))
            .build();
        assert!(limiter.try_acquire_costs(&[("requests", 1)]));
        assert_eq!(limiter.available("tokens"), Some(100));
        assert_eq!(limiter.available("requests"), Some(1));
    }

    #[test]
    fn test_unknown_dimension_name_is_ignored_when_lenient() {
        let limiter = MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(1))
            .strict(false)
            .build();
        assert!(!limiter.is_strict());
        assert!(limiter.try_acquire_costs(&[("requests", 1), ("nonexistent", 999)]));
    }

    #[test]
    fn test_strict_mode_refuses_unknown_names() {
        // Regression (TN-016): a typo charged nothing and admitted the call.
        let limiter = MultiLimiter::builder()
            .dimension("input_tokens", Throttle::per_second(100))
            .build();
        assert!(limiter.is_strict(), "strict by default since 2.0");
        assert_eq!(
            limiter.peek_costs(&[("input_token", 5)]),
            Decision::Impossible
        );
        assert!(!limiter.try_acquire_costs(&[("input_token", 5)]));
        assert_eq!(
            limiter.available("input_tokens"),
            Some(100),
            "nothing charged"
        );
        assert!(limiter.try_acquire_costs(&[("input_tokens", 5)]));
        let lenient = limiter.with_strict(false);
        assert!(lenient.try_acquire_costs(&[("input_token", 5)]));
    }

    #[test]
    fn test_repeated_entries_and_aliases_are_summed() {
        let limiter = MultiLimiter::builder()
            .dimension("tokens", Throttle::per_second(100))
            .alias("input_tokens", "tokens")
            .alias("output_tokens", "tokens")
            .build();
        assert!(limiter.try_acquire_costs(&[
            ("input_tokens", 30),
            ("output_tokens", 20),
            ("tokens", 10)
        ]));
        assert_eq!(limiter.available("tokens"), Some(40));
        assert_eq!(limiter.available("output_tokens"), Some(40));
        // Combined, 30 + 20 exceeds the 40 left: refused as one budget.
        assert!(!limiter.try_acquire_costs(&[("input_tokens", 30), ("output_tokens", 20)]));
        assert_eq!(limiter.available("tokens"), Some(40));
    }

    #[test]
    fn test_zero_costs_are_always_granted() {
        let limiter = MultiLimiter::builder()
            .dimension("a", Throttle::per_second(0))
            .dimension("b", Throttle::per_second(1))
            .build();
        assert!(limiter.try_acquire_costs(&[("a", 0), ("b", 0)]));
        assert!(limiter.try_acquire_costs(&[]));
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

    #[test]
    fn test_try_build_rejects_mistakes_and_build_keeps_them() {
        let invalid = |b: super::MultiLimiterBuilder| {
            matches!(b.try_build(), Err(ThrottleError::InvalidConfig { .. }))
        };
        assert!(invalid(
            MultiLimiter::builder()
                .dimension("a", Throttle::per_second(1))
                .dimension("a", Throttle::per_second(1))
        ));
        assert!(invalid(
            MultiLimiter::builder()
                .dimension("a", Throttle::per_second(1))
                .alias("a", "a")
        ));
        assert!(invalid(
            MultiLimiter::builder()
                .dimension("a", Throttle::per_second(1))
                .alias("b", "a")
                .alias("b", "a")
        ));
        assert!(invalid(
            MultiLimiter::builder()
                .dimension("a", Throttle::per_second(1))
                .alias("b", "missing")
        ));
        // `build` keeps duplicates, each charged in full (the 1.0 behaviour).
        let dup = MultiLimiter::builder()
            .dimension("a", Throttle::per_second(1))
            .dimension("a", Throttle::per_second(5))
            .build();
        assert!(dup.try_acquire_costs(&[("a", 1)]));
        assert!(!dup.try_acquire_costs(&[("a", 1)]), "the 1/s twin binds");
    }

    #[test]
    fn test_settle_refunds_underspend_and_charges_overspend() {
        use crate::sliding::SlidingWindowLog;

        let clock = Arc::new(ManualClock::new());
        let limiter = MultiLimiter::builder()
            .dimension(
                "tokens",
                Throttle::per_second(100).with_clock(clock.clone()),
            )
            .dimension(
                "window",
                SlidingWindowLog::per_second(100).with_clock(clock.clone()),
            )
            .build();
        let estimate = [("tokens", 50), ("window", 50)];
        assert!(limiter.try_acquire_costs(&estimate));
        // Overspend far beyond capacity: three capacity-sized chunks of debt on
        // the bucket; the sliding log (no reservations) is drained.
        limiter.settle_costs(&estimate, &[("tokens", 300), ("window", 300)]);
        assert_eq!(limiter.available("tokens"), Some(0));
        assert_eq!(limiter.available("window"), Some(0));
        clock.advance(Duration::from_secs(2));
        assert_eq!(
            limiter.available("tokens"),
            Some(0),
            "debt still being repaid"
        );
        clock.advance(Duration::from_secs(1));
        assert!(limiter.try_acquire_costs(&[("tokens", 50)]));
        // Underspend refunds; unknown names and equal costs do nothing.
        limiter.settle_costs(&[("tokens", 50), ("missing", 9)], &[("tokens", 10)]);
        assert_eq!(limiter.available("tokens"), Some(90));
        limiter.settle_costs(&[("tokens", 5)], &[("tokens", 5)]);
        assert_eq!(limiter.available("tokens"), Some(90));
        // A dimension that grants nothing is skipped rather than looped on.
        let closed = MultiLimiter::builder()
            .dimension("x", Throttle::per_second(0))
            .build();
        closed.settle_costs(&[], &[("x", 10)]);
    }

    #[test]
    fn test_drain_pause_refund_and_inspection() {
        let clock = Arc::new(ManualClock::new());
        let limiter = MultiLimiter::builder()
            .dimension(
                "requests",
                Throttle::per_second(10).with_clock(clock.clone()),
            )
            .dimension(
                "tokens",
                Throttle::per_second(100).with_clock(clock.clone()),
            )
            .name("llm")
            .build();
        assert_eq!(limiter.name(), Some("llm"));
        assert_eq!(limiter.drain_to("requests", 4), Some(6));
        assert_eq!(limiter.drain_to("missing", 4), None);
        assert!(limiter.try_acquire_costs(&[("tokens", 50)]));
        limiter.refund_costs(&[("tokens", 50), ("missing", 9)]);
        assert_eq!(limiter.available("tokens"), Some(100));
        limiter.pause_for(Duration::from_secs(5));
        assert_eq!(
            limiter.peek_costs(&[("requests", 1)]),
            Decision::Retry {
                after: Duration::from_secs(5)
            }
        );
        clock.advance(Duration::from_secs(5));
        assert!(limiter.try_acquire_costs(&[("requests", 1)]));
        let text = format!("{limiter:?}");
        assert!(text.contains("requests") && text.contains("\"llm\""));
        assert!(format!("{:?}", MultiLimiter::builder()).contains("MultiLimiterBuilder"));
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_costs_errors_and_names_the_overspent_dimension() {
        let limiter = MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(100))
            .dimension("tokens", Throttle::per_second(1000))
            .build();
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

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_strict_acquire_reports_the_unknown_index() {
        let limiter = MultiLimiter::builder()
            .dimension("requests", Throttle::per_second(100))
            .strict(true)
            .build();
        let err = limiter
            .acquire_costs(&[("requests", 1), ("tokns", 9)])
            .await
            .unwrap_err();
        assert_eq!(err, ThrottleError::UnknownDimension { index: 1 });
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(start_paused = true)]
    async fn test_acquire_costs_timeout_fails_fast() {
        let limiter = MultiLimiter::builder()
            .dimension(
                "requests",
                Throttle::per_duration(1, Duration::from_secs(60)),
            )
            .build();
        assert!(limiter.try_acquire_costs(&[("requests", 1)]));
        let start = tokio::time::Instant::now();
        let err = limiter
            .acquire_costs_timeout(&[("requests", 1)], Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(err, ThrottleError::DeadlineExceeded);
        assert_eq!(start.elapsed(), Duration::ZERO);
        assert!(limiter.acquire_costs(&[("requests", 1)]).await.is_ok());
    }
}
