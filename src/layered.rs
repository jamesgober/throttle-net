//! Layered scopes: a request must clear a global, a per-key, and a per-endpoint
//! limit, in that order.

use core::hash::Hash;
use core::time::Duration;
use std::borrow::Cow;
use std::sync::Arc;

use clock_lib::Clock;

use crate::decision::Decision;
#[cfg(feature = "runtime")]
use crate::error::ThrottleError;
use crate::limiter::{KeyedLimiter, Limiter, Verdict, finish_peek, fold_peek};
use crate::obs::{self, Label};
use crate::perkey::PerKey;

/// The limiter kind reported in metrics and events.
const KIND: &str = "layered";

/// Several scopes of limiting stacked so a request must clear every one.
///
/// A real service limits at more than one granularity at once: an overall
/// ceiling for the whole process (the *global* scope), a fair share per caller
/// (the *per-key* scope, keyed by tenant or token), and a ceiling per route (the
/// *per-endpoint* scope). `Layered` checks the scopes that are configured and
/// admits a request only when all of them can afford it — applied atomically by
/// the same peek-then-commit rule the other composites use, with a refund of
/// the scopes already charged if a concurrent caller wins a race, so a request
/// never spends in one scope when another blocks it.
///
/// The two key types are independent: a numeric tenant id and a string endpoint,
/// say. They default to the same type for the common all-string case.
///
/// Build one with [`Layered::builder`]. Every scope is optional; a builder with
/// none admits everything.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "runtime")]
/// # async fn run() -> Result<(), throttle_net::ThrottleError> {
/// use throttle_net::{Layered, PerKey, Throttle};
///
/// // 1000/s overall, 100/s per tenant, 50/s per endpoint.
/// let layered = Layered::<String>::builder()
///     .global(Throttle::per_second(1000))
///     .per_key(PerKey::per_second(100))
///     .per_endpoint(PerKey::per_second(50))
///     .build();
///
/// layered
///     .acquire(&"tenant:42".to_string(), &"/v1/chat".to_string())
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct Layered<K, E = K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    E: Eq + Hash + Clone + Send + Sync + 'static,
{
    global: Option<Arc<dyn Limiter>>,
    per_key: Option<Arc<dyn KeyedLimiter<K>>>,
    per_endpoint: Option<Arc<dyn KeyedLimiter<E>>>,
    label: Label,
}

impl<K, E> Layered<K, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    E: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Starts building a layered limiter.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Layered, Throttle};
    ///
    /// let layered = Layered::<&str>::builder().global(Throttle::per_second(5)).build();
    /// assert_eq!(layered.capacity(), 5);
    /// ```
    pub fn builder() -> LayeredBuilder<K, E> {
        LayeredBuilder {
            global: None,
            per_key: None,
            per_endpoint: None,
            label: Label::none(),
        }
    }

    /// The instance name, if one was set with [`LayeredBuilder::name`].
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.label.get()
    }

    /// Aggregate, non-consuming peek across every configured scope: the longest
    /// wait, or [`Decision::Impossible`] if any scope can never grant `cost`.
    fn peek_scopes(&self, key: &K, endpoint: &E, cost: u32) -> Decision {
        let peeks = [
            self.global.as_ref().map(|g| g.peek(cost)),
            self.per_key.as_ref().map(|pk| pk.peek(key, cost)),
            self.per_endpoint.as_ref().map(|pe| pe.peek(endpoint, cost)),
        ];
        finish_peek(peeks.into_iter().flatten().fold(Verdict::Ready, fold_peek))
    }

    /// Gives back `cost` to the first `charged` scopes, in commit order
    /// (global, per-key, per-endpoint): the ones a failed commit or a cancelled
    /// reservation had already charged. Unconfigured scopes count as charged.
    fn refund_scopes(&self, key: &K, endpoint: &E, cost: u32, charged: usize) {
        if charged >= 1 {
            if let Some(scope) = &self.global {
                scope.refund(cost);
            }
        }
        if charged >= 2 {
            if let Some(scope) = &self.per_key {
                scope.refund(key, cost);
            }
        }
        if charged >= 3 {
            if let Some(scope) = &self.per_endpoint {
                scope.refund(endpoint, cost);
            }
        }
    }

    /// The consuming core: peek every scope and, only if all would grant,
    /// commit each in order. A commit that loses a race after the peek refunds
    /// the scopes already charged and reports the refusing scope's decision.
    fn decide(&self, key: &K, endpoint: &E, cost: u32) -> Decision {
        match self.peek_scopes(key, endpoint, cost) {
            Decision::Acquired => {}
            other => return other,
        }
        if let Some(scope) = &self.global {
            let decision = scope.acquire_cost(cost);
            if !decision.is_acquired() {
                return decision;
            }
        }
        if let Some(scope) = &self.per_key {
            let decision = scope.acquire_cost(key, cost);
            if !decision.is_acquired() {
                self.refund_scopes(key, endpoint, cost, 1);
                return decision;
            }
        }
        if let Some(scope) = &self.per_endpoint {
            let decision = scope.acquire_cost(endpoint, cost);
            if !decision.is_acquired() {
                self.refund_scopes(key, endpoint, cost, 2);
                return decision;
            }
        }
        Decision::Acquired
    }

    /// Reserves `cost` in every scope within `max_wait`, or nothing at all.
    #[cfg(feature = "runtime")]
    fn reserve_scopes(
        &self,
        key: &K,
        endpoint: &E,
        cost: u32,
        max_wait: Duration,
    ) -> Option<Duration> {
        let mut longest = Duration::ZERO;
        if let Some(scope) = &self.global {
            longest = longest.max(scope.reserve(cost, max_wait)?);
        }
        if let Some(scope) = &self.per_key {
            let Some(wait) = scope.reserve(key, cost, max_wait) else {
                self.refund_scopes(key, endpoint, cost, 1);
                return None;
            };
            longest = longest.max(wait);
        }
        if let Some(scope) = &self.per_endpoint {
            let Some(wait) = scope.reserve(endpoint, cost, max_wait) else {
                self.refund_scopes(key, endpoint, cost, 2);
                return None;
            };
            longest = longest.max(wait);
        }
        Some(longest)
    }

    /// The binding capacity across configured scopes: the smallest, since that is
    /// the first ceiling a request hits. No scopes means unbounded ([`u32::MAX`]).
    #[must_use]
    pub fn capacity(&self) -> u32 {
        let caps = [
            self.global.as_ref().map(|g| g.capacity()),
            self.per_key.as_ref().map(|pk| pk.capacity()),
            self.per_endpoint.as_ref().map(|pe| pe.capacity()),
        ];
        caps.into_iter().flatten().min().unwrap_or(u32::MAX)
    }

    /// Attempts to admit one request for `(key, endpoint)` without waiting.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Layered, PerKey, Throttle};
    ///
    /// let layered = Layered::<&str>::builder()
    ///     .global(Throttle::per_second(2))
    ///     .per_key(PerKey::per_second(1))
    ///     .build();
    ///
    /// assert!(layered.try_acquire(&"a", &"/x"));
    /// // The per-key scope for "a" is now empty, even though the global has room.
    /// assert!(!layered.try_acquire(&"a", &"/x"));
    /// assert!(layered.try_acquire(&"b", &"/x")); // a different key is independent
    /// ```
    #[inline]
    #[must_use]
    pub fn try_acquire(&self, key: &K, endpoint: &E) -> bool {
        self.try_acquire_with_cost(key, endpoint, 1)
    }

    /// Attempts to admit a request of weight `cost` for `(key, endpoint)` without
    /// waiting.
    #[inline]
    #[must_use]
    pub fn try_acquire_with_cost(&self, key: &K, endpoint: &E, cost: u32) -> bool {
        let granted = self.decide(key, endpoint, cost).is_acquired();
        obs::attempt(KIND, &self.label, cost, granted);
        granted
    }

    /// Reports whether a request for `(key, endpoint)` would be admitted now,
    /// without taking anything.
    #[inline]
    #[must_use]
    pub fn peek(&self, key: &K, endpoint: &E, cost: u32) -> Decision {
        self.peek_scopes(key, endpoint, cost)
    }

    /// Gives back a request's `cost` to every configured scope, for a request
    /// that was admitted but never sent.
    pub fn refund(&self, key: &K, endpoint: &E, cost: u32) {
        self.refund_scopes(key, endpoint, cost, 3);
    }

    /// Pauses the global scope for `duration` (see [`Limiter::pause_for`]); a
    /// layered limiter without a global scope is unaffected.
    pub fn pause_for(&self, duration: Duration) {
        if let Some(scope) = &self.global {
            scope.pause_for(duration);
        }
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl<K, E> Layered<K, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    E: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Admits one request for `(key, endpoint)`, waiting until every scope can.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] if some scope's capacity is
    /// too small to ever admit the request.
    pub async fn acquire(&self, key: &K, endpoint: &E) -> Result<(), ThrottleError> {
        self.acquire_with_cost(key, endpoint, 1).await
    }

    /// Admits a request of weight `cost` for `(key, endpoint)`, waiting until
    /// every scope can. Each attempt reserves in every scope at once (rolling
    /// back if any cannot). **Cancel-safe:** dropping the future returns its
    /// reservations.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] if some scope can never
    /// admit `cost`.
    pub async fn acquire_with_cost(
        &self,
        key: &K,
        endpoint: &E,
        cost: u32,
    ) -> Result<(), ThrottleError> {
        self.wait(key, endpoint, cost, None).await
    }

    /// Admits a request of weight `cost`, waiting at most `timeout`.
    ///
    /// # Errors
    ///
    /// [`ThrottleError::DeadlineExceeded`] if the scopes cannot all admit the
    /// request within `timeout`; [`ThrottleError::CostExceedsCapacity`] if some
    /// scope can never admit `cost`.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use std::time::Duration;
    /// use throttle_net::{Layered, PerKey, Throttle};
    ///
    /// let layered = Layered::<&str>::builder()
    ///     .global(Throttle::per_second(100))
    ///     .per_key(PerKey::per_second(10))
    ///     .build();
    /// layered
    ///     .acquire_with_cost_timeout(&"t1", &"/x", 1, Duration::from_millis(200))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire_with_cost_timeout(
        &self,
        key: &K,
        endpoint: &E,
        cost: u32,
        timeout: Duration,
    ) -> Result<(), ThrottleError> {
        self.wait(key, endpoint, cost, Some(timeout)).await
    }

    async fn wait(
        &self,
        key: &K,
        endpoint: &E,
        cost: u32,
        timeout: Option<Duration>,
    ) -> Result<(), ThrottleError> {
        use crate::wait::{Refusal, Step, wait_for};
        let timer = obs::Timer::start();
        let result = wait_for(
            |budget| match self.reserve_scopes(key, endpoint, cost, budget) {
                Some(wait) => Step::Granted(wait),
                None => match self.peek_scopes(key, endpoint, cost) {
                    Decision::Impossible => Step::Never,
                    Decision::Retry { after } => Step::Retry(after),
                    _ => Step::Retry(Duration::ZERO),
                },
            },
            || self.refund(key, endpoint, cost),
            timeout,
        )
        .await
        .map_err(|refusal| match refusal {
            Refusal::Never => ThrottleError::CostExceedsCapacity {
                cost,
                capacity: self.capacity(),
            },
            Refusal::TimedOut => ThrottleError::DeadlineExceeded,
        });
        obs::waited(KIND, &self.label, &timer, cost, obs::Waited::of(&result));
        result
    }
}

impl<K, E> core::fmt::Debug for Layered<K, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    E: Eq + Hash + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Layered")
            .field("name", &self.label)
            .field("global", &self.global.is_some())
            .field("per_key", &self.per_key.is_some())
            .field("per_endpoint", &self.per_endpoint.is_some())
            .field("capacity", &self.capacity())
            .finish()
    }
}

/// Builder for a [`Layered`] limiter.
///
/// Set any subset of the three scopes; omitted scopes simply do not constrain.
///
/// # Examples
///
/// ```
/// use throttle_net::{Layered, PerKey, Throttle};
///
/// let layered = Layered::<u64, String>::builder()
///     .global(Throttle::per_second(1000))
///     .per_key(PerKey::per_second(100))   // keyed by numeric tenant id
///     .name("gateway")
///     .build();
/// assert_eq!(layered.name(), Some("gateway"));
/// ```
#[must_use]
pub struct LayeredBuilder<K, E = K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    E: Eq + Hash + Clone + Send + Sync + 'static,
{
    global: Option<Arc<dyn Limiter>>,
    per_key: Option<Arc<dyn KeyedLimiter<K>>>,
    per_endpoint: Option<Arc<dyn KeyedLimiter<E>>>,
    label: Label,
}

impl<K, E> LayeredBuilder<K, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    E: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Sets the global scope: one limiter shared by every request. Any
    /// [`Limiter`] works, so the global ceiling can itself be a
    /// [`Hybrid`](crate::Hybrid).
    pub fn global(mut self, limiter: impl Limiter + 'static) -> Self {
        self.global = Some(Arc::new(limiter));
        self
    }

    /// Sets the global scope to an already-shared limiter.
    pub fn shared_global(mut self, limiter: Arc<dyn Limiter>) -> Self {
        self.global = Some(limiter);
        self
    }

    /// Sets the per-key scope: independent state per caller key. Accepts a
    /// [`PerKey`] built on any clock.
    pub fn per_key<C>(mut self, limiter: PerKey<K, C>) -> Self
    where
        C: Clock + 'static,
    {
        self.per_key = Some(Arc::new(limiter));
        self
    }

    /// Sets the per-endpoint scope: independent state per endpoint. Accepts a
    /// [`PerKey`] built on any clock.
    pub fn per_endpoint<C>(mut self, limiter: PerKey<E, C>) -> Self
    where
        C: Clock + 'static,
    {
        self.per_endpoint = Some(Arc::new(limiter));
        self
    }

    /// Names the limiter for metrics and events.
    pub fn name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
        self
    }

    /// Builds the [`Layered`] limiter.
    #[must_use]
    pub fn build(self) -> Layered<K, E> {
        Layered {
            global: self.global,
            per_key: self.per_key,
            per_endpoint: self.per_endpoint,
            label: self.label,
        }
    }
}

impl<K, E> core::fmt::Debug for LayeredBuilder<K, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    E: Eq + Hash + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LayeredBuilder")
            .field("name", &self.label)
            .field("global", &self.global.is_some())
            .field("per_key", &self.per_key.is_some())
            .field("per_endpoint", &self.per_endpoint.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::Layered;
    use crate::decision::Decision;
    use crate::limiter::Limiter;
    use crate::perkey::PerKey;
    use crate::throttle::Throttle;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_layered_is_send_sync() {
        assert_send_sync::<Layered<String>>();
        assert_send_sync::<Layered<u64, String>>();
    }

    #[test]
    fn test_request_must_clear_all_three_scopes() {
        let layered = Layered::<&str>::builder()
            .global(Throttle::per_second(100))
            .per_key(PerKey::per_second(2))
            .per_endpoint(PerKey::per_second(100))
            .build();
        assert!(layered.try_acquire(&"tenant", &"/x"));
        assert!(layered.try_acquire(&"tenant", &"/x"));
        assert!(!layered.try_acquire(&"tenant", &"/x"));
    }

    #[test]
    fn test_keys_and_endpoints_are_independent() {
        let layered = Layered::<&str>::builder()
            .per_key(PerKey::per_second(1))
            .per_endpoint(PerKey::per_second(1))
            .build();
        assert!(layered.try_acquire(&"a", &"/x"));
        assert!(!layered.try_acquire(&"a", &"/x"));
        assert!(!layered.try_acquire(&"b", &"/x"));
        assert!(layered.try_acquire(&"b", &"/y"));
    }

    #[test]
    fn test_global_scope_binds_across_keys() {
        let layered = Layered::<&str>::builder()
            .global(Throttle::per_second(2))
            .per_key(PerKey::per_second(100))
            .build();
        assert!(layered.try_acquire(&"a", &"/x"));
        assert!(layered.try_acquire(&"b", &"/x"));
        assert!(!layered.try_acquire(&"c", &"/x"));
    }

    #[test]
    fn test_no_scope_admits_everything() {
        let layered = Layered::<&str>::builder().build();
        assert!(layered.try_acquire(&"anything", &"/anywhere"));
        assert_eq!(layered.capacity(), u32::MAX);
    }

    #[test]
    fn test_no_token_spent_in_one_scope_when_another_blocks() {
        let global: Arc<dyn Limiter> = Arc::new(Throttle::per_second(100));
        let layered = Layered::<&str>::builder()
            .shared_global(global.clone())
            .per_key(PerKey::per_second(1))
            .build();
        assert!(layered.try_acquire(&"a", &"/x"));
        assert!(!layered.try_acquire(&"a", &"/x"));
        assert_eq!(global.available(), 99, "the refused request cost nothing");
    }

    #[test]
    fn test_capacity_is_the_smallest_scope() {
        let layered = Layered::<&str>::builder()
            .global(Throttle::per_second(1000))
            .per_key(PerKey::per_second(100))
            .per_endpoint(PerKey::per_second(25))
            .build();
        assert_eq!(layered.capacity(), 25);
    }

    #[test]
    fn test_peek_refund_pause_and_debug() {
        let layered = Layered::<&str>::builder()
            .global(Throttle::per_second(10))
            .per_key(PerKey::per_second(5))
            .per_endpoint(PerKey::per_second(5))
            .name("edge")
            .build();
        assert!(layered.peek(&"k", &"/e", 5).is_acquired());
        assert_eq!(layered.peek(&"k", &"/e", 6), Decision::Impossible);
        assert!(layered.try_acquire_with_cost(&"k", &"/e", 5));
        assert!(!layered.try_acquire(&"k", &"/e"));
        layered.refund(&"k", &"/e", 5);
        assert!(layered.try_acquire_with_cost(&"k", &"/e", 5));
        layered.pause_for(Duration::from_secs(9));
        assert!(matches!(
            layered.peek(&"z", &"/z", 1),
            Decision::Retry { .. }
        ));
        assert!(format!("{layered:?}").contains("\"edge\""));
        assert!(format!("{:?}", Layered::<u8>::builder()).contains("LayeredBuilder"));
        // A layered limiter without a global scope ignores a pause.
        Layered::<u8>::builder()
            .build()
            .pause_for(Duration::from_secs(1));
    }

    #[cfg(feature = "runtime")]
    #[test]
    fn test_reserve_scopes_rolls_back_when_a_later_scope_cannot() {
        let global: Arc<dyn Limiter> = Arc::new(Throttle::per_second(10));
        let layered = Layered::<&str>::builder()
            .shared_global(global.clone())
            .per_key(PerKey::per_second(1))
            .build();
        assert!(layered.try_acquire(&"k", &"/"));
        assert_eq!(layered.reserve_scopes(&"k", &"/", 1, Duration::ZERO), None);
        assert_eq!(global.available(), 9, "the global reservation was refunded");
        assert!(
            layered
                .reserve_scopes(&"k", &"/", 1, Duration::from_secs(2))
                .is_some()
        );
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_errors_when_a_scope_can_never_admit() {
        use crate::error::ThrottleError;
        let layered = Layered::<&str>::builder()
            .global(Throttle::per_second(1000))
            .per_key(PerKey::per_second(5))
            .build();
        let err = layered.acquire_with_cost(&"a", &"/x", 9).await.unwrap_err();
        assert_eq!(
            err,
            ThrottleError::CostExceedsCapacity {
                cost: 9,
                capacity: 5
            }
        );
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(start_paused = true)]
    async fn test_acquire_waits_times_out_and_cancels_cleanly() {
        use crate::error::ThrottleError;
        let global: Arc<dyn Limiter> = Arc::new(Throttle::per_second(100));
        let layered = Layered::<&str>::builder()
            .shared_global(global.clone())
            .per_key(PerKey::per_duration(1, Duration::from_secs(10)))
            .build();
        assert!(layered.try_acquire(&"k", &"/"));
        let err = layered
            .acquire_with_cost_timeout(&"k", &"/", 1, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(err, ThrottleError::DeadlineExceeded);
        let cancelled =
            tokio::time::timeout(Duration::from_millis(1), layered.acquire(&"k", &"/")).await;
        assert!(cancelled.is_err());
        assert_eq!(
            global.available(),
            99,
            "the cancelled reservation was refunded"
        );
        assert!(layered.acquire(&"k", &"/").await.is_ok());
    }
}
