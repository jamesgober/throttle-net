//! Layered scopes: a request must clear a global, a per-key, and a per-endpoint
//! limit, in that order.

use core::hash::Hash;
use core::time::Duration;
use std::sync::Arc;

use clock_lib::Clock;

use crate::decision::Decision;
#[cfg(feature = "runtime")]
use crate::error::ThrottleError;
use crate::limiter::{KeyedLimiter, Limiter};
use crate::perkey::PerKey;

/// Several scopes of limiting stacked so a request must clear every one.
///
/// A real service limits at more than one granularity at once: an overall
/// ceiling for the whole process (the *global* scope), a fair share per caller
/// (the *per-key* scope, keyed by tenant or token), and a ceiling per route (the
/// *per-endpoint* scope). `Layered` checks the scopes that are configured and
/// admits a request only when all of them can afford it — applied atomically by
/// the same peek-then-commit rule the other composites use, so a request never
/// spends in one scope when another blocks it.
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
}

impl<K, E> Layered<K, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    E: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Starts building a layered limiter.
    #[must_use]
    pub fn builder() -> LayeredBuilder<K, E> {
        LayeredBuilder {
            global: None,
            per_key: None,
            per_endpoint: None,
        }
    }

    /// Aggregate, non-consuming peek across every configured scope: the longest
    /// wait, or [`Decision::Impossible`] if any scope can never grant `cost`.
    fn peek_scopes(&self, key: &K, endpoint: &E, cost: u32) -> Decision {
        let mut wait: Option<Duration> = None;
        let peeks = [
            self.global.as_ref().map(|g| g.peek(cost)),
            self.per_key.as_ref().map(|pk| pk.peek(key, cost)),
            self.per_endpoint.as_ref().map(|pe| pe.peek(endpoint, cost)),
        ];
        for decision in peeks.into_iter().flatten() {
            match decision {
                Decision::Acquired => {}
                Decision::Retry { after } => {
                    wait = Some(wait.map_or(after, |w| w.max(after)));
                }
                Decision::Impossible => return Decision::Impossible,
            }
        }
        wait.map_or(Decision::Acquired, |after| Decision::Retry { after })
    }

    /// Commits `cost` to each configured scope in order, short-circuiting on the
    /// first refusal. Returns whether every scope granted.
    fn commit_scopes(&self, key: &K, endpoint: &E, cost: u32) -> bool {
        if let Some(global) = &self.global {
            if !global.acquire_cost(cost).is_acquired() {
                return false;
            }
        }
        if let Some(per_key) = &self.per_key {
            if !per_key.try_acquire_with_cost(key, cost) {
                return false;
            }
        }
        if let Some(per_endpoint) = &self.per_endpoint {
            if !per_endpoint.try_acquire_with_cost(endpoint, cost) {
                return false;
            }
        }
        true
    }

    /// The consuming core: peek every scope, and only if all would grant, commit
    /// each. A commit that loses a race after the peek leaves the
    /// already-committed scopes debited and reports a retry (never an
    /// over-admission), exactly as the other composites do.
    fn decide(&self, key: &K, endpoint: &E, cost: u32) -> Decision {
        match self.peek_scopes(key, endpoint, cost) {
            Decision::Acquired => {}
            other => return other,
        }
        if self.commit_scopes(key, endpoint, cost) {
            return Decision::Acquired;
        }
        // Lost a race after the peek. Re-peek for an accurate wait; if it now
        // reads grantable again, nudge the caller to retry immediately.
        match self.peek_scopes(key, endpoint, cost) {
            Decision::Acquired => Decision::Retry {
                after: Duration::ZERO,
            },
            other => other,
        }
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
        self.decide(key, endpoint, cost).is_acquired()
    }

    /// Reports whether a request for `(key, endpoint)` would be admitted now,
    /// without taking anything.
    #[inline]
    #[must_use]
    pub fn peek(&self, key: &K, endpoint: &E, cost: u32) -> Decision {
        self.peek_scopes(key, endpoint, cost)
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
    /// every scope can.
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
        loop {
            match self.decide(key, endpoint, cost) {
                Decision::Acquired => return Ok(()),
                Decision::Impossible => {
                    return Err(ThrottleError::CostExceedsCapacity {
                        cost,
                        capacity: self.capacity(),
                    });
                }
                Decision::Retry { after } => crate::rt::sleep(after).await,
            }
        }
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
///     .build();
/// # let _ = layered;
/// ```
pub struct LayeredBuilder<K, E = K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    E: Eq + Hash + Clone + Send + Sync + 'static,
{
    global: Option<Arc<dyn Limiter>>,
    per_key: Option<Arc<dyn KeyedLimiter<K>>>,
    per_endpoint: Option<Arc<dyn KeyedLimiter<E>>>,
}

impl<K, E> LayeredBuilder<K, E>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    E: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Sets the global scope: one limiter shared by every request. Any
    /// [`Limiter`] works, so the global ceiling can itself be a
    /// [`Hybrid`](crate::Hybrid).
    #[must_use]
    pub fn global(mut self, limiter: impl Limiter + 'static) -> Self {
        self.global = Some(Arc::new(limiter));
        self
    }

    /// Sets the per-key scope: independent state per caller key. Accepts a
    /// [`PerKey`] built on any clock.
    #[must_use]
    pub fn per_key<C>(mut self, limiter: PerKey<K, C>) -> Self
    where
        C: Clock + Clone + 'static,
    {
        self.per_key = Some(Arc::new(limiter));
        self
    }

    /// Sets the per-endpoint scope: independent state per endpoint. Accepts a
    /// [`PerKey`] built on any clock.
    #[must_use]
    pub fn per_endpoint<C>(mut self, limiter: PerKey<E, C>) -> Self
    where
        C: Clock + Clone + 'static,
    {
        self.per_endpoint = Some(Arc::new(limiter));
        self
    }

    /// Builds the [`Layered`] limiter.
    #[must_use]
    pub fn build(self) -> Layered<K, E> {
        Layered {
            global: self.global,
            per_key: self.per_key,
            per_endpoint: self.per_endpoint,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::Layered;
    use crate::perkey::PerKey;
    use crate::throttle::Throttle;

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
        // The per-key scope (2/s) is exhausted though global and endpoint have
        // room, so the layered limiter refuses.
        assert!(!layered.try_acquire(&"tenant", &"/x"));
    }

    #[test]
    fn test_keys_and_endpoints_are_independent() {
        let layered = Layered::<&str>::builder()
            .per_key(PerKey::per_second(1))
            .per_endpoint(PerKey::per_second(1))
            .build();

        assert!(layered.try_acquire(&"a", &"/x"));
        // Same key, same endpoint: both scopes exhausted.
        assert!(!layered.try_acquire(&"a", &"/x"));
        // A different key on the same endpoint is blocked by the endpoint scope.
        assert!(!layered.try_acquire(&"b", &"/x"));
        // A different key on a different endpoint clears both.
        assert!(layered.try_acquire(&"b", &"/y"));
    }

    #[test]
    fn test_global_scope_binds_across_keys() {
        let layered = Layered::<&str>::builder()
            .global(Throttle::per_second(2))
            .per_key(PerKey::per_second(100))
            .build();

        // The global ceiling of 2 is spent across two different keys.
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
        let layered = Layered::<&str>::builder()
            .global(Throttle::per_second(100))
            .per_key(PerKey::per_second(1))
            .build();

        assert!(layered.try_acquire(&"a", &"/x")); // global: 99 left, key a: 0 left
        // Key "a" is blocked; the global scope must not be charged for the
        // refused request.
        assert!(!layered.try_acquire(&"a", &"/x"));
        // Global still has room for other keys: 99 - 1 (for b) succeeds.
        assert!(layered.try_acquire(&"b", &"/x"));
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

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_errors_when_a_scope_can_never_admit() {
        use crate::error::ThrottleError;

        let layered = Layered::<&str>::builder()
            .global(Throttle::per_second(1000))
            .per_key(PerKey::per_second(5))
            .build();
        let err = layered.acquire_with_cost(&"a", &"/x", 9).await.unwrap_err();
        assert!(matches!(
            err,
            ThrottleError::CostExceedsCapacity {
                cost: 9,
                capacity: 5
            }
        ));
    }
}
