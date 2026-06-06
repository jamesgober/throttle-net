//! Independent throttling per key, with sharded state and bounded memory.

use core::time::Duration;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use ahash::RandomState;
use clock_lib::{Clock, Monotonic, SystemClock};

use crate::decision::Decision;
#[cfg(feature = "runtime")]
use crate::error::ThrottleError;
use crate::eviction::Eviction;
use crate::limiter::Limiter;
use crate::throttle::Throttle;

/// Default shard count, before rounding up to a power of two. A handful of shards
/// keeps unrelated keys from serialising without wasting memory on a small store.
const DEFAULT_SHARDS: usize = 16;

/// Per-key state: that key's throttle, plus a "last seen" stamp for eviction.
///
/// The stamp is monotonic milliseconds since the store's epoch when an idle TTL
/// is configured (so idle expiry can compare against real time), and a per-shard
/// logical sequence number otherwise — same least-recently-seen *ordering* for
/// capacity eviction without a clock read on every access.
struct Entry<C: Clock> {
    throttle: Throttle<C>,
    last_seen: AtomicU64,
}

/// One shard: an independently locked slice of the key space, hashed with
/// `ahash` (fast, and collision-attack resistant via its random seed).
struct Shard<K, C: Clock> {
    map: RwLock<HashMap<K, Entry<C>, RandomState>>,
    /// Per-shard counter handing out "last seen" stamps when no TTL is set.
    /// Per-shard so unrelated shards never contend on it.
    seq: AtomicU64,
}

impl<K, C: Clock> Shard<K, C> {
    fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::default()),
            seq: AtomicU64::new(0),
        }
    }
}

/// A throttle that keeps independent state per key.
///
/// Each distinct key — a tenant, a user, an API token — gets its own token
/// bucket with the same configured rate, so one noisy key cannot spend another's
/// budget. State lives in a **sharded** concurrent map: keys are spread across
/// shards by hash, each shard has its own lock, and an existing key's acquire
/// takes only a shard *read* lock plus the bucket's own atomic accounting, so
/// unrelated keys never contend and throughput scales with cores.
///
/// Memory is **bounded by eviction** (see [`Eviction`]): idle keys expire and a
/// hard cap bounds the total, so a flood of unique keys reaches a ceiling instead
/// of growing without limit. Eviction is lazy and per-shard — it runs while
/// inserting a new key, never on a background thread or the steady-state path.
/// The default policy is bounded ([`Eviction::default`]).
///
/// Like [`Throttle`], the headline [`acquire`](Self::acquire) **waits**; the
/// `try_*` variants do not.
///
/// # Examples
///
/// ```
/// # async fn run() -> Result<(), throttle_net::ThrottleError> {
/// use throttle_net::PerKey;
///
/// // 100 requests per second, per tenant.
/// let limiter: PerKey<String> = PerKey::per_second(100);
/// limiter.acquire(&"tenant:42".to_string()).await?;
/// # Ok(())
/// # }
/// ```
pub struct PerKey<K, C = SystemClock>
where
    C: Clock,
{
    shards: Box<[Shard<K, C>]>,
    /// `shard_count - 1`; the count is a power of two, so this masks a hash to a
    /// shard index without a division.
    shard_mask: u64,
    hasher: RandomState,
    eviction: Eviction,
    amount: u32,
    period: Duration,
    clock: C,
    epoch: Monotonic,
}

impl<K> PerKey<K, SystemClock>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Creates a per-key limiter giving every key `rate` units per second,
    /// driven by the OS monotonic clock and the default [`Eviction`] policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<u64> = PerKey::per_second(10);
    /// assert!(limiter.try_acquire(&42));
    /// ```
    #[must_use]
    pub fn per_second(rate: u32) -> Self {
        Self::build(
            rate,
            Duration::from_secs(1),
            SystemClock::new(),
            DEFAULT_SHARDS,
            Eviction::default(),
        )
    }

    /// Creates a per-key limiter giving every key `amount` units every `period`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::PerKey;
    ///
    /// // 1000 units per minute, per key.
    /// let limiter: PerKey<String> = PerKey::per_duration(1000, Duration::from_secs(60));
    /// # let _ = limiter;
    /// ```
    #[must_use]
    pub fn per_duration(amount: u32, period: Duration) -> Self {
        Self::build(
            amount,
            period,
            SystemClock::new(),
            DEFAULT_SHARDS,
            Eviction::default(),
        )
    }
}

impl<K, C> PerKey<K, C>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    C: Clock + Clone,
{
    fn build(amount: u32, period: Duration, clock: C, shards: usize, eviction: Eviction) -> Self {
        let shard_count = shards.max(1).next_power_of_two();
        let shards = (0..shard_count)
            .map(|_| Shard::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let epoch = clock.now();
        Self {
            shards,
            shard_mask: shard_count as u64 - 1,
            hasher: RandomState::new(),
            eviction,
            amount,
            period,
            clock,
            epoch,
        }
    }

    /// Replaces the time source, for deterministic tests with a
    /// [`ManualClock`](clock_lib::ManualClock). The store is rebuilt empty around
    /// the new clock.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use clock_lib::ManualClock;
    /// use throttle_net::PerKey;
    ///
    /// let clock = Arc::new(ManualClock::new());
    /// let limiter = PerKey::<&str>::per_second(1).with_clock(clock.clone());
    ///
    /// assert!(limiter.try_acquire(&"k"));
    /// assert!(!limiter.try_acquire(&"k"));
    /// clock.advance(Duration::from_secs(1));
    /// assert!(limiter.try_acquire(&"k"));
    /// ```
    #[must_use]
    pub fn with_clock<C2>(self, clock: C2) -> PerKey<K, C2>
    where
        C2: Clock + Clone,
    {
        PerKey::build(
            self.amount,
            self.period,
            clock,
            self.shards.len(),
            self.eviction,
        )
    }

    /// Sets the memory-bound policy (idle TTL and/or hard key cap).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::{Eviction, PerKey};
    ///
    /// let limiter: PerKey<String> = PerKey::per_second(100)
    ///     .with_eviction(Eviction::capacity(50_000).with_idle(Duration::from_secs(300)));
    /// # let _ = limiter;
    /// ```
    #[must_use]
    pub fn with_eviction(mut self, eviction: Eviction) -> Self {
        self.eviction = eviction;
        self
    }

    /// Sets the shard count (rounded up to a power of two, at least one).
    ///
    /// More shards reduce contention between unrelated keys at the cost of a
    /// little memory. The store is rebuilt empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<u64> = PerKey::per_second(100).with_shards(64);
    /// assert_eq!(limiter.shard_count(), 64);
    /// ```
    #[must_use]
    pub fn with_shards(self, shards: usize) -> Self {
        PerKey::build(self.amount, self.period, self.clock, shards, self.eviction)
    }

    /// The per-key capacity (burst ceiling): the configured `amount`.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.amount
    }

    /// The number of shards (a power of two).
    #[inline]
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// The number of keys with live state across all shards.
    ///
    /// A momentary, advisory snapshot — useful for tests and metrics, not a
    /// synchronization point.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| read_guard(&shard.map).len())
            .sum()
    }

    /// Returns `true` if no key currently has live state.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.shards
            .iter()
            .all(|shard| read_guard(&shard.map).is_empty())
    }

    /// Attempts to take one token for `key` without waiting.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<&str> = PerKey::per_second(1);
    /// assert!(limiter.try_acquire(&"a"));
    /// assert!(!limiter.try_acquire(&"a"));
    /// assert!(limiter.try_acquire(&"b")); // a different key is independent
    /// ```
    #[inline]
    #[must_use]
    pub fn try_acquire(&self, key: &K) -> bool {
        self.try_acquire_with_cost(key, 1)
    }

    /// Attempts to take `cost` tokens for `key` without waiting.
    #[inline]
    #[must_use]
    pub fn try_acquire_with_cost(&self, key: &K, cost: u32) -> bool {
        self.decide(key, cost).is_acquired()
    }

    /// Reports whether `cost` tokens would be granted for `key` now, without
    /// taking them — and without creating state for an unseen key.
    #[inline]
    #[must_use]
    pub fn peek(&self, key: &K, cost: u32) -> Decision {
        let shard = self.shard_for(key);
        let guard = read_guard(&shard.map);
        match guard.get(key) {
            Some(entry) => entry.throttle.peek(cost),
            // An unseen key would be a fresh, full bucket of capacity `amount`.
            None if cost > self.amount => Decision::Impossible,
            None => Decision::Acquired,
        }
    }

    /// Current tokens available for `key`. An unseen key reports the full
    /// capacity, since acquiring would create a fresh bucket.
    #[must_use]
    pub fn available(&self, key: &K) -> u32 {
        let shard = self.shard_for(key);
        let guard = read_guard(&shard.map);
        guard
            .get(key)
            .map_or(self.amount, |entry| entry.throttle.available())
    }

    /// Builds a fresh throttle for a newly-seen key, sharing this store's clock.
    #[inline]
    fn make_throttle(&self) -> Throttle<C> {
        Throttle::per_duration(self.amount, self.period).with_clock(self.clock.clone())
    }

    /// Milliseconds since the store's epoch, saturating.
    #[inline]
    fn now_ms(&self) -> u64 {
        let elapsed = self.clock.now().saturating_duration_since(self.epoch);
        u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
    }

    /// The "last seen" stamp for an access: real elapsed milliseconds when an
    /// idle TTL is configured, otherwise a cheap per-shard sequence number.
    #[inline]
    fn stamp(&self, shard: &Shard<K, C>, now_ms: u64) -> u64 {
        if self.eviction.idle_ttl().is_some() {
            now_ms
        } else {
            shard.seq.fetch_add(1, Ordering::Relaxed)
        }
    }

    #[inline]
    fn shard_for(&self, key: &K) -> &Shard<K, C> {
        let index = (self.hasher.hash_one(key) & self.shard_mask) as usize;
        &self.shards[index]
    }

    /// The consuming core: acquire `cost` for `key`, creating its state on first
    /// sight. Deducts on success.
    fn decide(&self, key: &K, cost: u32) -> Decision {
        let now_ms = self.now_ms();
        let shard = self.shard_for(key);

        // Fast path: a shared read lock is enough for an existing key. The
        // bucket does its own atomic accounting, so concurrent acquires — of this
        // key or any other in the shard — proceed without serialising.
        {
            let guard = read_guard(&shard.map);
            if let Some(entry) = guard.get(key) {
                entry
                    .last_seen
                    .store(self.stamp(shard, now_ms), Ordering::Relaxed);
                return entry.throttle.acquire_cost(cost);
            }
        }

        // Slow path: first-seen key. Take the write lock, re-check (another
        // thread may have inserted in the gap), evict to make room, insert.
        let mut guard = write_guard(&shard.map);
        if let Some(entry) = guard.get(key) {
            entry
                .last_seen
                .store(self.stamp(shard, now_ms), Ordering::Relaxed);
            return entry.throttle.acquire_cost(cost);
        }

        let stamp = self.stamp(shard, now_ms);
        self.evict_for_insert(&mut guard, now_ms);
        let throttle = self.make_throttle();
        let outcome = throttle.acquire_cost(cost);
        let _ = guard.insert(
            key.clone(),
            Entry {
                throttle,
                last_seen: AtomicU64::new(stamp),
            },
        );
        outcome
    }

    /// Makes room in a shard about to receive a new key: drop idle-expired keys,
    /// then, if still at capacity, evict the least-recently-seen one. Runs under
    /// the caller's write lock and touches only this shard.
    fn evict_for_insert(&self, map: &mut HashMap<K, Entry<C>, RandomState>, now_ms: u64) {
        if let Some(ttl) = self.eviction.idle_ttl() {
            let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
            map.retain(|_, entry| {
                now_ms.saturating_sub(entry.last_seen.load(Ordering::Relaxed)) < ttl_ms
            });
        }

        if let Some(max) = self.eviction.max_keys() {
            let per_shard_cap = max.div_ceil(self.shards.len()).max(1);
            while map.len() >= per_shard_cap {
                let victim = map
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_seen.load(Ordering::Relaxed))
                    .map(|(key, _)| key.clone());
                match victim {
                    Some(key) => {
                        let _ = map.remove(&key);
                    }
                    None => break,
                }
            }
        }
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl<K, C> PerKey<K, C>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    C: Clock + Clone,
{
    /// Takes one token for `key`, waiting until one is available.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] when the per-key capacity
    /// is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<String> = PerKey::per_second(100);
    /// limiter.acquire(&"tenant:7".to_string()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire(&self, key: &K) -> Result<(), ThrottleError> {
        self.acquire_with_cost(key, 1).await
    }

    /// Takes `cost` tokens for `key`, waiting until they are available.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] when `cost` exceeds the
    /// per-key capacity, so the request can never be granted.
    pub async fn acquire_with_cost(&self, key: &K, cost: u32) -> Result<(), ThrottleError> {
        loop {
            match self.decide(key, cost) {
                Decision::Acquired => return Ok(()),
                Decision::Impossible => {
                    return Err(ThrottleError::CostExceedsCapacity {
                        cost,
                        capacity: self.amount,
                    });
                }
                Decision::Retry { after } => crate::rt::sleep(after).await,
            }
        }
    }
}

impl<K, C> crate::limiter::KeyedLimiter<K> for PerKey<K, C>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    C: Clock + Clone + 'static,
{
    #[inline]
    fn peek(&self, key: &K, cost: u32) -> Decision {
        PerKey::peek(self, key, cost)
    }

    #[inline]
    fn try_acquire_with_cost(&self, key: &K, cost: u32) -> bool {
        PerKey::try_acquire_with_cost(self, key, cost)
    }

    #[inline]
    fn capacity(&self) -> u32 {
        PerKey::capacity(self)
    }
}

/// Recovers a read guard even if a previous holder panicked: a poisoned shard
/// should keep limiting, not propagate a panic into every caller.
fn read_guard<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

/// Recovers a write guard even if a previous holder panicked. See [`read_guard`].
fn write_guard<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::PerKey;
    use crate::eviction::Eviction;
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_perkey_is_send_sync() {
        assert_send_sync::<PerKey<String>>();
        assert_send_sync::<PerKey<u64>>();
    }

    #[test]
    fn test_keys_are_independent() {
        let limiter: PerKey<&str> = PerKey::per_second(1);
        assert!(limiter.try_acquire(&"a"));
        assert!(!limiter.try_acquire(&"a")); // a is spent
        assert!(limiter.try_acquire(&"b")); // b is untouched
    }

    #[test]
    fn test_first_acquire_creates_exactly_one_key() {
        let limiter: PerKey<&str> = PerKey::per_second(10);
        assert_eq!(limiter.len(), 0);
        assert!(limiter.try_acquire(&"a"));
        assert_eq!(limiter.len(), 1);
        assert!(limiter.try_acquire(&"a"));
        assert_eq!(limiter.len(), 1);
    }

    #[test]
    fn test_shard_count_rounds_up_to_power_of_two() {
        assert_eq!(PerKey::<u64>::per_second(1).with_shards(5).shard_count(), 8);
        assert_eq!(
            PerKey::<u64>::per_second(1).with_shards(16).shard_count(),
            16
        );
        assert_eq!(PerKey::<u64>::per_second(1).with_shards(0).shard_count(), 1);
    }

    #[test]
    fn test_peek_does_not_create_state() {
        let limiter: PerKey<&str> = PerKey::per_second(5);
        assert!(limiter.peek(&"ghost", 1).is_acquired());
        assert_eq!(limiter.len(), 0, "peek must not insert a key");
    }

    #[test]
    fn test_available_reports_full_capacity_for_unseen_key() {
        let limiter: PerKey<&str> = PerKey::per_second(7);
        assert_eq!(limiter.available(&"unseen"), 7);
        assert!(limiter.try_acquire_with_cost(&"seen", 3));
        assert_eq!(limiter.available(&"seen"), 4);
    }

    #[test]
    fn test_refill_under_manual_clock() {
        let clock = Arc::new(ManualClock::new());
        let limiter = PerKey::<&str>::per_second(2).with_clock(clock.clone());

        assert!(limiter.try_acquire(&"k"));
        assert!(limiter.try_acquire(&"k"));
        assert!(!limiter.try_acquire(&"k"));

        clock.advance(Duration::from_secs(1));
        assert!(limiter.try_acquire(&"k"));
    }

    #[test]
    fn test_capacity_bounds_total_keys_under_unique_flood() {
        let shards = 8;
        let cap = 100usize;
        let limiter: PerKey<u64> = PerKey::per_second(10)
            .with_shards(shards)
            .with_eviction(Eviction::capacity(cap));

        for k in 0..10_000u64 {
            let _ = limiter.try_acquire(&k);
        }

        let per_shard_cap = cap.div_ceil(shards).max(1);
        let bound = per_shard_cap * shards;
        assert!(
            limiter.len() <= bound,
            "flood grew to {} keys, bound {bound}",
            limiter.len()
        );
    }

    #[test]
    fn test_ttl_reclaims_idle_keys_on_later_insert() {
        let clock = Arc::new(ManualClock::new());
        let limiter = PerKey::<&str>::per_second(10)
            .with_clock(clock.clone())
            .with_eviction(Eviction::idle(Duration::from_millis(1000)).with_capacity(1))
            .with_shards(1);

        assert!(limiter.try_acquire(&"idle"));
        assert_eq!(limiter.len(), 1);

        clock.advance(Duration::from_millis(2000));
        // Inserting a fresh key reclaims the idle one.
        assert!(limiter.try_acquire(&"fresh"));
        assert_eq!(limiter.len(), 1, "the idle key should have been reclaimed");
    }

    #[test]
    fn test_recently_seen_key_survives_eviction_pressure() {
        let limiter: PerKey<String> = PerKey::per_second(1_000)
            .with_shards(1)
            .with_eviction(Eviction::capacity(4));

        for round in 0..50u64 {
            assert!(limiter.try_acquire(&"hot".to_string()));
            let _ = limiter.try_acquire(&round.to_string());
        }
        // The hot key was touched every round, so it is never the eviction victim.
        assert!(limiter.try_acquire(&"hot".to_string()));
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_errors_when_cost_exceeds_capacity() {
        use crate::error::ThrottleError;

        let limiter: PerKey<&str> = PerKey::per_second(5);
        let err = limiter.acquire_with_cost(&"k", 9).await.unwrap_err();
        assert_eq!(
            err,
            ThrottleError::CostExceedsCapacity {
                cost: 9,
                capacity: 5,
            }
        );
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_waits_then_succeeds() {
        let limiter: PerKey<&str> = PerKey::per_second(1000);
        for _ in 0..1000 {
            assert!(limiter.try_acquire(&"k"));
        }
        assert!(!limiter.try_acquire(&"k"));
        assert!(limiter.acquire(&"k").await.is_ok());
    }
}
