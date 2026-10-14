//! Independent throttling per key, with sharded state and bounded memory.

use core::hash::Hash;
use core::marker::PhantomData;
use core::time::Duration;
use std::borrow::Cow;

use better_bucket::{BucketConfig, Decision as BucketDecision};
use clock_lib::{Clock, Monotonic, SystemClock};

use crate::decision::Decision;
use crate::error::ThrottleError;
use crate::eviction::Eviction;
use crate::obs::{self, Label};
use crate::store::Store;

/// The limiter kind reported in metrics and events.
const KIND: &str = "per_key";

/// Default shard count, before raising for the key cap and rounding up to a
/// power of two.
const DEFAULT_SHARDS: usize = 16;

/// A throttle that keeps independent state per key.
///
/// Each distinct key — a tenant, a user, an API token, an endpoint — gets its
/// own token bucket with the same configuration, so one noisy key cannot spend
/// another's budget. State lives in a **sharded** store: keys are spread across
/// shards by hash, each shard has its own lock, and an existing key's acquire
/// takes only a shard *read* lock plus the bucket's own atomic accounting (and
/// reads the clock once), so unrelated keys never contend and throughput scales
/// with cores.
///
/// Memory is **bounded by eviction** (see [`Eviction`]): a hard cap bounds the
/// number of keys and an optional idle TTL reclaims quiet ones, with a fixed,
/// small amount of work per new key — a flood of unique keys reaches a ceiling
/// and costs O(1) per insert instead of growing or stalling. Eviction never
/// hands a limited key a fresh allowance: the idle TTL reclaims only keys whose
/// bucket has **recovered** (is full again), capacity eviction prefers
/// recovered keys, and a still-limited key evicted to make room leaves its debt
/// in a compact per-shard ledger and comes back in it. When a shard and its
/// ledger are both full of limited keys, a *new* key waits (or, for `try_*`, is
/// refused) until one recovers — by default the store fails closed rather than
/// handing a flood fresh allowances ([`WhenFull`](crate::WhenFull) chooses).
/// Size the cap above your real key cardinality. The default policy is bounded
/// ([`Eviction::default`]).
///
/// Like [`Throttle`](crate::Throttle), the headline [`acquire`](Self::acquire)
/// **waits** (fairly, by reservation, and cancel-safely); the `try_*` variants
/// do not.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "runtime")]
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
    store: Store<K>,
    /// The per-key bucket configuration; `None` grants nothing (a zero rate,
    /// burst, or period).
    config: Option<BucketConfig>,
    eviction: Eviction,
    /// The shard count asked for, kept so a rebuild can honour it.
    shards: usize,
    clock: C,
    epoch: Monotonic,
    label: Label,
}

impl<K> PerKey<K, SystemClock>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Creates a per-key limiter giving every key `rate` units per second
    /// (burst `rate`), driven by the OS monotonic clock and the default
    /// [`Eviction`] policy.
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
        Self::per_duration(rate, Duration::from_secs(1))
    }

    /// Creates a per-key limiter giving every key `rate` units per minute.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<&str> = PerKey::per_minute(60);
    /// assert_eq!(limiter.capacity(), 60);
    /// ```
    #[must_use]
    pub fn per_minute(rate: u32) -> Self {
        Self::per_duration(rate, Duration::from_secs(60))
    }

    /// Creates a per-key limiter giving every key `amount` units every
    /// `period`. An `amount` of `0` or a zero `period` grants nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::PerKey;
    ///
    /// // 1000 units per minute, per key.
    /// let limiter: PerKey<String> = PerKey::per_duration(1000, Duration::from_secs(60));
    /// assert_eq!(limiter.capacity(), 1000);
    /// ```
    #[must_use]
    pub fn per_duration(amount: u32, period: Duration) -> Self {
        Self::builder().rate(amount, period).build()
    }

    /// Starts building a per-key limiter with full control over the rate, the
    /// burst, the initial fill, eviction, sharding, the clock, and the name.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::{Eviction, PerKey};
    ///
    /// let limiter: PerKey<u64> = PerKey::builder()
    ///     .per_second(5)
    ///     .burst(20)
    ///     .eviction(Eviction::capacity(100_000).with_idle(Duration::from_secs(300)))
    ///     .name("tenants")
    ///     .try_build()?;
    /// assert_eq!(limiter.capacity(), 20);
    /// # Ok::<(), throttle_net::ThrottleError>(())
    /// ```
    pub fn builder() -> PerKeyBuilder<K> {
        PerKeyBuilder::new()
    }
}

impl<K, C> PerKey<K, C>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    C: Clock,
{
    fn assemble(
        config: Option<BucketConfig>,
        eviction: Eviction,
        shards: usize,
        clock: C,
        label: Label,
    ) -> Self {
        let epoch = clock.now();
        Self {
            store: Store::new(
                shards,
                eviction,
                config.map_or(Duration::ZERO, time_to_full),
            ),
            config,
            eviction,
            shards,
            clock,
            epoch,
            label,
        }
    }

    /// Replaces the time source, for deterministic tests with a
    /// [`ManualClock`](clock_lib::ManualClock). The store is rebuilt empty
    /// around the new clock.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use throttle_net::{ManualClock, PerKey};
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
        C2: Clock,
    {
        PerKey::assemble(self.config, self.eviction, self.shards, clock, self.label)
    }

    /// Sets the memory-bound policy (key cap and/or idle TTL). The store is
    /// rebuilt empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::{Eviction, PerKey};
    ///
    /// let limiter: PerKey<String> = PerKey::per_second(100)
    ///     .with_eviction(Eviction::capacity(50_000).with_idle(Duration::from_secs(300)));
    /// assert_eq!(limiter.eviction().max_keys(), Some(50_000));
    /// ```
    #[must_use]
    pub fn with_eviction(self, eviction: Eviction) -> Self {
        Self::assemble(self.config, eviction, self.shards, self.clock, self.label)
    }

    /// Sets the shard count. The store is rebuilt empty.
    ///
    /// The count is rounded up to a power of two, kept within `1..=65_536`, and
    /// raised if needed so no shard's share of the key cap exceeds 16,384 keys
    /// (which keeps each shard's occasional table growth short). More shards
    /// reduce contention between unrelated keys at the cost of a little memory.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Eviction, PerKey};
    ///
    /// let limiter: PerKey<u64> = PerKey::per_second(100)
    ///     .with_eviction(Eviction::capacity(10_000))
    ///     .with_shards(48);
    /// assert_eq!(limiter.shard_count(), 64);
    /// ```
    #[must_use]
    pub fn with_shards(self, shards: usize) -> Self {
        Self::assemble(self.config, self.eviction, shards, self.clock, self.label)
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

    /// The memory-bound policy in force.
    #[must_use]
    pub fn eviction(&self) -> Eviction {
        self.eviction
    }

    /// The per-key capacity (burst ceiling). `0` when the configuration grants
    /// nothing.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.config.map_or(0, |config| config.capacity())
    }

    /// The number of shards (a power of two).
    #[inline]
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.store.shard_count()
    }

    /// The number of keys with live state across all shards.
    ///
    /// A momentary, advisory snapshot — useful for tests and metrics, not a
    /// synchronization point.
    #[must_use]
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Returns `true` if no key currently has live state.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Time since this store's epoch, the timeline every bucket shares.
    #[inline]
    fn now(&self) -> Duration {
        self.clock.now().saturating_duration_since(self.epoch)
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
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<u32> = PerKey::per_second(10);
    /// assert!(limiter.try_acquire_with_cost(&7, 8));
    /// assert!(!limiter.try_acquire_with_cost(&7, 3));
    /// ```
    #[inline]
    #[must_use]
    pub fn try_acquire_with_cost(&self, key: &K, cost: u32) -> bool {
        let granted = self.decide(key, cost).is_acquired();
        obs::attempt(KIND, &self.label, cost, granted);
        granted
    }

    /// Reports whether `cost` tokens would be granted for `key` now, without
    /// taking them — and without creating state for an unseen key (which is
    /// judged as a fresh bucket with the configured initial fill).
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Decision, PerKey};
    ///
    /// let limiter: PerKey<&str> = PerKey::per_second(5);
    /// assert_eq!(limiter.peek(&"ghost", 5), Decision::Acquired);
    /// assert_eq!(limiter.peek(&"ghost", 6), Decision::Impossible);
    /// assert!(limiter.is_empty());
    /// ```
    #[must_use]
    pub fn peek(&self, key: &K, cost: u32) -> Decision {
        let Some(config) = self.config else {
            return closed(cost);
        };
        if cost > config.capacity() {
            return Decision::Impossible;
        }
        let now = self.now();
        let wait = self
            .store
            .view(key, now, config, |bucket| bucket.time_until_at(cost, now));
        match wait {
            None => Decision::Impossible,
            Some(wait) if wait.is_zero() => Decision::Acquired,
            Some(after) => Decision::Retry { after },
        }
    }

    /// Current tokens available for `key`. An unseen key reports the configured
    /// initial fill (the full capacity by default), since acquiring would
    /// create a fresh bucket; a key evicted while limited reports what its
    /// remembered debt leaves.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<&str> = PerKey::per_second(7);
    /// assert_eq!(limiter.available(&"unseen"), 7);
    /// assert!(limiter.try_acquire_with_cost(&"seen", 3));
    /// assert_eq!(limiter.available(&"seen"), 4);
    /// ```
    #[must_use]
    pub fn available(&self, key: &K) -> u32 {
        let Some(config) = self.config else {
            return 0;
        };
        let now = self.now();
        self.store
            .view(key, now, config, |bucket| bucket.available_at(now))
    }

    /// Reserves `cost` tokens for `key` (see [`Throttle::reserve`]): takes them
    /// now if they will have accrued within `max_wait`, returning the wait, or
    /// takes nothing and returns `None`.
    ///
    /// [`Throttle::reserve`]: crate::Throttle::reserve
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<&str> = PerKey::per_second(10);
    /// assert_eq!(limiter.reserve(&"k", 10, Duration::ZERO), Some(Duration::ZERO));
    /// assert_eq!(limiter.reserve(&"k", 5, Duration::ZERO), None);
    /// ```
    #[must_use = "the returned wait must be honoured before the tokens are used"]
    pub fn reserve(&self, key: &K, cost: u32, max_wait: Duration) -> Option<Duration> {
        let config = self.config?;
        if cost > config.capacity() {
            return None;
        }
        if cost == 0 {
            return Some(Duration::ZERO);
        }
        let now = self.now();
        self.store
            .with_or_insert(key, now, config, |bucket| {
                bucket.reserve_at(cost, max_wait, now)
            })
            .ok()
            .flatten()
    }

    /// Returns `cost` tokens granted to `key` for work that never used them.
    /// A key evicted while limited has the refund taken off its remembered
    /// debt; a key with no state at all is left alone.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<&str> = PerKey::per_second(4);
    /// assert!(limiter.try_acquire_with_cost(&"k", 4));
    /// limiter.refund(&"k", 4);
    /// assert_eq!(limiter.available(&"k"), 4);
    /// ```
    pub fn refund(&self, key: &K, cost: u32) {
        if let Some(config) = self.config {
            let _known = self.store.refund(key, cost, config);
        }
    }

    /// Forgets `key`'s state. Its next acquire starts from a fresh bucket.
    /// Returns whether the key had state.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<&str> = PerKey::per_second(1);
    /// assert!(limiter.try_acquire(&"k"));
    /// assert!(limiter.remove(&"k"));
    /// assert!(!limiter.remove(&"k"));
    /// ```
    pub fn remove(&self, key: &K) -> bool {
        self.store.remove(key)
    }

    /// Restores `key`'s full allowance (keeping it tracked), for example after
    /// the downstream confirms the key's quota was reset. Returns whether the
    /// key had state.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<&str> = PerKey::per_second(2);
    /// assert!(limiter.try_acquire_with_cost(&"k", 2));
    /// assert!(limiter.reset(&"k"));
    /// assert_eq!(limiter.available(&"k"), 2);
    /// assert!(!limiter.reset(&"unseen"));
    /// ```
    pub fn reset(&self, key: &K) -> bool {
        let now = self.now();
        self.store
            .with_existing(key, |bucket| bucket.reset_at(now))
            .is_some()
    }

    /// Forgets every key's state.
    pub fn clear(&self) {
        self.store.clear();
    }

    /// The consuming core: acquire `cost` for `key`, creating its state on
    /// first sight. Deducts on success; emits nothing.
    fn decide(&self, key: &K, cost: u32) -> Decision {
        let Some(config) = self.config else {
            return closed(cost);
        };
        if cost > config.capacity() {
            // Never create state for a request that can never be granted.
            return Decision::Impossible;
        }
        if cost == 0 {
            return Decision::Acquired;
        }
        let now = self.now();
        self.store
            .with_or_insert(key, now, config, |bucket| {
                map_decision(bucket.acquire_at(cost, now))
            })
            // A new key refused by a shard full of unrecovered keys waits for
            // room rather than bypassing anyone's limit.
            .unwrap_or_else(|after| Decision::Retry { after })
    }

    /// One step of a waiting acquire for `key`.
    #[cfg(feature = "runtime")]
    fn step(&self, key: &K, cost: u32, budget: Duration) -> crate::wait::Step {
        use crate::wait::Step;
        let Some(config) = self.config else {
            return if cost == 0 {
                Step::Granted(Duration::ZERO)
            } else {
                Step::Never
            };
        };
        if cost > config.capacity() {
            return Step::Never;
        }
        if cost == 0 {
            return Step::Granted(Duration::ZERO);
        }
        let now = self.now();
        self.store
            .with_or_insert(key, now, config, |bucket| {
                match bucket.reserve_at(cost, budget, now) {
                    Some(wait) => Step::Granted(wait),
                    None => bucket
                        .time_until_at(cost, now)
                        .map_or(Step::Never, Step::Retry),
                }
            })
            .unwrap_or_else(Step::Retry)
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl<K, C> PerKey<K, C>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    C: Clock,
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

    /// Takes `cost` tokens for `key`, waiting until they are available. Waiters
    /// on one key are served in reservation order. **Cancel-safe:** dropping the
    /// future returns its reservation.
    ///
    /// # Errors
    ///
    /// Returns [`ThrottleError::CostExceedsCapacity`] when `cost` exceeds the
    /// per-key capacity, so the request can never be granted.
    pub async fn acquire_with_cost(&self, key: &K, cost: u32) -> Result<(), ThrottleError> {
        self.wait(key, cost, None).await
    }

    /// Takes `cost` tokens for `key`, waiting at most `timeout` (giving up at
    /// once if the tokens are known not to accrue in time).
    ///
    /// # Errors
    ///
    /// [`ThrottleError::DeadlineExceeded`] if the tokens cannot be had within
    /// `timeout`; [`ThrottleError::CostExceedsCapacity`] if `cost` exceeds the
    /// per-key capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use std::time::Duration;
    /// use throttle_net::PerKey;
    ///
    /// let limiter: PerKey<u64> = PerKey::per_second(100);
    /// limiter.acquire_with_cost_timeout(&9, 10, Duration::from_millis(500)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire_with_cost_timeout(
        &self,
        key: &K,
        cost: u32,
        timeout: Duration,
    ) -> Result<(), ThrottleError> {
        self.wait(key, cost, Some(timeout)).await
    }

    async fn wait(
        &self,
        key: &K,
        cost: u32,
        timeout: Option<Duration>,
    ) -> Result<(), ThrottleError> {
        use crate::wait::{Refusal, wait_for};
        let timer = obs::Timer::start();
        let result = wait_for(
            |budget| self.step(key, cost, budget),
            || self.refund(key, cost),
            || Duration::ZERO,
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

impl<K, C> crate::limiter::KeyedLimiter<K> for PerKey<K, C>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    C: Clock + 'static,
{
    #[inline]
    fn peek(&self, key: &K, cost: u32) -> Decision {
        PerKey::peek(self, key, cost)
    }

    #[inline]
    fn acquire_cost(&self, key: &K, cost: u32) -> Decision {
        self.decide(key, cost)
    }

    fn refund(&self, key: &K, cost: u32) {
        PerKey::refund(self, key, cost);
    }

    #[cfg(feature = "runtime")]
    fn reserve(&self, key: &K, cost: u32, max_wait: Duration) -> Option<Duration> {
        PerKey::reserve(self, key, cost, max_wait)
    }

    #[inline]
    fn capacity(&self) -> u32 {
        PerKey::capacity(self)
    }
}

impl<K, C: Clock> core::fmt::Debug for PerKey<K, C>
where
    K: Eq + Hash,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PerKey")
            .field("name", &self.label)
            .field("capacity", &self.config.map_or(0, |c| c.capacity()))
            .field("keys", &self.store.len())
            .field("shards", &self.store.shard_count())
            .field("eviction", &self.eviction)
            .finish_non_exhaustive()
    }
}

/// What a limiter that grants nothing answers for `cost`.
#[inline]
fn closed(cost: u32) -> Decision {
    if cost == 0 {
        Decision::Acquired
    } else {
        Decision::Impossible
    }
}

/// Maps a bucket decision onto the crate's [`Decision`].
#[inline]
fn map_decision(decision: BucketDecision) -> Decision {
    match decision {
        BucketDecision::Allowed => Decision::Acquired,
        BucketDecision::Denied { retry_after } if retry_after == Duration::MAX => {
            Decision::Impossible
        }
        BucketDecision::Denied { retry_after } => Decision::Retry { after: retry_after },
        // `better_bucket::Decision` is `#[non_exhaustive]`: refuse what this
        // version does not understand rather than risk over-sending.
        _ => Decision::Impossible,
    }
}

/// How long an idle bucket takes to refill from empty to full: the shortest
/// idle time after which forgetting a key can never reset a limited allowance.
fn time_to_full(config: BucketConfig) -> Duration {
    let nanos = config
        .refill_period()
        .as_nanos()
        .saturating_mul(u128::from(config.capacity()))
        .div_ceil(u128::from(config.refill_amount().max(1)));
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// Builder for a [`PerKey`] limiter.
///
/// [`build`](Self::build) never fails — an unusable rate yields a limiter that
/// grants nothing — while [`try_build`](Self::try_build) reports it as
/// [`ThrottleError::InvalidConfig`].
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
/// use throttle_net::{ManualClock, PerKey};
///
/// let clock = Arc::new(ManualClock::new());
/// let limiter: PerKey<&str, _> = PerKey::builder()
///     .per_minute(600)
///     .burst(10)
///     .initial(0)
///     .clock(clock.clone())
///     .try_build()?;
/// assert!(!limiter.try_acquire(&"k")); // first sight: a bucket starting empty
/// clock.advance(Duration::from_secs(1)); // 600/min = 10 per second
/// assert!(limiter.try_acquire_with_cost(&"k", 10));
/// # Ok::<(), throttle_net::ThrottleError>(())
/// ```
#[must_use]
pub struct PerKeyBuilder<K, C = SystemClock> {
    amount: u32,
    period: Duration,
    burst: Option<u32>,
    initial: Option<u32>,
    eviction: Eviction,
    shards: usize,
    clock: C,
    label: Label,
    key: PhantomData<fn() -> K>,
}

impl<K> PerKeyBuilder<K, SystemClock> {
    fn new() -> Self {
        Self {
            amount: 0,
            period: Duration::from_secs(1),
            burst: None,
            initial: None,
            eviction: Eviction::default(),
            shards: DEFAULT_SHARDS,
            clock: SystemClock::new(),
            label: Label::none(),
            key: PhantomData,
        }
    }
}

impl<K, C> PerKeyBuilder<K, C>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    C: Clock,
{
    /// Gives every key `amount` tokens every `period` (and, unless
    /// [`burst`](Self::burst) is set, a capacity of `amount`).
    pub fn rate(mut self, amount: u32, period: Duration) -> Self {
        self.amount = amount;
        self.period = period;
        self
    }

    /// Gives every key `amount` tokens per second.
    pub fn per_second(self, amount: u32) -> Self {
        self.rate(amount, Duration::from_secs(1))
    }

    /// Gives every key `amount` tokens per minute.
    pub fn per_minute(self, amount: u32) -> Self {
        self.rate(amount, Duration::from_secs(60))
    }

    /// Sets each key's capacity independently of the rate.
    pub fn burst(mut self, burst: u32) -> Self {
        self.burst = Some(burst);
        self
    }

    /// Sets how many tokens a newly seen key starts with (clamped to the
    /// capacity). Defaults to full.
    pub fn initial(mut self, initial: u32) -> Self {
        self.initial = Some(initial);
        self
    }

    /// Sets the memory-bound policy. Defaults to [`Eviction::default`].
    pub fn eviction(mut self, eviction: Eviction) -> Self {
        self.eviction = eviction;
        self
    }

    /// Sets the shard count (see [`PerKey::with_shards`]).
    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = shards;
        self
    }

    /// Names the limiter for metrics and events.
    pub fn name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
        self
    }

    /// Drives the limiter with `clock` instead of the system clock.
    pub fn clock<C2: Clock>(self, clock: C2) -> PerKeyBuilder<K, C2> {
        PerKeyBuilder {
            amount: self.amount,
            period: self.period,
            burst: self.burst,
            initial: self.initial,
            eviction: self.eviction,
            shards: self.shards,
            clock,
            label: self.label,
            key: PhantomData,
        }
    }

    fn config(&self) -> Result<BucketConfig, ThrottleError> {
        let invalid = |reason| ThrottleError::InvalidConfig { reason };
        if self.amount == 0 {
            return Err(invalid(
                "rate amount must be non-zero; set it with rate/per_second/per_minute",
            ));
        }
        if self.period.is_zero() {
            return Err(invalid("rate period must be non-zero"));
        }
        let capacity = self.burst.unwrap_or(self.amount);
        if capacity == 0 {
            return Err(invalid("burst must be non-zero"));
        }
        let initial = self.initial.unwrap_or(capacity).min(capacity);
        BucketConfig::new(capacity, self.amount, self.period, initial)
            .map_err(|_| invalid("the token bucket rejected the configuration"))
    }

    /// Builds the limiter, or reports why the configuration cannot work.
    ///
    /// # Errors
    ///
    /// [`ThrottleError::InvalidConfig`] when the rate was never set or is zero,
    /// the period is zero, or the burst is zero.
    pub fn try_build(self) -> Result<PerKey<K, C>, ThrottleError> {
        let config = self.config()?;
        Ok(PerKey::assemble(
            Some(config),
            self.eviction,
            self.shards,
            self.clock,
            self.label,
        ))
    }

    /// Builds the limiter; an unusable configuration grants nothing.
    #[must_use]
    pub fn build(self) -> PerKey<K, C> {
        let config = self.config().ok();
        PerKey::assemble(config, self.eviction, self.shards, self.clock, self.label)
    }
}

impl<K, C> core::fmt::Debug for PerKeyBuilder<K, C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PerKeyBuilder")
            .field("amount", &self.amount)
            .field("period", &self.period)
            .field("burst", &self.burst)
            .field("initial", &self.initial)
            .field("eviction", &self.eviction)
            .field("shards", &self.shards)
            .field("name", &self.label)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::PerKey;
    use crate::decision::Decision;
    use crate::error::ThrottleError;
    use crate::eviction::Eviction;
    use crate::limiter::KeyedLimiter;
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
        assert!(!limiter.try_acquire(&"a"));
        assert!(limiter.try_acquire(&"b"));
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
    fn test_shard_count_rounds_up_and_covers_the_cap() {
        let small = Eviction::capacity(1_000);
        assert_eq!(
            PerKey::<u64>::per_second(1)
                .with_eviction(small)
                .with_shards(5)
                .shard_count(),
            8
        );
        assert_eq!(
            PerKey::<u64>::per_second(1)
                .with_eviction(small)
                .with_shards(0)
                .shard_count(),
            1
        );
        // The default million-key cap needs 64 shards of at most 16,384 keys.
        assert_eq!(PerKey::<u64>::per_second(1).shard_count(), 64);
    }

    #[test]
    fn test_peek_does_not_create_state_and_judges_a_fresh_bucket() {
        let limiter: PerKey<&str> = PerKey::builder().per_second(5).initial(2).build();
        assert!(limiter.peek(&"ghost", 2).is_acquired());
        assert!(matches!(limiter.peek(&"ghost", 3), Decision::Retry { .. }));
        assert_eq!(limiter.peek(&"ghost", 6), Decision::Impossible);
        assert_eq!(limiter.len(), 0, "peek must not insert a key");
        assert_eq!(limiter.available(&"ghost"), 2);
    }

    #[test]
    fn test_peek_and_acquire_agree_for_a_seen_key() {
        let clock = Arc::new(ManualClock::new());
        let limiter = PerKey::<&str>::per_second(10).with_clock(clock.clone());
        assert!(limiter.try_acquire_with_cost(&"k", 10));
        assert_eq!(
            limiter.peek(&"k", 5),
            Decision::Retry {
                after: Duration::from_millis(500)
            }
        );
        clock.advance(Duration::from_millis(500));
        assert!(limiter.peek(&"k", 5).is_acquired());
        assert!(limiter.try_acquire_with_cost(&"k", 5));
    }

    #[test]
    fn test_zero_period_or_rate_grants_nothing_consistently() {
        // Regression (TN-021): capacity reported 5 and peek said Acquired while
        // every acquire was Impossible.
        let limiter: PerKey<&str> = PerKey::per_duration(5, Duration::ZERO);
        assert_eq!(limiter.capacity(), 0);
        assert_eq!(limiter.peek(&"k", 1), Decision::Impossible);
        assert!(!limiter.try_acquire(&"k"));
        assert!(limiter.try_acquire_with_cost(&"k", 0));
        assert_eq!(limiter.available(&"k"), 0);
        assert_eq!(limiter.reserve(&"k", 1, Duration::MAX), None);
        assert!(limiter.is_empty());
        let err = PerKey::<&str>::builder().try_build().unwrap_err();
        assert!(matches!(err, ThrottleError::InvalidConfig { .. }));
    }

    #[test]
    fn test_large_capacity_is_exact() {
        // Regression (TN-010): capacity() reported 10M while the buckets were
        // silently clamped to ~4.29M.
        let limiter = PerKey::<u8>::per_duration(10_000_000, Duration::from_secs(60))
            .with_clock(Arc::new(ManualClock::new()));
        assert_eq!(limiter.capacity(), 10_000_000);
        assert!(limiter.peek(&1, 5_000_000).is_acquired());
        assert!(limiter.try_acquire_with_cost(&1, 5_000_000));
        assert_eq!(limiter.available(&1), 5_000_000);
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
        let limiter: PerKey<u64> = PerKey::per_second(10)
            .with_eviction(Eviction::capacity(100))
            .with_shards(8);
        for k in 0..10_000u64 {
            let _ = limiter.try_acquire(&k);
        }
        let bound = 100usize.div_ceil(8) * 8;
        assert!(
            limiter.len() <= bound,
            "flood grew to {} keys",
            limiter.len()
        );
    }

    #[test]
    fn test_ttl_eviction_never_resets_a_drained_key() {
        // Regression (TN-011): a key drained of a 100/hour budget was evicted
        // after a 1s idle TTL and came back with a full bucket.
        let clock = Arc::new(ManualClock::new());
        let limiter = PerKey::<String>::per_duration(100, Duration::from_secs(3600))
            .with_clock(clock.clone())
            .with_eviction(Eviction::idle(Duration::from_secs(1)))
            .with_shards(1);
        let a = String::from("a");
        assert!(limiter.try_acquire_with_cost(&a, 100));
        clock.advance(Duration::from_secs(2));
        for key in 0..20u32 {
            assert!(limiter.try_acquire(&key.to_string()));
        }
        assert!(limiter.available(&a) <= 1, "a's debt survived the idle TTL");
        // After a full refill time it is reclaimed, losing nothing.
        clock.advance(Duration::from_secs(3600));
        for key in 100..140u32 {
            assert!(limiter.try_acquire(&key.to_string()));
        }
        assert_eq!(limiter.available(&a), 100);
    }

    #[test]
    fn test_full_store_of_limited_keys_fails_closed_for_new_keys() {
        let clock = Arc::new(ManualClock::new());
        let limiter = PerKey::<&str>::per_duration(1, Duration::from_secs(10))
            .with_clock(clock.clone())
            .with_eviction(Eviction::capacity(2))
            .with_shards(1);
        assert!(limiter.try_acquire(&"a"));
        assert!(limiter.try_acquire(&"b"));
        // Both keys are drained: newcomers move their debts to the ledger...
        assert!(limiter.try_acquire(&"x"));
        assert!(limiter.try_acquire(&"y"));
        // ...and the forgotten keys are still limited when they return.
        // With the ledger full too, evicting either would reset a limit, so
        // the newcomer is refused and told when room is expected.
        assert!(!limiter.try_acquire(&"c"));
        let keyed: &dyn KeyedLimiter<&str> = &limiter;
        assert_eq!(
            keyed.acquire_cost(&"c", 1),
            Decision::Retry {
                after: Duration::from_secs(10)
            }
        );
        assert_eq!(limiter.reserve(&"c", 1, Duration::MAX), None);
        assert_eq!(limiter.len(), 2);
        clock.advance(Duration::from_secs(10));
        assert!(limiter.try_acquire(&"c"), "a recovered key made room");
        assert!(!limiter.try_acquire(&"c"));
    }

    #[test]
    fn test_reset_restores_a_keys_allowance() {
        let limiter: PerKey<u8> = PerKey::per_second(3);
        assert!(limiter.try_acquire_with_cost(&1, 3));
        assert!(limiter.reset(&1));
        assert_eq!(limiter.available(&1), 3);
        assert!(!limiter.reset(&2));
    }

    #[test]
    fn test_ttl_reclaims_idle_keys_on_later_insert() {
        let clock = Arc::new(ManualClock::new());
        let limiter = PerKey::<&str>::per_second(10)
            .with_clock(clock.clone())
            .with_eviction(Eviction::idle(Duration::from_secs(1)).with_capacity(1))
            .with_shards(1);
        assert!(limiter.try_acquire(&"idle"));
        clock.advance(Duration::from_secs(2));
        assert!(limiter.try_acquire(&"fresh"));
        assert_eq!(limiter.len(), 1, "the idle key should have been reclaimed");
    }

    #[test]
    fn test_recently_seen_key_survives_eviction_pressure() {
        let limiter: PerKey<String> = PerKey::per_second(1_000)
            .with_eviction(Eviction::capacity(4))
            .with_shards(1);
        for round in 0..50u64 {
            assert!(limiter.try_acquire(&"hot".to_string()));
            let _ = limiter.try_acquire(&round.to_string());
        }
        assert!(limiter.try_acquire(&"hot".to_string()));
    }

    #[test]
    fn test_reserve_refund_remove_and_clear() {
        let clock = Arc::new(ManualClock::new());
        let limiter = PerKey::<&str>::per_second(10).with_clock(clock.clone());
        assert!(limiter.try_acquire_with_cost(&"k", 10));
        assert_eq!(
            limiter.reserve(&"k", 5, Duration::from_secs(1)),
            Some(Duration::from_millis(500))
        );
        limiter.refund(&"k", 5);
        assert_eq!(
            limiter.reserve(&"k", 5, Duration::from_secs(1)),
            Some(Duration::from_millis(500)),
            "the refund paid the reservation back"
        );
        assert_eq!(limiter.reserve(&"k", 11, Duration::MAX), None);
        limiter.refund(&"unknown", 3); // no state: nothing happens
        assert!(!limiter.is_empty());
        assert!(limiter.remove(&"k"));
        assert_eq!(limiter.available(&"k"), 10, "a removed key starts fresh");
        assert!(limiter.try_acquire(&"x"));
        limiter.clear();
        assert!(limiter.is_empty());
    }

    #[test]
    fn test_keyed_trait_surface_and_debug() {
        let limiter: PerKey<u8> = PerKey::builder().per_second(3).name("t").build();
        let keyed: &dyn KeyedLimiter<u8> = &limiter;
        assert_eq!(keyed.capacity(), 3);
        assert!(keyed.acquire_cost(&1, 3).is_acquired());
        assert!(matches!(keyed.peek(&1, 1), Decision::Retry { .. }));
        keyed.refund(&1, 3);
        #[cfg(feature = "runtime")]
        assert_eq!(keyed.reserve(&1, 3, Duration::ZERO), Some(Duration::ZERO));
        let text = format!("{limiter:?}");
        assert!(text.contains("PerKey") && text.contains("\"t\""));
        assert!(format!("{:?}", PerKey::<u8>::builder()).contains("PerKeyBuilder"));
        assert_eq!(limiter.name(), Some("t"));
        assert_eq!(PerKey::<u8>::per_minute(5).capacity(), 5);
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_errors_when_cost_exceeds_capacity() {
        let limiter: PerKey<&str> = PerKey::per_second(5);
        let err = limiter.acquire_with_cost(&"k", 9).await.unwrap_err();
        assert_eq!(
            err,
            ThrottleError::CostExceedsCapacity {
                cost: 9,
                capacity: 5
            }
        );
        assert!(limiter.is_empty(), "an impossible request creates no state");
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_acquire_waits_then_succeeds() {
        // One token every 20ms: drained, the waiter needs a real refill.
        let limiter: PerKey<&str> = PerKey::per_duration(1, Duration::from_millis(20));
        assert!(limiter.try_acquire(&"k"));
        assert!(!limiter.try_acquire(&"k"));
        assert!(limiter.acquire(&"k").await.is_ok());
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(start_paused = true)]
    async fn test_timeout_and_cancellation() {
        let limiter: PerKey<&str> = PerKey::per_duration(1, Duration::from_secs(3600));
        assert!(limiter.try_acquire(&"k"));
        let err = limiter
            .acquire_with_cost_timeout(&"k", 1, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(err, ThrottleError::DeadlineExceeded);
        let cancelled = tokio::time::timeout(Duration::from_millis(1), limiter.acquire(&"k")).await;
        assert!(cancelled.is_err());
        // The cancelled reservation was refunded: the wait is still ~1h, not ~2h.
        let wait = limiter.reserve(&"k", 1, Duration::MAX).unwrap();
        assert!(wait <= Duration::from_secs(3600), "leaked: {wait:?}");
    }
}
