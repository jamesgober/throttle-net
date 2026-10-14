//! A bounded, deadline-aware, priority queue in front of a limiter.
//!
//! When a limiter is saturated, callers can either be rejected or *wait*. A
//! [`Queue`] lets them wait in an orderly way: it admits up to a fixed number of
//! waiters, serves them by priority (and fairly across keys at equal priority),
//! and **drops a waiter whose deadline has passed rather than serving it**. When
//! the queue is full, an [`Overflow`] policy decides who is turned away.
//!
//! The queue requires an async runtime (`tokio` or `smol` feature).
//!
//! ## Scheduling
//!
//! At any moment one waiter holds the turn: the one with the highest priority;
//! among equal priorities, the one whose key was served least recently (fair
//! across keys); within a key, the first to arrive. Only the turn-holder draws
//! from the limiter, so lower-priority waiters never jump ahead. Expired waiters
//! are dropped before the turn is assigned, so a dead waiter never blocks a live
//! one.
//!
//! Every scheduling operation is O(log n) in the number of waiters, and only the
//! waiter that gains the turn is woken — not the whole queue.

use core::cmp::Reverse;
use core::hash::Hash;
use core::time::Duration;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use clock_lib::{Clock, Monotonic, SystemClock};
use event_listener::Event;

use crate::decision::Decision;
use crate::error::ThrottleError;
use crate::limiter::Limiter;
use crate::obs::{self, Label};

/// The limiter kind reported in metrics and events.
const KIND: &str = "queue";

/// How long a waiter without the turn sleeps before re-checking on its own, as
/// a backstop. Turn changes wake the new turn-holder directly, so this only
/// bounds the cost of a wake-up that never comes.
const RECHECK: Duration = Duration::from_secs(60);

/// What a full queue does with a new request.
///
/// `#[non_exhaustive]`: more policies may be added.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Overflow {
    /// Turn the new request away with [`ThrottleError::QueueFull`].
    #[default]
    Reject,
    /// Evict the oldest waiter to make room for the new one.
    DropOldest,
    /// Evict the lowest-priority waiter (the newest among equals) to make
    /// room — unless the new request does not outrank it, in which case the new
    /// request is rejected.
    DropLowestPriority,
}

impl Overflow {
    /// The policy's metrics label.
    const fn as_str(self) -> &'static str {
        match self {
            Self::Reject => "reject",
            Self::DropOldest => "drop_oldest",
            Self::DropLowestPriority => "drop_lowest_priority",
        }
    }
}

/// How a waiting task is told it gained the turn or was evicted.
#[derive(Debug, Default)]
struct Signal {
    event: Event,
    evicted: AtomicBool,
}

impl Signal {
    fn wake(&self) {
        let _woken = self.event.notify(usize::MAX);
    }
}

/// One queued waiter's bookkeeping.
struct Waiter {
    priority: u32,
    /// Absolute deadline in store-epoch milliseconds, or `None`.
    deadline_ms: Option<u64>,
    key_id: u64,
    signal: Arc<Signal>,
}

/// Per-key fairness state.
struct KeyState {
    /// A stable id, so keys need not be ordered or cloned into indexes.
    id: u64,
    /// Service stamp of the key's most recent grant (lower: served longer ago).
    recency: u64,
    /// Waiters this key currently has queued.
    waiting: usize,
}

/// All waiters of one priority.
#[derive(Default)]
struct Level {
    /// Keys with waiters at this priority, by `(recency, key id)`: the first is
    /// the least recently served.
    order: BTreeSet<(u64, u64)>,
    /// Each key's waiters at this priority, in arrival order.
    by_key: HashMap<u64, VecDeque<u64>>,
    /// Every waiter's sequence number at this priority (the last is the newest).
    seqs: BTreeSet<u64>,
}

/// The scheduler state, guarded by one mutex.
struct State<K> {
    /// Waiters by arrival sequence (the first is the oldest).
    waiters: BTreeMap<u64, Waiter>,
    /// Priority levels, highest first.
    levels: BTreeMap<Reverse<u32>, Level>,
    /// Waiters with a deadline, soonest first.
    deadlines: BTreeSet<(u64, u64)>,
    /// Fairness state per key. Entries for keys with no waiters are pruned once
    /// the map outgrows `prune_at`, so it stays bounded by the queue capacity.
    keys: HashMap<K, KeyState>,
    /// Key ids back to keys, for keys present in `keys`.
    key_of: HashMap<u64, K>,
    /// The recency assumed for a key with no state: the newest stamp among the
    /// pruned entries, so a pruned key never jumps ahead of one served since.
    recency_floor: u64,
    service_seq: u64,
    next_seq: u64,
    next_key_id: u64,
    prune_at: usize,
}

impl<K: Eq + Hash + Clone> State<K> {
    fn new(capacity: usize) -> Self {
        Self {
            waiters: BTreeMap::new(),
            levels: BTreeMap::new(),
            deadlines: BTreeSet::new(),
            keys: HashMap::new(),
            key_of: HashMap::new(),
            recency_floor: 0,
            service_seq: 0,
            next_seq: 0,
            next_key_id: 0,
            prune_at: capacity.saturating_mul(2).saturating_add(16),
        }
    }

    fn len(&self) -> usize {
        self.waiters.len()
    }

    /// Enqueues a waiter, returning its sequence number and signal.
    fn insert(&mut self, priority: u32, deadline_ms: Option<u64>, key: &K) -> (u64, Arc<Signal>) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let (key_id, recency) = if let Some(state) = self.keys.get_mut(key) {
            state.waiting += 1;
            (state.id, state.recency)
        } else {
            let id = self.next_key_id;
            self.next_key_id += 1;
            let _new = self.keys.insert(
                key.clone(),
                KeyState {
                    id,
                    recency: self.recency_floor,
                    waiting: 1,
                },
            );
            let _new = self.key_of.insert(id, key.clone());
            (id, self.recency_floor)
        };
        let level = self.levels.entry(Reverse(priority)).or_default();
        let queue = level.by_key.entry(key_id).or_default();
        if queue.is_empty() {
            let _new = level.order.insert((recency, key_id));
        }
        queue.push_back(seq);
        let _new = level.seqs.insert(seq);
        if let Some(deadline) = deadline_ms {
            let _new = self.deadlines.insert((deadline, seq));
        }
        let signal = Arc::new(Signal::default());
        let _new = self.waiters.insert(
            seq,
            Waiter {
                priority,
                deadline_ms,
                key_id,
                signal: Arc::clone(&signal),
            },
        );
        (seq, signal)
    }

    /// Removes a waiter from every index. Returns it if it was queued.
    fn remove(&mut self, seq: u64) -> Option<Waiter> {
        let waiter = self.waiters.remove(&seq)?;
        let recency = self.keys_recency(waiter.key_id);
        if let Some(level) = self.levels.get_mut(&Reverse(waiter.priority)) {
            if let Some(queue) = level.by_key.get_mut(&waiter.key_id) {
                if let Some(pos) = queue.iter().position(|&s| s == seq) {
                    let _removed = queue.remove(pos);
                }
                if queue.is_empty() {
                    let _gone = level.by_key.remove(&waiter.key_id);
                    let _gone = level.order.remove(&(recency, waiter.key_id));
                }
            }
            let _gone = level.seqs.remove(&seq);
            if level.seqs.is_empty() {
                let _gone = self.levels.remove(&Reverse(waiter.priority));
            }
        }
        if let Some(deadline) = waiter.deadline_ms {
            let _gone = self.deadlines.remove(&(deadline, seq));
        }
        if let Some(key) = self.key_of.get(&waiter.key_id) {
            if let Some(state) = self.keys.get_mut(key) {
                state.waiting = state.waiting.saturating_sub(1);
            }
        }
        self.maybe_prune_keys();
        Some(waiter)
    }

    /// A key's recency by id (the floor if it has no state).
    fn keys_recency(&self, key_id: u64) -> u64 {
        self.key_of
            .get(&key_id)
            .and_then(|key| self.keys.get(key))
            .map_or(self.recency_floor, |state| state.recency)
    }

    /// Drops fairness state for keys with no waiters once the map outgrows its
    /// bound: amortized O(1) per removal, and memory stays proportional to the
    /// queue capacity however many distinct keys pass through.
    fn maybe_prune_keys(&mut self) {
        if self.keys.len() <= self.prune_at {
            return;
        }
        let mut floor = self.recency_floor;
        let key_of = &mut self.key_of;
        self.keys.retain(|_, state| {
            if state.waiting > 0 {
                return true;
            }
            floor = floor.max(state.recency);
            let _gone = key_of.remove(&state.id);
            false
        });
        self.recency_floor = floor;
    }

    /// Drops every waiter whose deadline is at or before `now_ms`. Their tasks
    /// see the deadline themselves and return [`ThrottleError::DeadlineExceeded`].
    fn prune_expired(&mut self, now_ms: u64) {
        while let Some(&(deadline, seq)) = self.deadlines.first() {
            if deadline > now_ms {
                break;
            }
            let _expired = self.remove(seq);
        }
    }

    /// The waiter that holds the turn, after dropping expired ones.
    fn winner(&mut self, now_ms: u64) -> Option<u64> {
        self.prune_expired(now_ms);
        let level = self.levels.values().next()?;
        let &(_, key_id) = level.order.first()?;
        level.by_key.get(&key_id)?.front().copied()
    }

    /// Marks `seq` served: stamps its key as just served (re-ordering the key
    /// in every level where it has waiters), then removes the waiter. Stamping
    /// first means a key pruned by the removal is pruned with its new stamp.
    fn serve(&mut self, seq: u64) {
        let Some(key_id) = self.waiters.get(&seq).map(|waiter| waiter.key_id) else {
            return;
        };
        self.service_seq += 1;
        let stamp = self.service_seq;
        let state = self
            .key_of
            .get(&key_id)
            .and_then(|key| self.keys.get_mut(key));
        if let Some(state) = state {
            let old = state.recency;
            state.recency = stamp;
            for level in self.levels.values_mut() {
                if level.order.remove(&(old, key_id)) {
                    let _moved = level.order.insert((stamp, key_id));
                }
            }
        }
        let _served = self.remove(seq);
    }

    /// The oldest waiter, for [`Overflow::DropOldest`].
    fn oldest(&self) -> Option<u64> {
        self.waiters.keys().next().copied()
    }

    /// The weakest waiter — lowest priority, newest among equals — and its
    /// priority, for [`Overflow::DropLowestPriority`].
    fn weakest(&self) -> Option<(u64, u32)> {
        let (Reverse(priority), level) = self.levels.iter().next_back()?;
        level.seqs.last().map(|&seq| (seq, *priority))
    }

    /// Evicts a waiter on behalf of an overflow policy and tells its task.
    fn evict(&mut self, seq: u64) {
        if let Some(waiter) = self.remove(seq) {
            waiter.signal.evicted.store(true, Ordering::Release);
            waiter.signal.wake();
        }
    }

    /// Wakes whoever holds the turn now.
    fn wake_winner(&mut self, now_ms: u64) {
        if let Some(seq) = self.winner(now_ms) {
            if let Some(waiter) = self.waiters.get(&seq) {
                waiter.signal.wake();
            }
        }
    }
}

/// A bounded, deadline-aware, priority queue fronting a limiter `L`, keyed by `K`
/// for fairness and timed by clock `C`.
///
/// Build one with [`Queue::builder`]. Use `K = ()` for a plain priority queue
/// with no cross-key fairness.
///
/// # Examples
///
/// ```
/// # async fn run() -> Result<(), throttle_net::ThrottleError> {
/// use std::time::Duration;
/// use throttle_net::{Overflow, Queue, Throttle};
///
/// // 50 req/s, with room for 100 waiters; evict the oldest when full.
/// let queue: Queue<Throttle, &str> = Queue::builder()
///     .capacity(100)
///     .overflow(Overflow::DropOldest)
///     .build(Throttle::per_second(50));
///
/// // Wait for a slot, but give up after 2 seconds.
/// queue
///     .acquire("tenant:1", 0, Some(Duration::from_secs(2)))
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct Queue<L, K = (), C = SystemClock>
where
    K: Eq + Hash + Clone + Send + Sync,
    C: Clock,
{
    inner: L,
    state: Mutex<State<K>>,
    capacity: usize,
    overflow: Overflow,
    clock: C,
    epoch: Monotonic,
    label: Label,
}

// Anchored on a concrete, limiter- and key-free type so `Queue::builder()` needs
// no type annotation; `L` and `K` are fixed later by [`QueueBuilder::build`].
impl Queue<core::convert::Infallible, ()> {
    /// Starts building a queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Queue, Throttle};
    ///
    /// let queue: Queue<Throttle> = Queue::builder().capacity(8).build(Throttle::per_second(1));
    /// assert_eq!(queue.capacity(), 8);
    /// ```
    pub fn builder() -> QueueBuilder {
        QueueBuilder::new()
    }
}

impl<L, K, C> Queue<L, K, C>
where
    L: Limiter,
    K: Eq + Hash + Clone + Send + Sync,
    C: Clock,
{
    fn new(inner: L, capacity: usize, overflow: Overflow, clock: C, label: Label) -> Self {
        let epoch = clock.now();
        let capacity = capacity.max(1);
        obs::queue_depth(&label, 0);
        Self {
            inner,
            state: Mutex::new(State::new(capacity)),
            capacity,
            overflow,
            clock,
            epoch,
            label,
        }
    }

    /// Replaces the time source (the deadline clock), for deterministic tests.
    /// The queue is rebuilt empty around the new clock.
    #[must_use]
    pub fn with_clock<C2>(self, clock: C2) -> Queue<L, K, C2>
    where
        C2: Clock,
    {
        Queue::new(self.inner, self.capacity, self.overflow, clock, self.label)
    }

    /// Names this instance for metrics and events (an owned name; the builder's
    /// [`name`](QueueBuilder::name) takes a static one).
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Queue, Throttle};
    ///
    /// let tenant = String::from("tenant-7");
    /// let queue: Queue<Throttle> = Queue::builder()
    ///     .build(Throttle::per_second(5))
    ///     .with_name(format!("queue-{tenant}"));
    /// assert_eq!(queue.name(), Some("queue-tenant-7"));
    /// ```
    #[must_use]
    pub fn with_name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
        obs::queue_depth(&self.label, self.len());
        self
    }

    /// The number of waiters currently enqueued (a momentary snapshot).
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Returns `true` if no waiters are enqueued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().waiters.is_empty()
    }

    /// The configured waiter capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The configured overflow policy.
    #[must_use]
    pub fn overflow(&self) -> Overflow {
        self.overflow
    }

    /// The instance name, if one was set with [`QueueBuilder::name`].
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.label.get()
    }

    /// A shared reference to the wrapped limiter.
    pub fn inner(&self) -> &L {
        &self.inner
    }

    #[inline]
    fn lock(&self) -> MutexGuard<'_, State<K>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[inline]
    fn now_ms(&self) -> u64 {
        let elapsed = self.clock.now().saturating_duration_since(self.epoch);
        u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
    }

    /// Inserts a waiter, applying the overflow policy if the queue is full, and
    /// wakes whoever holds the turn afterwards (a newcomer may preempt it).
    fn register(
        &self,
        now_ms: u64,
        priority: u32,
        deadline_ms: Option<u64>,
        key: &K,
    ) -> Result<(u64, Arc<Signal>), ThrottleError> {
        let mut state = self.lock();
        state.prune_expired(now_ms);
        if state.len() >= self.capacity {
            let victim = match self.overflow {
                Overflow::Reject => None,
                Overflow::DropOldest => state.oldest(),
                Overflow::DropLowestPriority => state
                    .weakest()
                    .filter(|&(_, weakest)| priority > weakest)
                    .map(|(seq, _)| seq),
            };
            let Some(seq) = victim else {
                obs::queue_rejected(&self.label, Overflow::Reject.as_str());
                return Err(ThrottleError::QueueFull);
            };
            state.evict(seq);
            obs::queue_rejected(&self.label, self.overflow.as_str());
        }
        let registered = state.insert(priority, deadline_ms, key);
        state.wake_winner(now_ms);
        obs::queue_depth(&self.label, state.len());
        Ok(registered)
    }

    /// Acquires one unit, waiting in the queue until served, the deadline
    /// passes, or the overflow policy turns the request away.
    ///
    /// `priority` orders waiters (higher first). `key` is the fairness key —
    /// equal-priority waiters are served round-robin across keys. `deadline` is
    /// a wait budget; `None` waits indefinitely.
    ///
    /// **Cancel-safe:** dropping the future removes the waiter and passes the
    /// turn on.
    ///
    /// # Errors
    ///
    /// - [`ThrottleError::QueueFull`] when the queue is full and the policy
    ///   rejects (or later evicts) this request.
    /// - [`ThrottleError::DeadlineExceeded`] when the deadline passes first.
    /// - [`ThrottleError::CostExceedsCapacity`] when the wrapped limiter can
    ///   never grant a single unit.
    pub async fn acquire(
        &self,
        key: K,
        priority: u32,
        deadline: Option<Duration>,
    ) -> Result<(), ThrottleError> {
        self.acquire_with_cost(key, priority, deadline, 1).await
    }

    /// Acquires `cost` units, waiting in the queue as [`acquire`](Self::acquire)
    /// does. Only the turn-holder draws from the limiter, so a costly request at
    /// the head is not overtaken by cheaper ones behind it.
    ///
    /// # Errors
    ///
    /// As [`acquire`](Self::acquire); [`ThrottleError::CostExceedsCapacity`]
    /// names `cost` when the limiter can never grant it.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use throttle_net::{Queue, Throttle};
    ///
    /// let queue: Queue<Throttle, u64> = Queue::builder().build(Throttle::per_second(1000));
    /// queue.acquire_with_cost(7, 1, None, 250).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire_with_cost(
        &self,
        key: K,
        priority: u32,
        deadline: Option<Duration>,
        cost: u32,
    ) -> Result<(), ThrottleError> {
        let timer = obs::Timer::start();
        let result = self.wait(&key, priority, deadline, cost).await;
        obs::waited(KIND, &self.label, &timer, cost, obs::Waited::of(&result));
        result
    }

    async fn wait(
        &self,
        key: &K,
        priority: u32,
        deadline: Option<Duration>,
        cost: u32,
    ) -> Result<(), ThrottleError> {
        let start_ms = self.now_ms();
        let deadline_ms = deadline
            .map(|d| start_ms.saturating_add(u64::try_from(d.as_millis()).unwrap_or(u64::MAX)));
        let (seq, signal) = self.register(start_ms, priority, deadline_ms, key)?;
        // Removes the waiter and passes the turn on, on every exit path.
        let _leave = LeaveGuard { queue: self, seq };

        loop {
            // Register interest before checking, so a wake between the check and
            // the await is not lost: `listen` registers immediately, and a
            // notification arriving before the await is delivered on first poll.
            let listener = signal.event.listen();

            if signal.evicted.load(Ordering::Acquire) {
                return Err(ThrottleError::QueueFull);
            }
            let now_ms = self.now_ms();
            if deadline_ms.is_some_and(|d| now_ms >= d) {
                return Err(ThrottleError::DeadlineExceeded);
            }

            let wait = {
                let mut state = self.lock();
                if state.winner(now_ms) == Some(seq) {
                    match self.inner.acquire_cost(cost) {
                        Decision::Acquired => {
                            state.serve(seq);
                            state.wake_winner(now_ms);
                            obs::queue_depth(&self.label, state.len());
                            return Ok(());
                        }
                        Decision::Retry { after } => after,
                        _ => {
                            return Err(ThrottleError::CostExceedsCapacity {
                                cost,
                                capacity: self.inner.capacity(),
                            });
                        }
                    }
                } else {
                    // Not our turn: sleep until woken (or the backstop).
                    RECHECK
                }
            };

            let sleep_for = cap_to_deadline(wait, now_ms, deadline_ms);
            futures_lite::future::or(listener, crate::rt::sleep(sleep_for)).await;
        }
    }
}

/// Caps a wait so it never sleeps past the waiter's own deadline.
fn cap_to_deadline(wait: Duration, now_ms: u64, deadline_ms: Option<u64>) -> Duration {
    match deadline_ms {
        Some(d) => wait.min(Duration::from_millis(d.saturating_sub(now_ms))),
        None => wait,
    }
}

/// Removes a waiter and passes the turn on when its task leaves the queue.
struct LeaveGuard<'a, L, K, C>
where
    L: Limiter,
    K: Eq + Hash + Clone + Send + Sync,
    C: Clock,
{
    queue: &'a Queue<L, K, C>,
    seq: u64,
}

impl<L, K, C> Drop for LeaveGuard<'_, L, K, C>
where
    L: Limiter,
    K: Eq + Hash + Clone + Send + Sync,
    C: Clock,
{
    fn drop(&mut self) {
        let now_ms = self.queue.now_ms();
        let mut state = self.queue.lock();
        if state.remove(self.seq).is_some() {
            state.wake_winner(now_ms);
        }
        obs::queue_depth(&self.queue.label, state.len());
    }
}

impl<L, K, C> core::fmt::Debug for Queue<L, K, C>
where
    L: Limiter,
    K: Eq + Hash + Clone + Send + Sync,
    C: Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Queue")
            .field("name", &self.label)
            .field("waiters", &self.len())
            .field("capacity", &self.capacity)
            .field("overflow", &self.overflow)
            .finish_non_exhaustive()
    }
}

/// Builder for a [`Queue`].
///
/// # Examples
///
/// ```
/// use throttle_net::{Overflow, Queue, Throttle};
///
/// let queue: Queue<Throttle, u32> = Queue::builder()
///     .capacity(256)
///     .overflow(Overflow::DropLowestPriority)
///     .name("crawler")
///     .build(Throttle::per_second(20));
/// assert_eq!(queue.overflow(), Overflow::DropLowestPriority);
/// assert_eq!(queue.name(), Some("crawler"));
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct QueueBuilder {
    capacity: usize,
    overflow: Overflow,
    label: Label,
}

impl Default for QueueBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueBuilder {
    /// Creates a builder with a capacity of 1024 waiters and
    /// [`Overflow::Reject`].
    pub fn new() -> Self {
        Self {
            capacity: 1024,
            overflow: Overflow::Reject,
            label: Label::none(),
        }
    }

    /// Sets the maximum number of simultaneous waiters (clamped to at least one).
    pub fn capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity.max(1);
        self
    }

    /// Sets the policy applied when the queue is full.
    pub fn overflow(mut self, overflow: Overflow) -> Self {
        self.overflow = overflow;
        self
    }

    /// Names the queue for metrics and events.
    pub fn name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
        self
    }

    /// Wraps `limiter`, producing a queue driven by the system clock.
    #[must_use]
    pub fn build<L, K>(self, limiter: L) -> Queue<L, K, SystemClock>
    where
        L: Limiter,
        K: Eq + Hash + Clone + Send + Sync,
    {
        Queue::new(
            limiter,
            self.capacity,
            self.overflow,
            SystemClock::new(),
            self.label,
        )
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::{Overflow, Queue, State};
    use crate::error::ThrottleError;
    use crate::throttle::Throttle;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    fn hour_queue(capacity: usize, overflow: Overflow) -> Arc<Queue<Throttle, ()>> {
        Arc::new(
            Queue::builder()
                .capacity(capacity)
                .overflow(overflow)
                .build(Throttle::per_duration(1, Duration::from_secs(3600))),
        )
    }

    async fn until_len(queue: &Queue<Throttle, ()>, len: usize) {
        while queue.len() < len {
            tokio::task::yield_now().await;
        }
    }

    #[test]
    fn test_queue_is_send_sync() {
        assert_send_sync::<Queue<Throttle, &'static str>>();
    }

    #[tokio::test]
    async fn test_immediate_acquire_when_token_is_free() {
        let queue: Queue<Throttle, ()> = Queue::builder().build(Throttle::per_second(10));
        assert!(queue.acquire((), 0, None).await.is_ok());
        assert!(queue.is_empty());
        assert!(format!("{queue:?}").contains("Queue"));
        assert_eq!(queue.inner().capacity(), 10);
    }

    #[tokio::test]
    async fn test_cost_exceeds_capacity_is_reported() {
        let queue: Queue<Throttle, ()> = Queue::builder().build(Throttle::per_second(0));
        let err = queue.acquire((), 0, Some(Duration::from_secs(1))).await;
        assert!(matches!(
            err,
            Err(ThrottleError::CostExceedsCapacity { .. })
        ));
        let queue: Queue<Throttle, ()> = Queue::builder().build(Throttle::per_second(5));
        let err = queue.acquire_with_cost((), 0, None, 6).await;
        assert_eq!(
            err,
            Err(ThrottleError::CostExceedsCapacity {
                cost: 6,
                capacity: 5
            })
        );
        assert!(queue.is_empty());
    }

    #[tokio::test]
    async fn test_deadline_exceeded_when_no_token_arrives() {
        let queue = hour_queue(4, Overflow::Reject);
        assert!(queue.acquire((), 0, None).await.is_ok());
        let err = queue.acquire((), 0, Some(Duration::from_millis(30))).await;
        assert_eq!(err, Err(ThrottleError::DeadlineExceeded));
        assert!(queue.is_empty(), "the expired waiter is removed");
    }

    #[tokio::test]
    async fn test_zero_deadline_fails_without_waiting() {
        let queue = hour_queue(4, Overflow::Reject);
        assert!(queue.acquire((), 0, None).await.is_ok());
        let err = queue.acquire((), 0, Some(Duration::ZERO)).await;
        assert_eq!(err, Err(ThrottleError::DeadlineExceeded));
    }

    #[tokio::test]
    async fn test_reject_overflow_when_full() {
        let queue = hour_queue(1, Overflow::Reject);
        assert!(queue.acquire((), 0, None).await.is_ok());
        let q = Arc::clone(&queue);
        let parked = tokio::spawn(async move { q.acquire((), 0, None).await });
        until_len(&queue, 1).await;
        let rejected = queue.acquire((), 0, Some(Duration::from_secs(1))).await;
        assert_eq!(rejected, Err(ThrottleError::QueueFull));
        parked.abort();
    }

    #[tokio::test]
    async fn test_drop_oldest_overflow_evicts_the_first_waiter() {
        let queue = hour_queue(1, Overflow::DropOldest);
        assert!(queue.acquire((), 0, None).await.is_ok());
        let q = Arc::clone(&queue);
        let first = tokio::spawn(async move { q.acquire((), 0, None).await });
        until_len(&queue, 1).await;
        let q = Arc::clone(&queue);
        let second = tokio::spawn(async move { q.acquire((), 0, None).await });
        assert_eq!(first.await.unwrap(), Err(ThrottleError::QueueFull));
        second.abort();
    }

    #[tokio::test]
    async fn test_drop_lowest_priority_evicts_only_when_outranked() {
        let queue = hour_queue(1, Overflow::DropLowestPriority);
        assert!(queue.acquire((), 0, None).await.is_ok());
        let q = Arc::clone(&queue);
        let low = tokio::spawn(async move { q.acquire((), 1, None).await });
        until_len(&queue, 1).await;
        // An equal priority does not outrank the resident: rejected.
        assert_eq!(
            queue.acquire((), 1, Some(Duration::from_secs(1))).await,
            Err(ThrottleError::QueueFull)
        );
        // A higher one evicts it.
        let q = Arc::clone(&queue);
        let high = tokio::spawn(async move { q.acquire((), 9, None).await });
        assert_eq!(low.await.unwrap(), Err(ThrottleError::QueueFull));
        high.abort();
    }

    #[tokio::test]
    async fn test_priority_is_served_high_first() {
        let queue: Arc<Queue<Throttle, ()>> = Arc::new(
            Queue::builder()
                .capacity(10)
                .build(Throttle::per_duration(1, Duration::from_millis(50))),
        );
        assert!(queue.acquire((), 0, None).await.is_ok());
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for priority in [1u32, 5, 3] {
            let q = Arc::clone(&queue);
            let order = Arc::clone(&order);
            handles.push(tokio::spawn(async move {
                q.acquire((), priority, None).await.unwrap();
                order.lock().unwrap().push(priority);
            }));
        }
        while queue.len() < 3 {
            tokio::task::yield_now().await;
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![5, 3, 1]);
    }

    #[tokio::test]
    async fn test_cancelled_waiter_leaves_and_passes_the_turn() {
        let queue = hour_queue(4, Overflow::Reject);
        assert!(queue.acquire((), 0, None).await.is_ok());
        let cancelled =
            tokio::time::timeout(Duration::from_millis(20), queue.acquire((), 0, None)).await;
        assert!(cancelled.is_err());
        assert!(queue.is_empty(), "the cancelled waiter left the queue");
    }

    fn enqueue(state: &mut State<&'static str>, priority: u32, key: &'static str) -> u64 {
        state.insert(priority, None, &key).0
    }

    #[test]
    fn test_fair_winner_rotates_across_keys_at_equal_priority() {
        let mut state = State::<&'static str>::new(16);
        let a0 = enqueue(&mut state, 0, "a");
        let a1 = enqueue(&mut state, 0, "a");
        let b0 = enqueue(&mut state, 0, "b");
        assert_eq!(state.winner(0), Some(a0));
        state.serve(a0);
        assert_eq!(
            state.winner(0),
            Some(b0),
            "the least recently served key goes next"
        );
        state.serve(b0);
        assert_eq!(state.winner(0), Some(a1));
        state.serve(a1);
        assert_eq!(state.winner(0), None);
    }

    #[test]
    fn test_priority_beats_fairness_in_winner_selection() {
        let mut state = State::<&'static str>::new(16);
        let _low = enqueue(&mut state, 1, "a");
        let high = enqueue(&mut state, 9, "b");
        assert_eq!(state.winner(0), Some(high));
        // The weakest is the low-priority waiter; the oldest is the first.
        assert_eq!(state.weakest().map(|(_, p)| p), Some(1));
        assert_eq!(state.oldest(), Some(0));
    }

    #[test]
    fn test_winner_skips_and_drops_expired_waiters() {
        let mut state = State::<&'static str>::new(16);
        let (_, _) = state.insert(9, Some(100), &"a");
        let live = enqueue(&mut state, 1, "b");
        assert_eq!(state.winner(200), Some(live));
        assert_eq!(state.len(), 1, "the expired waiter was dropped");
    }

    #[test]
    fn test_key_state_stays_bounded_under_unique_key_churn() {
        // Regression (TN-004): every key ever served stayed in the fairness map.
        let mut state = State::<u64>::new(8);
        for key in 0..100_000u64 {
            let seq = state.insert(0, None, &key).0;
            assert_eq!(state.winner(0), Some(seq));
            state.serve(seq);
        }
        assert!(
            state.keys.len() <= 8 * 2 + 16 + 1,
            "map grew to {}",
            state.keys.len()
        );
        assert_eq!(state.keys.len(), state.key_of.len());
    }

    #[test]
    fn test_pruned_key_does_not_jump_ahead_of_a_recently_served_one() {
        let mut state = State::<u64>::new(1); // prunes past 18 idle keys
        for key in 0..30u64 {
            let seq = state.insert(0, None, &key).0;
            state.serve(seq);
        }
        assert!(state.recency_floor > 0, "early keys were pruned");
        // Key 100 is served after the prune; key 0 was pruned before it.
        let seq = state.insert(0, None, &100).0;
        state.serve(seq);
        let recent = state.insert(0, None, &100).0;
        let pruned = state.insert(0, None, &0).0;
        // Equal priority: the key served longer ago (the pruned one) goes first.
        assert_eq!(state.winner(0), Some(pruned));
        state.serve(pruned);
        assert_eq!(state.winner(0), Some(recent));
    }

    #[test]
    fn test_scheduling_is_fast_with_many_waiters() {
        // Regression (TN-005): each grant scanned every waiter, and every
        // waiter rescanned on each wake-up (O(n^2) per grant).
        let mut state = State::<u64>::new(100_000);
        let seqs: Vec<u64> = (0..50_000u64)
            .map(|k| {
                state
                    .insert(u32::try_from(k % 4).unwrap(), None, &(k % 500))
                    .0
            })
            .collect();
        let start = std::time::Instant::now();
        let mut served = 0;
        while let Some(seq) = state.winner(0) {
            state.serve(seq);
            served += 1;
        }
        assert_eq!(served, seqs.len());
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "{:?}",
            start.elapsed()
        );
    }
}
