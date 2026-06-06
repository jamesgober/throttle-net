//! A bounded, deadline-aware, priority queue in front of a limiter.
//!
//! When a limiter is saturated, callers can either be rejected or *wait*. A
//! [`Queue`] lets them wait in an orderly way: it admits up to a fixed number of
//! waiters, serves them by priority (and fairly across keys at equal priority),
//! and **drops a waiter whose deadline has passed rather than serving it**. When
//! the queue is full, an [`Overflow`] policy decides who is turned away.
//!
//! The queue requires an async runtime (`tokio` feature). Acquisition is a single
//! token from the wrapped limiter per call.
//!
//! ## Scheduling
//!
//! At any moment the eligible waiter with the highest priority holds the turn; it
//! is the one that draws from the limiter, so lower-priority waiters never jump
//! ahead. Among equal priorities the least-recently-served key goes next (fair
//! across keys), and within a key it is first-come-first-served. Expired waiters
//! are skipped when choosing who to serve, so a dead waiter never blocks a live
//! one.

use core::hash::Hash;
use core::time::Duration;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use clock_lib::{Clock, Monotonic, SystemClock};
use event_listener::Event;

use crate::decision::Decision;
use crate::error::ThrottleError;
use crate::limiter::Limiter;

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
    /// Evict the lowest-priority waiter to make room — unless the new request is
    /// itself the lowest, in which case it is rejected.
    DropLowestPriority,
}

/// One queued waiter's bookkeeping.
struct Waiter<K> {
    /// Enqueue order; the FIFO tie-break and the "oldest" key.
    seq: u64,
    /// Higher is served first.
    priority: u32,
    /// Absolute deadline in store-epoch milliseconds, or `None` for no deadline.
    deadline_ms: Option<u64>,
    /// The fairness key.
    key: K,
    /// Set by the scheduler when this waiter is evicted by an overflow policy, so
    /// its own task can return [`ThrottleError::QueueFull`].
    evicted: Arc<AtomicBool>,
}

/// The mutable scheduler state, guarded by one mutex.
struct State<K> {
    waiters: HashMap<u64, Waiter<K>>,
    /// Service order counter; also the "recency" stamp written to `last_served`.
    service_seq: u64,
    /// Enqueue counter handing out waiter ids.
    next_seq: u64,
    /// Per-key last service stamp, for fair-across-keys tie-breaking.
    last_served: HashMap<K, u64>,
}

impl<K: Eq + Hash + Clone> State<K> {
    fn new() -> Self {
        Self {
            waiters: HashMap::new(),
            service_seq: 0,
            next_seq: 0,
            last_served: HashMap::new(),
        }
    }

    /// Removes waiters whose deadline has already passed; their own tasks return
    /// [`ThrottleError::DeadlineExceeded`] when they next check.
    fn prune_expired(&mut self, now_ms: u64) {
        self.waiters
            .retain(|_, w| w.deadline_ms.is_none_or(|d| now_ms < d));
    }

    /// The id of the waiter that should be served next, skipping expired ones:
    /// highest priority, then least-recently-served key, then lowest seq.
    fn winner(&self, now_ms: u64) -> Option<u64> {
        self.waiters
            .iter()
            .filter(|(_, w)| w.deadline_ms.is_none_or(|d| now_ms < d))
            .min_by(|(_, a), (_, b)| {
                b.priority
                    .cmp(&a.priority) // higher priority first
                    .then_with(|| self.recency(&a.key).cmp(&self.recency(&b.key)))
                    .then_with(|| a.seq.cmp(&b.seq))
            })
            .map(|(&id, _)| id)
    }

    /// The last-served stamp for a key (never served sorts first).
    fn recency(&self, key: &K) -> u64 {
        self.last_served.get(key).copied().unwrap_or(0)
    }

    /// Marks `id` served: stamps its key's recency and removes it.
    fn serve(&mut self, id: u64) {
        if let Some(w) = self.waiters.remove(&id) {
            self.service_seq += 1;
            let _ = self.last_served.insert(w.key, self.service_seq);
        }
    }

    /// Inserts a new waiter, returning its id and its eviction flag.
    fn insert(
        &mut self,
        priority: u32,
        deadline_ms: Option<u64>,
        key: K,
    ) -> (u64, Arc<AtomicBool>) {
        let id = self.next_seq;
        self.next_seq += 1;
        let evicted = Arc::new(AtomicBool::new(false));
        let _ = self.waiters.insert(
            id,
            Waiter {
                seq: id,
                priority,
                deadline_ms,
                key,
                evicted: Arc::clone(&evicted),
            },
        );
        (id, evicted)
    }

    /// The id of the oldest waiter (smallest seq), for [`Overflow::DropOldest`].
    fn oldest(&self) -> Option<u64> {
        self.waiters
            .iter()
            .min_by_key(|(_, w)| w.seq)
            .map(|(&id, _)| id)
    }

    /// The id and priority of the weakest waiter (lowest priority, newest first),
    /// for [`Overflow::DropLowestPriority`].
    fn weakest(&self) -> Option<(u64, u32)> {
        self.waiters
            .iter()
            .min_by(|(_, a), (_, b)| a.priority.cmp(&b.priority).then_with(|| b.seq.cmp(&a.seq)))
            .map(|(&id, w)| (id, w.priority))
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
/// // 50 req/s, with room for 100 waiters; reject when full.
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
    notify: Event,
    capacity: usize,
    overflow: Overflow,
    clock: C,
    epoch: Monotonic,
}

// Anchored on a concrete, limiter- and key-free type so `Queue::builder()` needs
// no type annotation; `L` and `K` are fixed later by [`QueueBuilder::build`].
impl Queue<core::convert::Infallible, ()> {
    /// Starts building a queue.
    #[must_use]
    pub fn builder() -> QueueBuilder {
        QueueBuilder::new()
    }
}

impl<L, K, C> Queue<L, K, C>
where
    L: Limiter,
    K: Eq + Hash + Clone + Send + Sync,
    C: Clock + Clone,
{
    fn new(inner: L, capacity: usize, overflow: Overflow, clock: C) -> Self {
        let epoch = clock.now();
        Self {
            inner,
            state: Mutex::new(State::new()),
            notify: Event::new(),
            capacity: capacity.max(1),
            overflow,
            clock,
            epoch,
        }
    }

    /// Replaces the time source (the deadline clock), for deterministic tests.
    /// The queue is rebuilt empty around the new clock.
    #[must_use]
    pub fn with_clock<C2>(self, clock: C2) -> Queue<L, K, C2>
    where
        C2: Clock + Clone,
    {
        Queue::new(self.inner, self.capacity, self.overflow, clock)
    }

    /// The number of waiters currently enqueued (a momentary snapshot).
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().waiters.len()
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

    /// Inserts a waiter, applying the overflow policy if the queue is full.
    ///
    /// On success (and whenever a waiter is evicted) the peers are woken so the
    /// next-in-line re-evaluates its turn, an evicted waiter learns it was dropped,
    /// and a higher-priority newcomer can preempt a sleeping lower-priority one.
    fn register(
        &self,
        now_ms: u64,
        priority: u32,
        deadline_ms: Option<u64>,
        key: &K,
    ) -> Result<(u64, Arc<AtomicBool>), ThrottleError> {
        let mut did_evict = false;
        let outcome = {
            let mut state = self.lock();
            state.prune_expired(now_ms);

            if state.waiters.len() < self.capacity {
                Ok(state.insert(priority, deadline_ms, key.clone()))
            } else {
                match self.overflow {
                    Overflow::Reject => Err(ThrottleError::QueueFull),
                    Overflow::DropOldest => match state.oldest() {
                        Some(victim) => {
                            evict(&mut state, victim);
                            did_evict = true;
                            Ok(state.insert(priority, deadline_ms, key.clone()))
                        }
                        None => Err(ThrottleError::QueueFull),
                    },
                    Overflow::DropLowestPriority => match state.weakest() {
                        // Evict only if the newcomer outranks the weakest resident.
                        Some((victim, weakest)) if priority > weakest => {
                            evict(&mut state, victim);
                            did_evict = true;
                            Ok(state.insert(priority, deadline_ms, key.clone()))
                        }
                        _ => Err(ThrottleError::QueueFull),
                    },
                }
            }
        };

        if did_evict {
            crate::obs::queue_overflow(match self.overflow {
                Overflow::Reject => "reject",
                Overflow::DropOldest => "drop_oldest",
                Overflow::DropLowestPriority => "drop_lowest_priority",
            });
        } else if outcome.is_err() {
            crate::obs::queue_overflow("reject");
        }
        if did_evict || outcome.is_ok() {
            let _ = self.notify.notify(usize::MAX);
            crate::obs::queue_depth(self.len());
        }
        outcome
    }

    /// Acquires one token, waiting in the queue until served, the deadline
    /// passes, or the overflow policy turns the request away.
    ///
    /// `priority` orders waiters (higher first). `key` is the fairness key —
    /// equal-priority waiters are served round-robin across keys. `deadline` is a
    /// wait budget; `None` waits indefinitely.
    ///
    /// # Errors
    ///
    /// - [`ThrottleError::QueueFull`] when the queue is full and the policy
    ///   rejects (or evicts) this request.
    /// - [`ThrottleError::DeadlineExceeded`] when the deadline passes first.
    /// - [`ThrottleError::CostExceedsCapacity`] when the wrapped limiter can
    ///   never grant a single unit.
    pub async fn acquire(
        &self,
        key: K,
        priority: u32,
        deadline: Option<Duration>,
    ) -> Result<(), ThrottleError> {
        let start_ms = self.now_ms();
        let deadline_ms = deadline
            .map(|d| start_ms.saturating_add(u64::try_from(d.as_millis()).unwrap_or(u64::MAX)));

        let timer = crate::obs::Timer::start();
        let (id, evicted) = self.register(start_ms, priority, deadline_ms, &key)?;
        // Ensure the waiter is removed and peers are woken on any exit path.
        let _guard = LeaveGuard { queue: self, id };

        loop {
            // Register interest before checking, so a wake between the check and
            // the await is not lost: `Event::listen` registers immediately, and a
            // notification arriving before the await is delivered on the first poll.
            let listener = self.notify.listen();

            if evicted.load(Ordering::Acquire) {
                return Err(ThrottleError::QueueFull);
            }

            let now_ms = self.now_ms();
            if deadline_ms.is_some_and(|d| now_ms >= d) {
                crate::obs::deadline_exceeded();
                return Err(ThrottleError::DeadlineExceeded);
            }

            let wait = {
                let mut state = self.lock();
                if state.winner(now_ms) == Some(id) {
                    match self.inner.acquire_cost(1) {
                        Decision::Acquired => {
                            state.serve(id);
                            drop(state);
                            let _ = self.notify.notify(usize::MAX);
                            crate::obs::acquired("queue");
                            crate::obs::wait("queue", &timer);
                            crate::obs::trace_acquire("queue", 1, true, &timer);
                            return Ok(());
                        }
                        Decision::Impossible => {
                            return Err(ThrottleError::CostExceedsCapacity {
                                cost: 1,
                                capacity: self.inner.capacity(),
                            });
                        }
                        // The turn is ours but no token yet; wait for the refill.
                        Decision::Retry { after } => after,
                    }
                } else {
                    // Not our turn; wait to be promoted (or until our deadline).
                    Duration::from_secs(3600)
                }
            };

            let sleep_for = cap_to_deadline(wait, now_ms, deadline_ms);
            // Wake on a notification or the timeout, whichever comes first.
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

/// Evicts a waiter on behalf of an overflow policy.
fn evict<K: Eq + Hash + Clone>(state: &mut State<K>, id: u64) {
    if let Some(w) = state.waiters.remove(&id) {
        w.evicted.store(true, Ordering::Release);
    }
}

/// Removes a waiter and wakes its peers when its task leaves the queue.
struct LeaveGuard<'a, L, K, C>
where
    L: Limiter,
    K: Eq + Hash + Clone + Send + Sync,
    C: Clock + Clone,
{
    queue: &'a Queue<L, K, C>,
    id: u64,
}

impl<L, K, C> Drop for LeaveGuard<'_, L, K, C>
where
    L: Limiter,
    K: Eq + Hash + Clone + Send + Sync,
    C: Clock + Clone,
{
    fn drop(&mut self) {
        let depth = {
            let mut state = self.queue.lock();
            let _ = state.waiters.remove(&self.id);
            state.waiters.len()
        };
        // Wake peers so the next-in-line re-evaluates its turn.
        let _ = self.queue.notify.notify(usize::MAX);
        crate::obs::queue_depth(depth);
    }
}

/// Builder for a [`Queue`].
#[derive(Debug, Clone, Copy)]
pub struct QueueBuilder {
    capacity: usize,
    overflow: Overflow,
}

impl Default for QueueBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueBuilder {
    /// Creates a builder with a default capacity of 1024 and [`Overflow::Reject`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            capacity: 1024,
            overflow: Overflow::Reject,
        }
    }

    /// Sets the maximum number of simultaneous waiters (clamped to at least one).
    #[must_use]
    pub fn capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity.max(1);
        self
    }

    /// Sets the policy applied when the queue is full.
    #[must_use]
    pub fn overflow(mut self, overflow: Overflow) -> Self {
        self.overflow = overflow;
        self
    }

    /// Wraps `limiter`, producing a queue driven by the system clock.
    #[must_use]
    pub fn build<L, K>(self, limiter: L) -> Queue<L, K, SystemClock>
    where
        L: Limiter,
        K: Eq + Hash + Clone + Send + Sync,
    {
        Queue::new(limiter, self.capacity, self.overflow, SystemClock::new())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::{Overflow, Queue};
    use crate::throttle::Throttle;
    use core::time::Duration;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_queue_is_send_sync() {
        assert_send_sync::<Queue<Throttle, &'static str>>();
    }

    #[tokio::test]
    async fn test_immediate_acquire_when_token_is_free() {
        let queue: Queue<Throttle, ()> = Queue::builder().build(Throttle::per_second(10));
        assert!(queue.acquire((), 0, None).await.is_ok());
        assert!(queue.is_empty());
    }

    #[tokio::test]
    async fn test_cost_exceeds_capacity_is_reported() {
        let queue: Queue<Throttle, ()> = Queue::builder().build(Throttle::per_second(0));
        let err = queue.acquire((), 0, Some(Duration::from_secs(1))).await;
        assert!(matches!(
            err,
            Err(crate::ThrottleError::CostExceedsCapacity { .. })
        ));
    }

    #[tokio::test]
    async fn test_deadline_exceeded_when_no_token_arrives() {
        // A drained 1/hour limiter won't refill within the short deadline, so the
        // waiter is dropped with DeadlineExceeded. Real time, small deadline.
        let queue: Queue<Throttle, ()> =
            Queue::builder().build(Throttle::per_duration(1, Duration::from_secs(3600)));
        assert!(queue.acquire((), 0, None).await.is_ok()); // takes the only token

        let err = queue.acquire((), 0, Some(Duration::from_millis(30))).await;
        assert!(matches!(err, Err(crate::ThrottleError::DeadlineExceeded)));
        assert!(queue.is_empty(), "the expired waiter is removed");
    }

    #[tokio::test]
    async fn test_reject_overflow_when_full() {
        // Capacity 1; the first waiter occupies it (parked on a drained limiter),
        // the second is rejected immediately.
        let queue: Arc<Queue<Throttle, ()>> = Arc::new(
            Queue::builder()
                .capacity(1)
                .overflow(Overflow::Reject)
                .build(Throttle::per_duration(1, Duration::from_secs(3600))),
        );
        assert!(queue.acquire((), 0, None).await.is_ok()); // consumes the token

        let q = Arc::clone(&queue);
        let parked = tokio::spawn(async move { q.acquire((), 0, None).await });
        while queue.is_empty() {
            tokio::task::yield_now().await;
        }
        let rejected = queue.acquire((), 0, Some(Duration::from_secs(1))).await;
        assert!(matches!(rejected, Err(crate::ThrottleError::QueueFull)));
        parked.abort();
    }

    #[tokio::test]
    async fn test_drop_oldest_overflow_evicts_the_first_waiter() {
        let queue: Arc<Queue<Throttle, ()>> = Arc::new(
            Queue::builder()
                .capacity(1)
                .overflow(Overflow::DropOldest)
                .build(Throttle::per_duration(1, Duration::from_secs(3600))),
        );
        assert!(queue.acquire((), 0, None).await.is_ok()); // drain the token

        // First waiter parks, occupying the single slot.
        let q = Arc::clone(&queue);
        let first = tokio::spawn(async move { q.acquire((), 0, None).await });
        while queue.is_empty() {
            tokio::task::yield_now().await;
        }
        // Second waiter evicts the first; the first returns QueueFull.
        let q = Arc::clone(&queue);
        let second = tokio::spawn(async move { q.acquire((), 0, None).await });
        let first_result = first.await.unwrap();
        assert!(matches!(first_result, Err(crate::ThrottleError::QueueFull)));
        second.abort();
    }

    #[tokio::test]
    async fn test_priority_is_served_high_first() {
        use std::sync::atomic::{AtomicU32, Ordering};

        // One token every 50ms — a wide margin over the microseconds it takes the
        // three waiters to register, so all are parked before the first refill and
        // the served order is determined purely by priority, not by timing.
        let queue: Arc<Queue<Throttle, ()>> = Arc::new(
            Queue::builder()
                .capacity(10)
                .build(Throttle::per_duration(1, Duration::from_millis(50))),
        );
        assert!(queue.acquire((), 0, None).await.is_ok()); // drain the one token

        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let started = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for priority in [1u32, 5, 3] {
            let q = Arc::clone(&queue);
            let order = Arc::clone(&order);
            let started = Arc::clone(&started);
            handles.push(tokio::spawn(async move {
                let _ = started.fetch_add(1, Ordering::Relaxed);
                q.acquire((), priority, None).await.unwrap();
                order.lock().unwrap().push(priority);
            }));
        }
        // Ensure all three have registered before tokens start flowing.
        while queue.len() < 3 {
            tokio::task::yield_now().await;
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(*order.lock().unwrap(), vec![5, 3, 1]);
    }

    #[test]
    fn test_fair_winner_rotates_across_keys_at_equal_priority() {
        use super::{State, Waiter};
        use std::sync::atomic::AtomicBool;

        fn enqueue(state: &mut State<&'static str>, id: u64, priority: u32, key: &'static str) {
            let _ = state.waiters.insert(
                id,
                Waiter {
                    seq: id,
                    priority,
                    deadline_ms: None,
                    key,
                    evicted: Arc::new(AtomicBool::new(false)),
                },
            );
        }

        let mut state = State::<&'static str>::new();
        // Two for key "a", one for key "b", all equal priority.
        enqueue(&mut state, 0, 0, "a");
        enqueue(&mut state, 1, 0, "a");
        enqueue(&mut state, 2, 0, "b");

        // No key served yet: tie broken by seq, so the first "a" goes.
        assert_eq!(state.winner(0), Some(0));
        state.serve(0);
        // Key "a" was just served, so the least-recently-served key "b" goes next
        // even though another "a" is older by seq — fair across keys.
        assert_eq!(state.winner(0), Some(2));
        state.serve(2);
        // Only the second "a" remains.
        assert_eq!(state.winner(0), Some(1));
    }

    #[test]
    fn test_priority_beats_fairness_in_winner_selection() {
        use super::{State, Waiter};
        use std::sync::atomic::AtomicBool;

        let mut state = State::<&'static str>::new();
        let _ = state.waiters.insert(
            0,
            Waiter {
                seq: 0,
                priority: 1,
                deadline_ms: None,
                key: "a",
                evicted: Arc::new(AtomicBool::new(false)),
            },
        );
        let _ = state.waiters.insert(
            1,
            Waiter {
                seq: 1,
                priority: 9,
                deadline_ms: None,
                key: "b",
                evicted: Arc::new(AtomicBool::new(false)),
            },
        );
        // Higher priority wins regardless of key recency or seq.
        assert_eq!(state.winner(0), Some(1));
    }

    #[test]
    fn test_winner_skips_expired_waiters() {
        use super::{State, Waiter};
        use std::sync::atomic::AtomicBool;

        let mut state = State::<&'static str>::new();
        let _ = state.waiters.insert(
            0,
            Waiter {
                seq: 0,
                priority: 9,
                deadline_ms: Some(100),
                key: "a",
                evicted: Arc::new(AtomicBool::new(false)),
            },
        );
        let _ = state.waiters.insert(
            1,
            Waiter {
                seq: 1,
                priority: 1,
                deadline_ms: None,
                key: "b",
                evicted: Arc::new(AtomicBool::new(false)),
            },
        );
        // At t=200 the high-priority waiter has expired, so the live one wins.
        assert_eq!(state.winner(200), Some(1));
    }
}
