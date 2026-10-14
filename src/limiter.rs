//! The limiter seam every algorithm and composite shares.

use core::time::Duration;

use crate::decision::Decision;
use crate::decision::Decision as D;

/// The contract a limiter satisfies: take a cost and report the [`Decision`].
///
/// This is the extension point. [`Throttle`](crate::Throttle),
/// [`SlidingWindowLog`](crate::SlidingWindowLog), and the composites
/// ([`Hybrid`](crate::Hybrid), [`MultiLimiter`](crate::MultiLimiter)'s
/// dimensions, the global scope of [`Layered`](crate::Layered)) all implement
/// it, and the composites consume it — a hybrid holds several `Limiter`s and a
/// request must clear all of them. Implement it for your own algorithm and it
/// slots in anywhere a built-in limiter does.
///
/// [`acquire_cost`](Self::acquire_cost) is the **synchronous, consuming** core:
/// it deducts the units on success and never blocks. The waiting `acquire`
/// surface is built on top of it and on [`reserve`](Self::reserve).
///
/// Implementors are `Send + Sync` so a limiter can be shared across tasks behind
/// an [`Arc`](std::sync::Arc).
///
/// # Composition
///
/// [`peek`](Self::peek) is what makes "a request must pass *all* of these"
/// correct. A composite cannot simply call [`acquire_cost`](Self::acquire_cost)
/// on each constituent: if an early one grants (and deducts) while a later one
/// refuses, the early units are spent for a request that never happened. So a
/// composite first `peek`s every constituent — a non-consuming check — and only
/// commits once all of them would grant. If a concurrent caller wins a race
/// between the peek and the commit, the composite gives the units it already
/// took back with [`refund`](Self::refund).
///
/// # Provided methods
///
/// [`refund`](Self::refund), [`reserve`](Self::reserve), and
/// [`pause_for`](Self::pause_for) have conservative default bodies, so an
/// implementation only needs the four required methods; override them when the
/// algorithm can do better (the built-in limiters do).
///
/// # Examples
///
/// ```
/// use throttle_net::{Decision, Limiter, Throttle};
///
/// fn drain(limiter: &dyn Limiter) -> u32 {
///     let mut granted = 0;
///     while limiter.acquire_cost(1) == Decision::Acquired {
///         granted += 1;
///     }
///     granted
/// }
///
/// // A bucket that starts full grants exactly its capacity before refusing.
/// let throttle = Throttle::per_second(8);
/// assert_eq!(drain(&throttle), 8);
/// ```
pub trait Limiter: Send + Sync {
    /// Reports whether `cost` units *would* be granted now, **without** taking
    /// them.
    ///
    /// Returns [`Decision::Acquired`] when the units are available this instant,
    /// [`Decision::Retry`] with an estimate of the wait until they would be, or
    /// [`Decision::Impossible`] when no wait would ever be enough. Because it
    /// consumes nothing, a composite can poll every constituent before deciding
    /// to commit. The result is a snapshot: a concurrent acquisition can change
    /// it the next instant, so it is a guide, not a reservation.
    fn peek(&self, cost: u32) -> Decision;

    /// Attempts to take `cost` units now, deducting them on success.
    ///
    /// Returns [`Decision::Acquired`] when the units were granted,
    /// [`Decision::Retry`] with the wait until the same cost could succeed, or
    /// [`Decision::Impossible`] when `cost` exceeds the limiter's capacity and
    /// no wait would ever satisfy it. A `cost` of `0` is always granted.
    fn acquire_cost(&self, cost: u32) -> Decision;

    /// Returns the number of whole units available right now.
    ///
    /// This is a point-in-time read for observability and tests; it may change
    /// the instant another acquisition lands, so it is not a reservation.
    fn available(&self) -> u32;

    /// Returns the most units this limiter can ever hold at once (its burst
    /// ceiling). A request whose cost exceeds this can never be granted.
    fn capacity(&self) -> u32;

    /// Gives back `cost` units that this limiter granted (through
    /// [`acquire_cost`](Self::acquire_cost) or [`reserve`](Self::reserve)) for
    /// work that never used them — a composite that lost a race after
    /// committing, or a waiting acquire cancelled during its reserved wait.
    ///
    /// Only refund what was taken: refunding units that were never granted lets
    /// the limiter grant more than its configured rate. The default does
    /// nothing, which is always safe — the units simply stay spent.
    fn refund(&self, cost: u32) {
        let _ = cost;
    }

    /// Takes `cost` units now, even if they have not accrued yet, provided they
    /// will have accrued within `max_wait`. Returns the wait before the units
    /// may be used (`Some(Duration::ZERO)` when they were already there), or
    /// `None` — taking nothing — when the wait would exceed `max_wait` or the
    /// cost can never be granted.
    ///
    /// Reservations are what make waiting fair: later callers queue behind an
    /// earlier reservation instead of racing for each refilled unit, so a large
    /// request cannot be starved by a stream of small ones. A reservation that
    /// will not be honoured must be given back with [`refund`](Self::refund).
    ///
    /// The default takes the units only if they are available right now (it
    /// cannot queue), so a limiter without native reservations still composes.
    fn reserve(&self, cost: u32, max_wait: Duration) -> Option<Duration> {
        let _ = max_wait;
        self.acquire_cost(cost)
            .is_acquired()
            .then_some(Duration::ZERO)
    }

    /// Stops granting for `duration` from now — typically because the
    /// downstream reported its budget exhausted until a reset, or answered with
    /// a `Retry-After`. Pausing never adds capacity; a later, shorter pause does
    /// not shorten an earlier, longer one.
    ///
    /// The default drains the units currently available, which pauses the
    /// limiter only until its own refill. [`Throttle`](crate::Throttle),
    /// [`SlidingWindowLog`](crate::SlidingWindowLog), and the composites hold
    /// for the exact duration.
    fn pause_for(&self, duration: Duration) {
        let _ = duration;
        let available = self.available();
        if available > 0 {
            // Best effort: a concurrent grant can win part of the drain, which
            // leaves the limiter no fuller than a successful drain would.
            let _drained = self.acquire_cost(available);
        }
    }
}

/// A limiter addressed by a key, with its clock type erased so composites can
/// store it without carrying a clock type parameter.
///
/// Implemented by [`PerKey`](crate::PerKey) for every clock, this lets
/// [`Layered`](crate::Layered) hold per-key and per-endpoint scopes behind a
/// trait object and still accept stores built on any clock.
pub(crate) trait KeyedLimiter<K>: Send + Sync {
    /// Non-consuming check for `key`. See [`Limiter::peek`].
    fn peek(&self, key: &K, cost: u32) -> Decision;
    /// Consuming attempt for `key`. See [`Limiter::acquire_cost`].
    fn acquire_cost(&self, key: &K, cost: u32) -> Decision;
    /// Gives back units granted to `key`. See [`Limiter::refund`].
    fn refund(&self, key: &K, cost: u32);
    /// Reserves units for `key`. See [`Limiter::reserve`].
    #[cfg(feature = "runtime")]
    fn reserve(&self, key: &K, cost: u32, max_wait: Duration) -> Option<Duration>;
    /// The per-key capacity. See [`Limiter::capacity`].
    fn capacity(&self) -> u32;
}

/// A running "must pass all" verdict over several constituents' peeks.
#[derive(Clone, Copy)]
pub(crate) enum Verdict {
    /// Every constituent so far would grant now.
    Ready,
    /// Some constituent must wait; the longest wait so far.
    Wait(Duration),
    /// Some constituent can never grant.
    Never,
}

/// Folds one constituent's peek into the verdict: the longest wait wins, and
/// any `Impossible` is final.
#[inline]
pub(crate) fn fold_peek(acc: Verdict, decision: Decision) -> Verdict {
    match (acc, decision) {
        (Verdict::Never, _) => Verdict::Never,
        (acc, D::Acquired) => acc,
        (Verdict::Ready, D::Retry { after }) => Verdict::Wait(after),
        (Verdict::Wait(wait), D::Retry { after }) => Verdict::Wait(wait.max(after)),
        _ => Verdict::Never,
    }
}

/// Converts a folded verdict back into a [`Decision`].
#[inline]
pub(crate) fn finish_peek(acc: Verdict) -> Decision {
    match acc {
        Verdict::Never => D::Impossible,
        Verdict::Ready => D::Acquired,
        Verdict::Wait(after) => D::Retry { after },
    }
}

/// Aggregates a non-consuming peek across constituents that must *all* grant.
///
/// Returns [`Decision::Impossible`] if any constituent could never grant,
/// [`Decision::Retry`] with the longest constituent wait if any is short, or
/// [`Decision::Acquired`] only when every one would grant now. Consumes nothing.
pub(crate) fn peek_all<'a, I>(items: I) -> Decision
where
    I: Iterator<Item = (&'a dyn Limiter, u32)>,
{
    let mut acc = Verdict::Ready;
    for (limiter, cost) in items {
        acc = fold_peek(acc, limiter.peek(cost));
        if matches!(acc, Verdict::Never) {
            break;
        }
    }
    finish_peek(acc)
}

/// Gives back the first `count` constituents' units.
fn refund_first<'a, I>(items: I, count: usize)
where
    I: Iterator<Item = (&'a dyn Limiter, u32)>,
{
    for (limiter, cost) in items.take(count) {
        limiter.refund(cost);
    }
}

/// Peeks every constituent and, only if all would grant, commits each.
///
/// This is the correctness core of "must pass all": [`peek_all`] confirms
/// availability before any units are taken. If a concurrent acquisition makes a
/// commit fail after the peek succeeded, the constituents already committed are
/// refunded (through [`Limiter::refund`]) and the failing constituent's
/// decision is returned, so no request is admitted that one constituent
/// refused, and — for constituents that support refunds — no units are lost.
///
/// `items` is iterated up to three times (peek, commit, rollback), hence the
/// `Clone` bound; for slice-backed iterators this is a cheap pointer copy.
pub(crate) fn acquire_all<'a, I>(items: I) -> Decision
where
    I: Iterator<Item = (&'a dyn Limiter, u32)> + Clone,
{
    match peek_all(items.clone()) {
        D::Acquired => {}
        other => return other,
    }
    for (index, (limiter, cost)) in items.clone().enumerate() {
        match limiter.acquire_cost(cost) {
            D::Acquired => {}
            refused => {
                refund_first(items, index);
                return refused;
            }
        }
    }
    D::Acquired
}

/// Reserves on every constituent; if any cannot reserve within `max_wait`, the
/// ones already reserved are refunded and nothing is held. Returns the longest
/// wait.
pub(crate) fn reserve_all<'a, I>(items: I, max_wait: Duration) -> Option<Duration>
where
    I: Iterator<Item = (&'a dyn Limiter, u32)> + Clone,
{
    let mut longest = Duration::ZERO;
    for (index, (limiter, cost)) in items.clone().enumerate() {
        let Some(wait) = limiter.reserve(cost, max_wait) else {
            refund_first(items, index);
            return None;
        };
        longest = longest.max(wait);
    }
    Some(longest)
}

/// Takes units from `limiter` until at most `remaining` are available, and
/// returns how many were taken. Never adds units. A concurrent grant can race
/// the drain; it is retried a few times, and whatever it leaves is no fuller
/// than a completed drain.
pub(crate) fn drain_to<L: Limiter + ?Sized>(limiter: &L, remaining: u32) -> u32 {
    const ATTEMPTS: u32 = 4;
    for _ in 0..ATTEMPTS {
        let available = limiter.available();
        if available <= remaining {
            return 0;
        }
        let excess = available - remaining;
        if limiter.acquire_cost(excess).is_acquired() {
            return excess;
        }
    }
    0
}

/// Refunds every constituent's units.
pub(crate) fn refund_all<'a, I>(items: I)
where
    I: Iterator<Item = (&'a dyn Limiter, u32)>,
{
    for (limiter, cost) in items {
        limiter.refund(cost);
    }
}

/// One step of a composite's waiting acquire: reserve everywhere, or report how
/// long until every constituent could grant.
#[cfg(feature = "runtime")]
pub(crate) fn step_all<'a, I>(items: I, budget: Duration) -> crate::wait::Step
where
    I: Iterator<Item = (&'a dyn Limiter, u32)> + Clone,
{
    use crate::wait::Step;
    if let Some(wait) = reserve_all(items.clone(), budget) {
        return Step::Granted(wait);
    }
    match peek_all(items) {
        D::Impossible => Step::Never,
        D::Retry { after } => Step::Retry(after),
        // Every constituent reads grantable now: a race with another caller
        // (or a constituent without native reservations). Ask again.
        _ => Step::Retry(Duration::ZERO),
    }
}

#[cfg(test)]
mod tests {
    use super::{Limiter, acquire_all, peek_all, reserve_all};
    use crate::decision::Decision;
    use crate::throttle::Throttle;
    use core::time::Duration;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A minimal limiter using every provided default: a fixed pool that never
    /// refills.
    struct Pool {
        left: AtomicU32,
        capacity: u32,
    }

    impl Pool {
        fn new(capacity: u32) -> Self {
            Self {
                left: AtomicU32::new(capacity),
                capacity,
            }
        }
    }

    impl Limiter for Pool {
        fn peek(&self, cost: u32) -> Decision {
            if cost > self.capacity {
                Decision::Impossible
            } else if cost <= self.left.load(Ordering::Relaxed) {
                Decision::Acquired
            } else {
                Decision::Retry {
                    after: Duration::from_secs(1),
                }
            }
        }

        fn acquire_cost(&self, cost: u32) -> Decision {
            match self
                .left
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                    left.checked_sub(cost)
                }) {
                Ok(_) => Decision::Acquired,
                Err(_) => self.peek(cost),
            }
        }

        fn available(&self) -> u32 {
            self.left.load(Ordering::Relaxed)
        }

        fn capacity(&self) -> u32 {
            self.capacity
        }
    }

    #[test]
    fn test_default_reserve_takes_only_what_is_there() {
        let pool = Pool::new(3);
        assert_eq!(pool.reserve(2, Duration::MAX), Some(Duration::ZERO));
        assert_eq!(pool.reserve(2, Duration::MAX), None, "cannot queue");
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn test_default_refund_is_a_safe_noop() {
        let pool = Pool::new(3);
        assert!(pool.acquire_cost(3).is_acquired());
        pool.refund(3);
        assert_eq!(pool.available(), 0, "the default keeps the units spent");
    }

    #[test]
    fn test_default_pause_drains_what_is_available() {
        let pool = Pool::new(5);
        pool.pause_for(Duration::from_secs(10));
        assert_eq!(pool.available(), 0);
        // Pausing an already-empty pool is a no-op.
        pool.pause_for(Duration::from_secs(10));
        assert_eq!(pool.available(), 0);
    }

    #[test]
    fn test_peek_all_takes_the_longest_wait_and_short_circuits_impossible() {
        let full = Throttle::per_second(10);
        let empty = Pool::new(1);
        assert!(empty.acquire_cost(1).is_acquired());
        let items = [(&full as &dyn Limiter, 1), (&empty as &dyn Limiter, 1)];
        assert_eq!(
            peek_all(items.iter().copied()),
            Decision::Retry {
                after: Duration::from_secs(1)
            }
        );
        let impossible = [(&full as &dyn Limiter, 11), (&empty as &dyn Limiter, 1)];
        assert_eq!(peek_all(impossible.iter().copied()), Decision::Impossible);
        assert_eq!(peek_all(core::iter::empty()), Decision::Acquired);
    }

    #[test]
    fn test_acquire_all_refunds_earlier_commits_when_a_later_one_refuses() {
        // A pool whose peek lies (says yes) but whose commit refuses, standing in
        // for a concurrent caller winning the race between peek and commit.
        struct Liar;
        impl Limiter for Liar {
            fn peek(&self, _: u32) -> Decision {
                Decision::Acquired
            }
            fn acquire_cost(&self, _: u32) -> Decision {
                Decision::Retry {
                    after: Duration::from_millis(7),
                }
            }
            fn available(&self) -> u32 {
                0
            }
            fn capacity(&self) -> u32 {
                1
            }
        }

        let first = Throttle::per_second(10);
        let items = [(&first as &dyn Limiter, 4), (&Liar as &dyn Limiter, 1)];
        assert_eq!(
            acquire_all(items.iter().copied()),
            Decision::Retry {
                after: Duration::from_millis(7)
            }
        );
        assert_eq!(first.available(), 10, "the first constituent was refunded");
    }

    #[test]
    fn test_reserve_all_rolls_back_on_failure() {
        let a = Throttle::per_second(10);
        let b = Pool::new(1);
        assert!(b.acquire_cost(1).is_acquired());
        let items = [(&a as &dyn Limiter, 5), (&b as &dyn Limiter, 1)];
        assert_eq!(reserve_all(items.iter().copied(), Duration::MAX), None);
        assert_eq!(a.available(), 10, "a's reservation was refunded");

        let ok = [(&a as &dyn Limiter, 5)];
        assert_eq!(
            reserve_all(ok.iter().copied(), Duration::MAX),
            Some(Duration::ZERO)
        );
        assert_eq!(a.available(), 5);
    }
}
