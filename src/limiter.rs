//! The limiter seam every algorithm and composite shares.

use core::time::Duration;

use crate::decision::Decision;
use crate::decision::Decision as D;

/// The contract a limiter satisfies: take a cost and report the [`Decision`].
///
/// This is the Tier-3 extension point. The single token bucket
/// ([`Throttle`](crate::Throttle)) implements it, and the composite limiters
/// that arrive with the rest of v0.2 (hybrid, layered, per-key) both implement
/// it and consume it — a hybrid limiter, for instance, holds several `Limiter`s
/// and a request must clear all of them.
///
/// [`acquire_cost`](Self::acquire_cost) is the **synchronous, consuming** core:
/// it deducts the tokens on success and never blocks. The waiting
/// [`acquire`](crate::Throttle::acquire) surface is a thin layer on top that
/// sleeps for the returned [`Decision::Retry`] interval and tries again. Keeping
/// the core synchronous is what lets composites combine outcomes (take the
/// longest wait, fail fast on [`Decision::Impossible`]) without spawning tasks.
///
/// Implementors are `Send + Sync` so a limiter can be shared across tasks behind
/// an [`Arc`](std::sync::Arc).
///
/// # Composition
///
/// [`peek`](Self::peek) is what makes "a request must pass *all* of these"
/// correct. A composite cannot simply call [`acquire_cost`](Self::acquire_cost)
/// on each constituent: if an early one grants (and deducts) while a later one
/// refuses, the early tokens are spent for a request that never happened. So a
/// composite first `peek`s every constituent — a non-consuming check — and only
/// commits (via `acquire_cost`) once all of them would grant.
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
    /// Reports whether `cost` tokens *would* be granted now, **without** taking
    /// them.
    ///
    /// Returns [`Decision::Acquired`] when the tokens are available this instant,
    /// [`Decision::Retry`] with an estimate of the wait until they would be, or
    /// [`Decision::Impossible`] when `cost` exceeds capacity. Because it consumes
    /// nothing, a composite can poll every constituent before deciding to commit.
    /// The result is a snapshot: a concurrent acquisition can change it the next
    /// instant, so it is a guide, not a reservation.
    fn peek(&self, cost: u32) -> Decision;

    /// Attempts to take `cost` tokens now, deducting them on success.
    ///
    /// Returns [`Decision::Acquired`] when the tokens were granted,
    /// [`Decision::Retry`] with the wait until the same cost could succeed, or
    /// [`Decision::Impossible`] when `cost` exceeds the limiter's capacity and
    /// no wait would ever satisfy it. A `cost` of `0` is always granted.
    fn acquire_cost(&self, cost: u32) -> Decision;

    /// Returns the number of whole tokens available right now.
    ///
    /// This is a point-in-time read for observability and tests; it may change
    /// the instant another acquisition lands, so it is not a reservation.
    fn available(&self) -> u32;

    /// Returns the most tokens this limiter can ever hold at once (its burst
    /// ceiling). A request whose cost exceeds this can never be granted.
    fn capacity(&self) -> u32;
}

/// A limiter addressed by a key, with its clock type erased so composites can
/// store it without carrying a clock type parameter.
///
/// Implemented by [`PerKey`](crate::PerKey) for every clock, this lets
/// [`Layered`](crate::Layered) hold per-key and per-endpoint scopes behind a
/// trait object and still accept stores built on any clock (a `ManualClock` in
/// tests, the `SystemClock` in production).
pub(crate) trait KeyedLimiter<K>: Send + Sync {
    /// Non-consuming check for `key`. See [`Limiter::peek`].
    fn peek(&self, key: &K, cost: u32) -> Decision;
    /// Consuming attempt for `key`, returning whether it was granted.
    fn try_acquire_with_cost(&self, key: &K, cost: u32) -> bool;
    /// The per-key capacity. See [`Limiter::capacity`].
    fn capacity(&self) -> u32;
}

/// Aggregates a non-consuming peek across constituents that must *all* grant.
///
/// Returns [`Decision::Impossible`] if any constituent could never grant,
/// [`Decision::Retry`] with the longest constituent wait if any is short of
/// tokens, or [`Decision::Acquired`] only when every one would grant now.
/// Consumes nothing.
pub(crate) fn peek_all<'a, I>(items: I) -> Decision
where
    I: Iterator<Item = (&'a dyn Limiter, u32)>,
{
    let mut max_wait: Option<Duration> = None;
    for (limiter, cost) in items {
        match limiter.peek(cost) {
            D::Acquired => {}
            D::Retry { after } => {
                max_wait = Some(max_wait.map_or(after, |w| w.max(after)));
            }
            D::Impossible => return D::Impossible,
        }
    }
    match max_wait {
        Some(after) => D::Retry { after },
        None => D::Acquired,
    }
}

/// Peeks every constituent and, only if all would grant, commits each.
///
/// This is the correctness core of "must pass all": [`peek_all`] confirms
/// availability before any tokens are taken. If a concurrent acquisition makes a
/// commit fail after the peek succeeded, the already-committed constituents keep
/// their deduction and the call reports the resulting [`Decision::Retry`] — the
/// composite is then momentarily *more* conservative (tokens spent without
/// admitting), never less, so it can never over-admit.
///
/// `items` is iterated twice (peek pass, then commit pass), hence the `Clone`
/// bound; for slice-backed iterators this is a cheap pointer copy.
pub(crate) fn acquire_all<'a, I>(items: I) -> Decision
where
    I: Iterator<Item = (&'a dyn Limiter, u32)> + Clone,
{
    match peek_all(items.clone()) {
        D::Acquired => {}
        other => return other,
    }
    for (limiter, cost) in items {
        match limiter.acquire_cost(cost) {
            D::Acquired => {}
            other => return other,
        }
    }
    D::Acquired
}
