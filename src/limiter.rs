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
/// [`refund`](Self::refund), [`reserve`](Self::reserve),
/// [`pause_for`](Self::pause_for), [`paused_for`](Self::paused_for), and
/// [`resume`](Self::resume) have conservative default bodies, so an
/// implementation only needs the four required methods; override them when the
/// algorithm can do better (the built-in limiters do).
///
/// # Shared limiters
///
/// `Limiter` is implemented for `&L`, `Box<L>`, and `Arc<L>` whenever `L` is a
/// `Limiter` (sized or not), forwarding every method. So a limiter shared
/// behind an `Arc` can be passed wherever a `Limiter` is expected — to
/// [`RateLimitInfo::sync_requests`](crate::provider::RateLimitInfo::sync_requests),
/// or as a [`Hybrid`](crate::Hybrid) constituent — exactly as in 1.0.
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

    /// How much longer this limiter stays paused (`Duration::ZERO` when it is
    /// not). A waiting acquire that has already reserved its units sleeps this
    /// out before it returns, so a pause set after it reserved still holds it.
    ///
    /// The default reports no pause (the default [`pause_for`](Self::pause_for)
    /// drains instead of pausing).
    fn paused_for(&self) -> Duration {
        Duration::ZERO
    }

    /// Lifts any pause immediately, waking waiters that were sleeping it out.
    /// The default does nothing.
    fn resume(&self) {}

    /// Charges `cost` units that were **already spent** downstream, whether or
    /// not they fit — the overrun of a call admitted on an estimate. Returns
    /// how many units could not be charged (`0` when all were).
    ///
    /// Unlike [`acquire_cost`](Self::acquire_cost) this never refuses for lack
    /// of units, and unlike [`reserve`](Self::reserve) it ignores a pause: the
    /// units are gone either way. A token bucket records them as debt that
    /// later callers wait behind.
    ///
    /// The default reserves in capacity-sized chunks with no wait limit and,
    /// where the limiter cannot reserve, drains what is available and reports
    /// the rest as uncharged.
    fn charge(&self, cost: u32) -> u32 {
        let capacity = self.capacity();
        if capacity == 0 {
            return cost;
        }
        let mut left = cost;
        while left > 0 {
            let chunk = left.min(capacity);
            if self.reserve(chunk, Duration::MAX).is_none() {
                let available = self.available().min(left);
                if available > 0 && self.acquire_cost(available).is_acquired() {
                    left -= available;
                }
                return left;
            }
            left -= chunk;
        }
        0
    }
}

/// Forwards every method, so `&L` is a limiter wherever `L` is.
impl<L: Limiter + ?Sized> Limiter for &L {
    #[inline]
    fn peek(&self, cost: u32) -> Decision {
        (**self).peek(cost)
    }
    #[inline]
    fn acquire_cost(&self, cost: u32) -> Decision {
        (**self).acquire_cost(cost)
    }
    #[inline]
    fn available(&self) -> u32 {
        (**self).available()
    }
    #[inline]
    fn capacity(&self) -> u32 {
        (**self).capacity()
    }
    #[inline]
    fn refund(&self, cost: u32) {
        (**self).refund(cost);
    }
    #[inline]
    fn reserve(&self, cost: u32, max_wait: Duration) -> Option<Duration> {
        (**self).reserve(cost, max_wait)
    }
    #[inline]
    fn pause_for(&self, duration: Duration) {
        (**self).pause_for(duration);
    }
    #[inline]
    fn paused_for(&self) -> Duration {
        (**self).paused_for()
    }
    #[inline]
    fn resume(&self) {
        (**self).resume();
    }
    #[inline]
    fn charge(&self, cost: u32) -> u32 {
        (**self).charge(cost)
    }
}

/// Forwards every method, so `Box<L>` (including `Box<dyn Limiter>`) is a
/// limiter wherever `L` is.
impl<L: Limiter + ?Sized> Limiter for std::boxed::Box<L> {
    #[inline]
    fn peek(&self, cost: u32) -> Decision {
        (**self).peek(cost)
    }
    #[inline]
    fn acquire_cost(&self, cost: u32) -> Decision {
        (**self).acquire_cost(cost)
    }
    #[inline]
    fn available(&self) -> u32 {
        (**self).available()
    }
    #[inline]
    fn capacity(&self) -> u32 {
        (**self).capacity()
    }
    #[inline]
    fn refund(&self, cost: u32) {
        (**self).refund(cost);
    }
    #[inline]
    fn reserve(&self, cost: u32, max_wait: Duration) -> Option<Duration> {
        (**self).reserve(cost, max_wait)
    }
    #[inline]
    fn pause_for(&self, duration: Duration) {
        (**self).pause_for(duration);
    }
    #[inline]
    fn paused_for(&self) -> Duration {
        (**self).paused_for()
    }
    #[inline]
    fn resume(&self) {
        (**self).resume();
    }
    #[inline]
    fn charge(&self, cost: u32) -> u32 {
        (**self).charge(cost)
    }
}

/// Forwards every method, so a limiter shared behind an `Arc` (including
/// `Arc<dyn Limiter>`) is a limiter wherever `L` is.
impl<L: Limiter + ?Sized> Limiter for std::sync::Arc<L> {
    #[inline]
    fn peek(&self, cost: u32) -> Decision {
        (**self).peek(cost)
    }
    #[inline]
    fn acquire_cost(&self, cost: u32) -> Decision {
        (**self).acquire_cost(cost)
    }
    #[inline]
    fn available(&self) -> u32 {
        (**self).available()
    }
    #[inline]
    fn capacity(&self) -> u32 {
        (**self).capacity()
    }
    #[inline]
    fn refund(&self, cost: u32) {
        (**self).refund(cost);
    }
    #[inline]
    fn reserve(&self, cost: u32, max_wait: Duration) -> Option<Duration> {
        (**self).reserve(cost, max_wait)
    }
    #[inline]
    fn pause_for(&self, duration: Duration) {
        (**self).pause_for(duration);
    }
    #[inline]
    fn paused_for(&self) -> Duration {
        (**self).paused_for()
    }
    #[inline]
    fn resume(&self) {
        (**self).resume();
    }
    #[inline]
    fn charge(&self, cost: u32) -> u32 {
        (**self).charge(cost)
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

/// How long a composite may hold one constituent's units while it waits for
/// the others. A held reservation ties up capacity that may be shared with
/// other callers (a [`Layered`](crate::Layered) global scope, a
/// [`HybridBuilder::shared`](crate::HybridBuilder::shared) limiter), so a
/// composite never holds one for longer than this before the request can
/// actually go (TN2-002).
pub(crate) const HOLD_SLACK: Duration = Duration::from_millis(10);

/// Reserves on every constituent, refunding the ones already reserved if any
/// cannot reserve within `max_wait`. Returns the longest wait.
fn reserve_each<'a, I>(items: I, max_wait: Duration) -> Option<Duration>
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

/// A composite's [`Limiter::reserve`]: reserves on every constituent, but only
/// when no constituent would be held more than [`HOLD_SLACK`] longer than it is
/// needed — otherwise it takes nothing and returns `None`, and the caller
/// waits without holding anyone's units. If any constituent cannot reserve
/// within `max_wait`, the ones already reserved are refunded.
pub(crate) fn reserve_all<'a, I>(items: I, max_wait: Duration) -> Option<Duration>
where
    I: Iterator<Item = (&'a dyn Limiter, u32)> + Clone,
{
    let mut longest = Duration::ZERO;
    let mut shortest = Duration::MAX;
    for (limiter, cost) in items.clone() {
        let wait = match limiter.peek(cost) {
            D::Acquired => Duration::ZERO,
            D::Retry { after } => after,
            _ => return None,
        };
        longest = longest.max(wait);
        shortest = shortest.min(wait);
    }
    if longest > max_wait || longest.saturating_sub(shortest) > HOLD_SLACK {
        return None;
    }
    reserve_each(items, max_wait)
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

/// What a composite's waiting acquire holds between steps.
#[cfg(feature = "runtime")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Held {
    /// Nothing is held.
    Nothing,
    /// The bottleneck constituent at this index holds a reservation.
    One(usize),
    /// Every constituent's units are taken.
    All,
}

/// The per-wait state of a composite's waiting acquire.
#[cfg(feature = "runtime")]
#[derive(Debug, Clone, Copy)]
pub(crate) struct Progress {
    held: Held,
    /// When the held bottleneck reservation matures (runtime time): a waiter
    /// woken early (by a `resume` elsewhere) must not take the rest before.
    due: Option<crate::rt::Instant>,
    /// How many times a matured bottleneck reservation was given back because
    /// another constituent turned out to need a longer wait.
    switches: u8,
}

#[cfg(feature = "runtime")]
impl Progress {
    /// A wait that holds nothing yet.
    pub(crate) const fn new() -> Self {
        Self {
            held: Held::Nothing,
            due: None,
            switches: 0,
        }
    }
}

/// After this many bottleneck switches, a composite holds its matured
/// reservation and reserves the rest too, so two constituents that keep
/// overtaking each other cannot postpone a waiter indefinitely.
#[cfg(feature = "runtime")]
const MAX_SWITCHES: u8 = 4;

/// One step of a composite's waiting acquire.
///
/// A request that must clear several constituents waits on the *bottleneck*
/// only: it peeks every constituent, reserves on the one with the longest wait
/// (queueing fairly behind that constituent's earlier waiters), and takes
/// nothing from the others. When that reservation matures it takes the others
/// at once. If one of them now needs a longer wait, the matured reservation is
/// given back and the wait moves to the new bottleneck — unless that wait is
/// within [`HOLD_SLACK`], or the bottleneck has already moved
/// [`MAX_SWITCHES`] times, in which case the rest are reserved too.
///
/// So a fast constituent — typically a scope shared with other callers — is
/// charged only when the request is (nearly) ready to go, never minutes ahead:
/// one caller queued on its own slow scope cannot drain a shared one and starve
/// everyone else (TN2-002).
///
/// The fallback after [`MAX_SWITCHES`] guarantees progress when two
/// contended constituents keep overtaking each other, at a bounded cost: that
/// one waiter holds its matured bottleneck units (at most its own `cost`)
/// while it waits for the rest, for at most its remaining budget. Reaching it
/// takes four consecutive bottleneck switches, each of which already waited a
/// full bottleneck wait.
#[cfg(feature = "runtime")]
pub(crate) fn step_all<'a, I>(
    items: I,
    budget: Duration,
    progress: &mut Progress,
) -> crate::wait::Step
where
    I: Iterator<Item = (&'a dyn Limiter, u32)> + Clone,
{
    use crate::wait::Step;
    if let Held::One(bottleneck) = progress.held {
        if let Some(due) = progress.due {
            let left = due.saturating_duration_since(crate::rt::Instant::now());
            if !left.is_zero() {
                return Step::Holding(left);
            }
        }
        let rest = || {
            items
                .clone()
                .enumerate()
                .filter(move |&(index, _)| index != bottleneck)
                .map(|(_, item)| item)
        };
        match peek_all(rest()) {
            D::Acquired => {
                if acquire_all(rest()).is_acquired() {
                    progress.held = Held::All;
                    return Step::Granted(Duration::ZERO);
                }
                // A concurrent caller won a race on another constituent: keep
                // the bottleneck's units and try again in a moment.
                return Step::Holding(Duration::ZERO);
            }
            D::Retry { after } if after <= HOLD_SLACK || progress.switches >= MAX_SWITCHES => {
                if let Some(wait) = reserve_each(rest(), budget) {
                    progress.held = Held::All;
                    return Step::Granted(wait);
                }
            }
            D::Retry { .. } => {}
            _ => {
                release_all(items, progress);
                return Step::Never;
            }
        }
        // Another constituent became the bottleneck: give the matured units
        // back rather than hold them while waiting on it.
        release_all(items.clone(), progress);
        progress.switches = progress.switches.saturating_add(1);
    }

    let mut bottleneck: Option<(usize, Duration)> = None;
    for (index, (limiter, cost)) in items.clone().enumerate() {
        match limiter.peek(cost) {
            D::Acquired => {}
            D::Retry { after } => {
                if bottleneck.is_none_or(|(_, longest)| after > longest) {
                    bottleneck = Some((index, after));
                }
            }
            _ => return Step::Never,
        }
    }
    let Some((index, wait)) = bottleneck else {
        // Every constituent would grant now: take them all at once.
        return match acquire_all(items) {
            D::Acquired => {
                progress.held = Held::All;
                Step::Granted(Duration::ZERO)
            }
            D::Impossible => Step::Never,
            // A concurrent caller won a race between the peek and the commit.
            _ => Step::Retry(Duration::ZERO),
        };
    };
    if wait > budget {
        return Step::Retry(wait);
    }
    match items.clone().nth(index) {
        Some((limiter, cost)) => match limiter.reserve(cost, budget) {
            Some(wait) => {
                progress.held = Held::One(index);
                progress.due = crate::rt::Instant::now().checked_add(wait);
                Step::Holding(wait)
            }
            // The constituent cannot queue (no native reservations, or it is
            // paused): sleep until it can grant and look again.
            None => Step::Retry(wait),
        },
        None => Step::Retry(wait),
    }
}

/// Gives back whatever a composite's waiting acquire holds.
#[cfg(feature = "runtime")]
pub(crate) fn release_all<'a, I>(items: I, progress: &mut Progress)
where
    I: Iterator<Item = (&'a dyn Limiter, u32)> + Clone,
{
    match progress.held {
        Held::Nothing => {}
        Held::One(index) => {
            if let Some((limiter, cost)) = items.clone().nth(index) {
                limiter.refund(cost);
            }
        }
        Held::All => refund_all(items),
    }
    progress.held = Held::Nothing;
    progress.due = None;
}

/// The longest remaining pause among the constituents.
#[cfg(feature = "runtime")]
pub(crate) fn paused_all<'a, I>(items: I) -> Duration
where
    I: Iterator<Item = (&'a dyn Limiter, u32)>,
{
    items
        .map(|(limiter, _)| limiter.paused_for())
        .max()
        .unwrap_or(Duration::ZERO)
}

/// A keyed scope bound to one key, so a [`Layered`](crate::Layered) scope
/// takes part in the composite algorithm like any other constituent.
#[cfg(feature = "runtime")]
pub(crate) struct KeyScope<'a, K> {
    pub(crate) scope: &'a dyn KeyedLimiter<K>,
    pub(crate) key: &'a K,
}

#[cfg(feature = "runtime")]
impl<K: Sync> Limiter for KeyScope<'_, K> {
    fn peek(&self, cost: u32) -> Decision {
        self.scope.peek(self.key, cost)
    }
    fn acquire_cost(&self, cost: u32) -> Decision {
        self.scope.acquire_cost(self.key, cost)
    }
    /// Not tracked per key here; the composite algorithm never reads it.
    fn available(&self) -> u32 {
        0
    }
    fn capacity(&self) -> u32 {
        self.scope.capacity()
    }
    fn refund(&self, cost: u32) {
        self.scope.refund(self.key, cost);
    }
    fn reserve(&self, cost: u32, max_wait: Duration) -> Option<Duration> {
        self.scope.reserve(self.key, cost, max_wait)
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
            let mut left = self.left.load(Ordering::Relaxed);
            while let Some(rest) = left.checked_sub(cost) {
                match self.left.compare_exchange_weak(
                    left,
                    rest,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Decision::Acquired,
                    Err(actual) => left = actual,
                }
            }
            self.peek(cost)
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

    #[test]
    fn test_default_provided_methods() {
        let pool = Pool::new(4);
        assert_eq!(pool.paused_for(), Duration::ZERO);
        pool.resume(); // a no-op
        // The default charge reserves what is there and drains the rest.
        assert_eq!(pool.charge(3), 0);
        assert_eq!(pool.available(), 1);
        assert_eq!(pool.charge(5), 4, "only one more unit could be charged");
        assert_eq!(pool.available(), 0);
        let closed = Pool::new(0);
        assert_eq!(closed.charge(2), 2, "a closed limiter charges nothing");
    }

    /// Calls every trait method through `L`, so the forwarding impls for
    /// `&L`, `Box<L>`, and `Arc<L>` are exercised.
    fn exercise<L: Limiter>(limiter: &L) {
        assert!(limiter.peek(1).is_acquired());
        assert!(limiter.acquire_cost(1).is_acquired());
        assert_eq!(limiter.capacity(), 10);
        assert_eq!(limiter.available(), 9);
        limiter.refund(1);
        assert_eq!(limiter.available(), 10);
        assert_eq!(limiter.reserve(2, Duration::MAX), Some(Duration::ZERO));
        assert_eq!(limiter.charge(2), 0);
        limiter.pause_for(Duration::from_secs(5));
        assert!(limiter.paused_for() > Duration::ZERO);
        limiter.resume();
        assert_eq!(limiter.paused_for(), Duration::ZERO);
    }

    #[test]
    fn test_shared_limiters_forward_every_method() {
        let throttle = Throttle::per_duration(10, Duration::from_secs(3600));
        exercise(&&throttle);
        exercise(&Box::new(Throttle::per_duration(
            10,
            Duration::from_secs(3600),
        )));
        exercise(&std::sync::Arc::new(Throttle::per_duration(
            10,
            Duration::from_secs(3600),
        )));
        let boxed: Box<dyn Limiter> =
            Box::new(Throttle::per_duration(10, Duration::from_secs(3600)));
        exercise(&boxed);
    }

    #[test]
    fn test_reserve_all_refuses_when_a_constituent_would_be_held() {
        let fast = Throttle::per_second(10);
        let slow = Pool::new(1);
        assert!(slow.acquire_cost(1).is_acquired()); // slow now waits 1s
        let items = [(&fast as &dyn Limiter, 1), (&slow as &dyn Limiter, 1)];
        assert_eq!(reserve_all(items.iter().copied(), Duration::MAX), None);
        assert_eq!(fast.available(), 10, "nothing held");
        let over_budget = [(&slow as &dyn Limiter, 1)];
        assert_eq!(
            reserve_all(over_budget.iter().copied(), Duration::from_millis(1)),
            None
        );
        let impossible = [(&fast as &dyn Limiter, 11)];
        assert_eq!(reserve_all(impossible.iter().copied(), Duration::MAX), None);
    }

    #[cfg(feature = "runtime")]
    #[allow(clippy::unwrap_used)]
    mod composite {
        use super::super::{Limiter, Progress, release_all, step_all};
        use crate::decision::Decision;
        use crate::wait::Step;
        use core::time::Duration;
        use std::collections::VecDeque;
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicU32, Ordering};

        /// A limiter that answers from scripts (then grants), and counts refunds.
        #[derive(Default)]
        struct Scripted {
            peeks: Mutex<VecDeque<Decision>>,
            acquires: Mutex<VecDeque<Decision>>,
            reserves: Mutex<VecDeque<Option<Duration>>>,
            refunds: AtomicU32,
        }

        impl Scripted {
            fn peeks(self, script: &[Decision]) -> Self {
                self.peeks.lock().unwrap().extend(script.iter().copied());
                self
            }
            fn acquires(self, script: &[Decision]) -> Self {
                self.acquires.lock().unwrap().extend(script.iter().copied());
                self
            }
            fn reserves(self, script: &[Option<Duration>]) -> Self {
                self.reserves.lock().unwrap().extend(script.iter().copied());
                self
            }
            fn refunds(&self) -> u32 {
                self.refunds.load(Ordering::Relaxed)
            }
        }

        impl Limiter for Scripted {
            fn peek(&self, _: u32) -> Decision {
                self.peeks
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(Decision::Acquired)
            }
            fn acquire_cost(&self, _: u32) -> Decision {
                self.acquires
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(Decision::Acquired)
            }
            fn available(&self) -> u32 {
                1
            }
            fn capacity(&self) -> u32 {
                1
            }
            fn refund(&self, _: u32) {
                let _ = self.refunds.fetch_add(1, Ordering::Relaxed);
            }
            fn reserve(&self, _: u32, _: Duration) -> Option<Duration> {
                self.reserves
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(Some(Duration::ZERO))
            }
        }

        fn wait(ms: u64) -> Decision {
            Decision::Retry {
                after: Duration::from_millis(ms),
            }
        }

        fn is_holding(step: &Step) -> bool {
            matches!(step, Step::Holding(_))
        }

        #[test]
        fn test_a_matured_bottleneck_takes_the_rest_or_keeps_trying_on_a_lost_race() {
            let a = Scripted::default().peeks(&[wait(1000)]);
            let b = Scripted::default().acquires(&[wait(5)]);
            let items = [(&a as &dyn Limiter, 1), (&b as &dyn Limiter, 1)];
            let mut progress = Progress::new();
            // Phase 1: `a` is the bottleneck and reserves (due at once).
            assert!(is_holding(&step_all(
                items.iter().copied(),
                Duration::MAX,
                &mut progress
            )));
            // Phase 2: `b` peeks ready but loses the race: keep `a`, try again.
            assert!(matches!(
                step_all(items.iter().copied(), Duration::MAX, &mut progress),
                Step::Holding(d) if d.is_zero()
            ));
            assert!(matches!(
                step_all(items.iter().copied(), Duration::MAX, &mut progress),
                Step::Granted(d) if d.is_zero()
            ));
            assert_eq!(a.refunds() + b.refunds(), 0);
            release_all(items.iter().copied(), &mut progress);
            assert_eq!((a.refunds(), b.refunds()), (1, 1), "everything goes back");
        }

        #[test]
        fn test_a_short_wait_on_the_rest_is_reserved_while_holding() {
            let a = Scripted::default().peeks(&[wait(1000)]);
            let b = Scripted::default()
                .peeks(&[Decision::Acquired, wait(5)])
                .reserves(&[Some(Duration::from_millis(5))]);
            let items = [(&a as &dyn Limiter, 1), (&b as &dyn Limiter, 1)];
            let mut progress = Progress::new();
            assert!(is_holding(&step_all(
                items.iter().copied(),
                Duration::MAX,
                &mut progress
            )));
            assert!(matches!(
                step_all(items.iter().copied(), Duration::MAX, &mut progress),
                Step::Granted(d) if d == Duration::from_millis(5)
            ));
        }

        #[test]
        fn test_a_long_wait_on_the_rest_switches_the_bottleneck() {
            // `a` matures but `b` then needs 1s: give `a` back and queue on `b`.
            let a = Scripted::default().peeks(&[wait(1000)]);
            let b = Scripted::default().peeks(&[Decision::Acquired, wait(1000), wait(1000)]);
            let items = [(&a as &dyn Limiter, 1), (&b as &dyn Limiter, 1)];
            let mut progress = Progress::new();
            assert!(is_holding(&step_all(
                items.iter().copied(),
                Duration::MAX,
                &mut progress
            )));
            assert!(is_holding(&step_all(
                items.iter().copied(),
                Duration::MAX,
                &mut progress
            )));
            assert_eq!(a.refunds(), 1, "the matured units went back");
            assert_eq!(progress.switches, 1);
            assert_eq!(progress.held, super::super::Held::One(1), "now queued on b");
            // Then `a` is free: take it and go.
            assert!(matches!(
                step_all(items.iter().copied(), Duration::MAX, &mut progress),
                Step::Granted(_)
            ));
        }

        #[test]
        fn test_after_four_switches_the_rest_is_reserved_while_holding() {
            let a = Scripted::default();
            let b = Scripted::default().peeks(&[wait(1000)]);
            let items = [(&a as &dyn Limiter, 1), (&b as &dyn Limiter, 1)];
            let mut progress = Progress::new();
            progress.switches = 4;
            progress.held = super::super::Held::One(0);
            assert!(matches!(
                step_all(items.iter().copied(), Duration::MAX, &mut progress),
                Step::Granted(_)
            ));
            assert_eq!(a.refunds(), 0, "held, not given back");
        }

        #[test]
        fn test_phase_two_reports_never_and_releases() {
            let a = Scripted::default().peeks(&[wait(1000)]);
            let b = Scripted::default().peeks(&[Decision::Acquired, Decision::Impossible]);
            let items = [(&a as &dyn Limiter, 1), (&b as &dyn Limiter, 1)];
            let mut progress = Progress::new();
            assert!(is_holding(&step_all(
                items.iter().copied(),
                Duration::MAX,
                &mut progress
            )));
            assert!(matches!(
                step_all(items.iter().copied(), Duration::MAX, &mut progress),
                Step::Never
            ));
            assert_eq!(a.refunds(), 1);
        }

        #[test]
        fn test_phase_one_edge_cases() {
            // Over budget: retry without reserving.
            let slow = Scripted::default().peeks(&[wait(1000)]);
            let items = [(&slow as &dyn Limiter, 1)];
            let mut progress = Progress::new();
            assert!(matches!(
                step_all(
                    items.iter().copied(),
                    Duration::from_millis(1),
                    &mut progress
                ),
                Step::Retry(_)
            ));
            // A bottleneck that cannot reserve: retry after its wait.
            let unreservable = Scripted::default().peeks(&[wait(10)]).reserves(&[None]);
            let items = [(&unreservable as &dyn Limiter, 1)];
            assert!(matches!(
                step_all(items.iter().copied(), Duration::MAX, &mut progress),
                Step::Retry(d) if d == Duration::from_millis(10)
            ));
            // Everything ready but the commit says impossible, or loses a race.
            let impossible = Scripted::default().acquires(&[Decision::Impossible]);
            let items = [(&impossible as &dyn Limiter, 1)];
            assert!(matches!(
                step_all(items.iter().copied(), Duration::MAX, &mut progress),
                Step::Never
            ));
            let racing = Scripted::default().acquires(&[wait(1)]);
            let items = [(&racing as &dyn Limiter, 1)];
            assert!(matches!(
                step_all(items.iter().copied(), Duration::MAX, &mut progress),
                Step::Retry(d) if d.is_zero()
            ));
            // A held reservation not yet due keeps holding.
            let due_later = Scripted::default()
                .peeks(&[wait(1000)])
                .reserves(&[Some(Duration::from_secs(60))]);
            let other = Scripted::default();
            let items = [(&due_later as &dyn Limiter, 1), (&other as &dyn Limiter, 1)];
            let mut progress = Progress::new();
            assert!(is_holding(&step_all(
                items.iter().copied(),
                Duration::MAX,
                &mut progress
            )));
            assert!(matches!(
                step_all(items.iter().copied(), Duration::MAX, &mut progress),
                Step::Holding(d) if d > Duration::from_secs(59)
            ));
        }
    }
}
