//! A pause: "grant nothing until this instant".
//!
//! Limiters reconcile with a downstream that reports its budget exhausted until
//! a reset, or that answers with a `Retry-After`, by pausing. The pause is one
//! atomic word — the end of the hold in nanoseconds since the owner's epoch,
//! `0` meaning "not held" — so checking it on the acquire path is a single
//! relaxed load in the common, unpaused case.

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;

/// The end of a pause, in nanoseconds since the owning limiter's epoch.
#[derive(Debug, Default)]
pub(crate) struct Hold {
    /// `0` when not held; otherwise the first instant the hold no longer
    /// applies. Only ever moved forward by [`extend`](Self::extend).
    until_ns: AtomicU64,
}

impl Hold {
    /// A hold that is not set.
    pub(crate) const fn new() -> Self {
        Self {
            until_ns: AtomicU64::new(0),
        }
    }

    /// Whether a pause has ever been set (and not cleared). The cheap pre-check
    /// that lets the acquire path skip computing `now` in nanoseconds.
    #[inline]
    pub(crate) fn is_set(&self) -> bool {
        self.until_ns.load(Ordering::Relaxed) != 0
    }

    /// How long the hold still applies at `now_ns` (`ZERO` when it does not).
    #[inline]
    pub(crate) fn remaining(&self, now_ns: u64) -> Duration {
        let until = self.until_ns.load(Ordering::Relaxed);
        Duration::from_nanos(until.saturating_sub(now_ns))
    }

    /// Holds until at least `now_ns + duration`. A shorter hold never shortens
    /// a longer one already in place. `Duration::MAX` holds until [`clear`]ed.
    ///
    /// [`clear`]: Self::clear
    pub(crate) fn extend(&self, now_ns: u64, duration: Duration) {
        if duration.is_zero() {
            return;
        }
        let span = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        // `max(1)` keeps a hold that ends at the epoch distinguishable from
        // "not held" (it is already over, so it changes nothing).
        let until = now_ns.saturating_add(span).max(1);
        let _previous = self.until_ns.fetch_max(until, Ordering::Relaxed);
    }

    /// Lifts the hold immediately, waking any waiter sleeping it out.
    pub(crate) fn clear(&self) {
        let previous = self.until_ns.swap(0, Ordering::AcqRel);
        #[cfg(feature = "runtime")]
        if previous != 0 {
            crate::rt::announce_resume();
        }
        #[cfg(not(feature = "runtime"))]
        let _ = previous;
    }
}

/// Nanoseconds from `earlier` to `now`, saturating at zero and at `u64::MAX`
/// (about 584 years).
#[inline]
pub(crate) fn nanos_between(earlier: clock_lib::Monotonic, now: clock_lib::Monotonic) -> u64 {
    u64::try_from(now.saturating_duration_since(earlier).as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::Hold;
    use core::time::Duration;

    #[test]
    fn test_unset_hold_has_no_remaining_time() {
        let hold = Hold::new();
        assert!(!hold.is_set());
        assert_eq!(hold.remaining(0), Duration::ZERO);
        assert_eq!(hold.remaining(u64::MAX), Duration::ZERO);
    }

    #[test]
    fn test_extend_sets_and_never_shortens() {
        let hold = Hold::new();
        hold.extend(100, Duration::from_nanos(50));
        assert!(hold.is_set());
        assert_eq!(hold.remaining(100), Duration::from_nanos(50));
        assert_eq!(hold.remaining(140), Duration::from_nanos(10));
        assert_eq!(hold.remaining(150), Duration::ZERO);
        // A shorter hold leaves the longer one in place.
        hold.extend(100, Duration::from_nanos(10));
        assert_eq!(hold.remaining(100), Duration::from_nanos(50));
        // A longer one extends it.
        hold.extend(100, Duration::from_nanos(500));
        assert_eq!(hold.remaining(100), Duration::from_nanos(500));
    }

    #[test]
    fn test_zero_duration_is_a_noop_and_max_saturates() {
        let hold = Hold::new();
        hold.extend(5, Duration::ZERO);
        assert!(!hold.is_set());
        hold.extend(5, Duration::MAX);
        assert_eq!(hold.remaining(u64::MAX - 1), Duration::from_nanos(1));
    }

    #[test]
    fn test_clear_lifts_the_hold() {
        let hold = Hold::new();
        hold.extend(0, Duration::from_secs(60));
        hold.clear();
        assert!(!hold.is_set());
        assert_eq!(hold.remaining(0), Duration::ZERO);
    }
}
