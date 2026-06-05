//! How the per-key store bounds its memory.

use core::time::Duration;

/// The default key-capacity cap, applied unless overridden.
///
/// Roughly a million keys: generous for legitimate traffic, but a hard ceiling
/// so a flood of unique keys can never grow the store without limit. Match it to
/// your real key cardinality with [`Eviction::capacity`].
pub const DEFAULT_MAX_KEYS: usize = 1 << 20;

/// How a [`PerKey`](crate::PerKey) limiter bounds the memory its per-key state
/// can occupy.
///
/// A limiter that keeps state per key is a denial-of-service vector against
/// itself if that state can grow without limit: a flood of unique keys would
/// exhaust memory. `Eviction` is the defense, and two independent bounds compose:
///
/// - **Capacity** — a hard ceiling on live keys. Inserting a new key past the
///   ceiling evicts the least-recently-seen one. This caps a unique-key flood.
/// - **Idle TTL** — keys not seen for longer than the TTL become evictable, so
///   long-idle state is reclaimed rather than held forever.
///
/// Eviction is **lazy and incremental**: it runs while inserting a new key,
/// touches only the one shard being written, and never sweeps the whole store or
/// blocks the steady-state path. The capacity is enforced per shard, so the
/// live-key count stays within a small factor of the cap.
///
/// The [`Default`] is safe: a [`DEFAULT_MAX_KEYS`] cap and no TTL — bounded out
/// of the box. Opt into unbounded growth explicitly with
/// [`unbounded`](Self::unbounded), understanding the risk.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use throttle_net::Eviction;
///
/// // Cap at 100k keys and reclaim anything idle for five minutes.
/// let policy = Eviction::capacity(100_000).with_idle(Duration::from_secs(300));
/// assert_eq!(policy.max_keys(), Some(100_000));
/// assert_eq!(policy.idle_ttl(), Some(Duration::from_secs(300)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Eviction {
    max_keys: Option<usize>,
    idle_ttl: Option<Duration>,
}

impl Eviction {
    /// A hard cap of `max_keys` live keys, with no idle expiry.
    ///
    /// A `max_keys` of `0` is treated as `1` (the store always holds at least
    /// one key per shard).
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Eviction;
    ///
    /// let policy = Eviction::capacity(50_000);
    /// assert_eq!(policy.max_keys(), Some(50_000));
    /// assert_eq!(policy.idle_ttl(), None);
    /// ```
    #[must_use]
    pub const fn capacity(max_keys: usize) -> Self {
        Self {
            max_keys: Some(max_keys),
            idle_ttl: None,
        }
    }

    /// Reclaim keys idle for longer than `ttl`, keeping the default capacity cap.
    ///
    /// Idle expiry alone does not bound a unique-key flood — flooded keys are not
    /// idle — so this keeps the [`DEFAULT_MAX_KEYS`] cap as the flood defense and
    /// layers the TTL on top. Use [`capacity`](Self::capacity) plus
    /// [`with_idle`](Self::with_idle) to choose both bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::{Eviction, DEFAULT_MAX_KEYS};
    ///
    /// let policy = Eviction::idle(Duration::from_secs(300));
    /// assert_eq!(policy.idle_ttl(), Some(Duration::from_secs(300)));
    /// assert_eq!(policy.max_keys(), Some(DEFAULT_MAX_KEYS));
    /// ```
    #[must_use]
    pub const fn idle(ttl: Duration) -> Self {
        Self {
            max_keys: Some(DEFAULT_MAX_KEYS),
            idle_ttl: Some(ttl),
        }
    }

    /// Both bounds at once: a `max_keys` cap and an idle `ttl`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Eviction;
    ///
    /// let policy = Eviction::new(10_000, Duration::from_secs(60));
    /// assert_eq!(policy.max_keys(), Some(10_000));
    /// assert_eq!(policy.idle_ttl(), Some(Duration::from_secs(60)));
    /// ```
    #[must_use]
    pub const fn new(max_keys: usize, ttl: Duration) -> Self {
        Self {
            max_keys: Some(max_keys),
            idle_ttl: Some(ttl),
        }
    }

    /// No bounds at all — the store grows without limit.
    ///
    /// Only safe when the key space is intrinsically bounded (a fixed set of
    /// tenants, say). Against untrusted keys this is a self-inflicted
    /// denial-of-service: prefer [`capacity`](Self::capacity).
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Eviction;
    ///
    /// let policy = Eviction::unbounded();
    /// assert_eq!(policy.max_keys(), None);
    /// ```
    #[must_use]
    pub const fn unbounded() -> Self {
        Self {
            max_keys: None,
            idle_ttl: None,
        }
    }

    /// Returns a copy with the capacity cap set to `max_keys`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Eviction;
    ///
    /// let policy = Eviction::idle(Duration::from_secs(30)).with_capacity(1_000);
    /// assert_eq!(policy.max_keys(), Some(1_000));
    /// ```
    #[must_use]
    pub const fn with_capacity(mut self, max_keys: usize) -> Self {
        self.max_keys = Some(max_keys);
        self
    }

    /// Returns a copy with idle expiry set to `ttl`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Eviction;
    ///
    /// let policy = Eviction::capacity(1_000).with_idle(Duration::from_secs(30));
    /// assert_eq!(policy.idle_ttl(), Some(Duration::from_secs(30)));
    /// ```
    #[must_use]
    pub const fn with_idle(mut self, ttl: Duration) -> Self {
        self.idle_ttl = Some(ttl);
        self
    }

    /// Returns a copy with the capacity cap removed (unbounded key count).
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Eviction;
    ///
    /// let policy = Eviction::default().without_capacity();
    /// assert_eq!(policy.max_keys(), None);
    /// ```
    #[must_use]
    pub const fn without_capacity(mut self) -> Self {
        self.max_keys = None;
        self
    }

    /// The configured capacity cap, if any.
    #[must_use]
    pub const fn max_keys(&self) -> Option<usize> {
        self.max_keys
    }

    /// The configured idle TTL, if any.
    #[must_use]
    pub const fn idle_ttl(&self) -> Option<Duration> {
        self.idle_ttl
    }
}

impl Default for Eviction {
    /// A [`DEFAULT_MAX_KEYS`] capacity cap and no idle TTL — bounded memory out
    /// of the box.
    fn default() -> Self {
        Self::capacity(DEFAULT_MAX_KEYS)
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_MAX_KEYS, Eviction};
    use core::time::Duration;

    #[test]
    fn test_default_is_bounded() {
        let policy = Eviction::default();
        assert_eq!(policy.max_keys(), Some(DEFAULT_MAX_KEYS));
        assert_eq!(policy.idle_ttl(), None);
    }

    #[test]
    fn test_idle_keeps_default_cap() {
        let policy = Eviction::idle(Duration::from_secs(5));
        assert_eq!(policy.max_keys(), Some(DEFAULT_MAX_KEYS));
        assert_eq!(policy.idle_ttl(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_unbounded_has_no_bounds() {
        let policy = Eviction::unbounded();
        assert_eq!(policy.max_keys(), None);
        assert_eq!(policy.idle_ttl(), None);
    }

    #[test]
    fn test_builders_compose() {
        let policy = Eviction::capacity(10).with_idle(Duration::from_secs(1));
        assert_eq!(policy.max_keys(), Some(10));
        assert_eq!(policy.idle_ttl(), Some(Duration::from_secs(1)));

        let dropped = policy.without_capacity();
        assert_eq!(dropped.max_keys(), None);
        assert_eq!(dropped.idle_ttl(), Some(Duration::from_secs(1)));
    }
}
