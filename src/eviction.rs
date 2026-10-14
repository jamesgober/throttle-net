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
/// - **Capacity** — a hard ceiling on live keys. Inserting a new key into a
///   full shard evicts a key whose bucket has **recovered** (is full again —
///   forgetting it loses nothing), preferring one not used recently (CLOCK,
///   second chance). If none of the scanned keys has recovered, a *limited*
///   key is evicted but its **debt** — when its bucket will be full again — is
///   kept in a compact per-shard ledger (16 bytes, keyed by the key's hash),
///   and a key that comes back while its debt is there is re-created in that
///   debt. So eviction never hands a limited key a fresh allowance; `peek`,
///   `available`, and `refund` see the remembered debt too. Only when
///   the ledger is full of unsettled debts too does [`WhenFull`] decide what
///   gives: by default the *new* key waits or is refused
///   ([`WhenFull::RefuseNew`]).
/// - **Idle TTL** — keys not seen for longer than the TTL are reclaimed, so
///   long-idle state is not held forever — but only once their bucket has
///   recovered: a key in debt (behind its own queued reservations, say) is
///   kept until it is full again, however idle. The effective TTL is never
///   shorter than a bucket's time to refill completely.
///
/// # Key floods and lockout
///
/// A bounded store must give way somewhere when an attacker floods it with
/// distinct keys and drains each one: either it forgets limited keys (and they
/// regain a fresh allowance — a limit bypass) or it refuses new keys (a
/// lockout of legitimate newcomers). With the default
/// [`WhenFull::RefuseNew`], the limit always holds and a lockout needs the
/// attacker to keep **twice the cap** — every key slot *and* every ledger
/// slot — filled with unrecovered keys. A drained key recovers in
/// `T = drained cost × period / refill amount`, so the flood must sustain
/// about `2 × max_keys / T` new drained keys per second. With the default
/// one-million-key cap:
///
/// | Per-key limit | Drained per key | Flood rate needed to lock out |
/// |---|---|---|
/// | `per_second(10)` | 1 token (0.1 s) | ~21 M keys/s |
/// | `per_minute(60)` | 1 token (1 s) | ~2.1 M keys/s |
/// | `per_minute(100)` | all 100 (60 s) | ~35 k keys/s |
/// | `per_duration(1000, 1 h)` | all 1,000 (1 h) | ~580 keys/s |
///
/// Existing keys are never locked out, and a locked-out newcomer is refused
/// with the wait until room is expected. Long windows with large bursts are
/// the exposed case: size `max_keys` well above your real key cardinality, or
/// choose [`WhenFull::ForgetDebt`] to keep admitting newcomers by forgetting
/// the debt that settles soonest — a bounded bypass (that key regains at most
/// what it still owed) traded for availability. (rate-net's store always
/// forgets; this crate does so only when you opt in.)
///
/// Eviction is **lazy and bounded**: it runs only while inserting a new key,
/// touches only the one shard being written, and does a fixed amount of work —
/// at most 32 slots scanned for a victim and 8 for idle expiry — however large
/// the shard. The capacity is divided evenly over the shards (rounded up, at
/// least one key per shard), so the live-key count stays at or below the cap
/// plus one key per shard. Size the cap above your real key cardinality.
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
    when_full: WhenFull,
}

/// What a capped [`PerKey`](crate::PerKey) store does when a new key arrives
/// and both a shard and its debt ledger are full of keys that are still
/// limited. See [`Eviction`] for the trade-off and its numbers.
///
/// # Examples
///
/// ```
/// use throttle_net::{Eviction, WhenFull};
///
/// let strict = Eviction::capacity(10_000);
/// assert_eq!(strict.when_full(), WhenFull::RefuseNew);
///
/// let available = Eviction::capacity(10_000).with_when_full(WhenFull::ForgetDebt);
/// assert_eq!(available.when_full(), WhenFull::ForgetDebt);
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WhenFull {
    /// Refuse the new key (its acquire waits, or fails with the wait until room
    /// is expected) — every per-key limit holds exactly. The default.
    #[default]
    RefuseNew,
    /// Admit the new key by forgetting the remembered debt that settles
    /// soonest: that key, if it returns before its bucket would have refilled,
    /// regains what it still owed early. Never locks out newcomers.
    ForgetDebt,
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
            when_full: WhenFull::RefuseNew,
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
            when_full: WhenFull::RefuseNew,
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
            when_full: WhenFull::RefuseNew,
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
            when_full: WhenFull::RefuseNew,
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

    /// Returns a copy with the full-store policy set to `when_full`.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Eviction, WhenFull};
    ///
    /// let policy = Eviction::capacity(1_000).with_when_full(WhenFull::ForgetDebt);
    /// assert_eq!(policy.when_full(), WhenFull::ForgetDebt);
    /// ```
    #[must_use]
    pub const fn with_when_full(mut self, when_full: WhenFull) -> Self {
        self.when_full = when_full;
        self
    }

    /// What a full store does with a new key ([`WhenFull::RefuseNew`] unless
    /// set).
    #[must_use]
    pub const fn when_full(&self) -> WhenFull {
        self.when_full
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
    use super::{DEFAULT_MAX_KEYS, Eviction, WhenFull};
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

    #[test]
    fn test_full_policy_defaults_to_refusing_and_composes() {
        assert_eq!(Eviction::default().when_full(), WhenFull::RefuseNew);
        assert_eq!(Eviction::unbounded().when_full(), WhenFull::RefuseNew);
        let forgetful = Eviction::idle(Duration::from_secs(1)).with_when_full(WhenFull::ForgetDebt);
        assert_eq!(forgetful.when_full(), WhenFull::ForgetDebt);
        assert_eq!(forgetful.with_capacity(5).when_full(), WhenFull::ForgetDebt);
        assert_eq!(WhenFull::default(), WhenFull::RefuseNew);
    }
}
