//! Backoff strategies and their jitter variants.
//!
//! A [`Backoff`] turns an attempt number into a delay. The base curve is
//! constant, linear, or exponential; on top of it sits a [`Jitter`] mode that
//! spreads retries so a fleet of clients that failed together does not retry in
//! lockstep (the thundering herd). [`Jitter::Decorrelated`] is the recommended
//! default and the one the [`Backoff::default`] uses.
//!
//! A backoff is a *policy*; call [`iter`](Backoff::iter) to get a
//! [`BackoffIter`] that yields one delay per attempt. The iterator carries the
//! state the jittered curves need (the previous delay, the random generator), so
//! a single policy can drive many independent retry loops.

use core::time::Duration;

/// The default ceiling a backoff delay is clamped to: 30 seconds.
const DEFAULT_MAX: Duration = Duration::from_secs(30);
/// The default exponential base delay: 100 milliseconds.
const DEFAULT_BASE: Duration = Duration::from_millis(100);
/// The default exponential growth factor.
const DEFAULT_FACTOR: f64 = 2.0;

/// How retry delays are randomized to avoid synchronized retries.
///
/// The non-`None` modes follow the taxonomy from the AWS Architecture Blog's
/// "Exponential Backoff And Jitter": full, equal, and decorrelated.
///
/// `#[non_exhaustive]`: more modes may be added, so a `match` needs a wildcard.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Jitter {
    /// No randomization: the delay is exactly the (capped) base curve.
    None,
    /// Uniform in `[0, delay]`. Maximum spread; a retry can fire almost
    /// immediately.
    Full,
    /// Half the delay plus a uniform half: `delay/2 + rand(0, delay/2)`. Keeps a
    /// floor under each wait while still spreading.
    Equal,
    /// `min(max, rand(base, previous * 3))`. Self-correlated growth that adapts to
    /// observed waits; the strongest at breaking up a thundering herd, and the
    /// default.
    #[default]
    Decorrelated,
}

/// The base delay curve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Constant,
    Linear,
    Exponential,
}

/// A backoff policy: a base curve plus a jitter mode and a delay ceiling.
///
/// Construct with [`constant`](Self::constant), [`linear`](Self::linear), or
/// [`exponential`](Self::exponential), then tune with
/// [`with_max`](Self::with_max) and [`with_jitter`](Self::with_jitter). It is
/// usable on its own — pair it with [`Retry`](crate::Retry), or call
/// [`iter`](Self::iter) and drive your own loop.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use throttle_net::{Backoff, Jitter};
///
/// // Exponential from 50ms, doubling, capped at 5s, with full jitter.
/// let backoff = Backoff::exponential(Duration::from_millis(50), 2.0)
///     .with_max(Duration::from_secs(5))
///     .with_jitter(Jitter::Full);
///
/// let mut delays = backoff.iter();
/// let first = delays.next_delay();
/// assert!(first <= Duration::from_millis(50)); // full jitter: in [0, 50ms]
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Backoff {
    kind: Kind,
    base: Duration,
    factor: f64,
    increment: Duration,
    max: Duration,
    jitter: Jitter,
}

impl Backoff {
    /// A fixed delay on every attempt, with no jitter.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Backoff;
    ///
    /// let mut delays = Backoff::constant(Duration::from_millis(200)).iter();
    /// assert_eq!(delays.next_delay(), Duration::from_millis(200));
    /// assert_eq!(delays.next_delay(), Duration::from_millis(200));
    /// ```
    #[must_use]
    pub fn constant(delay: Duration) -> Self {
        Self {
            kind: Kind::Constant,
            base: delay,
            factor: DEFAULT_FACTOR,
            increment: Duration::ZERO,
            max: delay,
            jitter: Jitter::None,
        }
    }

    /// A delay that grows by `increment` each attempt: `initial`, `initial +
    /// increment`, `initial + 2*increment`, … capped at the maximum.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Backoff;
    ///
    /// let mut delays = Backoff::linear(Duration::from_millis(100), Duration::from_millis(100)).iter();
    /// assert_eq!(delays.next_delay(), Duration::from_millis(100));
    /// assert_eq!(delays.next_delay(), Duration::from_millis(200));
    /// assert_eq!(delays.next_delay(), Duration::from_millis(300));
    /// ```
    #[must_use]
    pub fn linear(initial: Duration, increment: Duration) -> Self {
        Self {
            kind: Kind::Linear,
            base: initial,
            factor: DEFAULT_FACTOR,
            increment,
            max: DEFAULT_MAX,
            jitter: Jitter::None,
        }
    }

    /// A delay that multiplies by `factor` each attempt: `initial`, `initial *
    /// factor`, `initial * factor^2`, … capped at the maximum.
    ///
    /// A `factor` of `2.0` doubles each time. Non-finite or sub-one factors are
    /// accepted but make the curve flat or shrinking; pair with jitter for the
    /// usual behavior.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Backoff;
    ///
    /// let mut delays = Backoff::exponential(Duration::from_millis(100), 2.0).iter();
    /// assert_eq!(delays.next_delay(), Duration::from_millis(100));
    /// assert_eq!(delays.next_delay(), Duration::from_millis(200));
    /// assert_eq!(delays.next_delay(), Duration::from_millis(400));
    /// ```
    #[must_use]
    pub fn exponential(initial: Duration, factor: f64) -> Self {
        Self {
            kind: Kind::Exponential,
            base: initial,
            factor,
            increment: Duration::ZERO,
            max: DEFAULT_MAX,
            jitter: Jitter::None,
        }
    }

    /// Returns a copy with the delay ceiling set to `max`. No delay this backoff
    /// produces will exceed it.
    #[must_use]
    pub fn with_max(mut self, max: Duration) -> Self {
        self.max = max;
        self
    }

    /// Returns a copy with the jitter mode set.
    #[must_use]
    pub fn with_jitter(mut self, jitter: Jitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// The configured delay ceiling.
    #[must_use]
    pub const fn max(&self) -> Duration {
        self.max
    }

    /// The configured jitter mode.
    #[must_use]
    pub const fn jitter(&self) -> Jitter {
        self.jitter
    }

    /// Starts a delay sequence, seeded from system entropy.
    ///
    /// Each call returns an independent [`BackoffIter`] with its own random
    /// state, so two retry loops sharing one policy still jitter independently.
    #[must_use]
    pub fn iter(&self) -> BackoffIter {
        self.iter_seeded(entropy_seed())
    }

    /// Starts a delay sequence with an explicit seed, for deterministic,
    /// reproducible tests.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::{Backoff, Jitter};
    ///
    /// let backoff = Backoff::exponential(Duration::from_millis(10), 2.0)
    ///     .with_jitter(Jitter::Full);
    /// // The same seed always yields the same sequence.
    /// let a: Vec<_> = (0..5).scan(backoff.iter_seeded(7), |it, _| Some(it.next_delay())).collect();
    /// let b: Vec<_> = (0..5).scan(backoff.iter_seeded(7), |it, _| Some(it.next_delay())).collect();
    /// assert_eq!(a, b);
    /// ```
    #[must_use]
    pub fn iter_seeded(&self, seed: u64) -> BackoffIter {
        BackoffIter {
            policy: *self,
            attempt: 0,
            previous: self.base,
            rng: Rng::new(seed),
        }
    }
}

impl Default for Backoff {
    /// Exponential from 100ms, doubling, capped at 30s, with decorrelated jitter
    /// — a sane production default that resists thundering herds.
    fn default() -> Self {
        Self::exponential(DEFAULT_BASE, DEFAULT_FACTOR)
            .with_max(DEFAULT_MAX)
            .with_jitter(Jitter::Decorrelated)
    }
}

/// A live delay sequence produced by a [`Backoff`].
///
/// Each [`next_delay`](Self::next_delay) returns the wait for the next attempt.
/// The sequence is infinite — bounding the attempt count is the retry policy's
/// job (see [`Retry`](crate::Retry)) — so it also implements [`Iterator`] for
/// convenience, always yielding `Some`.
#[derive(Debug, Clone)]
pub struct BackoffIter {
    policy: Backoff,
    attempt: u32,
    previous: Duration,
    rng: Rng,
}

impl BackoffIter {
    /// Returns the delay for the next attempt and advances the sequence.
    pub fn next_delay(&mut self) -> Duration {
        let max = dur_to_nanos(self.policy.max);
        let nanos = if let Jitter::Decorrelated = self.policy.jitter {
            self.decorrelated(max)
        } else {
            let capped = self.raw_nanos(self.attempt).min(max);
            match self.policy.jitter {
                Jitter::Full => self.rng.range(0, capped),
                Jitter::Equal => {
                    let half = capped / 2;
                    half + self.rng.range(0, capped - half)
                }
                // `None`, and the unreachable `Decorrelated` (handled above):
                // both fall through to the deterministic capped value.
                _ => capped,
            }
        };
        self.attempt = self.attempt.saturating_add(1);
        Duration::from_nanos(nanos)
    }

    /// The base-curve delay in nanoseconds for `attempt`, before jitter or cap.
    fn raw_nanos(&self, attempt: u32) -> u64 {
        let base = dur_to_nanos(self.policy.base);
        match self.policy.kind {
            Kind::Constant => base,
            Kind::Linear => {
                let inc = dur_to_nanos(self.policy.increment);
                base.saturating_add(inc.saturating_mul(u64::from(attempt)))
            }
            Kind::Exponential => {
                // Clamp the exponent so `powi` cannot overflow f64 to infinity for
                // pathological attempt counts; the result is clamped to the cap
                // by the caller anyway.
                let exp = i32::try_from(attempt.min(64)).unwrap_or(64);
                let scaled = (base as f64) * self.policy.factor.powi(exp);
                if scaled.is_finite() && scaled >= 0.0 && scaled < (u64::MAX as f64) {
                    scaled as u64
                } else {
                    u64::MAX
                }
            }
        }
    }

    /// One step of the decorrelated-jitter recurrence:
    /// `min(max, rand(base, previous * 3))`, remembering the result.
    fn decorrelated(&mut self, max: u64) -> u64 {
        let base = dur_to_nanos(self.policy.base);
        let prev = dur_to_nanos(self.previous);
        let hi = prev.saturating_mul(3).max(base).min(max);
        let lo = base.min(hi);
        let chosen = self.rng.range(lo, hi);
        self.previous = Duration::from_nanos(chosen.max(base));
        chosen
    }
}

impl Iterator for BackoffIter {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        Some(self.next_delay())
    }
}

/// Converts a duration to nanoseconds, saturating at `u64::MAX` (~584 years), so
/// all delay math can stay in integer nanoseconds.
#[inline]
fn dur_to_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// A small, fast, non-cryptographic generator (SplitMix64) for jitter.
///
/// Jitter needs uniform spread, not cryptographic randomness, so a tiny
/// well-distributed generator is the right tool — no dependency, no syscall on
/// the hot path. It is seedable for deterministic tests.
#[derive(Debug, Clone)]
struct Rng(u64);

impl Rng {
    #[inline]
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// SplitMix64: advance the state and return a well-mixed 64-bit value.
    #[inline]
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform value in the inclusive range `[lo, hi]`.
    #[inline]
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        let span = hi - lo + 1;
        // Modulo bias is immaterial for jitter spread over nanosecond spans.
        lo + (self.next_u64() % span)
    }
}

/// Derives a seed from a monotonically-increasing counter mixed with the wall
/// clock, so distinct `BackoffIter`s — even within one process and one instant —
/// start from different states.
fn entropy_seed() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);

    // Mix the two sources through the SplitMix64 finalizer for avalanche.
    let mut z = nanos ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::{Backoff, Jitter, Rng};
    use core::time::Duration;

    #[test]
    fn test_constant_is_flat() {
        let mut it = Backoff::constant(Duration::from_millis(50)).iter();
        for _ in 0..5 {
            assert_eq!(it.next_delay(), Duration::from_millis(50));
        }
    }

    #[test]
    fn test_linear_grows_by_increment_and_caps() {
        let backoff = Backoff::linear(Duration::from_millis(100), Duration::from_millis(100))
            .with_max(Duration::from_millis(250));
        let mut it = backoff.iter();
        assert_eq!(it.next_delay(), Duration::from_millis(100));
        assert_eq!(it.next_delay(), Duration::from_millis(200));
        assert_eq!(it.next_delay(), Duration::from_millis(250)); // capped
        assert_eq!(it.next_delay(), Duration::from_millis(250));
    }

    #[test]
    fn test_exponential_doubles_and_caps() {
        let backoff = Backoff::exponential(Duration::from_millis(100), 2.0)
            .with_max(Duration::from_millis(500));
        let mut it = backoff.iter();
        assert_eq!(it.next_delay(), Duration::from_millis(100));
        assert_eq!(it.next_delay(), Duration::from_millis(200));
        assert_eq!(it.next_delay(), Duration::from_millis(400));
        assert_eq!(it.next_delay(), Duration::from_millis(500)); // capped
    }

    #[test]
    fn test_full_jitter_stays_within_zero_and_cap() {
        let backoff = Backoff::exponential(Duration::from_millis(100), 2.0)
            .with_max(Duration::from_secs(10))
            .with_jitter(Jitter::Full);
        let mut it = backoff.iter_seeded(1);
        for attempt in 0..6u32 {
            let ceiling =
                Duration::from_millis(100 * 2u64.pow(attempt)).min(Duration::from_secs(10));
            let d = it.next_delay();
            assert!(d <= ceiling, "{d:?} exceeded {ceiling:?}");
        }
    }

    #[test]
    fn test_equal_jitter_keeps_a_floor() {
        let backoff = Backoff::constant(Duration::from_millis(1000)).with_jitter(Jitter::Equal);
        let mut it = backoff.iter_seeded(42);
        for _ in 0..20 {
            let d = it.next_delay();
            // Equal jitter: at least half the base, at most the base.
            assert!(d >= Duration::from_millis(500), "{d:?} below the floor");
            assert!(d <= Duration::from_millis(1000), "{d:?} above the cap");
        }
    }

    #[test]
    fn test_decorrelated_respects_base_and_cap() {
        let backoff = Backoff::exponential(Duration::from_millis(100), 2.0)
            .with_max(Duration::from_secs(2))
            .with_jitter(Jitter::Decorrelated);
        let mut it = backoff.iter_seeded(99);
        for _ in 0..50 {
            let d = it.next_delay();
            assert!(d >= Duration::from_millis(100), "{d:?} below base");
            assert!(d <= Duration::from_secs(2), "{d:?} above cap");
        }
    }

    #[test]
    fn test_seeded_sequences_are_reproducible() {
        let backoff = Backoff::default();
        let a: Vec<_> = {
            let mut it = backoff.iter_seeded(123);
            (0..8).map(|_| it.next_delay()).collect()
        };
        let b: Vec<_> = {
            let mut it = backoff.iter_seeded(123);
            (0..8).map(|_| it.next_delay()).collect()
        };
        assert_eq!(a, b);
    }

    #[test]
    fn test_default_is_decorrelated_exponential() {
        let backoff = Backoff::default();
        assert_eq!(backoff.jitter(), Jitter::Decorrelated);
        assert_eq!(backoff.max(), Duration::from_secs(30));
    }

    #[test]
    fn test_rng_range_is_within_bounds_and_handles_degenerate() {
        let mut rng = Rng::new(7);
        for _ in 0..1000 {
            let v = rng.range(10, 20);
            assert!((10..=20).contains(&v));
        }
        assert_eq!(rng.range(5, 5), 5);
        assert_eq!(rng.range(9, 4), 9); // hi <= lo returns lo
    }
}
