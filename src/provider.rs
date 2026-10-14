//! Parse rate-limit response headers and reconcile a limiter with the server.
//!
//! Downstreams advertise their own view of your remaining budget in response
//! headers — and every major provider spells it differently. [`HeaderProfile`]
//! captures one provider's convention; a set of ready-made profiles
//! ([`HeaderProfile::OPENAI`], [`HeaderProfile::GITHUB`], …) covers the common
//! ones, and [`parse`](HeaderProfile::parse) turns a header set into a normalized
//! [`RateLimitInfo`].
//!
//! Parsing is defensive: unrecognized or malformed values are dropped, never a
//! panic. Reconciliation then brings a local limiter in line with the server:
//!
//! - [`Window::apply`] drains any [`Limiter`] to the server's reported
//!   remaining count and, when the server reports the window **exhausted**,
//!   pauses the limiter until the reported reset;
//! - [`RateLimitInfo::sync_requests`] / [`sync_tokens`](RateLimitInfo::sync_tokens)
//!   do that for one window and also honour a `Retry-After` as a pause;
//! - [`RateLimitInfo::sync_multi`] reconciles a whole [`MultiLimiter`] — such as
//!   an LLM tier preset — in one call.
//!
//! Reconciliation only ever *reduces* the local budget, so it can never raise a
//! limiter above its configured capacity.
//!
//! # Server-driven pauses are capped
//!
//! A reset or `Retry-After` comes from the server, so it is untrusted input: a
//! malformed or hostile `Retry-After: 999999999` would otherwise pause a client
//! for decades. Every pause reconciliation sets is clamped to
//! [`DEFAULT_MAX_PAUSE`] (five minutes, the same ceiling
//! [`Retry`](crate::Retry) puts on a `Retry-After`); the `*_capped` variants take
//! a ceiling of your choosing.

use core::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::limiter::Limiter;
use crate::multi::MultiLimiter;
use crate::retry_after::parse_retry_after_at;
use crate::timeutil::civil_to_unix;

/// The longest pause reconciliation sets from a server's reset or
/// `Retry-After`: five minutes. A longer server value is clamped to it.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use throttle_net::Throttle;
/// use throttle_net::provider::{DEFAULT_MAX_PAUSE, HeaderProfile};
///
/// let throttle = Throttle::per_second(10);
/// let info = HeaderProfile::STRIPE.parse(&[("Retry-After", "999999999")]);
/// info.sync_requests(&throttle);
/// assert!(throttle.paused_for() <= DEFAULT_MAX_PAUSE);
/// assert_eq!(DEFAULT_MAX_PAUSE, Duration::from_secs(300));
/// ```
pub const DEFAULT_MAX_PAUSE: Duration = Duration::from_secs(300);

/// One metered dimension's reported window: its ceiling, what is left, and how
/// long until it refills. Any field may be absent if the server did not send it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Window {
    /// The dimension's ceiling for the window.
    pub limit: Option<u64>,
    /// Units remaining in the current window.
    pub remaining: Option<u64>,
    /// Time until the window resets.
    pub reset: Option<Duration>,
}

impl Window {
    /// Reconciles `limiter` with this window: takes units until at most
    /// `remaining` are available locally and, if the server reports nothing
    /// remaining and a reset time, pauses the limiter until that reset (at most
    /// [`DEFAULT_MAX_PAUSE`]). Returns the units drained.
    ///
    /// Never adds units. A `remaining` larger than the limiter can hold, or a
    /// window without `remaining`, drains nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use throttle_net::{ManualClock, Throttle};
    /// use throttle_net::provider::Window;
    ///
    /// let throttle = Throttle::per_minute(100).with_clock(Arc::new(ManualClock::new()));
    /// let exhausted = Window {
    ///     limit: Some(100),
    ///     remaining: Some(0),
    ///     reset: Some(Duration::from_secs(20)),
    /// };
    /// assert_eq!(exhausted.apply(&throttle), 100);
    /// assert_eq!(throttle.paused_for(), Duration::from_secs(20)); // held until the reset
    /// ```
    pub fn apply<L: Limiter + ?Sized>(&self, limiter: &L) -> u32 {
        self.apply_capped(limiter, DEFAULT_MAX_PAUSE)
    }

    /// [`apply`](Self::apply) with a pause of at most `max_pause` (a longer
    /// reset is clamped to it; `Duration::ZERO` never pauses).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Throttle;
    /// use throttle_net::provider::Window;
    ///
    /// let throttle = Throttle::per_minute(100);
    /// let window = Window {
    ///     remaining: Some(0),
    ///     reset: Some(Duration::from_secs(3600)),
    ///     ..Window::default()
    /// };
    /// window.apply_capped(&throttle, Duration::from_secs(60));
    /// assert!(throttle.paused_for() <= Duration::from_secs(60));
    /// ```
    pub fn apply_capped<L: Limiter + ?Sized>(&self, limiter: &L, max_pause: Duration) -> u32 {
        let Some(remaining) = self.remaining else {
            return 0;
        };
        let drained = drain(limiter, remaining);
        if remaining == 0 {
            if let Some(reset) = self.reset {
                pause(limiter, reset, max_pause);
            }
        }
        drained
    }
}

/// Pauses `limiter` for the server's `duration`, clamped to `max_pause`.
fn pause<L: Limiter + ?Sized>(limiter: &L, duration: Duration, max_pause: Duration) {
    let duration = duration.min(max_pause);
    if !duration.is_zero() {
        limiter.pause_for(duration);
    }
}

/// Drains `limiter` to `remaining`. A remaining count beyond `u32` exceeds every
/// limiter's capacity, so there is nothing to drain.
fn drain<L: Limiter + ?Sized>(limiter: &L, remaining: u64) -> u32 {
    u32::try_from(remaining).map_or(0, |remaining| crate::limiter::drain_to(limiter, remaining))
}

/// A normalized view of the rate-limit headers on a response.
///
/// Providers that meter requests and tokens separately (the LLM APIs) populate
/// both [`requests`](Self::requests) and [`tokens`](Self::tokens); single-limit
/// providers populate only `requests`. [`retry_after`](Self::retry_after) carries
/// a `Retry-After` when present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RateLimitInfo {
    /// The request-count window, if the response carried one.
    pub requests: Option<Window>,
    /// The token window, if the response carried one (LLM providers).
    pub tokens: Option<Window>,
    /// The `Retry-After` delay, if present.
    pub retry_after: Option<Duration>,
}

impl RateLimitInfo {
    /// Reconciles `limiter` with the server's request window and `Retry-After`:
    /// drains it to the reported requests-remaining, pauses it until the reset
    /// when that is zero (see [`Window::apply`]), and pauses it for the
    /// `Retry-After`, if any. Every pause is clamped to [`DEFAULT_MAX_PAUSE`];
    /// use [`sync_requests_capped`](Self::sync_requests_capped) to choose the
    /// ceiling. Returns the units drained.
    ///
    /// This only ever *reduces* the local budget — it never adds units — so it
    /// corrects client/server drift without ever raising the limiter above its
    /// configured capacity. Any [`Limiter`] works: a
    /// [`Throttle`](crate::Throttle), a [`Hybrid`](crate::Hybrid), one dimension
    /// of a [`MultiLimiter`].
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Throttle;
    /// use throttle_net::provider::{RateLimitInfo, Window};
    ///
    /// let throttle = Throttle::per_second(100); // locally believes 100 are free
    /// let info = RateLimitInfo {
    ///     requests: Some(Window { remaining: Some(10), ..Window::default() }),
    ///     ..RateLimitInfo::default()
    /// };
    /// let drained = info.sync_requests(&throttle);
    /// assert_eq!(drained, 90);
    /// assert_eq!(throttle.available(), 10); // now matches the server
    /// ```
    pub fn sync_requests<L: Limiter + ?Sized>(&self, limiter: &L) -> u32 {
        self.sync_window(limiter, self.requests, DEFAULT_MAX_PAUSE)
    }

    /// [`sync_requests`](Self::sync_requests) with every pause clamped to
    /// `max_pause` (`Duration::ZERO`: drain only, never pause).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Throttle;
    /// use throttle_net::provider::HeaderProfile;
    ///
    /// let throttle = Throttle::per_second(10);
    /// let info = HeaderProfile::STRIPE.parse(&[("Retry-After", "120")]);
    /// info.sync_requests_capped(&throttle, Duration::from_secs(30));
    /// assert!(throttle.paused_for() <= Duration::from_secs(30));
    /// ```
    pub fn sync_requests_capped<L: Limiter + ?Sized>(
        &self,
        limiter: &L,
        max_pause: Duration,
    ) -> u32 {
        self.sync_window(limiter, self.requests, max_pause)
    }

    /// Reconciles `limiter` with the server's token window (and `Retry-After`),
    /// the same way as [`sync_requests`](Self::sync_requests). Returns the units
    /// drained.
    pub fn sync_tokens<L: Limiter + ?Sized>(&self, limiter: &L) -> u32 {
        self.sync_window(limiter, self.tokens, DEFAULT_MAX_PAUSE)
    }

    /// [`sync_tokens`](Self::sync_tokens) with every pause clamped to
    /// `max_pause`.
    pub fn sync_tokens_capped<L: Limiter + ?Sized>(&self, limiter: &L, max_pause: Duration) -> u32 {
        self.sync_window(limiter, self.tokens, max_pause)
    }

    fn sync_window<L: Limiter + ?Sized>(
        &self,
        limiter: &L,
        window: Option<Window>,
        max_pause: Duration,
    ) -> u32 {
        let drained = window.map_or(0, |window| window.apply_capped(limiter, max_pause));
        if let Some(after) = self.retry_after {
            pause(limiter, after, max_pause);
        }
        drained
    }

    /// Reconciles a whole [`MultiLimiter`] — such as an LLM tier
    /// [preset](crate::presets) — with this response, and returns the units
    /// drained across its dimensions.
    ///
    /// The request window applies to the dimension named `requests`. The token
    /// window applies to `tokens` when the limiter has it (directly or as an
    /// alias); otherwise to each of `input_tokens` and `output_tokens` it has —
    /// a conservative reading, since a provider's single token count binds
    /// whichever budget it reports on. A `Retry-After` pauses every dimension.
    /// Dimensions the limiter lacks are skipped. For other names, apply
    /// [`Window::apply`] to [`MultiLimiter::dimension`] directly.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{MultiLimiter, Throttle};
    /// use throttle_net::provider::HeaderProfile;
    ///
    /// let limiter = MultiLimiter::builder()
    ///     .dimension("requests", Throttle::per_minute(60))
    ///     .dimension("tokens", Throttle::per_minute(10_000))
    ///     .build();
    /// let headers = [
    ///     ("x-ratelimit-remaining-requests", "12"),
    ///     ("x-ratelimit-remaining-tokens", "4000"),
    /// ];
    /// let info = HeaderProfile::OPENAI.parse(&headers);
    /// assert_eq!(info.sync_multi(&limiter), (60 - 12) + (10_000 - 4000));
    /// assert_eq!(limiter.available("requests"), Some(12));
    /// assert_eq!(limiter.available("tokens"), Some(4000));
    /// ```
    // Callers usually reconcile for the side effect; the count is informative.
    #[allow(clippy::must_use_candidate)]
    pub fn sync_multi(&self, limiter: &MultiLimiter) -> u32 {
        self.sync_multi_capped(limiter, DEFAULT_MAX_PAUSE)
    }

    /// [`sync_multi`](Self::sync_multi) with every pause clamped to
    /// `max_pause`.
    // Callers usually reconcile for the side effect; the count is informative.
    #[allow(clippy::must_use_candidate)]
    pub fn sync_multi_capped(&self, limiter: &MultiLimiter, max_pause: Duration) -> u32 {
        let mut drained = 0u32;
        if let (Some(window), Some(dimension)) = (self.requests, limiter.dimension("requests")) {
            drained = drained.saturating_add(window.apply_capped(dimension, max_pause));
        }
        if let Some(window) = self.tokens {
            if let Some(dimension) = limiter.dimension("tokens") {
                drained = drained.saturating_add(window.apply_capped(dimension, max_pause));
            } else {
                for name in ["input_tokens", "output_tokens"] {
                    if let Some(dimension) = limiter.dimension(name) {
                        drained = drained.saturating_add(window.apply_capped(dimension, max_pause));
                    }
                }
            }
        }
        if let Some(after) = self.retry_after {
            let after = after.min(max_pause);
            if !after.is_zero() {
                limiter.pause_for(after);
            }
        }
        drained
    }
}

/// How a provider encodes the "reset" value of a window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResetFormat {
    /// Whole seconds until the window resets (the IETF `RateLimit` draft).
    DeltaSeconds,
    /// A duration string such as `1s`, `6m0s`, or `100ms` (OpenAI).
    DurationString,
    /// An absolute Unix timestamp in seconds (GitHub).
    UnixSeconds,
    /// An absolute RFC 3339 / ISO 8601 instant (Anthropic).
    Rfc3339,
}

/// The limit/remaining/reset header names for one dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Triple {
    limit: &'static str,
    remaining: &'static str,
    reset: &'static str,
}

/// One provider's header convention.
///
/// Use a built-in profile ([`HeaderProfile::OPENAI`] and friends) and call
/// [`parse`](Self::parse).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderProfile {
    requests: Option<Triple>,
    tokens: Option<Triple>,
    retry_after: Option<&'static str>,
    reset: ResetFormat,
}

impl HeaderProfile {
    /// OpenAI: `x-ratelimit-{limit,remaining,reset}-{requests,tokens}`, reset as a
    /// duration string (`1s`, `6m0s`, `1.5s`, `20ms`), plus `retry-after`.
    pub const OPENAI: Self = Self {
        requests: Some(Triple {
            limit: "x-ratelimit-limit-requests",
            remaining: "x-ratelimit-remaining-requests",
            reset: "x-ratelimit-reset-requests",
        }),
        tokens: Some(Triple {
            limit: "x-ratelimit-limit-tokens",
            remaining: "x-ratelimit-remaining-tokens",
            reset: "x-ratelimit-reset-tokens",
        }),
        retry_after: Some("retry-after"),
        reset: ResetFormat::DurationString,
    };

    /// Anthropic: `anthropic-ratelimit-{requests,tokens}-{limit,remaining,reset}`,
    /// reset as an RFC 3339 instant, plus `retry-after`. The `tokens` headers
    /// report the most restrictive token limit currently in force.
    pub const ANTHROPIC: Self = Self {
        requests: Some(Triple {
            limit: "anthropic-ratelimit-requests-limit",
            remaining: "anthropic-ratelimit-requests-remaining",
            reset: "anthropic-ratelimit-requests-reset",
        }),
        tokens: Some(Triple {
            limit: "anthropic-ratelimit-tokens-limit",
            remaining: "anthropic-ratelimit-tokens-remaining",
            reset: "anthropic-ratelimit-tokens-reset",
        }),
        retry_after: Some("retry-after"),
        reset: ResetFormat::Rfc3339,
    };

    /// GitHub: `x-ratelimit-{limit,remaining,reset}`, reset as an absolute Unix
    /// timestamp, plus `retry-after`.
    pub const GITHUB: Self = Self {
        requests: Some(Triple {
            limit: "x-ratelimit-limit",
            remaining: "x-ratelimit-remaining",
            reset: "x-ratelimit-reset",
        }),
        tokens: None,
        retry_after: Some("retry-after"),
        reset: ResetFormat::UnixSeconds,
    };

    /// The IETF `RateLimit` header draft: `RateLimit-{Limit,Remaining,Reset}`,
    /// reset as delta-seconds, plus `Retry-After`. A reasonable default for
    /// standards-compliant or unknown APIs.
    pub const RFC: Self = Self {
        requests: Some(Triple {
            limit: "ratelimit-limit",
            remaining: "ratelimit-remaining",
            reset: "ratelimit-reset",
        }),
        tokens: None,
        retry_after: Some("retry-after"),
        reset: ResetFormat::DeltaSeconds,
    };

    /// Stripe: no standard rate-limit headers; it signals back-off with
    /// `Retry-After` on a 429.
    pub const STRIPE: Self = Self {
        requests: None,
        tokens: None,
        retry_after: Some("retry-after"),
        reset: ResetFormat::DeltaSeconds,
    };

    /// AWS: like Stripe, back-off is signalled with `Retry-After`.
    pub const AWS: Self = Self {
        requests: None,
        tokens: None,
        retry_after: Some("retry-after"),
        reset: ResetFormat::DeltaSeconds,
    };

    /// Parses `headers` into a [`RateLimitInfo`], using the system clock to
    /// resolve absolute reset timestamps.
    ///
    /// `headers` is a slice of `(name, value)` pairs; lookups are
    /// case-insensitive.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::provider::HeaderProfile;
    ///
    /// let headers = [
    ///     ("x-ratelimit-limit-requests", "100"),
    ///     ("x-ratelimit-remaining-requests", "42"),
    ///     ("x-ratelimit-reset-requests", "1s"),
    /// ];
    /// let info = HeaderProfile::OPENAI.parse(&headers);
    /// let requests = info.requests.unwrap();
    /// assert_eq!(requests.limit, Some(100));
    /// assert_eq!(requests.remaining, Some(42));
    /// ```
    #[must_use]
    pub fn parse(&self, headers: &[(&str, &str)]) -> RateLimitInfo {
        self.parse_at(headers, current_unix_secs())
    }

    /// Parses `headers` relative to an explicit current time (Unix seconds), for
    /// deterministic tests and absolute-timestamp providers.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::provider::HeaderProfile;
    ///
    /// let headers = [("X-RateLimit-Remaining", "0"), ("X-RateLimit-Reset", "1767225660")];
    /// let info = HeaderProfile::GITHUB.parse_at(&headers, 1_767_225_600);
    /// assert_eq!(info.requests.unwrap().reset, Some(Duration::from_secs(60)));
    /// ```
    #[must_use]
    pub fn parse_at(&self, headers: &[(&str, &str)], now_unix_secs: i64) -> RateLimitInfo {
        RateLimitInfo {
            requests: self
                .requests
                .and_then(|t| self.window(headers, &t, now_unix_secs)),
            tokens: self
                .tokens
                .and_then(|t| self.window(headers, &t, now_unix_secs)),
            retry_after: self
                .retry_after
                .and_then(|name| header(headers, name))
                .and_then(|value| parse_retry_after_at(value, now_unix_secs)),
        }
    }

    /// Builds a [`Window`] from a triple, or `None` if none of its headers are
    /// present.
    fn window(&self, headers: &[(&str, &str)], triple: &Triple, now: i64) -> Option<Window> {
        let limit = header(headers, triple.limit).and_then(parse_u64);
        let remaining = header(headers, triple.remaining).and_then(parse_u64);
        let reset = header(headers, triple.reset).and_then(|v| self.parse_reset(v, now));
        if limit.is_none() && remaining.is_none() && reset.is_none() {
            return None;
        }
        Some(Window {
            limit,
            remaining,
            reset,
        })
    }

    /// Parses a reset value into a time-until-reset, per this profile's format.
    fn parse_reset(&self, value: &str, now: i64) -> Option<Duration> {
        match self.reset {
            ResetFormat::DeltaSeconds => value.trim().parse::<u64>().ok().map(Duration::from_secs),
            ResetFormat::DurationString => parse_duration_string(value.trim()),
            ResetFormat::UnixSeconds => value
                .trim()
                .parse::<i64>()
                .ok()
                // Saturating: an extreme reset timestamp or `now` must not overflow
                // the subtraction (a past reset already clamps to zero below).
                .map(|at| Duration::from_secs(u64::try_from(at.saturating_sub(now)).unwrap_or(0))),
            ResetFormat::Rfc3339 => parse_rfc3339(value.trim())
                .map(|at| Duration::from_secs(u64::try_from(at.saturating_sub(now)).unwrap_or(0))),
        }
    }
}

/// Case-insensitive header lookup.
fn header<'a>(headers: &'a [(&str, &str)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| *v)
}

/// Parses a trimmed non-negative integer.
fn parse_u64(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

/// Parses a Go-style duration string (`1s`, `6m0s`, `100ms`, `1h2m3s`, with
/// optional decimal fractions such as `1.5s` or `6m0.5s`, and the `ns`, `us`,
/// `µs` units), or a bare non-negative integer as seconds, into a [`Duration`].
/// Saturates rather than overflowing.
fn parse_duration_string(value: &str) -> Option<Duration> {
    if value.is_empty() {
        return None;
    }
    // A bare number is treated as seconds.
    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }

    let mut rest = value;
    let mut total_nanos = 0u128;
    while !rest.is_empty() {
        // The number: digits, optionally a '.' and more digits.
        let number_len = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(rest.len());
        let (number, tail) = rest.split_at(number_len);
        let unit_len = tail
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(tail.len());
        let (unit, tail) = tail.split_at(unit_len);
        let unit_nanos: u128 = match unit {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return None,
        };
        total_nanos = total_nanos.saturating_add(scaled(number, unit_nanos)?);
        rest = tail;
    }
    Some(Duration::from_nanos(
        u64::try_from(total_nanos).unwrap_or(u64::MAX),
    ))
}

/// `number` (decimal, with an optional fraction) times `unit_nanos`, in
/// nanoseconds, truncated and saturating. `None` if `number` is not a decimal.
fn scaled(number: &str, unit_nanos: u128) -> Option<u128> {
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole.bytes().all(|b| b.is_ascii_digit()) || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let whole_value = if whole.is_empty() {
        0
    } else {
        // Saturate absurdly long digit strings instead of rejecting them.
        whole.parse::<u128>().unwrap_or(u128::MAX)
    };
    let mut nanos = whole_value.saturating_mul(unit_nanos);
    // Each fractional digit is worth a tenth of the previous; digits beyond the
    // unit's nanosecond resolution contribute nothing.
    let mut place = unit_nanos;
    for digit in fraction.bytes().map(|b| u128::from(b - b'0')) {
        place /= 10;
        if place == 0 {
            break;
        }
        nanos = nanos.saturating_add(digit * place);
    }
    Some(nanos)
}

/// Parses an RFC 3339 / ISO 8601 instant into Unix seconds: `Z` or a numeric
/// offset (`+02:00`, `-0530`), with an optional fractional second (dropped).
fn parse_rfc3339(value: &str) -> Option<i64> {
    let (date, rest) = value.split_once(['T', 't'])?;
    let (time, offset_secs) = split_offset(rest)?;
    // Drop any fractional-second part.
    let time = time.split('.').next()?;

    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i64>().ok()?;
    let month = date_parts.next()?.parse::<u32>().ok()?;
    let day = date_parts.next()?.parse::<u32>().ok()?;
    if date_parts.next().is_some() {
        return None;
    }

    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<u32>().ok()?;
    let minute = time_parts.next()?.parse::<u32>().ok()?;
    let second = time_parts.next()?.parse::<u32>().ok()?;
    if time_parts.next().is_some() {
        return None;
    }

    civil_to_unix(year, month, day, hour, minute, second)?.checked_sub(offset_secs)
}

/// Splits the zone off an RFC 3339 time, returning the time and the zone's
/// offset east of UTC in seconds.
fn split_offset(rest: &str) -> Option<(&str, i64)> {
    if let Some(time) = rest.strip_suffix(['Z', 'z']) {
        return Some((time, 0));
    }
    let sign_at = rest.rfind(['+', '-'])?;
    let (time, zone) = rest.split_at(sign_at);
    let sign: i64 = if zone.starts_with('-') { -1 } else { 1 };
    let digits: String = zone[1..].chars().filter(|c| *c != ':').collect();
    if digits.len() != 4 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let hours: i64 = digits[..2].parse().ok()?;
    let minutes: i64 = digits[2..].parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some((time, sign * (hours * 3600 + minutes * 60)))
}

/// Current time as Unix seconds, saturating.
fn current_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::{HeaderProfile, RateLimitInfo, Window, parse_duration_string, parse_rfc3339};
    use crate::multi::MultiLimiter;
    use crate::throttle::Throttle;
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    #[test]
    fn test_openai_recorded_headers() {
        // A representative OpenAI response header set.
        let headers = [
            ("x-ratelimit-limit-requests", "5000"),
            ("x-ratelimit-remaining-requests", "4999"),
            ("x-ratelimit-reset-requests", "12ms"),
            ("x-ratelimit-limit-tokens", "160000"),
            ("x-ratelimit-remaining-tokens", "159952"),
            ("x-ratelimit-reset-tokens", "6m0s"),
        ];
        let info = HeaderProfile::OPENAI.parse_at(&headers, 0);
        let req = info.requests.unwrap();
        assert_eq!(req.limit, Some(5000));
        assert_eq!(req.remaining, Some(4999));
        assert_eq!(req.reset, Some(Duration::from_millis(12)));
        let tok = info.tokens.unwrap();
        assert_eq!(tok.limit, Some(160_000));
        assert_eq!(tok.remaining, Some(159_952));
        assert_eq!(tok.reset, Some(Duration::from_secs(360)));
    }

    #[test]
    fn test_anthropic_recorded_headers_rfc3339_reset() {
        // Anthropic reports an absolute RFC 3339 reset instant.
        let headers = [
            ("anthropic-ratelimit-requests-limit", "50"),
            ("anthropic-ratelimit-requests-remaining", "49"),
            ("anthropic-ratelimit-requests-reset", "2026-01-01T00:01:00Z"),
            ("anthropic-ratelimit-tokens-limit", "40000"),
            ("anthropic-ratelimit-tokens-remaining", "39000"),
            ("anthropic-ratelimit-tokens-reset", "2026-01-01T00:01:00Z"),
        ];
        // now = 2026-01-01T00:00:00Z = 1_767_225_600; reset is 60s later.
        let info = HeaderProfile::ANTHROPIC.parse_at(&headers, 1_767_225_600);
        assert_eq!(info.requests.unwrap().remaining, Some(49));
        assert_eq!(info.requests.unwrap().reset, Some(Duration::from_secs(60)));
        assert_eq!(info.tokens.unwrap().remaining, Some(39000));
    }

    #[test]
    fn test_github_recorded_headers_unix_reset() {
        let headers = [
            ("X-RateLimit-Limit", "60"),
            ("X-RateLimit-Remaining", "57"),
            ("X-RateLimit-Reset", "1767225660"), // 2026-01-01T00:01:00Z
            ("X-RateLimit-Used", "3"),
        ];
        // Case-insensitive header names; now 60s before the reset.
        let info = HeaderProfile::GITHUB.parse_at(&headers, 1_767_225_600);
        let req = info.requests.unwrap();
        assert_eq!(req.limit, Some(60));
        assert_eq!(req.remaining, Some(57));
        assert_eq!(req.reset, Some(Duration::from_secs(60)));
        assert!(info.tokens.is_none());
    }

    #[test]
    fn test_rfc_draft_delta_seconds() {
        let headers = [
            ("RateLimit-Limit", "100"),
            ("RateLimit-Remaining", "0"),
            ("RateLimit-Reset", "30"),
            ("Retry-After", "30"),
        ];
        let info = HeaderProfile::RFC.parse_at(&headers, 0);
        let req = info.requests.unwrap();
        assert_eq!(req.remaining, Some(0));
        assert_eq!(req.reset, Some(Duration::from_secs(30)));
        assert_eq!(info.retry_after, Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_stripe_retry_after_only() {
        let headers = [("Retry-After", "5")];
        let info = HeaderProfile::STRIPE.parse_at(&headers, 0);
        assert!(info.requests.is_none());
        assert_eq!(info.retry_after, Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_missing_headers_yield_none() {
        let info = HeaderProfile::OPENAI.parse_at(&[], 0);
        assert_eq!(info, RateLimitInfo::default());
    }

    #[test]
    fn test_extreme_reset_and_now_do_not_overflow() {
        // A reset timestamp and `now` at the i64 extremes must not overflow the
        // `at - now` subtraction (regression: the fuzz-smoke suite caught a panic
        // here). A past or unreachable reset clamps the time-until-reset to zero.
        let headers = [("x-ratelimit-reset", "9223372036854775807")]; // i64::MAX
        let info = HeaderProfile::GITHUB.parse_at(&headers, i64::MIN);
        assert!(
            info.requests.is_some(),
            "an extreme reset still parses a window"
        );

        let past = [("x-ratelimit-reset", "-9223372036854775808")]; // i64::MIN
        let info = HeaderProfile::GITHUB.parse_at(&past, i64::MAX);
        assert_eq!(info.requests.unwrap().reset, Some(Duration::ZERO));
    }

    #[test]
    fn test_malformed_values_are_dropped() {
        let headers = [
            ("x-ratelimit-limit-requests", "lots"),
            ("x-ratelimit-remaining-requests", "42"),
            ("x-ratelimit-reset-requests", "soon"),
        ];
        let info = HeaderProfile::OPENAI.parse_at(&headers, 0);
        let req = info.requests.unwrap();
        assert_eq!(req.limit, None); // "lots" dropped
        assert_eq!(req.remaining, Some(42));
        assert_eq!(req.reset, None); // "soon" dropped
    }

    #[test]
    fn test_duration_string_parsing() {
        assert_eq!(parse_duration_string("1s"), Some(Duration::from_secs(1)));
        assert_eq!(
            parse_duration_string("6m0s"),
            Some(Duration::from_secs(360))
        );
        assert_eq!(
            parse_duration_string("100ms"),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            parse_duration_string("1h2m3s"),
            Some(Duration::from_secs(3723))
        );
        assert_eq!(parse_duration_string("30"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration_string("nope"), None);
    }

    #[test]
    fn test_fractional_duration_strings() {
        // Regression (TN-018): fractional values dropped the whole reset.
        assert_eq!(
            parse_duration_string("1.5s"),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(
            parse_duration_string("6m0.5s"),
            Some(Duration::from_millis(360_500))
        );
        assert_eq!(
            parse_duration_string("0.25ms"),
            Some(Duration::from_micros(250))
        );
        assert_eq!(
            parse_duration_string(".5s"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_duration_string("2.s"), Some(Duration::from_secs(2)));
        assert_eq!(
            parse_duration_string("15us"),
            Some(Duration::from_micros(15))
        );
        assert_eq!(
            parse_duration_string("15\u{b5}s"),
            Some(Duration::from_micros(15))
        );
        assert_eq!(parse_duration_string("7ns"), Some(Duration::from_nanos(7)));
        assert_eq!(
            parse_duration_string("1.123456789123s"),
            Some(Duration::from_nanos(1_123_456_789))
        );
        assert_eq!(parse_duration_string("."), None);
        assert_eq!(parse_duration_string("1.2.3s"), None);
        assert_eq!(parse_duration_string("5"), Some(Duration::from_secs(5)));
        assert_eq!(parse_duration_string("5x"), None);
        assert_eq!(parse_duration_string("s"), None);
        // Absurd values saturate instead of overflowing.
        assert_eq!(
            parse_duration_string("99999999999999999999999999999999999999999h"),
            Some(Duration::from_nanos(u64::MAX))
        );
    }

    #[test]
    fn test_rfc3339_parsing() {
        assert_eq!(parse_rfc3339("2026-01-01T00:00:00Z"), Some(1_767_225_600));
        // Fractional seconds are tolerated.
        assert_eq!(
            parse_rfc3339("2026-01-01T00:00:00.123Z"),
            Some(1_767_225_600)
        );
        // Numeric offsets are applied: 02:00 at +02:00 is midnight UTC.
        assert_eq!(
            parse_rfc3339("2026-01-01T02:00:00+02:00"),
            Some(1_767_225_600)
        );
        assert_eq!(
            parse_rfc3339("2025-12-31T18:30:00-0530"),
            Some(1_767_225_600)
        );
        assert_eq!(parse_rfc3339("2026-01-01t00:00:00.5z"), Some(1_767_225_600));
        assert_eq!(parse_rfc3339("2026-01-01T00:00:00+2:00"), None);
        assert_eq!(parse_rfc3339("2026-01-01T00:00:00+24:00"), None);
        assert_eq!(
            parse_rfc3339("2026-01-01T00:00:00"),
            None,
            "a zone is required"
        );
        assert_eq!(parse_rfc3339("garbage"), None);
    }

    #[test]
    fn test_sync_drains_to_server_remaining() {
        let throttle = Throttle::per_second(100); // locally 100 available
        let info = RateLimitInfo {
            requests: Some(Window {
                remaining: Some(10),
                ..Window::default()
            }),
            ..RateLimitInfo::default()
        };
        let drained = info.sync_requests(&throttle);
        assert_eq!(drained, 90);
        assert_eq!(throttle.available(), 10);
    }

    #[test]
    fn test_sync_never_raises_above_hard_limit() {
        let throttle = Throttle::per_second(100);
        assert!(throttle.try_acquire_with_cost(95)); // local available now 5
        // Server claims 50 remaining — more than we have. Sync must NOT add.
        let info = RateLimitInfo {
            requests: Some(Window {
                remaining: Some(50),
                ..Window::default()
            }),
            ..RateLimitInfo::default()
        };
        let drained = info.sync_requests(&throttle);
        assert_eq!(drained, 0);
        assert_eq!(throttle.available(), 5); // unchanged; never exceeds capacity
        assert!(throttle.available() <= throttle.capacity());
    }

    #[test]
    fn test_exhausted_window_pauses_until_reset() {
        // Regression (TN-017): reset times were parsed but never used, so a
        // server-reported exhaustion did not hold the local limiter.
        let clock = Arc::new(ManualClock::new());
        let throttle = Throttle::per_minute(60).with_clock(clock.clone());
        let info = RateLimitInfo {
            requests: Some(Window {
                limit: Some(60),
                remaining: Some(0),
                reset: Some(Duration::from_secs(30)),
            }),
            ..RateLimitInfo::default()
        };
        assert_eq!(info.sync_requests(&throttle), 60);
        clock.advance(Duration::from_secs(29));
        assert!(!throttle.try_acquire(), "held until the server's reset");
        clock.advance(Duration::from_secs(1));
        assert!(throttle.try_acquire());
    }

    #[test]
    fn test_retry_after_pauses_and_large_remaining_is_ignored() {
        let throttle = Throttle::per_second(10);
        let info = RateLimitInfo {
            tokens: Some(Window {
                remaining: Some(u64::from(u32::MAX) + 5),
                ..Window::default()
            }),
            retry_after: Some(Duration::from_secs(5)),
            ..RateLimitInfo::default()
        };
        assert_eq!(info.sync_tokens(&throttle), 0);
        let paused = throttle.paused_for();
        assert!(paused > Duration::from_secs(4) && paused <= Duration::from_secs(5));
        assert_eq!(Window::default().apply(&throttle), 0);
    }

    #[test]
    fn test_sync_multi_maps_windows_onto_preset_dimensions() {
        let separate = MultiLimiter::builder()
            .dimension("requests", Throttle::per_minute(50))
            .dimension("input_tokens", Throttle::per_minute(1000))
            .dimension("output_tokens", Throttle::per_minute(400))
            .build();
        let info = RateLimitInfo {
            requests: Some(Window {
                remaining: Some(10),
                ..Window::default()
            }),
            tokens: Some(Window {
                remaining: Some(300),
                ..Window::default()
            }),
            retry_after: None,
        };
        assert_eq!(info.sync_multi(&separate), 40 + 700 + 100);
        assert_eq!(separate.available("input_tokens"), Some(300));
        assert_eq!(separate.available("output_tokens"), Some(300));

        let combined = MultiLimiter::builder()
            .dimension("tokens", Throttle::per_minute(1000))
            .alias("input_tokens", "tokens")
            .build();
        assert_eq!(
            info.sync_multi(&combined),
            700,
            "no requests dimension: skipped"
        );

        let paused = RateLimitInfo {
            retry_after: Some(Duration::from_secs(9)),
            ..RateLimitInfo::default()
        };
        assert_eq!(paused.sync_multi(&combined), 0);
        assert!(!combined.try_acquire_costs(&[("tokens", 1)]));
    }

    #[test]
    fn test_sync_with_no_info_is_a_noop() {
        let throttle = Throttle::per_second(10);
        assert_eq!(RateLimitInfo::default().sync_requests(&throttle), 0);
        assert_eq!(throttle.available(), 10);
    }
}
