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
//! panic. [`RateLimitInfo::sync_requests`] then reconciles a [`Throttle`] with the
//! server's reported remaining count — only ever *reducing* the local budget, so
//! synchronization can never raise it above the hard limit.

use core::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use clock_lib::Clock;

use crate::retry_after::parse_retry_after_at;
use crate::throttle::Throttle;
use crate::timeutil::civil_to_unix;

/// One metered dimension's reported window: its ceiling, what is left, and how
/// long until it refills. Any field may be absent if the server did not send it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Window {
    /// The dimension's ceiling for the window.
    pub limit: Option<u64>,
    /// Units remaining in the current window.
    pub remaining: Option<u64>,
    /// Time until the window resets.
    pub reset: Option<Duration>,
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
    /// Reconciles `throttle` with the server's reported requests-remaining,
    /// draining tokens so the local available count does not exceed it.
    ///
    /// This only ever *reduces* the local budget — it never adds tokens — so it
    /// corrects client/server drift without ever raising the throttle above its
    /// hard capacity. Returns the number of tokens drained.
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
    pub fn sync_requests<C: Clock + Clone>(&self, throttle: &Throttle<C>) -> u32 {
        drain_to(throttle, self.requests.and_then(|w| w.remaining))
    }

    /// Reconciles `throttle` with the server's reported tokens-remaining, the same
    /// way as [`sync_requests`](Self::sync_requests). Returns the tokens drained.
    pub fn sync_tokens<C: Clock + Clone>(&self, throttle: &Throttle<C>) -> u32 {
        drain_to(throttle, self.tokens.and_then(|w| w.remaining))
    }
}

/// Drains `throttle` down to `remaining`, never adding. Returns the count drained.
fn drain_to<C: Clock + Clone>(throttle: &Throttle<C>, remaining: Option<u64>) -> u32 {
    let Some(remaining) = remaining else {
        return 0;
    };
    let remaining = u32::try_from(remaining).unwrap_or(u32::MAX);
    let available = throttle.available();
    if remaining >= available {
        return 0;
    }
    let excess = available - remaining;
    if throttle.try_acquire_with_cost(excess) {
        excess
    } else {
        0
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
#[derive(Debug, Clone, Copy)]
struct Triple {
    limit: &'static str,
    remaining: &'static str,
    reset: &'static str,
}

/// One provider's header convention.
///
/// Use a built-in profile ([`HeaderProfile::OPENAI`] and friends) and call
/// [`parse`](Self::parse).
#[derive(Debug, Clone, Copy)]
pub struct HeaderProfile {
    requests: Option<Triple>,
    tokens: Option<Triple>,
    retry_after: Option<&'static str>,
    reset: ResetFormat,
}

impl HeaderProfile {
    /// OpenAI: `x-ratelimit-{limit,remaining,reset}-{requests,tokens}`, reset as a
    /// duration string, plus `retry-after`.
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
    /// reset as an RFC 3339 instant, plus `retry-after`.
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
                .map(|at| Duration::from_secs(u64::try_from(at - now).unwrap_or(0))),
            ResetFormat::Rfc3339 => parse_rfc3339(value.trim())
                .map(|at| Duration::from_secs(u64::try_from(at - now).unwrap_or(0))),
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

/// Parses a Go-style duration string (`1s`, `6m0s`, `100ms`, `1h2m3s`), or a bare
/// integer as seconds, into a [`Duration`].
fn parse_duration_string(value: &str) -> Option<Duration> {
    if value.is_empty() {
        return None;
    }
    // A bare number is treated as seconds.
    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }

    let bytes = value.as_bytes();
    let mut total = Duration::ZERO;
    let mut i = 0;
    let mut saw_unit = false;
    while i < bytes.len() {
        // Read the numeric part.
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return None; // expected a number
        }
        let number: u64 = value.get(start..i)?.parse().ok()?;
        // Read the unit (longest first: "ms" before "m"/"s").
        let unit_start = i;
        while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
            i += 1;
        }
        let unit = value.get(unit_start..i)?;
        let part = match unit {
            "ms" => Duration::from_millis(number),
            "s" => Duration::from_secs(number),
            "m" => Duration::from_secs(number.saturating_mul(60)),
            "h" => Duration::from_secs(number.saturating_mul(3600)),
            _ => return None,
        };
        total = total.saturating_add(part);
        saw_unit = true;
    }
    saw_unit.then_some(total)
}

/// Parses an RFC 3339 / ISO 8601 UTC instant (`2026-01-01T00:00:00Z`, with an
/// optional fractional second) into Unix seconds. Only the `Z` (UTC) form is
/// accepted; other offsets return `None`.
fn parse_rfc3339(value: &str) -> Option<i64> {
    let (date, rest) = value.split_once('T')?;
    // Strip the zone: require a trailing 'Z'.
    let time = rest.strip_suffix('Z').or_else(|| rest.strip_suffix('z'))?;
    // Drop any fractional-second part.
    let time = time.split('.').next()?;

    let mut d = date.split('-');
    let year = d.next()?.parse::<i64>().ok()?;
    let month = d.next()?.parse::<u32>().ok()?;
    let day = d.next()?.parse::<u32>().ok()?;
    if d.next().is_some() {
        return None;
    }

    let mut t = time.split(':');
    let h = t.next()?.parse::<u32>().ok()?;
    let m = t.next()?.parse::<u32>().ok()?;
    let s = t.next()?.parse::<u32>().ok()?;
    if t.next().is_some() {
        return None;
    }

    civil_to_unix(year, month, day, h, m, s)
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
    use crate::throttle::Throttle;
    use core::time::Duration;

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
    fn test_rfc3339_parsing() {
        assert_eq!(parse_rfc3339("2026-01-01T00:00:00Z"), Some(1_767_225_600));
        // Fractional seconds are tolerated.
        assert_eq!(
            parse_rfc3339("2026-01-01T00:00:00.123Z"),
            Some(1_767_225_600)
        );
        // A non-UTC offset is refused.
        assert_eq!(parse_rfc3339("2026-01-01T00:00:00+02:00"), None);
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
    fn test_sync_with_no_info_is_a_noop() {
        let throttle = Throttle::per_second(10);
        assert_eq!(RateLimitInfo::default().sync_requests(&throttle), 0);
        assert_eq!(throttle.available(), 10);
    }
}
