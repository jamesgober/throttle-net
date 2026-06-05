//! Parsing of the HTTP `Retry-After` header.
//!
//! [RFC 9110 §10.2.3](https://www.rfc-editor.org/rfc/rfc9110#name-retry-after)
//! allows two forms: a non-negative number of seconds (`Retry-After: 120`) or an
//! HTTP date (`Retry-After: Wed, 21 Oct 2026 07:28:00 GMT`). [`parse_retry_after`]
//! handles both and returns the delay from now; [`parse_retry_after_at`] takes an
//! explicit "now" for deterministic use.
//!
//! All parsing is defensive: malformed input returns `None`, never a panic.

use core::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds in a day, for civil-date arithmetic.
const SECS_PER_DAY: i64 = 86_400;

/// Parses a `Retry-After` header value into a delay from now.
///
/// Accepts the seconds form and the HTTP-date forms (IMF-fixdate, RFC 850, and
/// asctime). Returns `None` for malformed input. A date already in the past
/// yields [`Duration::ZERO`] (retry immediately).
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use throttle_net::parse_retry_after;
///
/// assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(120)));
/// assert_eq!(parse_retry_after("not a header"), None);
/// ```
#[must_use]
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
    parse_retry_after_at(value, now)
}

/// Parses a `Retry-After` value relative to an explicit current time, given as
/// Unix seconds. The date form needs a reference point; this lets tests and
/// clock-injecting callers supply one.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use throttle_net::parse_retry_after_at;
///
/// // Seconds form ignores `now`.
/// assert_eq!(parse_retry_after_at("30", 0), Some(Duration::from_secs(30)));
///
/// // 2026-01-01T00:00:00Z is 1_767_225_600 Unix seconds; 60s before that:
/// let when = "Thu, 01 Jan 2026 00:00:00 GMT";
/// assert_eq!(parse_retry_after_at(when, 1_767_225_540), Some(Duration::from_secs(60)));
///
/// // A date in the past means retry now.
/// assert_eq!(parse_retry_after_at(when, 1_767_225_600 + 10), Some(Duration::ZERO));
/// ```
#[must_use]
pub fn parse_retry_after_at(value: &str, now_unix_secs: i64) -> Option<Duration> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Seconds form: a bare non-negative integer.
    if trimmed.bytes().all(|b| b.is_ascii_digit()) {
        return trimmed.parse::<u64>().ok().map(Duration::from_secs);
    }

    // Otherwise an HTTP date.
    let target = parse_http_date(trimmed)?;
    let delta = target.saturating_sub(now_unix_secs);
    Some(Duration::from_secs(
        u64::try_from(delta.max(0)).unwrap_or(0),
    ))
}

/// Parses an HTTP date (any of the three RFC 9110 formats) to Unix seconds.
fn parse_http_date(value: &str) -> Option<i64> {
    let tokens: Vec<&str> = value.split_whitespace().collect();
    match tokens.as_slice() {
        // IMF-fixdate: "Sun, 06 Nov 1994 08:49:37 GMT"
        [_dow, day, month, year, time, "GMT"] => {
            let day = day.parse::<u32>().ok()?;
            let month = month_index(month)?;
            let year = year.parse::<i64>().ok()?;
            let (h, m, s) = parse_hms(time)?;
            civil_to_unix(year, month, day, h, m, s)
        }
        // asctime: "Sun Nov  6 08:49:37 1994"
        [_dow, month, day, time, year] => {
            let day = day.parse::<u32>().ok()?;
            let month = month_index(month)?;
            let year = year.parse::<i64>().ok()?;
            let (h, m, s) = parse_hms(time)?;
            civil_to_unix(year, month, day, h, m, s)
        }
        // RFC 850: "Sunday, 06-Nov-94 08:49:37 GMT"
        [_dow, date, time, "GMT"] => {
            let mut parts = date.split('-');
            let day = parts.next()?.parse::<u32>().ok()?;
            let month = month_index(parts.next()?)?;
            let yy = parts.next()?.parse::<i64>().ok()?;
            if parts.next().is_some() {
                return None;
            }
            // Two-digit year windowing: 70-99 => 1900s, 00-69 => 2000s.
            let year = if yy >= 70 { 1900 + yy } else { 2000 + yy };
            let (h, m, s) = parse_hms(time)?;
            civil_to_unix(year, month, day, h, m, s)
        }
        _ => None,
    }
}

/// Parses `HH:MM:SS` into validated components.
fn parse_hms(value: &str) -> Option<(u32, u32, u32)> {
    let mut parts = value.split(':');
    let h = parts.next()?.parse::<u32>().ok()?;
    let m = parts.next()?.parse::<u32>().ok()?;
    let s = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() || h > 23 || m > 59 || s > 60 {
        return None;
    }
    Some((h, m, s))
}

/// Maps a three-letter English month abbreviation to `1..=12`.
fn month_index(name: &str) -> Option<u32> {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    MONTHS
        .iter()
        .position(|m| name.eq_ignore_ascii_case(m))
        .map(|i| u32::try_from(i).unwrap_or(0) + 1)
}

/// Converts a validated civil date-time (UTC) to Unix seconds, or `None` if the
/// day/month is out of range.
fn civil_to_unix(year: i64, month: u32, day: u32, h: u32, m: u32, s: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let secs_of_day = i64::from(h) * 3600 + i64::from(m) * 60 + i64::from(s);
    Some(days * SECS_PER_DAY + secs_of_day)
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian date, by
/// Howard Hinnant's `days_from_civil` algorithm.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let m = i64::from(month);
    let d = i64::from(day);
    let y = if m <= 2 { year - 1 } else { year };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::{days_from_civil, parse_retry_after_at};
    use core::time::Duration;

    #[test]
    fn test_seconds_form() {
        assert_eq!(parse_retry_after_at("0", 999), Some(Duration::ZERO));
        assert_eq!(
            parse_retry_after_at("120", 0),
            Some(Duration::from_secs(120))
        );
        assert_eq!(
            parse_retry_after_at("  45  ", 0),
            Some(Duration::from_secs(45))
        );
    }

    #[test]
    fn test_malformed_is_none() {
        assert_eq!(parse_retry_after_at("", 0), None);
        assert_eq!(parse_retry_after_at("soon", 0), None);
        assert_eq!(parse_retry_after_at("-5", 0), None);
        assert_eq!(parse_retry_after_at("12.5", 0), None);
        assert_eq!(
            parse_retry_after_at("Mon, 99 Zzz 2026 99:99:99 GMT", 0),
            None
        );
    }

    #[test]
    fn test_days_from_civil_anchors() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1970, 1, 2), 1);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(2000, 1, 1), 10_957);
    }

    #[test]
    fn test_imf_fixdate_form() {
        // 1994-11-06T08:49:37Z = 784_111_777 Unix seconds.
        let target = 784_111_777;
        let header = "Sun, 06 Nov 1994 08:49:37 GMT";
        assert_eq!(
            parse_retry_after_at(header, target - 100),
            Some(Duration::from_secs(100))
        );
        // Already elapsed -> retry now.
        assert_eq!(
            parse_retry_after_at(header, target + 50),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn test_asctime_and_rfc850_forms_agree() {
        let target = 784_111_777; // same instant as the IMF test
        let asctime = "Sun Nov  6 08:49:37 1994";
        let rfc850 = "Sunday, 06-Nov-94 08:49:37 GMT";
        assert_eq!(
            parse_retry_after_at(asctime, target - 10),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            parse_retry_after_at(rfc850, target - 10),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn test_case_insensitive_month() {
        let header = "Thu, 01 jan 2026 00:00:00 GMT";
        assert_eq!(
            parse_retry_after_at(header, 1_767_225_600),
            Some(Duration::ZERO)
        );
    }
}
