//! Small calendar helpers shared by the header parsers (`Retry-After`,
//! RFC 3339 reset timestamps).

/// Seconds in a day.
pub(crate) const SECS_PER_DAY: i64 = 86_400;

/// Maps a three-letter English month abbreviation to `1..=12`, case-insensitively.
pub(crate) fn month_index(name: &str) -> Option<u32> {
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
pub(crate) fn civil_to_unix(
    year: i64,
    month: u32,
    day: u32,
    h: u32,
    m: u32,
    s: u32,
) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || h > 23 || m > 59 || s > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let secs_of_day = i64::from(h) * 3600 + i64::from(m) * 60 + i64::from(s);
    Some(days * SECS_PER_DAY + secs_of_day)
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian date, by
/// Howard Hinnant's `days_from_civil` algorithm.
pub(crate) fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
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
    use super::{civil_to_unix, days_from_civil, month_index};

    #[test]
    fn test_days_from_civil_anchors() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1970, 1, 2), 1);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(2000, 1, 1), 10_957);
    }

    #[test]
    fn test_civil_to_unix_known_instant() {
        // 1994-11-06T08:49:37Z = 784_111_777.
        assert_eq!(civil_to_unix(1994, 11, 6, 8, 49, 37), Some(784_111_777));
    }

    #[test]
    fn test_civil_to_unix_rejects_out_of_range() {
        assert_eq!(civil_to_unix(2026, 13, 1, 0, 0, 0), None);
        assert_eq!(civil_to_unix(2026, 1, 32, 0, 0, 0), None);
        assert_eq!(civil_to_unix(2026, 1, 1, 24, 0, 0), None);
    }

    #[test]
    fn test_month_index_case_insensitive() {
        assert_eq!(month_index("Jan"), Some(1));
        assert_eq!(month_index("dec"), Some(12));
        assert_eq!(month_index("Zzz"), None);
    }
}
