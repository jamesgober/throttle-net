//! Deterministic "nasty input" regression tests for the byte-facing parsers.
//!
//! These assert the headline robustness contract — **malformed input never
//! panics, only ever returns `None` / drops the value** — over a corpus of
//! adversarial inputs plus a large pseudo-random sweep. They run on every
//! platform with a plain `cargo test`, and stand alongside the deeper coverage
//! from the `cargo-fuzz` targets in [`fuzz/`](../fuzz) (run in CI). A crash
//! discovered by the fuzzer should be distilled into a case here.

#![allow(clippy::unwrap_used)]
#![cfg(feature = "std")]

use throttle_net::parse_retry_after_at;

/// A tiny deterministic byte-string generator: a SplitMix64 step seeding lengths
/// and bytes, so the sweep is reproducible without any RNG dependency.
struct Mix(u64);

impl Mix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A string of up to 32 bytes drawn from a set rich in parser-relevant
    /// characters (digits, signs, separators, date words, control bytes).
    fn string(&mut self) -> String {
        const ALPHABET: &[u8] = b"0123456789 ,:-+.GMTawecdfijlnoprstuvy/\t\r\n\0\x7f\xff";
        let len = (self.next() % 33) as usize;
        (0..len)
            .map(|_| {
                let idx = (self.next() as usize) % ALPHABET.len();
                ALPHABET[idx] as char
            })
            .collect()
    }
}

/// Hand-picked adversarial `Retry-After` values that have historically broken
/// naive parsers: overflow, negatives, partial dates, wrong field counts.
const RETRY_AFTER_CORPUS: &[&str] = &[
    "",
    " ",
    "0",
    "-1",
    "+5",
    "99999999999999999999999999",
    "18446744073709551616", // u64::MAX + 1
    "9223372036854775808",  // i64::MAX + 1
    "1.5",
    "0x10",
    "Thu, 01 Jan 2026 00:00:00 GMT",
    "Thu, 32 Jan 2026 00:00:00 GMT", // impossible day
    "Xxx, 01 Foo 2026 99:99:99 GMT", // garbage fields
    "Thursday, 01-Jan-26 00:00:00 GMT",
    "Thu Jan  1 00:00:00 2026",
    "Thu, 01 Jan", // truncated
    "Mon, 01 Jan 0000 00:00:00 GMT",
    // Astronomically large years that overflowed the day-count arithmetic
    // (regression, found by the retry_after fuzz target).
    "Thu, 01 Jan 1777777777777777 00:00:00 GMT",
    "Sun Jan  1 02:02:22 1777777777777777",
    "Sunday, 01-Jan-99999999999999 00:00:00 GMT",
    ",,,,,,",
    "GMT GMT GMT",
    "\0\0\0",
    "２０", // full-width digits (not ASCII)
];

#[test]
fn parse_retry_after_never_panics_on_corpus() {
    for &value in RETRY_AFTER_CORPUS {
        // The contract is "no panic"; the return value is intentionally ignored.
        let _ = parse_retry_after_at(value, 1_767_225_600);
        // A few representative "now" values, including extremes.
        let _ = parse_retry_after_at(value, 0);
        let _ = parse_retry_after_at(value, i64::MAX);
        let _ = parse_retry_after_at(value, i64::MIN);
    }
}

#[test]
fn parse_retry_after_never_panics_on_random_sweep() {
    let mut mix = Mix(0x1234_5678_9ABC_DEF0);
    for _ in 0..50_000 {
        let value = mix.string();
        let now = mix.next() as i64;
        let _ = parse_retry_after_at(&value, now);
    }
}

#[cfg(feature = "provider-headers")]
mod provider {
    use super::Mix;
    use throttle_net::provider::HeaderProfile;

    const PROFILES: &[HeaderProfile] = &[
        HeaderProfile::OPENAI,
        HeaderProfile::ANTHROPIC,
        HeaderProfile::GITHUB,
        HeaderProfile::RFC,
        HeaderProfile::STRIPE,
        HeaderProfile::AWS,
    ];

    /// Header values that stress every reset encoding the profiles understand
    /// (delta-seconds, duration strings, Unix timestamps, RFC 3339 instants).
    const VALUE_CORPUS: &[&str] = &[
        "",
        "0",
        "-0",
        "abc",
        "99999999999999999999",
        "6m0s",
        "0m0s",
        "9999h99m99s",
        "1767225600",
        "-1767225600",
        "2026-01-01T00:00:00Z",
        "2026-13-99T99:99:99Z",
        "not-a-date",
        "\0",
    ];

    /// Every profile, every name it looks for, every adversarial value: parsing
    /// must never panic, regardless of `now`.
    #[test]
    fn parse_never_panics_on_corpus() {
        // Use a broad set of names so each profile finds something to parse.
        let names = [
            "x-ratelimit-limit-requests",
            "x-ratelimit-remaining-requests",
            "x-ratelimit-reset-requests",
            "x-ratelimit-limit-tokens",
            "x-ratelimit-remaining-tokens",
            "x-ratelimit-reset-tokens",
            "anthropic-ratelimit-requests-remaining",
            "anthropic-ratelimit-tokens-reset",
            "ratelimit-remaining",
            "ratelimit-reset",
            "retry-after",
            "x-ratelimit-reset",
        ];
        for profile in PROFILES {
            for &name in &names {
                for &value in VALUE_CORPUS {
                    let headers = [(name, value)];
                    let _ = profile.parse_at(&headers, 1_767_225_600);
                    let _ = profile.parse_at(&headers, 0);
                    let _ = profile.parse_at(&headers, i64::MAX);
                }
            }
        }
    }

    #[test]
    fn parse_never_panics_on_random_sweep() {
        let mut mix = Mix(0xDEAD_BEEF_CAFE_F00D);
        for _ in 0..20_000 {
            let name = mix.string();
            let value = mix.string();
            let headers = [(name.as_str(), value.as_str())];
            let now = mix.next() as i64;
            for profile in PROFILES {
                let _ = profile.parse_at(&headers, now);
            }
        }
    }
}
