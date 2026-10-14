//! Fuzz target: every provider header profile must parse arbitrary header sets
//! without panicking, for any reference instant, and reconciling a limiter with
//! the result must never raise its budget.
//!
//! Header names are drawn mostly from the names the profiles actually read, so
//! the value parsers (delta seconds, Go durations, Unix timestamps, RFC 3339,
//! HTTP dates) are reached on nearly every input instead of almost never.
#![no_main]

use std::sync::Arc;
use std::time::Duration;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use throttle_net::provider::HeaderProfile;
use throttle_net::{ManualClock, MultiLimiter, Throttle};

/// Every header name a built-in profile reads.
const NAMES: &[&str] = &[
    "x-ratelimit-limit-requests",
    "x-ratelimit-remaining-requests",
    "x-ratelimit-reset-requests",
    "x-ratelimit-limit-tokens",
    "x-ratelimit-remaining-tokens",
    "x-ratelimit-reset-tokens",
    "anthropic-ratelimit-requests-limit",
    "anthropic-ratelimit-requests-remaining",
    "anthropic-ratelimit-requests-reset",
    "anthropic-ratelimit-tokens-limit",
    "anthropic-ratelimit-tokens-remaining",
    "anthropic-ratelimit-tokens-reset",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "ratelimit-limit",
    "ratelimit-remaining",
    "ratelimit-reset",
    "retry-after",
];

const PROFILES: &[HeaderProfile] = &[
    HeaderProfile::OPENAI,
    HeaderProfile::ANTHROPIC,
    HeaderProfile::GITHUB,
    HeaderProfile::RFC,
    HeaderProfile::STRIPE,
    HeaderProfile::AWS,
];

#[derive(Arbitrary, Debug)]
struct Entry {
    /// Index into `NAMES`; out of range means the arbitrary `raw_name`.
    name: u8,
    raw_name: String,
    value: String,
}

#[derive(Arbitrary, Debug)]
struct Input {
    now: i64,
    entries: Vec<Entry>,
}

fuzz_target!(|input: Input| {
    let headers: Vec<(&str, &str)> = input
        .entries
        .iter()
        .map(|e| {
            let name = NAMES
                .get(usize::from(e.name))
                .copied()
                .unwrap_or(e.raw_name.as_str());
            (name, e.value.as_str())
        })
        .collect();

    for profile in PROFILES {
        // Contract under test: malformed values are dropped, never a panic.
        let info = profile.parse_at(&headers, input.now);

        // Reconciliation only ever reduces the local budget.
        let clock = Arc::new(ManualClock::new());
        let throttle = Throttle::per_second(100).with_clock(clock.clone());
        let before = throttle.available();
        let _ = info.sync_requests(&throttle);
        assert!(throttle.available() <= before);

        let multi = MultiLimiter::builder()
            .dimension(
                "requests",
                Throttle::per_minute(50).with_clock(clock.clone()),
            )
            .dimension(
                "tokens",
                Throttle::per_minute(1_000).with_clock(clock.clone()),
            )
            .build();
        let _ = info.sync_multi(&multi);
        assert!(multi.available("requests").unwrap_or(0) <= 50);
        clock.advance(Duration::from_secs(1));
        assert!(throttle.available() <= throttle.capacity());
    }
});
