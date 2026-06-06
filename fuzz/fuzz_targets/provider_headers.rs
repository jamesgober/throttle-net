//! Fuzz target: every provider header profile must parse arbitrary header sets
//! without panicking, for any reference instant.
#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use throttle_net::provider::HeaderProfile;

#[derive(Arbitrary, Debug)]
struct Input {
    now: i64,
    headers: Vec<(String, String)>,
}

const PROFILES: &[HeaderProfile] = &[
    HeaderProfile::OPENAI,
    HeaderProfile::ANTHROPIC,
    HeaderProfile::GITHUB,
    HeaderProfile::RFC,
    HeaderProfile::STRIPE,
    HeaderProfile::AWS,
];

fuzz_target!(|input: Input| {
    let headers: Vec<(&str, &str)> = input
        .headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();

    for profile in PROFILES {
        // Contract under test: malformed values are dropped, never a panic.
        let _ = profile.parse_at(&headers, input.now);
    }
});
