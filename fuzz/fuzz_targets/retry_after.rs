//! Fuzz target: `parse_retry_after_at` must never panic on arbitrary input.
//!
//! The first up-to-8 bytes seed the reference "now" (so the date form is
//! exercised against every possible instant); the remainder is the header value.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let split = core::cmp::min(8, data.len());
    let (now_bytes, rest) = data.split_at(split);
    let mut buf = [0u8; 8];
    buf[..now_bytes.len()].copy_from_slice(now_bytes);
    let now = i64::from_le_bytes(buf);

    if let Ok(value) = core::str::from_utf8(rest) {
        // Contract under test: no panic, ever — only `Some(delay)` or `None`.
        let _ = throttle_net::parse_retry_after_at(value, now);
    }
});
