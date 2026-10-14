//! Keep a local limiter in step with what the provider reports.
//!
//! Start from an LLM tier preset, and after each response reconcile it with the
//! rate-limit headers the provider sent: the local budget is drained to the
//! server's remaining count, and when the server says a window is exhausted
//! the limiter pauses until the reported reset — so the next call waits instead
//! of earning a 429. A `Retry-After` on a 429 pauses it too.
//!
//! Run with: `cargo run --example provider_sync --features provider-llm`

use std::time::{Duration, Instant};

use throttle_net::presets;
use throttle_net::provider::HeaderProfile;

#[tokio::main]
async fn main() -> Result<(), throttle_net::ThrottleError> {
    // OpenAI tier 1: 500 requests and 30,000 tokens per minute (one combined
    // token budget; `input_tokens` / `output_tokens` are aliases for it).
    let limiter = presets::openai::tier_1().with_strict(true);

    // A response says: 3 requests left, and the token window is exhausted
    // until it resets in 1.5 seconds.
    let headers = [
        ("x-ratelimit-remaining-requests", "3"),
        ("x-ratelimit-remaining-tokens", "0"),
        ("x-ratelimit-reset-tokens", "1.5s"),
    ];
    let info = HeaderProfile::OPENAI.parse(&headers);
    let drained = info.sync_multi(&limiter);
    println!(
        "reconciled: drained {drained} units; requests left {:?}, tokens left {:?}",
        limiter.available("requests").unwrap_or(0),
        limiter.available("tokens").unwrap_or(0),
    );

    // The next call waits out the reset instead of being rejected upstream.
    let started = Instant::now();
    limiter
        .acquire_costs(&[
            ("requests", 1),
            ("input_tokens", 800),
            ("output_tokens", 200),
        ])
        .await?;
    println!(
        "next call admitted after {:.2}s (the 1.5s reset, then the tokens it needs)",
        started.elapsed().as_secs_f64()
    );

    // A 429 with `Retry-After: 1` pauses every dimension for a second.
    let throttled = HeaderProfile::OPENAI.parse(&[("retry-after", "1")]);
    let _ = throttled.sync_multi(&limiter);
    let started = Instant::now();
    limiter.acquire_costs(&[("requests", 1)]).await?;
    println!(
        "after a 429, the next call waited {:.2}s",
        started.elapsed().as_secs_f64()
    );
    assert!(started.elapsed() >= Duration::from_millis(900));
    Ok(())
}
