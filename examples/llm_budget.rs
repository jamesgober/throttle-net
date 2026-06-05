//! Pace calls to an LLM provider against three budgets at once.
//!
//! Providers bill more than one way simultaneously: a request-per-minute ceiling
//! *and* an input-tokens-per-minute ceiling *and* an output-tokens-per-minute
//! ceiling. A single per-request limiter cannot express that — exhaust the token
//! budget and you must stop sending even though you are well under the request
//! count. [`MultiLimiter`] meters each dimension independently and admits a call
//! only when all three can afford it.
//!
//! Run with: `cargo run --example llm_budget`

use std::time::{Duration, Instant};

use throttle_net::{MultiLimiter, Throttle};

/// One queued LLM call: its prompt size and the output ceiling we reserve.
struct Call {
    label: &'static str,
    input_tokens: u32,
    output_tokens: u32,
}

#[tokio::main]
async fn main() -> Result<(), throttle_net::ThrottleError> {
    let minute = Duration::from_secs(60);

    // Deliberately small budgets so the example actually has to wait: 5 requests,
    // 4 000 input tokens, and 1 000 output tokens per minute.
    let limiter = MultiLimiter::builder()
        .dimension("requests", Throttle::per_duration(5, minute))
        .dimension("input_tokens", Throttle::per_duration(4_000, minute))
        .dimension("output_tokens", Throttle::per_duration(1_000, minute))
        .build();

    let calls = [
        Call {
            label: "summarize",
            input_tokens: 1_800,
            output_tokens: 300,
        },
        Call {
            label: "translate",
            input_tokens: 1_200,
            output_tokens: 250,
        },
        Call {
            label: "extract",
            input_tokens: 900,
            output_tokens: 200,
        },
        Call {
            label: "classify",
            input_tokens: 400,
            output_tokens: 100,
        },
    ];

    let started = Instant::now();
    for call in &calls {
        let costs = [
            ("requests", 1),
            ("input_tokens", call.input_tokens),
            ("output_tokens", call.output_tokens),
        ];

        // Returns as soon as every budget can afford this call.
        limiter.acquire_costs(&costs).await?;

        println!(
            "[{:>6.2}s] sent {:<10} (in {:>4}, out {:>3}) | remaining: req {:?}, in {:?}, out {:?}",
            started.elapsed().as_secs_f64(),
            call.label,
            call.input_tokens,
            call.output_tokens,
            limiter.available("requests").unwrap_or(0),
            limiter.available("input_tokens").unwrap_or(0),
            limiter.available("output_tokens").unwrap_or(0),
        );
    }

    // A non-blocking probe: is there budget for one more small call right now?
    let more = limiter.try_acquire_costs(&[
        ("requests", 1),
        ("input_tokens", 100),
        ("output_tokens", 50),
    ]);
    println!("room for one more small call without waiting: {more}");

    Ok(())
}
