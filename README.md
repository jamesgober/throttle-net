<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br>
    <b>throttle-net</b>
    <br>
    <sub><sup>OUTBOUND THROTTLING & RESILIENCE</sup></sub>
</h1>

<div align="center">
    <a href="https://crates.io/crates/throttle-net"><img alt="Crates.io" src="https://img.shields.io/crates/v/throttle-net"></a>
    <a href="https://crates.io/crates/throttle-net" alt="Download throttle-net"><img alt="Crates.io Downloads" src="https://img.shields.io/crates/d/throttle-net?color=%230099ff"></a>
    <a href="https://docs.rs/throttle-net" title="throttle-net Documentation"><img alt="docs.rs" src="https://img.shields.io/docsrs/throttle-net"></a>
    <a href="https://github.com/jamesgober/throttle-net/actions"><img alt="GitHub CI" src="https://github.com/jamesgober/throttle-net/actions/workflows/ci.yml/badge.svg"></a>
    <a href="https://github.com/rust-lang/rfcs/blob/master/text/2495-min-rust-version.md" title="MSRV"><img alt="MSRV" src="https://img.shields.io/badge/MSRV-1.85%2B-blue"></a>
</div>

<br>

<div align="left">
    <p>
        <strong>throttle-net</strong> is a general-purpose <b>outbound throttling and resilience</b> library. Where <code>rate-net</code> protects your service from being overwhelmed (inbound), <code>throttle-net</code> protects your service from <em>overwhelming downstream APIs</em> and from being banned by them (outbound).
    </p>
    <p>
        Today you assemble this from three or four crates: <code>governor</code> for a token bucket, a hand-rolled backoff loop, <code>failsafe-rs</code> for a circuit breaker, and bespoke header parsing per provider. <code>throttle-net</code> is the single library that does all of it, composes the algorithms into hybrid and layered policies, and adds the parts nobody ships: <b>multi-dimensional cost-aware limits</b> and <b>adaptive throttling</b>.
    </p>
    <p>
        The common case is one builder and one <code>acquire().await?</code>. The hard cases &mdash; LLM token budgets, per-tenant quotas, adaptive backpressure &mdash; are first-class.
    </p>
    <br>
    <hr>
    <p>
        <strong>MSRV is 1.85+</strong> (Rust 2024 edition). Async-first. Runtime-agnostic. Multi-dimensional, cost-aware, adaptive.
    </p>
    <blockquote>
        <strong>Status: pre-1.0, in active development.</strong> The algorithm and composition surface is being built and frozen across the 0.x series; <code>1.0.0</code> freezes the public API. See <a href="./CHANGELOG.md"><code>CHANGELOG.md</code></a> for detail.
    </blockquote>
</div>

<hr>
<br>

<h2>What it does</h2>

**Available now (v0.4):**

- **Token-bucket throttling** &mdash; smooth refill with burst headroom; lock-free accounting (one atomic compare-and-swap per acquire)
- **Exact sliding-window-log** &mdash; when you need no boundary burst at all, an exact alternative that composes everywhere the bucket does
- **Wait, don't reject** &mdash; the outbound default is `acquire().await`, which paces the caller; `try_acquire()` is there when you need the non-blocking answer
- **Cost-aware acquisition** &mdash; `acquire_with_cost(n)` &mdash; not every request weighs one unit
- **Multi-dimensional limits** &mdash; enforce req/min AND input-tokens/min AND output-tokens/min at once; the killer feature for LLM APIs
- **Composition** &mdash; hybrid (must pass all), per-key (independent state per tenant), and layered (global / per-key / per-endpoint) limiters, combined without the call site changing
- **Bounded memory** &mdash; per-key state is sharded and evicted (idle TTL + hard cap), so a flood of unique keys hits a ceiling instead of growing without limit
- **Retry + backoff** &mdash; constant / linear / exponential backoff with full, equal, or decorrelated jitter; a retry policy with per-error classification; `Retry-After` parsed and honored
- **Circuit breaker** &mdash; closed / open / half-open recovery; wraps any limiter and fails fast when open, without consuming it
- **Queueing** &mdash; a bounded, deadline-aware, priority queue with fair-across-keys scheduling and reject / drop-oldest / drop-lowest-priority overflow

**On the roadmap:**

- **Adaptive throttling** (v0.5) &mdash; AIMD and latency-based controllers that slow down when a downstream struggles, with no explicit signal
- **Provider-aware** (v0.6) &mdash; parse `x-ratelimit-*` / `retry-after` headers and sync internal state
- **Runtime-agnostic** (v0.8) &mdash; tokio today, with async-std and smol planned

<br>

## Installation

```toml
[dependencies]
throttle-net = "0.4"

# Optional features:
throttle-net = { version = "0.4", features = ["circuit-breaker"] }
```

<br>

## Quick start

Pace your outbound calls so you never overwhelm a downstream:

```rust
use throttle_net::Throttle;

#[tokio::main]
async fn main() -> Result<(), throttle_net::ThrottleError> {
    // 100 requests per second, bursting up to 100.
    let throttle = Throttle::per_second(100);

    throttle.acquire().await?; // returns as soon as a token is free
    // ... call the downstream ...
    Ok(())
}
```

Budget an LLM provider across several limits at once &mdash; requests, input tokens, and output tokens:

```rust
use std::time::Duration;
use throttle_net::{MultiLimiter, Throttle};

#[tokio::main]
async fn main() -> Result<(), throttle_net::ThrottleError> {
    let minute = Duration::from_secs(60);
    let limiter = MultiLimiter::builder()
        .dimension("requests", Throttle::per_duration(60, minute))
        .dimension("input_tokens", Throttle::per_duration(100_000, minute))
        .dimension("output_tokens", Throttle::per_duration(20_000, minute))
        .build();

    // Admitted only when every budget can afford this call.
    limiter
        .acquire_costs(&[("requests", 1), ("input_tokens", 1500), ("output_tokens", 200)])
        .await?;
    Ok(())
}
```

Throttle independently per tenant, with bounded memory:

```rust
use throttle_net::PerKey;

#[tokio::main]
async fn main() -> Result<(), throttle_net::ThrottleError> {
    // 100 requests per second, per tenant.
    let limiter: PerKey<String> = PerKey::per_second(100);
    limiter.acquire(&"tenant:42".to_string()).await?;
    Ok(())
}
```

Stack scopes &mdash; an overall ceiling, a per-tenant share, and a per-endpoint cap:

```rust
use throttle_net::{Layered, PerKey, Throttle};

#[tokio::main]
async fn main() -> Result<(), throttle_net::ThrottleError> {
    let layered = Layered::<String>::builder()
        .global(Throttle::per_second(1000))
        .per_key(PerKey::per_second(100))
        .per_endpoint(PerKey::per_second(50))
        .build();

    layered
        .acquire(&"tenant:42".to_string(), &"/v1/chat".to_string())
        .await?;
    Ok(())
}
```

Retry a flaky call with jittered backoff, honoring a server `Retry-After`:

```rust
use std::time::Duration;
use throttle_net::{Backoff, Retry, RetryAction, parse_retry_after};

struct Rejected { retry_after: Option<String> }

#[tokio::main]
async fn main() {
    // Exponential from 100ms, doubling, capped at 5s, decorrelated jitter (the default).
    let retry = Retry::new(Backoff::default().with_max(Duration::from_secs(5))).max_attempts(5);

    let result: Result<&str, Rejected> = retry
        .run(
            || async { Err(Rejected { retry_after: None }) }, // your fallible call
            |err: &Rejected| match err.retry_after.as_deref().and_then(parse_retry_after) {
                Some(after) => RetryAction::RetryAfter(after), // honor the server's hint
                None => RetryAction::Retry,                    // else use the backoff
            },
        )
        .await;
    let _ = result;
}
```

Wrap a flaky downstream in a circuit breaker (needs the `circuit-breaker` feature):

```rust
use std::time::Duration;
use throttle_net::{CircuitBreaker, Throttle, Trip};

#[tokio::main]
async fn main() {
    let breaker = CircuitBreaker::builder()
        .trip(Trip::Consecutive(5))           // open after 5 failures in a row
        .cooldown(Duration::from_secs(10))
        .build(Throttle::per_second(100));

    match breaker.acquire().await {
        Ok(permit) => {
            // ... call the downstream ...
            let ok = true;
            if ok { permit.success() } else { permit.failure() }
        }
        Err(_shed) => { /* breaker open: fail fast */ }
    }
}
```

Full runnable examples live in [`examples/`](./examples/):

```bash
cargo run --example llm_budget                              # multi-dimensional LLM budgets
cargo run --example retry_backoff                           # retry with backoff + Retry-After
cargo run --example circuit_breaker --features circuit-breaker  # trip, shed, recover
```

<br>

## Performance

Local criterion means (`cargo bench --bench throttle_bench`, Windows x86_64, Rust stable):

- **Single-throttle `try_acquire`** (uncontended): ~27 ns &mdash; one atomic compare-and-swap
- **Per-key lookup, 10 000 live keys**: ~70 ns &mdash; hash, shard read lock, map get, acquire

<hr>
<br>

## Where It Fits

`throttle-net` is the outbound resilience layer. It is used by:

- [`rate-net`](https://github.com/jamesgober/rate-net) &mdash; the inbound counterpart; throttle-net is outbound
- `pack-io` / `network-protocol` &mdash; clients that call rate-limited downstreams
- AVA / agent-provider &mdash; LLM API budgeting with multi-dimensional token limits
- Hive DB &mdash; cluster RPC backpressure and downstream protection

It stays foreign-compatible: the obvious default for "I need to call an external API in Rust and not get banned."

<br>

## Contributing

Before opening a PR, `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all-features` must be clean. Hot-path changes require a `criterion` benchmark; correctness-critical paths require property and/or `loom` tests.

<br>

<div id="license">
    <h2>License</h2>
    <p>Licensed under either of</p>
    <ul>
        <li><b>Apache License, Version 2.0</b> &mdash; see <a href="./LICENSE-APACHE">LICENSE-APACHE</a></li>
        <li><b>MIT License</b> &mdash; see <a href="./LICENSE-MIT">LICENSE-MIT</a></li>
    </ul>
    <p>at your option.</p>
</div>

<div align="center">
  <h2></h2>
  <sup>COPYRIGHT <small>&copy;</small> 2026 <strong>JAMES GOBER.</strong></sup>
</div>
