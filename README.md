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
        <strong>throttle-net</strong> is a general-purpose <b>outbound throttling and resilience</b> library. Where <code>rate-net</code> protects your service from being overwhelmed (inbound), <code>throttle-net</code> protects your service from <em>overwhelming the APIs it calls</em> and from being banned by them (outbound).
    </p>
    <p>
        Without it you assemble this from several crates: a token bucket, a hand-rolled backoff loop, a circuit breaker, a bulkhead, and bespoke header parsing per provider. <code>throttle-net</code> does all of it, composes the algorithms into hybrid, multi-dimensional, per-key and layered policies, and adds the parts that are hard to get right: <b>cost-aware, fair, cancel-safe waiting</b>, <b>reconciliation with what the provider reports</b>, and <b>adaptive concurrency</b>.
    </p>
    <p>
        The common case is one constructor and one <code>acquire().await?</code>. The hard cases &mdash; LLM token budgets, per-tenant quotas, retry storms, adaptive backpressure &mdash; are first-class.
    </p>
    <br>
    <hr>
    <p>
        <strong>MSRV is 1.85+</strong> (Rust 2024 edition). Async-first, runtime-agnostic (tokio or smol), <code>no_std</code> algorithm core.
    </p>
    <blockquote>
        <strong>Status: stable (2.x).</strong> The public API follows Semantic Versioning. 2.0 is a major release: it fixes over-admission under thread contention inherited from better-bucket 1.0, and changes several defaults to fail safe (capped server pauses, a per-key store that never resets a limited key, strict multi-limiter names, readiness-based tower layers). Upgrading from 1.x: <a href="./docs/MIGRATION_1_TO_2.md"><code>docs/MIGRATION_1_TO_2.md</code></a>. History: <a href="./CHANGELOG.md"><code>CHANGELOG.md</code></a>.
    </blockquote>
</div>

<hr>
<br>

<h2>What it does</h2>

**Limiting**

- **Token bucket** (`Throttle`) &mdash; rate, burst, and initial fill set independently; `per_second` / `per_minute` / `per_duration`; `smooth` for GCRA-style even spacing. Lock-free: one atomic compare-and-swap per grant, and it never over-admits under any number of threads.
- **Exact sliding window** (`SlidingWindowLog`) &mdash; no boundary burst, nanosecond-exact windows.
- **Cost-aware** &mdash; `acquire_with_cost(n)`: not every request weighs one unit.
- **Multi-dimensional** (`MultiLimiter`) &mdash; requests/min *and* tokens/min at once, with aliases for combined budgets, misspelt dimensions refused rather than silently free, and `settle_costs` for calls admitted on an estimate.
- **Composition** &mdash; `Hybrid` (must pass all), `PerKey` (independent state per tenant, bounded memory that never resets a limited key), `Layered` (global / per-key / per-endpoint). A tenant with a deep backlog on its own budget cannot drain a shared global scope ahead of time.

**Waiting**

- **Wait, don't reject** &mdash; `acquire().await` paces the caller; `try_acquire()` is the non-blocking answer; `peek()` consumes nothing.
- **Fair** &mdash; token-bucket waits are reservations: later callers queue behind earlier ones, so a large request is never starved by small ones. Composites queue on their bottleneck constituent and charge the others only when the request can go. (The exact `SlidingWindowLog` cannot reserve; its waiters retry as the window frees.)
- **Timeouts** &mdash; every waiting `acquire` (including the owned-permit ones) has a `*_timeout` variant whose deadline bounds the whole wait; a token bucket gives up at once when the wait is known to be too long.
- **Pauses** &mdash; `pause_for` a server's `Retry-After` holds every waiter, including those that had already reserved; `resume` wakes them at once.
- **Cancel-safe** &mdash; dropping a waiting future returns what it held: reserved units, a breaker's trial slot (also when the breaker is a composite's constituent), a concurrency slot.

**Resilience**

- **Retry** with constant / linear / exponential backoff and full / equal / decorrelated jitter, a `Retry-After` cap, a total time budget, and a shared `RetryBudget` against retry storms.
- **Circuit breaker** &mdash; closed / open / half-open; owned permits for spawned tasks; `call(future)` settles itself; stale outcomes never count against a later state.
- **Bulkhead**, **timeout**, **fallback**, and **hedging**.
- **Deadline-aware priority queue** &mdash; fair across keys, O(log n) scheduling, bounded memory.
- **Adaptive concurrency** (`AdaptiveLimiter`) &mdash; AIMD and Vegas learn the in-flight limit from outcomes, decreasing once per congestion event.

**Integration**

- **Providers** &mdash; parse rate-limit headers from OpenAI, Anthropic, GitHub, the IETF draft, Stripe, AWS; drain limiters to the server's remaining count and pause them until its reset (capped, so a hostile header cannot stall a client for years); LLM tier presets.
- **Observability** &mdash; per-instance named metrics (grants, denials, timeouts, sheds, waits, state gauges) and tracing events; zero-cost when off.
- **`tower` layers** (opt-in) for any `tower::Service` client stack, waiting in `poll_ready` so `load_shed`, `buffer`, and balancers see real backpressure.
- **Deterministic testing** &mdash; inject a `ManualClock`, or a `TokioClock` that follows tokio's paused time.
- **Runtime-agnostic** &mdash; tokio or smol; the `no_std` core (`Backoff`, `Jitter`, `Decision`) builds without the standard library.

<br>

## Installation

```toml
[dependencies]
throttle-net = "2"

# Optional features:
throttle-net = { version = "2", features = ["circuit-breaker", "adaptive", "provider-llm", "metrics", "tracing", "tower"] }

# Run the waiting surface on smol instead of tokio:
throttle-net = { version = "2", default-features = false, features = ["smol"] }

# no_std algorithm core only (Backoff, Jitter, Decision):
throttle-net = { version = "2", default-features = false }
```

<br>

## Quick start

Pace your outbound calls so you never overwhelm a downstream:

```rust
use std::time::Duration;
use throttle_net::Throttle;

#[tokio::main]
async fn main() -> Result<(), throttle_net::ThrottleError> {
    // 100 requests per second, bursting up to 100.
    let throttle = Throttle::per_second(100);

    throttle.acquire().await?; // returns as soon as a token is free
    // ... call the downstream ...

    // Or give up at once if a token will not be free within 50ms.
    throttle.acquire_timeout(Duration::from_millis(50)).await?;
    Ok(())
}
```

Budget an LLM provider across several limits at once, admitting each call on its `max_tokens` estimate and settling with the reported usage:

```rust
use std::time::Duration;
use throttle_net::{MultiLimiter, Throttle};

#[tokio::main]
async fn main() -> Result<(), throttle_net::ThrottleError> {
    let limiter = MultiLimiter::builder()
        .dimension("requests", Throttle::per_minute(60))
        .dimension("input_tokens", Throttle::per_minute(100_000))
        .dimension("output_tokens", Throttle::per_minute(20_000))
        .build(); // a misspelt dimension is refused, not silently free

    let estimate = [("requests", 1), ("input_tokens", 1500), ("output_tokens", 1024)];
    limiter.acquire_costs(&estimate).await?; // waits until every budget can afford it
    // ... call the model; the response reports 180 output tokens ...
    limiter.settle_costs(&estimate, &[("requests", 1), ("input_tokens", 1500), ("output_tokens", 180)]);
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

Stack scopes &mdash; an overall ceiling, a per-tenant share, and a per-endpoint cap. A waiter queues on its own tightest scope and charges the shared global one only when it can go, so one tenant's backlog never starves the others:

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

Retry a flaky call with jittered backoff, honoring a server `Retry-After` within bounds, and sharing a retry budget so an outage cannot become a retry storm:

```rust
use std::time::Duration;
use throttle_net::{Backoff, Retry, RetryAction, RetryBudget, parse_retry_after};

struct Rejected { retry_after: Option<String> }

#[tokio::main]
async fn main() {
    let budget = RetryBudget::new(0.1, 1); // retries add at most 10% (floor: 1/s)
    let retry = Retry::new(Backoff::default().with_max(Duration::from_secs(5)))
        .max_attempts(5)
        .max_retry_after(Duration::from_secs(60)) // a longer Retry-After ends the loop
        .max_elapsed(Duration::from_secs(20));

    let result: Result<&str, Rejected> = retry
        .run_with_budget(
            &budget,
            || async { Err(Rejected { retry_after: None }) }, // your fallible call
            |err: &Rejected| match err.retry_after.as_deref().and_then(parse_retry_after) {
                Some(after) => RetryAction::RetryAfter(after),
                None => RetryAction::Retry,
            },
        )
        .await;
    let _ = result;
}
```

Wrap a flaky downstream in a circuit breaker (needs the `circuit-breaker` feature):

```rust
use std::time::Duration;
use throttle_net::{CallError, CircuitBreaker, Throttle, Trip};

#[tokio::main]
async fn main() {
    let breaker = CircuitBreaker::builder()
        .trip(Trip::Consecutive(5))           // open after 5 failures in a row
        .cooldown(Duration::from_secs(10))
        .build(Throttle::per_second(100));

    // Admits (or sheds at once while open), runs the call, settles from its Result.
    let result: Result<&str, CallError<std::io::Error>> =
        breaker.call(|| async { Ok("response") }).await;
    let _ = result;
}
```

Stay in sync with a provider's rate-limit headers, starting from a tier preset (needs the `provider-llm` feature):

```rust
use throttle_net::presets;
use throttle_net::provider::HeaderProfile;

#[tokio::main]
async fn main() -> Result<(), throttle_net::ThrottleError> {
    let limiter = presets::anthropic::tier_2(); // requests + input/output token budgets

    // After a response, reconcile with what the server reported: drain to its
    // remaining counts, and pause until its reset if a window is exhausted
    // (at most five minutes; `sync_multi_capped` chooses the ceiling).
    let headers = [
        ("anthropic-ratelimit-requests-remaining", "12"),
        ("anthropic-ratelimit-tokens-remaining", "40000"),
    ];
    let info = HeaderProfile::ANTHROPIC.parse(&headers);
    let _drained = info.sync_multi(&limiter);

    limiter.acquire_costs(&[("requests", 1), ("input_tokens", 1500)]).await?;
    Ok(())
}
```

Runnable examples live in [`examples/`](./examples/):

```bash
cargo run --example llm_budget                                           # multi-dimensional budgets, estimate + settle
cargo run --example provider_sync        --features provider-llm         # reconcile a preset with response headers
cargo run --example retry_backoff                                        # retry with backoff + Retry-After
cargo run --example circuit_breaker      --features circuit-breaker      # trip, shed, recover
cargo run --example resilience_stack     --features circuit-breaker      # bulkhead + breaker + timeout + budgeted retry + fallback
cargo run --example adaptive_concurrency --features adaptive             # learn the limit from feedback
cargo run --example per_tenant_quotas                                    # per-tenant budgets under a global cap
```

<br>

## Documentation

- [API reference](./docs/API.md) &mdash; every public item, with examples
- [Cookbook](./docs/COOKBOOK.md) &mdash; task-oriented recipes
- [Migrating from 1.x to 2.0](./docs/MIGRATION_1_TO_2.md) &mdash; every behaviour and API change, with before/after code
- [Migrating from `governor`](./docs/MIGRATING_FROM_GOVERNOR.md) &mdash; API mapping and before/after
- [CHANGELOG](./CHANGELOG.md) and [release notes](./docs/release/)

<br>

## Performance

Criterion means from `cargo bench --bench throttle_bench --bench comparison_bench --bench eviction_bench` (2s warm-up, 6s measurement) on Windows 11 x86_64, rustc 1.95.0, on a development machine shared with other builds, so treat them as indicative. Effectively unlimited rates keep every call on the full grant path.

| Operation | Time |
|---|---|
| `Throttle::try_acquire`, uncontended | ~35 ns (dominated by the OS monotonic clock read) |
| `governor` 0.10 `check`, same machine | ~9 ns |
| `Throttle::reserve` + `refund` | ~39 ns |
| `PerKey::try_acquire`, existing key among 10,000 | ~51 ns |
| `Hybrid` of two buckets | ~141 ns |
| `MultiLimiter`, three dimensions | ~316 ns |
| `SlidingWindowLog::try_acquire` | ~38 ns |
| `PerKey`, new key into a full 1M-key store (unique-key flood) | ~0.36 µs (1.0: ~1.8 ms) |
| `PerKey`, new key into a store whose keys and debt ledger are full of limited keys (refused) | ~0.52 µs |
| `PerKey`, new key with an idle TTL configured, nothing expired | ~0.17 µs |

<hr>
<br>

## Where it fits

`throttle-net` is the outbound resilience layer of the portfolio:

- [`rate-net`](https://github.com/jamesgober/rate-net) is the inbound counterpart (protecting your service); throttle-net protects the services you call. They share `better-bucket` and the `Eviction` policy shape.
- [`better-bucket`](https://github.com/jamesgober/better-bucket) provides the token-bucket engine; [`clock-lib`](https://github.com/jamesgober/clock-lib) the injectable clock.

It aims to be the obvious default for "I need to call an external API in Rust and not get banned."

<br>

## Contributing

Before opening a PR: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all-features` must be clean, and so must the feature matrix in `.github/workflows/ci.yml` (including `--no-default-features --features std` and the smol backend). Hot-path changes need a `criterion` benchmark; concurrency-critical changes need stress, property, or `loom` tests.

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
