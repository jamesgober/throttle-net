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
        <strong>Status: pre-1.0, in active development.</strong> The `Limiter` trait and algorithm surface are being designed and frozen across the 0.x series; <code>1.0.0</code> freezes the public API. See <a href="./CHANGELOG.md"><code>CHANGELOG.md</code></a> for detail.
    </blockquote>
</div>

<hr>
<br>

<h2>What it does</h2>

- **Multi-algorithm** &mdash; token bucket, leaky bucket, GCRA, fixed/sliding window, concurrency limiter &mdash; one library
- **Multi-dimensional limits** &mdash; enforce req/min AND tokens/min AND concurrency simultaneously; the killer feature for LLM APIs
- **Cost-aware acquisition** &mdash; `acquire_with_cost(n)` &mdash; not every request costs one unit
- **Adaptive throttling** &mdash; AIMD and latency-based controllers slow down when downstream struggles, with no explicit signal
- **Circuit breakers** &mdash; closed / open / half-open recovery, wrapping any limiter
- **Backoff + retry** &mdash; exponential with decorrelated jitter; `Retry-After` honored
- **Provider-aware** &mdash; parse `x-ratelimit-*` / `retry-after` headers and sync internal state
- **Runtime-agnostic** &mdash; tokio today, with additional runtimes planned &mdash; no middleware-framework lock-in


<br>

## Installation

```toml
[dependencies]
throttle-net = "0.1"
```

<br>

## Status

This is the <code>v0.1.0</code> scaffold: structure, tooling, and quality gates are in place; the implementation lands across the 0.x series per <a href="./.dev/ROADMAP.md"><code>ROADMAP</code></a> (development copy) and <a href="./docs/API.md"><code>docs/API.md</code></a>.

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
