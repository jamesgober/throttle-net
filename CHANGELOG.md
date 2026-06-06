<h1 align="center">
    <img width="90px" height="auto" src="https://raw.githubusercontent.com/jamesgober/jamesgober/main/media/icons/hexagon-3.svg" alt="Triple Hexagon">
    <br><b>CHANGELOG</b>
</h1>
<p>
  All notable changes to <code>throttle-net</code> will be documented in this file. The format is based on <a href="https://keepachangelog.com/en/1.1.0/">Keep a Changelog</a>,
  and this project adheres to <a href="https://semver.org/spec/v2.0.0.html/">Semantic Versioning</a>.
</p>

---

## [Unreleased]

### Added

### Changed

### Fixed

### Security

---

## [0.5.0] - 2026-06-05

Adaptive. Find the right limit without being told it: a concurrency limiter that learns the downstream's capacity from observed outcomes and never exceeds the hard ceiling.

### Added

- `AdaptiveLimiter` + `AdaptiveLimiterBuilder` + `AdaptivePermit` (behind the `adaptive` feature) — a concurrency limiter whose in-flight limit adapts to feedback. Grows while requests succeed and the limit is saturated, shrinks when they fail or slow, bounded by a floor and a ceiling, and **never exceeds the ceiling** (the hard limit). Outcomes are reported through an `AdaptivePermit` whose drop counts as a failure.
- `AdaptiveStrategy` trait plus two strategies: `Aimd` (additive increase, multiplicative decrease) and `Vegas` (latency-based, estimating downstream queue depth from round-trip time against the learned no-load latency). `Outcome::{Success { rtt }, Failure}` carries the feedback.
- `examples/adaptive_concurrency.rs` — the limit growing while healthy, collapsing to the floor on degradation, and recovering, all bounded.

### Changed

- The `adaptive` feature now implies `std` and `tokio` (the adaptive limiter waits on a freed slot via tokio's `Notify`).

### Fixed

- The crate-level docs no longer link to the feature-gated `CircuitBreaker` type, so `cargo doc` is warning-free on the default feature set (not only `--all-features`).

---

## [0.4.0] - 2026-06-05

Resilience. The pieces that decide *whether* to call at all and how callers wait: a circuit breaker, a bounded deadline-aware queue, and an exact sliding-window-log limiter.

### Added

- `CircuitBreaker` + `CircuitBreakerBuilder` + `Trip` + `BreakerState` + `Permit` (behind the `circuit-breaker` feature) — wraps any `Limiter` and fails fast when open *without consuming the wrapped limiter*. Closed → open → half-open recovery with three trip conditions: consecutive failures, failure ratio over a rolling window, and failures within a rolling time window. Outcomes are reported through a `Permit` whose drop counts as a failure. State transitions are covered by a proptest; half-open recovery by integration tests.
- `Queue` + `QueueBuilder` + `Overflow` (behind the `tokio` feature) — a bounded, deadline-aware, priority queue in front of a limiter. Serves by priority, fairly across keys at equal priority, and **drops waiters whose deadline has passed rather than serving them**. Overflow policies: reject, drop-oldest, drop-lowest-priority.
- `SlidingWindowLog` — an exact sliding-window-log limiter implementing `Limiter`, so it composes everywhere the token bucket does. No boundary burst, at the cost of remembering recent grants; offers the same waiting `acquire` surface.
- `ThrottleError::CircuitOpen { retry_after }` (retryable), `ThrottleError::QueueFull` (retryable), and `ThrottleError::DeadlineExceeded` — added to the existing `#[non_exhaustive]` error.
- `examples/circuit_breaker.rs` — a flaky downstream tripping the breaker and recovering through half-open.
- `tests/circuit.rs` — the state-transition proptest and async fail-fast test.

### Changed

- The `circuit-breaker` feature now implies `std`. The `tokio` feature additionally enables `sync` and `macros` (for the queue's waiter coordination).

---

## [0.3.0] - 2026-06-05

Retry and backoff. Standalone resilience that composes with the limiters but stands on its own: a full backoff taxonomy with jitter, a retry policy with per-error classification, and `Retry-After` parsing.

### Added

- `Backoff` + `BackoffIter` + `Jitter` &mdash; constant, linear, and exponential backoff, each combinable with no jitter or the AWS full / equal / decorrelated jitter modes. Decorrelated jitter is the [`Backoff::default`], verified against a thundering-herd simulation. Backed by a small no-dependency SplitMix64 generator; `iter_seeded` gives reproducible sequences for tests.
- `Retry` + `RetryAction` + `retry_if_retryable` &mdash; an async retry policy with a configurable attempt ceiling, per-error classification (retry / retry-after / give up), and optional `Retry-After` honoring. `retry_if_retryable` classifies any `error-forge` `ForgeError` by its own retryability.
- `parse_retry_after` + `parse_retry_after_at` &mdash; `Retry-After` header parsing covering both the delay-seconds form and all three HTTP-date forms (IMF-fixdate, RFC 850, asctime), with no date-library dependency. Defensive: malformed input returns `None`, never a panic.
- `examples/retry_backoff.rs` &mdash; retrying a flaky downstream with jittered backoff and a server `Retry-After`.
- `tests/retry.rs` &mdash; the thundering-herd scatter property and an end-to-end `Retry-After` parse-and-honor test.

---

## [0.2.0] - 2026-06-05

Composition release. The foundation and the differentiators nobody else ships: the waiting outbound `acquire`, the `Limiter` trait every algorithm and composite shares, and hybrid, multi-dimensional, per-key, and layered composition — all with the same peek-then-commit correctness so a request never spends in one limiter when another would block it.

### Added

- `Throttle` &mdash; the Tier-1 token bucket. Infallible `per_second` / `per_duration` constructors; a waiting `acquire().await` / `acquire_with_cost(n).await` surface (the outbound default: it paces the caller rather than rejecting it); non-blocking `try_acquire` / `try_acquire_with_cost`; `peek`, `available`, `capacity`, and a `with_clock` test seam.
- `Limiter` trait &mdash; the Tier-3 extension point: `peek` (non-consuming), `acquire_cost` (consuming), `available`, `capacity`. Object-safe and `Send + Sync`.
- `Decision` &mdash; the synchronous outcome (`Acquired` / `Retry { after }` / `Impossible`) with `is_acquired` / `retry_after` helpers.
- `ThrottleError` &mdash; the domain error type built on `error-forge`'s `ForgeError` (`#[non_exhaustive]`).
- `Hybrid` + `HybridBuilder` &mdash; combine limiters so a request must clear all of them; peek-then-commit guarantees no token is lost to a request a later constituent blocks.
- `MultiLimiter` + `MultiLimiterBuilder` &mdash; named per-dimension budgets (`acquire_costs(&[(dim, cost)])`), the multi-dimensional LLM rate-limiting case.
- `PerKey` &mdash; independent throttling per key with a sharded, lock-free read path and bounded memory; an existing-key lookup with 10k keys benchmarks at ~70ns.
- `Eviction` + `DEFAULT_MAX_KEYS` &mdash; the per-key memory policy: idle TTL and/or a hard key cap, enforced lazily and per-shard. Bounded by default.
- `Layered` + `LayeredBuilder` &mdash; ordered global / per-key / per-endpoint scopes; a request must clear every configured scope.
- Re-exports of `clock-lib`'s `Clock`, `SystemClock`, and `ManualClock` so the `with_clock` seam is usable without depending on `clock-lib` directly.
- `examples/llm_budget.rs` &mdash; an end-to-end multi-dimensional LLM budgeting example.
- `tests/proptests.rs` &mdash; property tests for the never-over-admit invariant across `Throttle`, `Hybrid`, `PerKey`, `Layered`, and `MultiLimiter`, plus the per-key flood bound.
- `benches/throttle_bench.rs` &mdash; criterion benchmarks for the single-throttle acquire (~27ns) and the 10k-key per-key lookup (~70ns).

### Changed

- Dependencies: builds on `better-bucket` (token-bucket accounting), `clock-lib` (mockable time), `error-forge` (domain error), and `ahash` (DoS-resistant shard hashing) &mdash; consumed under the `std` feature. No token-bucket accounting is reimplemented.
- `tokio` feature now implies `std` (the waiting surface sits on the limiter).

---

## [0.1.0] - 2026-05-28

Initial scaffold and repository bootstrap. No throttle-net logic yet &mdash; this release establishes the structure, tooling, and quality gates the implementation will be built on.

### Added

- `Cargo.toml` with full crate metadata, Rust 2024 edition, MSRV 1.85, dual `Apache-2.0 OR MIT` license, `docs.rs` configuration, perf-tuned release profile.
- Feature flags and first-party dependency wiring (see `Cargo.toml`).
- Dev-dependencies for the test stack: `criterion`, `proptest`, and `loom` under `cfg(loom)`.
- `README.md` &mdash; overview, positioning, install, and "where it fits".
- `docs/API.md` reference skeleton.
- `REPS.md` compliance baseline at the repository root.
- `.github/workflows/ci.yml` &mdash; Linux/macOS/Windows CI matrix on stable and MSRV, plus loom and audit/deny jobs.
- `deny.toml`, `clippy.toml`, `rustfmt.toml`, `.gitattributes`, `.gitignore`.
- `.dev/` AI-editor briefing (`PROMPT.md`, `ROADMAP.md`) &mdash; gitignored.

[Unreleased]: https://github.com/jamesgober/throttle-net/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/jamesgober/throttle-net/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/jamesgober/throttle-net/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/jamesgober/throttle-net/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/jamesgober/throttle-net/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/throttle-net/releases/tag/v0.1.0
