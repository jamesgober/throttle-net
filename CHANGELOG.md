# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---


## [Unreleased]

### Added

### Changed

### Fixed

### Security

---

## [1.1.0] - 2026-10-13

A correctness and hardening release with additive features. **Upgrade from
1.0.0 is strongly recommended**: under ordinary multi-threaded use, 1.0.0's
token buckets admitted 25–180x their configured rate. Every 1.0 call site
compiles; `cargo semver-checks` reports a minor release.

### Security

- **Over-admission under thread contention (TN-001, critical).** better-bucket
  1.0 reused a stale clock reading in its compare-and-swap loop, refilling the
  bucket to full; a `per_second(1000)` throttle granted ~77,000 tokens in two
  seconds on four threads. The floor is now better-bucket 1.1, whose
  single-instant engine can only deny more on a stale reading. Every
  bucket-backed type inherits the fix; real-clock multi-threaded stress tests
  now guard it.
- **Unique-key flood was a CPU denial of service (TN-003).** A new key at the
  per-key cap scanned its whole shard under the write lock (~1.8 ms per key at
  the default cap), and an idle TTL made every insert O(n). Eviction is now a
  bounded CLOCK scan plus an incremental TTL sweep: ~0.4 µs per new key at the
  default 1M-key cap.
- **Idle and capacity eviction reset limited keys (TN-011).** Forgetting a key
  that had not recovered handed it a fresh allowance. Now only recovered keys
  (full bucket, or idle past a TTL that is never shorter than the refill time)
  are evicted; when a shard is full of limited keys a new key waits or is
  refused instead (fails closed).
- **Unbounded queue fairness map (TN-004).** Per-key fairness state is pruned
  to stay proportional to the queue capacity.
- **`Retry-After` stall (TN-013).** A hostile or broken `Retry-After` (31 years)
  parked a retry loop forever. `Retry` now caps honored waits (`max_retry_after`,
  default 5 minutes) and can bound the total time (`max_elapsed`).
- `event-listener` floor raised to 5.4.2 (RUSTSEC-2026-0221; throttle-net never
  used the affected API). `cargo audit` and `cargo deny` are clean.

### Fixed

- **Circuit breaker stuck half-open after a cancelled probe (TN-002).** A trial
  admitted while half-open leaked its slot if its future was dropped while
  waiting on the limiter (a `timeout` around `acquire`), and the breaker shed
  everything forever. Admission is now cancel-safe.
- **Late outcomes counted against the wrong breaker state (TN-008).** Permits
  carry the state generation they were admitted under; a slow call admitted
  while closed no longer settles a half-open probe or frees its slot.
- **Lost adaptive-limit updates (TN-006).** Concurrent outcomes overwrote each
  other; the limit now moves by compare-and-swap.
- **Adaptive collapse on correlated failures, Vegas pinned at the floor
  (TN-007).** A failure lowers the limit only if its request started after the
  latest decrease (one decrease per congestion event); Vegas re-probes its
  no-load latency every 1000 samples.
- **Sub-millisecond and zero sliding windows were unlimited (TN-009).** The log
  tracks nanoseconds; a zero window is a per-instant limit as documented.
- **Sliding-window overflow near `u32::MAX` (TN-019)** — accounting is `u64`.
- **Capacities above ~4.29M tokens were silently clamped (TN-010)**, and
  `PerKey::capacity` disagreed with its buckets (TN-021); capacities are exact
  and derived from the bucket configuration.
- **Very slow rates never refilled, and `peek` disagreed with `acquire`
  (TN-020)** — `peek` now uses the grant's own arithmetic.
- **Backoff divide-by-zero panic (TN-012)** on an unbounded ceiling with full
  jitter.
- **Unknown `MultiLimiter` names silently charged nothing (TN-016)** — see the
  new strict mode; repeated names in one call are now summed.
- **OpenAI presets admitted up to twice the real token budget (TN-033)** — they
  now meter one combined `tokens` budget (`input_tokens` / `output_tokens`
  remain as aliases).
- **Composites leaked units on a lost race (TN-022)** — the constituents
  already charged are refunded; contended `Layered` acquires no longer spin.
- **Large-cost waiters could starve (TN-023)** — waits are reservations served
  in order.
- **Provider resets were parsed but unused (TN-017)**; fractional Go durations
  (`1.5s`, TN-018) and RFC 3339 offsets were rejected.
- **Metrics went to the wrong facade (TN-015)** — `metrics` 0.24.
- **Gauges collided across instances and most acquire paths emitted nothing
  (TN-014)**; the queue-depth hook evaluated its argument when metrics were off
  (TN-030).
- **`--no-default-features --features std` tests did not compile (TN-025).**
- **The `no_std` core did not build on targets without 64-bit atomics** (such
  as `thumbv7em`); its seed counter is now pointer-sized.
- **`PerKey` read the clock twice per access (TN-029).**
- Out-of-range `Trip::Ratio`/`Consecutive`/`Windowed` and `Aimd` parameters are
  normalised instead of misbehaving.

### Added

- **Fair, cancel-safe waiting with timeouts on every waiting surface.**
  `acquire_timeout` / `acquire_with_cost_timeout` (and equivalents on
  `SlidingWindowLog`, `Hybrid`, `MultiLimiter`, `PerKey`, `Layered`,
  `CircuitBreaker`, `AdaptiveLimiter`, `Bulkhead`) fail at once when the wait is
  known to be too long. Dropping a waiting future returns its reservation.
- **`Throttle` builder** (`Throttle::builder()`: rate, independent `burst`,
  `initial`, `name`, `clock`; `try_build` / `build`), `per_minute`, `smooth`
  (GCRA-style pacing), `reserve`, `refund`, `drain_to`, `pause_for` / `resume` /
  `paused_for`, `with_name` / `name`.
- **`Limiter` provided methods** `refund`, `reserve`, `pause_for`, with
  conservative defaults.
- **Retry guardrails:** `Retry::max_retry_after`, `max_elapsed`, `name`, and a
  shared `RetryBudget` with `Retry::run_with_budget`.
- **`MultiLimiter`:** `alias`, `strict` / `with_strict` /
  `ThrottleError::UnknownDimension`, `try_build`, `settle_costs` (admit on an
  estimate, settle with actual usage), `refund_costs`, `drain_to`, `pause_for`,
  `dimension`, `dimensions`, `acquire_costs_timeout`.
- **`PerKey`:** `PerKeyBuilder` (burst, initial, eviction, shards, name, clock),
  `per_minute`, `reserve`, `refund`, `reset`, `remove`, `clear`, `eviction`;
  `MAX_SHARDS`.
- **`SlidingWindowLog`:** `per_minute`, `refund`, `pause_for`, `resume`,
  `paused_for`, `window`, names.
- **`CircuitBreaker`:** `OwnedPermit` (`try_acquire_owned`, `acquire_owned`),
  `call(operation)` with `CallError`, `acquire_timeout`, `reset`, `with_name`, a
  `Limiter` impl.
- **`AdaptiveLimiter`:** `OwnedAdaptivePermit`, `acquire_timeout`, `release`, a
  `Limiter` impl, `floor`, names; `Vegas::with_probe_interval`.
- **`Bulkhead`** (fixed concurrency limit with owned permits, `call`,
  `try_call`), **`resilience`** module (`timeout`, `fallback`, `hedge`,
  `Elapsed`), **`TokioClock`** (a clock that follows tokio's pausable time).
- **Provider reconciliation:** `Window::apply` (drain and pause until the
  reset), `RateLimitInfo::sync_multi` for `MultiLimiter` and presets;
  `sync_requests` / `sync_tokens` accept any `Limiter` and honour reset and
  `Retry-After`.
- **Queue:** `acquire_with_cost`, `with_name`, `overflow`; builder `name`.
- **`tower` feature:** `ThrottleLayer`, `CircuitBreakerLayer`, `AdaptiveLayer`.
- **Observability:** an instance `name` label on every metric; new
  `throttle_denied_total`, `throttle_timeouts_total`, `throttle_shed_total`,
  `throttle_queue_rejected_total`, `throttle_retries_total`,
  `throttle_retry_budget_exhausted_total`; breaker and adaptive gauges are set
  at construction; composites emit for themselves.
- `ThrottleError::{BulkheadFull, InvalidConfig, UnknownDimension}`; `Hash` on
  `ThrottleError`, `RetryAction`, `BreakerState`, `Outcome`, and
  `provider::Window`; `PartialEq` on `HeaderProfile`; `Debug` on
  every public type (TN-026); `Backoff::with_seed` (TN-027) and `IntoIterator`
  for `&Backoff`.
- Tests: real-clock multi-threaded stress tests for every limiter kind,
  property tests over arithmetic edges and a sliding-window reference model,
  loom models of the adaptive limit and bulkhead slots, cancellation tests,
  per-instance metrics tests; a `limiter_ops` fuzz target, a profile-aware
  header fuzzer, and committed seed corpora (TN-032).
- Examples `provider_sync` and `resilience_stack`; benches for reservations,
  composites, and adversarial per-key floods.

### Changed

- **Waiting acquires reserve.** A waiter takes its units at once and sleeps
  exactly until they accrue, so waiters are served in arrival order. Under a
  `ManualClock` a waiting acquire now returns after its (real) sleep rather
  than looping until the clock is advanced.
- **Behaviour you may notice:** a `Retry-After` above 5 minutes ends a `Retry`
  loop with the error (restore 1.0 with `max_retry_after(Duration::MAX)`);
  correlated failures halve an AIMD limit once, not once each; a full `PerKey`
  shard of still-limited keys refuses a new key instead of evicting one; the
  shard count is raised so no shard holds more than 16,384 keys of the cap;
  repeated `MultiLimiter` names in one call are summed; `available()` reads 0
  while a limiter is paused; `RateLimitInfo::sync_*` also pause on an exhausted
  window or a `Retry-After`.
- Dependency floors: better-bucket 1.1, clock-lib 1.1, error-forge 1.0.2,
  event-listener 5.4.2, metrics 0.24.6, tracing 0.1.40; new optional
  `hashbrown` (per-key index) and `tower-layer` / `tower-service`.
  Benchmark baseline `governor` 0.6 → 0.10.
- Documentation rewritten to match the code (D-01–D-18); every Rust block in
  the README and `docs/` runs as a doctest.
- CI: `actions/checkout` v7, an explicit feature matrix, a `thumbv7em`
  `no_std` build, release-mode stress tests, a third fuzz target,
  direct-minimal-versions, `cargo-semver-checks`, coverage, and audit/deny on a
  fresh lockfile.
- `rustfmt.toml` drops nightly-only options; `deny.toml` allows only the
  licenses in use.

### Notes

- **No breaking API changes.** `cargo semver-checks` against 1.0.0 passes as a
  minor release (its one finding is `#[must_use]` added to builder structs).
- **MSRV unchanged at Rust 1.85.**

---

## [1.0.0] - 2026-06-06

First stable release. The public API is frozen and will not change incompatibly before 2.0. No functional change from 0.9.0 — this release declares the surface stable after the 0.x series built and hardened it.

### Changed

- **Stable.** Per Semantic Versioning, the public API is now frozen; future 1.x releases are backward-compatible additions and fixes only.

### Removed

- The unimplemented `serde` feature flag, which pulled the dependency but wired up no `Serialize`/`Deserialize`. Removing a no-op flag keeps the 1.0 surface honest; serializable limiter configs may return as an additive feature in a future 1.x release, with a designed and documented on-disk representation.

---

## [0.9.0] - 2026-06-06

Polish and hardening. Adversarial testing, a concurrency model check, expanded property tests, and comparative benchmarks. No API change (the surface was frozen at 0.8).

### Added

- Fuzz targets (`fuzz/`, cargo-fuzz): `retry_after` and `provider_headers` assert the byte-facing parsers never panic on arbitrary input. A CI job runs each as a timed smoke check; `tests/fuzz_smoke.rs` carries an always-on deterministic corpus + pseudo-random sweep of the same contract.
- `loom` concurrency model check (`tests/loom_throttle.rs`): exhaustively explores the adaptive limiter's lock-free slot accounting and proves it never over-admits past the ceiling and never leaks a slot. Wired through a crate-private `--cfg throttle_loom` so it does not disturb dependencies' own loom paths.
- Property tests: exact sliding-window bound and capacity reclamation, backoff delays staying within the ceiling across all jitter modes, and the adaptive limit always staying within `[floor, ceiling]`.
- Benchmarks: `comparison_bench` (uncontended `try_acquire` against `governor`) and `contention_bench` (aggregate throughput at 1 / 4 / 16 / 64 concurrent acquirers).
- Documentation: `docs/COOKBOOK.md` (task-oriented recipes) and `docs/MIGRATING_FROM_GOVERNOR.md` (API mapping + before/after).

### Fixed

- Provider header parsing: an extreme rate-limit reset timestamp combined with an extreme reference time could overflow the `reset - now` subtraction and panic in debug builds. The subtraction now saturates; a past or unreachable reset clamps to zero. Found by the new fuzz-smoke suite; covered by a regression test.
- `Retry-After` / RFC 3339 date parsing: a date with an astronomically large year (e.g. a 16-digit year) overflowed the day-count arithmetic (`days * 86400`) and panicked in debug builds. Years are now bounded to the four digits both formats mandate, with checked arithmetic as a backstop. Found by the `retry_after` fuzz target; covered by a regression test and added to the smoke corpus.

---

## [0.8.0] - 2026-06-05

Runtime flexibility and feature freeze. The waiting surface now runs on either tokio or smol, the pure algorithm core compiles `no_std`, and the public API is frozen for 1.0.

### Added

- smol runtime support (behind the `smol` feature) — an alternative timer backend for the waiting `acquire` surface. The async code is runtime-agnostic; you pick the backend by feature (`tokio` stays the default). Both are exercised in CI.
- `no_std` core: with `std` off, the pure algorithm types (`Backoff`, `BackoffIter`, `Jitter`, `Decision`) and their math compile without the standard library, alongside `VERSION`. The limiter surface still requires `std`. CI builds the `no_std` core and runs its doctests.
- `examples/per_tenant_quotas.rs` — per-tenant budgets under a shared global ceiling via a `Layered` global + `PerKey` stack, so one noisy tenant cannot starve the others.

### Changed

- The waiting surface (`Queue`, the waiting `acquire`, and the adaptive limiter's slot wait) was made runtime-agnostic: it waits on an `event-listener` `Event` and races a wake-up against a timeout with `futures-lite`, instead of a tokio-specific `Notify` / `select!`. The internal `runtime` marker feature is enabled by `tokio` or `smol`; selecting neither while requesting the waiting surface is a clear compile error.
- The `tokio` feature now pulls only tokio's `time` feature (the timer is all that is used), trimming the default dependency footprint.
- **Feature freeze.** The public API is frozen as of this release and will not change incompatibly before 1.0.

### Security

- async-std is intentionally unsupported: it is discontinued (RUSTSEC-2025-0052). Runtime flexibility is delivered through tokio and smol instead.

---

## [0.7.0] - 2026-06-05

Observability. See what the limiters are doing in production — metrics and tracing, feature-gated and zero-cost when off.

### Added

- Metrics (behind the `metrics` feature) emitted through the `metrics` facade: `throttle_acquired_total` and `throttle_wait_duration` (labelled by limiter) on the waiting acquire, `throttle_queue_depth` on the queue, `throttle_circuit_state` on circuit transitions, and `throttle_rate_current` on adaptive-limit changes.
- Tracing (behind the `tracing` feature): an `acquire` event with limiter/cost/granted/wait attributes, and structured events for circuit-breaker transitions, adaptive limit changes, queue overflow, and queue deadline exhaustion — covering every documented state transition.
- `tests/observability.rs` — verifies, with a local capturing recorder, that the circuit-state gauge fires on a transition.

### Changed

- Internal: a small `obs` layer routes all instrumentation through `#[inline]` hooks that compile to nothing — and do not evaluate their inputs — when the features are off, so disabled observability is genuinely zero-cost (the wait timer is zero-sized in that case, asserted by a test). Each hook is gated to the feature of the limiter that calls it, so no hook is dead code in any build.

---

## [0.6.0] - 2026-06-05

Provider integration. Read a downstream's own rate-limit headers, reconcile the limiter with them, and start from a provider tier preset.

### Added

- `provider` module (behind the `provider-headers` feature) — `HeaderProfile` with built-in profiles (`OPENAI`, `ANTHROPIC`, `GITHUB`, `RFC`, `STRIPE`, `AWS`) that parse a response's rate-limit headers into a normalized `RateLimitInfo` (`requests` / `tokens` windows + `retry_after`). Handles all four reset encodings — delta-seconds, duration strings (`6m0s`), Unix timestamps, and RFC 3339 instants — with no date-library dependency, and drops malformed values rather than panicking.
- `RateLimitInfo::sync_requests` / `sync_tokens` — reconcile a `Throttle` with the server's reported remaining count by draining the local budget down to it. Only ever reduces, so synchronization can never raise the throttle above its hard limit.
- `presets` module (behind the `provider-llm` feature) — ready-made `MultiLimiter` tier configurations: `presets::anthropic::{tier_1, tier_2, tier_4}` and `presets::openai::{tier_1, tier_2}`, pre-wiring the requests / input-tokens / output-tokens dimensions.

### Changed

- `provider-headers` now implies `std`; `provider-llm` implies `provider-headers`.
- Internal: the civil-date math behind `Retry-After` and RFC 3339 parsing was extracted into a shared module (no public change).

### Fixed

- The token-bucket module no longer imports the error type unconditionally, so the `std`-without-`tokio` feature combination (reachable via `default-features = false, features = ["provider-llm"]`) builds clean.

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

[Unreleased]: https://github.com/jamesgober/throttle-net/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/jamesgober/throttle-net/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/jamesgober/throttle-net/compare/v0.9.0...v1.0.0
[0.9.0]: https://github.com/jamesgober/throttle-net/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/jamesgober/throttle-net/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/jamesgober/throttle-net/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/jamesgober/throttle-net/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/jamesgober/throttle-net/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/jamesgober/throttle-net/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/jamesgober/throttle-net/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/jamesgober/throttle-net/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/throttle-net/releases/tag/v0.1.0
