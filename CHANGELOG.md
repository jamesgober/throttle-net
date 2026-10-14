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

## [2.0.0] - 2026-10-14

A correctness and hardening release. **Upgrade from 1.0.0 is strongly
recommended**: under ordinary multi-threaded use, 1.0.0's token buckets
admitted 25–180x their configured rate. It is a major release because several
documented defaults change to fail safe and a few signatures change; every
change, with before/after code, is in
[`docs/MIGRATION_1_TO_2.md`](docs/MIGRATION_1_TO_2.md). The MSRV stays at Rust
1.85. (A 1.1.0 candidate was prepared and audited but never published; its
work ships here.)

### Breaking changes

- **`Retry` caps an honoured `Retry-After` at 5 minutes** (`max_retry_after`).
  A longer server delay ends the loop with the error instead of sleeping.
  Restore 1.0 with `max_retry_after(Duration::MAX)`.
- **A full `PerKey` store never forgets a limited key's debt.** 1.0 evicted the
  least-recently-seen key, handing it a fresh allowance. A limited key is now
  evicted only into a per-shard debt ledger (it returns in its debt); when keys
  and ledger are both full of limited keys, a new key is refused until one
  recovers (`WhenFull::RefuseNew`, the default). `WhenFull::ForgetDebt`
  forgets the debt that settles soonest instead.
- **`RateLimitInfo::sync_requests` / `sync_tokens` pause** the limiter for a
  `Retry-After`, and until the reset when a window is exhausted; 1.0 only
  drained. Every server-driven pause is clamped to
  `provider::DEFAULT_MAX_PAUSE` (5 minutes); `sync_*_capped(.., Duration::ZERO)`
  restores drain-only.
- **`MultiLimiter` is strict by default:** a cost naming an unknown dimension is
  refused (`Decision::Impossible`, `ThrottleError::UnknownDimension`) instead of
  silently charging nothing. `.strict(false)` restores 1.0.
- **The `tower` layers wait in `poll_ready`** and hold their admission until
  `call`; `call` without a ready `poll_ready` fails with the new
  `ThrottleError::NotReady`. The inner service no longer needs to be `Clone`.
- **Waiting acquires reserve.** A waiter takes its units at once and sleeps
  exactly until they accrue; composites queue on their bottleneck constituent.
  `try_acquire` and `available()` therefore see queued waiters' debt, and a
  paused limiter reports `available() == 0`.
- **`CircuitBreakerBuilder`, `AdaptiveLimiterBuilder`, `QueueBuilder`, and
  `Retry` are `Clone` but no longer `Copy`**, because `name` now takes an owned
  or borrowed name (`impl Into<Cow<'static, str>>`).
- **`Limiter` gained provided methods** (`refund`, `reserve`, `pause_for`,
  `paused_for`, `resume`, `charge`) and blanket impls for `&L`, `Box<L>`, and
  `Arc<L>`. A downstream blanket `impl<T: …> Limiter for T` that overlaps them
  no longer compiles.
- **`ThrottleError` has new variants** (`BulkheadFull`, `InvalidConfig`,
  `UnknownDimension`, `NotReady`); it is `#[non_exhaustive]`, so a wildcard arm
  covers them.
- Correlated failures lower an AIMD limit once per congestion event, not once
  each; repeated `MultiLimiter` names in one call are summed; the `PerKey`
  shard count is raised so no shard holds more than 16,384 keys of the cap;
  OpenAI presets meter one combined `tokens` budget.

### Security

- **Over-admission under thread contention (TN-001, critical).** better-bucket
  1.0 reused a stale clock reading in its compare-and-swap loop, refilling the
  bucket to full; a `per_second(1000)` throttle granted ~77,000 tokens in two
  seconds on four threads. The floor is now better-bucket 1.1, whose
  single-instant engine can only deny more on a stale reading. Every
  bucket-backed type inherits the fix; real-clock multi-threaded stress tests
  guard it.
- **Unique-key flood was a CPU denial of service (TN-003).** A new key at the
  per-key cap scanned its whole shard under the write lock (~1.8 ms per key at
  the default cap), and an idle TTL made every insert O(n). Eviction is now a
  bounded CLOCK scan plus an incremental TTL sweep: ~0.36 µs per new key at the
  default 1M-key cap.
- **Eviction reset limited keys (TN-011, TN2-007).** Forgetting a key that had
  not recovered handed it a fresh allowance — through the capacity path in 1.0,
  and through idle-TTL reclamation of keys in reservation debt in the 1.1
  candidate. Only recovered keys are forgotten outright now; a limited key's
  debt survives eviction in the ledger.
- **Key-flood lockout quantified and halved (TN2-011).** Locking newcomers out
  now needs a flood that fills both the key slots and the ledger (2× the cap)
  with unrecovered keys; the trade-off and its numbers are in the `Eviction`
  docs, and `WhenFull::ForgetDebt` opts out of lockout entirely.
- **Server-driven pauses are capped (TN2-008).** One `Retry-After: 999999999`
  or `x-ratelimit-reset: i64::MAX` could pause a client for 31–584 years.
- **Unbounded queue fairness map (TN-004)** and **`Retry-After` stall in
  `Retry` (TN-013)** are bounded.
- `event-listener` floor raised to 5.4.2 (RUSTSEC-2026-0221; throttle-net never
  used the affected API). `cargo audit` and `cargo deny` are clean.

### Fixed

- **Lost wake-ups hung `Bulkhead` and `AdaptiveLimiter` waiters (TN2-001).**
  event-listener's `notify(n)` is not additive, so two slots freed before the
  first woken waiter ran woke only one. Every freed slot (and every slot a
  raised adaptive limit adds) now wakes one more waiter.
- **One tenant could starve a shared scope (TN2-002).** A composite waiter
  reserved on every constituent at once, so a tenant queued on its slow per-key
  scope drained the shared `Layered` global (or a `HybridBuilder::shared`
  constituent) far ahead of use; other tenants waited ~19 s. Waiters now queue
  on the bottleneck and charge the rest when it matures.
- **A `CircuitBreaker` used as a `Limiter` wedged half-open (TN2-003)** when a
  composite rolled back or a waiting composite was cancelled: `refund` now
  frees the trial slot the trait admission took. Manual `record_*` outcomes
  free only such slots (TN2-015).
- **`sync_requests(&arc_throttle)` stopped compiling (TN2-004)**; `Limiter` is
  implemented for `&L`, `Box<L>`, and `Arc<L>`.
- **A timed acquire could wait forever (TN2-006)** while a constituent kept
  answering "retry now"; the timeout is now a real deadline.
- **A pause did not hold waiters that had already reserved, and `resume` woke
  nobody (TN2-009, TN2-010).**
- **Queued breaker waiters reached a downstream the breaker had opened on
  (TN2-013)**; they are shed if the breaker opened during their wait.
- **A composite charged a sliding-log constituent at reservation time
  (TN2-014)**, measuring its window from the wrong instant.
- **`settle_costs` dropped an overrun while a dimension was paused
  (TN2-016)**; overruns are charged as debt even while paused.
- **CI on Rust 1.85 failed clippy (`comparison_chain`, TN2-005)**, and Rust
  1.99 deprecates `fetch_update`; both are clean.
- **Circuit breaker stuck half-open after a cancelled probe (TN-002)**, and
  late outcomes counted against a later breaker state (TN-008).
- **Lost adaptive-limit updates (TN-006)**; AIMD collapse on correlated
  failures and Vegas pinned at the floor (TN-007).
- **Sub-millisecond and zero sliding windows were unlimited (TN-009)**;
  sliding-window overflow near `u32::MAX` (TN-019).
- **Capacities above ~4.29M tokens were silently clamped (TN-010)**, and
  `PerKey::capacity` disagreed with its buckets (TN-021).
- **Very slow rates never refilled, and `peek` disagreed with `acquire`
  (TN-020).**
- **Backoff divide-by-zero panic (TN-012)** on an unbounded ceiling with full
  jitter.
- **Unknown `MultiLimiter` names silently charged nothing (TN-016).**
- **OpenAI presets admitted up to twice the real token budget (TN-033).**
- **Composites leaked units on a lost race (TN-022)** and contended `Layered`
  acquires spun; **large-cost waiters could starve (TN-023).**
- **Provider resets were parsed but unused (TN-017)**; fractional Go durations
  (`1.5s`, TN-018) and RFC 3339 offsets were rejected.
- **Metrics went to the wrong facade (TN-015)**; gauges collided across
  instances and most acquire paths emitted nothing (TN-014); the queue-depth
  hook evaluated its argument when metrics were off (TN-030).
- **`--no-default-features --features std` tests did not compile (TN-025)**;
  **the `no_std` core did not build on targets without 64-bit atomics.**
- **`PerKey` read the clock twice per access** (part of TN-029; the
  uncontended gap to `governor` remains and is documented).
- Out-of-range `Trip` and `Aimd` parameters are normalised.

### Added

- **Fair, cancel-safe waiting with real deadlines.** `*_timeout` methods on
  `Throttle`, `SlidingWindowLog`, `Hybrid`, `MultiLimiter`, `PerKey`, `Layered`,
  `CircuitBreaker`, `AdaptiveLimiter`, and `Bulkhead`, plus
  `acquire_owned_timeout` on the breaker, adaptive limiter, and bulkhead and
  `Bulkhead::call_timeout`.
- **`Limiter`:** provided `refund`, `reserve`, `pause_for`, `paused_for`,
  `resume`, `charge`; blanket impls for `&L`, `Box<L>`, `Arc<L>`.
- **Pauses:** `resume` / `paused_for` on `Hybrid`, `MultiLimiter`, and
  `Layered`; pause-aware waiting everywhere.
- **`Throttle`:** a builder with independent `burst` and `initial` fill,
  `per_minute`, `smooth`, `reserve`, `refund`, `drain_to`, `pause_for` /
  `resume` / `paused_for`, names.
- **Retry guardrails:** `Retry::max_retry_after`, `max_elapsed`, `name`, and a
  shared `RetryBudget` with `run_with_budget`.
- **`MultiLimiter`:** `alias`, `strict` / `with_strict`, `try_build`,
  `settle_costs`, `refund_costs`, `drain_to`, `pause_for`, `dimension`,
  `dimensions`, `acquire_costs_timeout`.
- **`PerKey` and `Eviction`:** a builder, `per_minute`, `reserve`, `refund`,
  `reset`, `remove`, `clear`, `eviction`, `MAX_SHARDS`; `WhenFull` with
  `Eviction::with_when_full` / `when_full`.
- **Circuit breaker:** `OwnedPermit`, `call` with `CallError`,
  `acquire_timeout`, `reset`, names, a `Limiter` impl, and `cancel()` on
  permits to give an admission back without recording an outcome (TN2-020).
- **Adaptive limiter:** `OwnedAdaptivePermit`, `acquire_timeout`, `release`,
  `cancel()` on permits, a `Limiter` impl, `Vegas::with_probe_interval`.
- **`Bulkhead`**, the **`resilience`** module (`timeout`, `fallback`, `hedge`,
  `Elapsed`), and **`TokioClock`**.
- **Providers:** `Window::apply` / `apply_capped`, `RateLimitInfo::sync_multi`,
  `*_capped` variants, `provider::DEFAULT_MAX_PAUSE`.
- **`tower` feature:** `ThrottleLayer`, `CircuitBreakerLayer`, `AdaptiveLayer`,
  each with `with_timeout`.
- **Observability:** instance names on every metric; denial, timeout, shed,
  queue-rejection, retry, and retry-budget counters; gauges set at
  construction.
- `ThrottleError::{BulkheadFull, InvalidConfig, UnknownDimension, NotReady}`;
  `Hash`/`PartialEq`/`Debug` where useful; `Backoff::with_seed`;
  `IntoIterator` for `&Backoff`.
- Tests: a regression test for every re-audit reproduction
  (`tests/regressions.rs`), real-clock stress tests including waiting
  composites, property tests, loom models, cancellation tests, fuzz targets
  with committed seeds.
- Examples `provider_sync` and `resilience_stack`; benches for reservations,
  composites, and adversarial per-key floods.

### Changed

- Dependency floors: better-bucket 1.1, clock-lib 1.1, error-forge 1.0.2,
  event-listener 5.4.2, metrics 0.24.6, tracing 0.1.40; new optional
  `hashbrown` and `tower-layer` / `tower-service`.
- `REPS.md`, `.github/`, and `.dev/` are excluded from the published package.
- The locks that guard the breaker, queue, and sliding log stay
  `std::sync::Mutex`: each is held for a bounded synchronous section, never
  across an `.await`, and is shared with the synchronous API (documented in
  `src/sync.rs`).

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

[Unreleased]: https://github.com/jamesgober/throttle-net/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/jamesgober/throttle-net/compare/v1.0.0...v2.0.0
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
