# throttle-net &mdash; API Reference

> Complete reference for every public item in `throttle-net`, with examples.
>
> **Status: pre-1.0, public API frozen (v0.8).** The surface documented here is complete and frozen; the remaining 0.x work is hardening, not API change. Sections marked _(planned)_ describe an intended surface that is not yet shipped. Everything else is available as of the version in [`CHANGELOG.md`](../CHANGELOG.md).
>
> **Runtime backends.** The waiting `acquire` surface (every method marked _(runtime)_ below, plus the [`Queue`](#queue)) needs an async runtime backend: enable **either** `tokio` (the default) **or** `smol`. The async code is identical on both — you only pick the timer. async-std is not supported (it is discontinued, RUSTSEC-2025-0052). See [Runtime backends](#runtime-backends).
>
> **`no_std`.** With `std` off, the pure algorithm core — [`Backoff`](#backoff), [`BackoffIter`](#backoff), [`Jitter`](#backoff), and [`Decision`](#decision) — compiles without the standard library. Everything else (the limiters, the clock seam, the error type) needs `std`.

## Table of Contents

- [Overview](#overview)
- [The three tiers](#the-three-tiers)
- [`Throttle`](#throttle) &mdash; the single token bucket
- [`Decision`](#decision) &mdash; the outcome of an attempt
- [`ThrottleError`](#throttleerror) &mdash; the domain error
- [`Limiter`](#limiter) &mdash; the composition trait
- [`Hybrid`](#hybrid) &mdash; must pass all constituents
- [`MultiLimiter`](#multilimiter) &mdash; multi-dimensional budgets
- [`PerKey`](#perkey) &mdash; independent state per key
- [`Eviction`](#eviction) &mdash; per-key memory policy
- [`Layered`](#layered) &mdash; ordered scopes
- [`SlidingWindowLog`](#slidingwindowlog) &mdash; exact window limiter
- [`Backoff`](#backoff) &mdash; retry delays with jitter
- [`Retry`](#retry) &mdash; the retry policy
- [`parse_retry_after`](#parse_retry_after) &mdash; `Retry-After` parsing
- [`CircuitBreaker`](#circuitbreaker) &mdash; fail fast on failures
- [`Queue`](#queue) &mdash; bounded, deadline-aware waiting
- [`AdaptiveLimiter`](#adaptivelimiter) &mdash; learn the limit from feedback
- [Provider integration](#provider-integration) &mdash; header parsing, sync, presets
- [Observability](#observability) &mdash; metrics and tracing
- [Clock seam](#clock-seam) &mdash; deterministic time
- [Runtime backends](#runtime-backends) &mdash; tokio or smol
- [`no_std` core](#no_std-core) &mdash; the runtime-free algorithm types
- [Feature flags](#feature-flags)

---

## Overview

throttle-net is an outbound throttling library: it protects the services *you call* from being overwhelmed, and protects you from being banned by them. The defining operation is to **wait** for capacity rather than reject the caller &mdash; you pace your own requests.

It does not reimplement token-bucket accounting; it consumes [`better-bucket`](https://crates.io/crates/better-bucket) for that and reads time from [`clock-lib`](https://crates.io/crates/clock-lib), then builds the waiting, cost-aware, composable surface on top.

Every limiter exposes the same shape:

- a **waiting** acquire (`acquire().await`) that paces the caller &mdash; requires a runtime backend (`tokio` or `smol`);
- a **non-blocking** attempt (`try_acquire()`) that returns a `bool` immediately;
- a **non-consuming** check (`peek()`) that reports what *would* happen.

Throughout this document, a method marked _(runtime)_ is part of that waiting surface and needs `tokio` or `smol`. The non-blocking and inspection methods need only `std`.

---

## The three tiers

- **Tier 1** &mdash; the common case in a couple of calls: [`Throttle::per_second`](#throttle) then [`acquire().await`](#throttle).
- **Tier 2** &mdash; the builders that compose limiters: [`Hybrid`](#hybrid), [`MultiLimiter`](#multilimiter), [`PerKey`](#perkey), [`Layered`](#layered).
- **Tier 3** &mdash; the [`Limiter`](#limiter) trait seam, for writing your own limiter or holding a heterogeneous set behind `Arc<dyn Limiter>`.

---

## `Throttle`

```rust
pub struct Throttle<C: Clock = SystemClock> { /* ... */ }
```

A single outbound throttle backed by a token bucket. It refills smoothly and starts full, so a burst up to the capacity is admitted at once and the sustained rate is the refill rate. Token accounting is lock-free (one atomic compare-and-swap per acquire).

### Constructors

| Method | Description |
|---|---|
| `Throttle::per_second(rate: u32) -> Throttle` | `rate` units per second, bursting up to `rate`. A `rate` of `0` grants nothing. |
| `Throttle::per_duration(amount: u32, period: Duration) -> Throttle` | `amount` units every `period`, bursting up to `amount`. |

Both are infallible and use the OS monotonic clock.

```rust
use std::time::Duration;
use throttle_net::Throttle;

let per_sec = Throttle::per_second(100);                       // 100/s
let per_min = Throttle::per_duration(60, Duration::from_secs(60)); // 60/min
assert_eq!(per_sec.capacity(), 100);
assert_eq!(per_min.capacity(), 60);
```

### Waiting acquire (requires a runtime: `tokio` or `smol`)

| Method | Description |
|---|---|
| `async fn acquire(&self) -> Result<(), ThrottleError>` | Takes one token, waiting until one is free. |
| `async fn acquire_with_cost(&self, cost: u32) -> Result<(), ThrottleError>` | Takes `cost` tokens, waiting until they are free. |

Both return [`ThrottleError::CostExceedsCapacity`](#throttleerror) when the request can never be satisfied (cost greater than capacity), so they fail fast instead of waiting forever.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::Throttle;

let throttle = Throttle::per_second(1000);
throttle.acquire().await?;             // one unit
throttle.acquire_with_cost(250).await?; // a heavier request
# Ok(())
# }
```

### Non-blocking and inspection

| Method | Returns | Description |
|---|---|---|
| `try_acquire(&self)` | `bool` | Take one token if available now. |
| `try_acquire_with_cost(&self, cost: u32)` | `bool` | Take `cost` tokens, all-or-nothing. |
| `peek(&self, cost: u32)` | [`Decision`](#decision) | Would `cost` be granted now? Takes nothing. |
| `available(&self)` | `u32` | Whole tokens available right now. |
| `capacity(&self)` | `u32` | Burst ceiling. |

```rust
use throttle_net::Throttle;

let throttle = Throttle::per_second(10);
assert!(throttle.try_acquire_with_cost(7));  // 3 left
assert!(!throttle.try_acquire_with_cost(7)); // not enough; took nothing
assert_eq!(throttle.available(), 3);
assert!(throttle.peek(3).is_acquired());     // would grant, took nothing
assert_eq!(throttle.available(), 3);
```

### `with_clock`

```rust
pub fn with_clock<C2: Clock>(self, clock: C2) -> Throttle<C2>
```

Replaces the time source, for deterministic tests. See the [clock seam](#clock-seam).

```rust
use std::sync::Arc;
use std::time::Duration;
use throttle_net::{ManualClock, Throttle};

let clock = Arc::new(ManualClock::new());
let throttle = Throttle::per_second(2).with_clock(clock.clone());

assert!(throttle.try_acquire());
assert!(throttle.try_acquire());
assert!(!throttle.try_acquire());        // drained
clock.advance(Duration::from_secs(1));   // a full period refills it
assert!(throttle.try_acquire());
```

---

## `Decision`

```rust
#[non_exhaustive]
pub enum Decision {
    Acquired,
    Retry { after: Duration },
    Impossible,
}
```

The synchronous outcome of an attempt. `Acquired` means the tokens were granted and deducted; `Retry { after }` means they will be available after `after`; `Impossible` means the cost exceeds capacity and no wait would ever satisfy it. `Decision` is part of the [`no_std` core](#no_std-core) — it compiles with `std` off.

| Method | Returns | Description |
|---|---|---|
| `is_acquired(&self)` | `bool` | `true` only for `Acquired`. |
| `retry_after(&self)` | `Option<Duration>` | The wait for `Retry`, else `None`. |

```rust
use std::time::Duration;
use throttle_net::Decision;

let d = Decision::Retry { after: Duration::from_millis(20) };
assert_eq!(d.retry_after(), Some(Duration::from_millis(20)));
assert!(!d.is_acquired());
assert!(Decision::Acquired.is_acquired());
```

---

## `ThrottleError`

```rust
#[non_exhaustive]
pub enum ThrottleError {
    CostExceedsCapacity { cost: u32, capacity: u32 },
}
```

The domain error, returned by the waiting acquire methods. `CostExceedsCapacity` is **not** retryable: it is a configuration mismatch, so retrying the same cost on the same limiter never succeeds. It implements [`error_forge::ForgeError`](https://docs.rs/error-forge), so it carries kind/retryability metadata consistent with the rest of the portfolio stack.

```rust
# async fn run() {
use throttle_net::{Throttle, ThrottleError};

let throttle = Throttle::per_second(5);
let err = throttle.acquire_with_cost(9).await.unwrap_err();
assert!(matches!(err, ThrottleError::CostExceedsCapacity { cost: 9, capacity: 5 }));
# }
```

---

## `Limiter`

```rust
pub trait Limiter: Send + Sync {
    fn peek(&self, cost: u32) -> Decision;
    fn acquire_cost(&self, cost: u32) -> Decision;
    fn available(&self) -> u32;
    fn capacity(&self) -> u32;
}
```

The contract every algorithm and composite shares, and the Tier-3 extension point. [`acquire_cost`](#limiter) is the synchronous, **consuming** core; the waiting `acquire` surfaces are thin layers on top. [`peek`](#limiter) is the **non-consuming** check that makes "must pass all" composition correct &mdash; a composite peeks every constituent before committing, so an early limiter never spends a token for a request a later one blocks.

Implemented by [`Throttle`](#throttle) and [`Hybrid`](#hybrid). Hold a heterogeneous set behind `Arc<dyn Limiter>`.

```rust
use throttle_net::{Decision, Limiter, Throttle};

fn drain(limiter: &dyn Limiter) -> u32 {
    let mut granted = 0;
    while limiter.acquire_cost(1) == Decision::Acquired {
        granted += 1;
    }
    granted
}

let throttle = Throttle::per_second(8);
assert_eq!(drain(&throttle), 8);
```

---

## `Hybrid`

```rust
pub struct Hybrid { /* ... */ }
```

Several limiters combined so a request must satisfy **all** of them &mdash; for example "10 per second *and* 100 per minute" on one resource, where either ceiling can bind. A `Hybrid` is itself a [`Limiter`](#limiter), so hybrids nest. Acquisition is peek-then-commit, so no constituent loses a token to a request another blocks.

Build with `Hybrid::builder()`:

| Builder method | Description |
|---|---|
| `.limiter(impl Limiter + 'static)` | Add a constituent. |
| `.shared(Arc<dyn Limiter>)` | Add an already-shared constituent. |
| `.build() -> Hybrid` | Finish. |

| Method | Returns | Description |
|---|---|---|
| `try_acquire(&self)` | `bool` | One token from every constituent, non-blocking. |
| `try_acquire_with_cost(&self, cost)` | `bool` | `cost` tokens from every constituent. |
| `acquire(&self).await` _(runtime)_ | `Result<(), ThrottleError>` | Waiting. |
| `acquire_with_cost(&self, cost).await` _(runtime)_ | `Result<(), ThrottleError>` | Waiting, cost-aware. |
| `peek`, `acquire_cost`, `available`, `capacity` | via [`Limiter`](#limiter) | `available`/`capacity` report the binding (smallest) constituent. |

```rust
use std::time::Duration;
use throttle_net::{Hybrid, Throttle};

// 10 per second, and no more than 100 per minute.
let hybrid = Hybrid::builder()
    .limiter(Throttle::per_second(10))
    .limiter(Throttle::per_duration(100, Duration::from_secs(60)))
    .build();

assert!(hybrid.try_acquire());
```

```rust
use throttle_net::{Hybrid, Limiter, Throttle};

// The tighter constituent binds, and no token is lost to a blocked request.
let hybrid = Hybrid::builder()
    .limiter(Throttle::per_second(10))
    .limiter(Throttle::per_second(2))
    .build();

assert_eq!(hybrid.capacity(), 2); // smallest constituent
assert!(hybrid.try_acquire());
assert!(hybrid.try_acquire());
assert!(!hybrid.try_acquire());   // the 2/s limiter binds
```

---

## `MultiLimiter`

```rust
pub struct MultiLimiter { /* ... */ }
```

A limiter with several **named dimensions**, each metered independently. One outbound call spends against more than one budget at once &mdash; an LLM request counts as one *request*, some *input tokens*, and some *output tokens*, each with its own ceiling. A call is admitted only when every dimension can afford its share, applied atomically.

Build with `MultiLimiter::builder()`:

| Builder method | Description |
|---|---|
| `.dimension(name: impl Into<Box<str>>, limiter: impl Limiter + 'static)` | Add a named dimension. |
| `.shared(name, Arc<dyn Limiter>)` | Add a dimension backed by a shared limiter. |
| `.build() -> MultiLimiter` | Finish. |

Costs are supplied per call as `&[(dimension, cost)]`. A dimension not named in a call is charged nothing; a name with no matching dimension is ignored.

| Method | Returns | Description |
|---|---|---|
| `peek_costs(&self, costs)` | [`Decision`](#decision) | Would all dimensions grant? Takes nothing. |
| `try_acquire_costs(&self, costs)` | `bool` | Charge all dimensions, non-blocking, all-or-nothing. |
| `acquire_costs(&self, costs).await` _(runtime)_ | `Result<(), ThrottleError>` | Waiting. |
| `available(&self, dimension: &str)` | `Option<u32>` | Tokens left in a dimension, or `None` if unknown. |

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use std::time::Duration;
use throttle_net::{MultiLimiter, Throttle};

let minute = Duration::from_secs(60);
let limiter = MultiLimiter::builder()
    .dimension("requests", Throttle::per_duration(60, minute))
    .dimension("input_tokens", Throttle::per_duration(100_000, minute))
    .dimension("output_tokens", Throttle::per_duration(20_000, minute))
    .build();

limiter
    .acquire_costs(&[("requests", 1), ("input_tokens", 1500), ("output_tokens", 200)])
    .await?;
# Ok(())
# }
```

```rust
use throttle_net::{MultiLimiter, Throttle};

let limiter = MultiLimiter::builder()
    .dimension("requests", Throttle::per_second(10))
    .dimension("tokens", Throttle::per_second(1000))
    .build();

// The token dimension binds even though requests are fine.
assert!(limiter.try_acquire_costs(&[("requests", 1), ("tokens", 1000)]));
assert!(!limiter.try_acquire_costs(&[("requests", 1), ("tokens", 1)]));
assert_eq!(limiter.available("requests"), Some(9)); // the refused call charged nothing
```

---

## `PerKey`

```rust
pub struct PerKey<K, C = SystemClock>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    C: Clock,
{ /* ... */ }
```

A throttle that keeps independent state per key &mdash; a tenant, a user, an API token &mdash; so one noisy key cannot spend another's budget. State lives in a sharded concurrent map: an existing key's acquire takes only a shard *read* lock plus the bucket's atomic accounting, so unrelated keys never contend and throughput scales with cores. Memory is bounded by [`Eviction`](#eviction).

`K` is any hashable key type (a `String`, a numeric id, a tuple). The default eviction policy is bounded.

### Constructors and configuration

| Method | Description |
|---|---|
| `PerKey::<K>::per_second(rate)` | `rate`/s per key. |
| `PerKey::<K>::per_duration(amount, period)` | `amount` per `period`, per key. |
| `.with_clock(clock)` | Inject a clock (rebuilds the store empty). |
| `.with_eviction(Eviction)` | Set the memory policy. |
| `.with_shards(n)` | Set shard count (rounded up to a power of two). |

### Operations

| Method | Returns | Description |
|---|---|---|
| `try_acquire(&self, key: &K)` | `bool` | One token for `key`, non-blocking. |
| `try_acquire_with_cost(&self, key, cost)` | `bool` | `cost` tokens for `key`. |
| `peek(&self, key, cost)` | [`Decision`](#decision) | Non-consuming; does not create state for an unseen key. |
| `acquire(&self, key).await` _(runtime)_ | `Result<(), ThrottleError>` | Waiting. |
| `acquire_with_cost(&self, key, cost).await` _(runtime)_ | `Result<(), ThrottleError>` | Waiting, cost-aware. |
| `available(&self, key)` | `u32` | Tokens for `key` (full capacity if unseen). |
| `capacity(&self)` | `u32` | Per-key burst ceiling. |
| `len(&self)` / `is_empty(&self)` | `usize` / `bool` | Live-key count snapshot. |
| `shard_count(&self)` | `usize` | Number of shards. |

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::PerKey;

// 100 requests per second, per tenant.
let limiter: PerKey<String> = PerKey::per_second(100);
limiter.acquire(&"tenant:42".to_string()).await?;
# Ok(())
# }
```

```rust
use throttle_net::PerKey;

// Keys are independent.
let limiter: PerKey<u64> = PerKey::per_second(1);
assert!(limiter.try_acquire(&42));
assert!(!limiter.try_acquire(&42)); // 42 is spent
assert!(limiter.try_acquire(&7));   // 7 is untouched
```

```rust
use std::time::Duration;
use throttle_net::{Eviction, PerKey};

// Cap memory: at most 50k keys, reclaim anything idle for five minutes.
let limiter: PerKey<String> = PerKey::per_second(100)
    .with_eviction(Eviction::capacity(50_000).with_idle(Duration::from_secs(300)))
    .with_shards(64);
# let _ = limiter;
```

---

## `Eviction`

```rust
pub struct Eviction { /* ... */ }
pub const DEFAULT_MAX_KEYS: usize = 1 << 20;
```

How a [`PerKey`](#perkey) limiter bounds the memory its per-key state can occupy. Two independent bounds compose: a **capacity** (a hard ceiling on live keys, evicting the least-recently-seen to make room) and an **idle TTL** (reclaim keys not seen for a while). Eviction is lazy and per-shard. The [`Default`] is a [`DEFAULT_MAX_KEYS`] cap with no TTL &mdash; bounded out of the box.

| Constructor | Bounds |
|---|---|
| `Eviction::capacity(max_keys)` | hard cap, no TTL |
| `Eviction::idle(ttl)` | TTL + the default cap |
| `Eviction::new(max_keys, ttl)` | both |
| `Eviction::unbounded()` | neither (use only for an intrinsically bounded key space) |
| `Eviction::default()` | `DEFAULT_MAX_KEYS` cap |

| Builder / accessor | Description |
|---|---|
| `.with_capacity(max_keys)` / `.with_idle(ttl)` / `.without_capacity()` | Adjust one bound. |
| `.max_keys()` / `.idle_ttl()` | Read the configured bounds. |

```rust
use std::time::Duration;
use throttle_net::Eviction;

let policy = Eviction::capacity(100_000).with_idle(Duration::from_secs(300));
assert_eq!(policy.max_keys(), Some(100_000));
assert_eq!(policy.idle_ttl(), Some(Duration::from_secs(300)));
```

---

## `Layered`

```rust
pub struct Layered<K, E = K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    E: Eq + Hash + Clone + Send + Sync + 'static,
{ /* ... */ }
```

Several scopes of limiting stacked so a request must clear every configured one: a process-wide **global** ceiling, a per-caller **per-key** share, and a per-route **per-endpoint** cap. Applied atomically by the same peek-then-commit rule, so a request never spends in one scope when another blocks it. The two key types are independent and default to the same type for the common all-string case.

Build with `Layered::<K>::builder()` (or `Layered::<K, E>::builder()`):

| Builder method | Description |
|---|---|
| `.global(impl Limiter + 'static)` | The shared ceiling (any limiter, even a [`Hybrid`](#hybrid)). |
| `.per_key(PerKey<K, _>)` | Independent state per caller key (any clock). |
| `.per_endpoint(PerKey<E, _>)` | Independent state per endpoint (any clock). |
| `.build() -> Layered<K, E>` | Finish. Every scope is optional. |

| Method | Returns | Description |
|---|---|---|
| `try_acquire(&self, key, endpoint)` | `bool` | Admit one request, non-blocking. |
| `try_acquire_with_cost(&self, key, endpoint, cost)` | `bool` | Weighted, non-blocking. |
| `peek(&self, key, endpoint, cost)` | [`Decision`](#decision) | Non-consuming. |
| `acquire(&self, key, endpoint).await` _(runtime)_ | `Result<(), ThrottleError>` | Waiting. |
| `acquire_with_cost(&self, key, endpoint, cost).await` _(runtime)_ | `Result<(), ThrottleError>` | Waiting, weighted. |
| `capacity(&self)` | `u32` | The smallest scope capacity. |

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::{Layered, PerKey, Throttle};

// 1000/s overall, 100/s per tenant, 50/s per endpoint.
let layered = Layered::<String>::builder()
    .global(Throttle::per_second(1000))
    .per_key(PerKey::per_second(100))
    .per_endpoint(PerKey::per_second(50))
    .build();

layered
    .acquire(&"tenant:42".to_string(), &"/v1/chat".to_string())
    .await?;
# Ok(())
# }
```

```rust
use throttle_net::{Layered, PerKey, Throttle};

// Mixed key types: numeric tenant id, string endpoint.
let layered = Layered::<u64, String>::builder()
    .global(Throttle::per_second(1000))
    .per_key(PerKey::per_second(100))
    .build();

assert!(layered.try_acquire(&42, &"/v1/chat".to_string()));
```

Per-tenant quotas under a shared ceiling — each tenant gets its own budget, and no
tenant can starve another or the service (a global cap over a per-tenant cap):

```rust
use throttle_net::{Layered, PerKey, Throttle};

// 1000/s overall, but at most 10/s for any one tenant.
let limiter = Layered::<String>::builder()
    .global(Throttle::per_second(1000))
    .per_key(PerKey::per_second(10))
    .build();

let endpoint = "/v1/api".to_string();
let mut admitted = 0;
for _ in 0..15 {
    if limiter.try_acquire(&"acme".to_string(), &endpoint) {
        admitted += 1;
    }
}
assert_eq!(admitted, 10);                                   // capped at the per-tenant 10
assert!(limiter.try_acquire(&"globex".to_string(), &endpoint)); // a different tenant is untouched
```

See [`examples/per_tenant_quotas.rs`](../examples/per_tenant_quotas.rs) for the
waiting form, which paces a throttled tenant instead of dropping it.

---

## `SlidingWindowLog`

```rust
pub struct SlidingWindowLog<C = SystemClock> { /* ... */ }
```

An exact sliding-window-log limiter: at most `limit` units in any trailing
`window`. Where [`Throttle`](#throttle) is a token bucket (smooth, cheap, but
permits a full burst at any instant), `SlidingWindowLog` records each grant and
admits a request only if fewer than `limit` units were granted in the trailing
window — no boundary burst, at the cost of remembering recent grants. It
implements [`Limiter`](#limiter), so it composes everywhere the bucket does.

| Method | Description |
|---|---|
| `SlidingWindowLog::new(limit, window)` | At most `limit` units per trailing `window`. |
| `SlidingWindowLog::per_second(rate)` | At most `rate` units per one-second window. |
| `.with_clock(clock)` | Inject a clock for tests. |
| `try_acquire` / `try_acquire_with_cost(n)` | Non-blocking; returns `bool`. |
| `peek(cost)` | Non-consuming [`Decision`](#decision). |
| `acquire().await` / `acquire_with_cost(n).await` _(runtime)_ | Waiting. |
| `available()` / `capacity()` | Units left in the window / the limit. |

```rust
use std::time::Duration;
use throttle_net::SlidingWindowLog;

// At most 5 requests in any 1-second window — no boundary burst.
let limiter = SlidingWindowLog::new(5, Duration::from_secs(1));
for _ in 0..5 {
    assert!(limiter.try_acquire());
}
assert!(!limiter.try_acquire()); // the 6th in this window is refused
```

---

## `Backoff`

```rust
pub struct Backoff { /* ... */ }
pub struct BackoffIter { /* ... */ }

#[non_exhaustive]
pub enum Jitter { None, Full, Equal, Decorrelated }
```

A backoff *policy*: a base delay curve (constant, linear, or exponential) plus a [`Jitter`](#backoff) mode and a delay ceiling. It is independent of the limiters — pair it with [`Retry`](#retry), or call [`iter`](#backoff) and drive your own loop. Jitter spreads retries so a fleet that failed together does not retry in lockstep; `Decorrelated` is the default and the strongest at breaking up a thundering herd.

`Backoff`, `BackoffIter`, and `Jitter` are part of the [`no_std` core](#no_std-core): they compile and run with `std` off (under `no_std`, `iter()` seeds from a monotonic counter rather than system entropy; `iter_seeded` is unaffected).

### Constructors and tuning

| Method | Description |
|---|---|
| `Backoff::constant(delay)` | The same delay every attempt. |
| `Backoff::linear(initial, increment)` | Grows by `increment` each attempt. |
| `Backoff::exponential(initial, factor)` | Multiplies by `factor` each attempt. |
| `Backoff::default()` | Exponential 100ms × 2, capped at 30s, decorrelated jitter. |
| `.with_max(Duration)` | Set the delay ceiling. |
| `.with_jitter(Jitter)` | Set the jitter mode. |
| `.iter()` / `.iter_seeded(u64)` | Start a delay sequence (random / reproducible seed). |

### `Jitter` modes

| Mode | Delay |
|---|---|
| `None` | exactly the capped curve |
| `Full` | uniform in `[0, delay]` |
| `Equal` | `delay/2 + rand(0, delay/2)` |
| `Decorrelated` | `min(max, rand(base, previous*3))` — the default |

[`BackoffIter`](#backoff) yields one delay per attempt via `next_delay()` (and implements [`Iterator`], always `Some`). The sequence is infinite; bounding attempts is [`Retry`](#retry)'s job.

```rust
use std::time::Duration;
use throttle_net::{Backoff, Jitter};

// Exponential, capped, with full jitter.
let backoff = Backoff::exponential(Duration::from_millis(100), 2.0)
    .with_max(Duration::from_secs(5))
    .with_jitter(Jitter::Full);

let mut delays = backoff.iter();
let first = delays.next_delay();
assert!(first <= Duration::from_millis(100)); // full jitter: in [0, 100ms]
```

```rust
use std::time::Duration;
use throttle_net::Backoff;

// Plain exponential doubling, no jitter, is exact.
let mut delays = Backoff::exponential(Duration::from_millis(100), 2.0).iter();
assert_eq!(delays.next_delay(), Duration::from_millis(100));
assert_eq!(delays.next_delay(), Duration::from_millis(200));
assert_eq!(delays.next_delay(), Duration::from_millis(400));
```

---

## `Retry`

```rust
pub struct Retry { /* ... */ }

#[non_exhaustive]
pub enum RetryAction { Retry, RetryAfter(Duration), GiveUp }
```

A retry policy: a [`Backoff`](#backoff), an attempt ceiling, and whether to honor a server `Retry-After`. It retries any fallible async operation, classifying each error with a closure you supply, so it works with any error type.

| Method | Description |
|---|---|
| `Retry::new(Backoff)` | Default 5 attempts, `Retry-After` honored. |
| `.max_attempts(u32)` | Total attempts including the first (`0` ⇒ `1`). |
| `.respect_retry_after(bool)` | Whether [`RetryAction::RetryAfter`] overrides the backoff. |
| `async fn run(op, classify)` _(runtime)_ | Run `op`, retrying per `classify` until it succeeds, the classifier gives up, or attempts run out. |

`classify: Fn(&E) -> RetryAction` decides per error: retry with the backoff delay, retry honoring a `Retry-After`, or give up. For `error-forge` errors, [`retry_if_retryable`](#retry) classifies by the error's own `is_retryable()`.

```rust
# async fn run() {
use throttle_net::{Backoff, Retry, RetryAction};

let retry = Retry::new(Backoff::default()).max_attempts(4);

let result: Result<u32, &str> = retry
    .run(|| async { Err("transient") }, |_err| RetryAction::Retry)
    .await;
assert_eq!(result, Err("transient")); // gave up after 4 attempts
# }
```

Honor a parsed `Retry-After` over the computed backoff:

```rust
# async fn run() {
use std::time::Duration;
use throttle_net::{Backoff, Retry, RetryAction, parse_retry_after};

struct Rejected { retry_after: Option<&'static str> }

let retry = Retry::new(Backoff::default()).respect_retry_after(true);
let _: Result<(), Rejected> = retry
    .run(
        || async { Err(Rejected { retry_after: Some("2") }) },
        |err: &Rejected| match err.retry_after.and_then(parse_retry_after) {
            Some(after) => RetryAction::RetryAfter(after),
            None => RetryAction::Retry,
        },
    )
    .await;
# }
```

---

## `parse_retry_after`

```rust
pub fn parse_retry_after(value: &str) -> Option<Duration>;
pub fn parse_retry_after_at(value: &str, now_unix_secs: i64) -> Option<Duration>;
```

Parses an HTTP `Retry-After` header into a delay from now. Accepts the delay-seconds form (`120`) and all three HTTP-date forms (IMF-fixdate, RFC 850, asctime). Malformed input returns `None` — never a panic. A date in the past yields [`Duration::ZERO`]. `parse_retry_after_at` takes an explicit "now" (Unix seconds) for deterministic use and testing; `parse_retry_after` reads the system clock.

```rust
use std::time::Duration;
use throttle_net::{parse_retry_after, parse_retry_after_at};

assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(120)));
assert_eq!(parse_retry_after("not a header"), None);

// Date form, evaluated against a fixed "now":
let when = "Thu, 01 Jan 2026 00:00:00 GMT"; // 1_767_225_600 Unix seconds
assert_eq!(parse_retry_after_at(when, 1_767_225_540), Some(Duration::from_secs(60)));
```

---

## `CircuitBreaker`

```rust
pub struct CircuitBreaker<L, C = SystemClock> { /* ... */ } // feature = "circuit-breaker"

#[non_exhaustive]
pub enum Trip {
    Consecutive(u32),
    Ratio { window: u32, ratio: f64, min_calls: u32 },
    Windowed { failures: u32, period: Duration },
}

#[non_exhaustive]
pub enum BreakerState { Closed, Open, HalfOpen }
```

Wraps any [`Limiter`](#limiter) and **fails fast when a downstream is unhealthy**.
A limiter paces requests; a breaker *stops* them. After enough failures it trips
**open** and sheds requests immediately — without consuming the wrapped limiter's
tokens — then after a cooldown goes **half-open** to test recovery, and **closes**
on success. Behind the `circuit-breaker` feature.

Build with `CircuitBreaker::builder()`:

| Builder method | Description |
|---|---|
| `.trip(Trip)` | The condition that opens the breaker. |
| `.cooldown(Duration)` | How long to stay open before a trial. |
| `.half_open(trials, required)` | Concurrent trials and successes needed to close. |
| `.build(limiter)` | Wrap `limiter`. |

| Method | Description |
|---|---|
| `try_acquire()` | Non-blocking. `Ok(Some(permit))` granted, `Ok(None)` rate-limited, `Err(CircuitOpen)` shed. |
| `acquire().await` _(runtime)_ | Fail fast if open; otherwise pace on the limiter. Returns a [`Permit`]. |
| `record_success()` / `record_failure()` | Report an outcome directly. |
| `state()` | Current [`BreakerState`]. |

Outcomes are reported through a `Permit`: settle it with `.success()` or
`.failure()`. **Dropping a permit unsettled counts as a failure**, so an early
return or panic is treated conservatively.

```rust
# async fn run() {
use std::time::Duration;
use throttle_net::{CircuitBreaker, Throttle, Trip};

let breaker = CircuitBreaker::builder()
    .trip(Trip::Consecutive(5))
    .cooldown(Duration::from_secs(10))
    .build(Throttle::per_second(100));

match breaker.acquire().await {
    Ok(permit) => {
        // ... call the downstream ...
        let ok = true;
        if ok { permit.success() } else { permit.failure() }
    }
    Err(_shed) => { /* fail fast: the breaker is open */ }
}
# }
```

---

## `Queue`

```rust
pub struct Queue<L, K = (), C = SystemClock> { /* ... */ } // feature = "tokio" or "smol"

#[non_exhaustive]
pub enum Overflow { Reject, DropOldest, DropLowestPriority }
```

A bounded, deadline-aware, priority queue in front of a limiter. When a limiter
is saturated, callers wait here in an orderly way: the queue admits up to a fixed
number of waiters, serves them by priority (and fairly across keys at equal
priority), and **drops a waiter whose deadline has passed rather than serving
it**. When full, an `Overflow` policy decides who is turned away. `K = ()` gives a
plain priority queue with no cross-key fairness.

Build with `Queue::builder()`:

| Builder method | Description |
|---|---|
| `.capacity(n)` | Maximum simultaneous waiters. |
| `.overflow(Overflow)` | Policy when full: reject, drop-oldest, drop-lowest-priority. |
| `.build(limiter)` | Wrap `limiter`. |

| Method | Description |
|---|---|
| `acquire(key, priority, deadline).await` | Wait for a token; higher `priority` first, `deadline` bounds the wait. |
| `len()` / `is_empty()` / `capacity()` | Waiter-count snapshot / capacity. |

The acquire returns [`ThrottleError::QueueFull`](#throttleerror) when the policy
turns the request away and [`ThrottleError::DeadlineExceeded`](#throttleerror)
when the wait budget runs out.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use std::time::Duration;
use throttle_net::{Overflow, Queue, Throttle};

let queue: Queue<Throttle, &str> = Queue::builder()
    .capacity(100)
    .overflow(Overflow::DropOldest)
    .build(Throttle::per_second(50));

// Wait for a slot at normal priority, giving up after 2 seconds.
queue.acquire("tenant:1", 0, Some(Duration::from_secs(2))).await?;
# Ok(())
# }
```

---

## `AdaptiveLimiter`

```rust
pub struct AdaptiveLimiter<S, C = SystemClock> { /* ... */ } // feature = "adaptive"

pub trait AdaptiveStrategy { fn adjust(&self, current: u32, in_flight: u32, outcome: Outcome) -> u32; }
pub struct Aimd { /* ... */ }
pub struct Vegas { /* ... */ }

#[non_exhaustive]
pub enum Outcome { Success { rtt: Duration }, Failure }
```

A concurrency limiter that **discovers** the right in-flight limit instead of
being told it. It caps the number of concurrent requests at a limit it adjusts
from observed outcomes — growing while requests succeed (and the limit is
saturated), shrinking when they fail or slow down — bounded by a floor and a
ceiling. The limit **never exceeds the ceiling**, so adaptation is only ever more
conservative than your hard cap. Behind the `adaptive` feature.

Unlike the rate limiters, the waiting `acquire` blocks on a *slot* freeing, not on
a timer; the clock is used only to measure round-trip time.

### Strategies

| Strategy | Behavior |
|---|---|
| `Aimd::new(increase, decrease)` | Add `increase` on a saturated success; multiply by `decrease` on failure. The classic congestion response. |
| `Vegas::new(alpha, beta)` | Estimate downstream queue depth from RTT vs the learned no-load latency; grow below `alpha`, shrink above `beta`. |
| custom | Implement `AdaptiveStrategy::adjust`. |

### Build and use

Build with `AdaptiveLimiter::builder()`:

| Builder method | Description |
|---|---|
| `.floor(n)` / `.ceiling(n)` | Bounds; the ceiling is the hard limit. |
| `.initial(n)` | Starting limit (defaults to the floor). |
| `.build(strategy)` | Wrap the strategy. |

| Method | Description |
|---|---|
| `try_acquire()` | `Some(permit)` if a slot is free, else `None`. |
| `acquire().await` _(runtime)_ | Wait until a slot frees. |
| `current_limit()` / `in_flight()` / `ceiling()` | Observe the adapting state. |

Outcomes are reported through an `AdaptivePermit`: settle it with `.success()` (its
RTT is measured from acquisition) or `.failure()`. **Dropping it unsettled counts
as a failure.**

```rust
# async fn run() {
use throttle_net::{Aimd, AdaptiveLimiter};

let limiter = AdaptiveLimiter::builder()
    .floor(2)
    .ceiling(50)
    .initial(10)
    .build(Aimd::default()); // +1 on saturated success, halve on failure

if let Some(permit) = limiter.try_acquire() {
    // ... call the downstream, then report how it went ...
    let ok = true;
    if ok { permit.success() } else { permit.failure() }
}
# }
```

---

## Provider integration

### Header parsing &mdash; `provider` (feature `provider-headers`)

```rust
pub struct HeaderProfile { /* ... */ }
pub struct RateLimitInfo { pub requests: Option<Window>, pub tokens: Option<Window>, pub retry_after: Option<Duration> }
pub struct Window { pub limit: Option<u64>, pub remaining: Option<u64>, pub reset: Option<Duration> }
```

Downstreams advertise your remaining budget in response headers, and every
provider spells it differently. A `HeaderProfile` captures one convention;
[`parse`](#provider-integration) turns a header set (a slice of `(name, value)`
pairs, matched case-insensitively) into a normalized `RateLimitInfo`. Parsing is
defensive — malformed values are dropped, never a panic.

| Profile | Headers | Reset format |
|---|---|---|
| `HeaderProfile::OPENAI` | `x-ratelimit-*-{requests,tokens}` | duration string (`6m0s`) |
| `HeaderProfile::ANTHROPIC` | `anthropic-ratelimit-{requests,tokens}-*` | RFC 3339 instant |
| `HeaderProfile::GITHUB` | `x-ratelimit-*` | Unix timestamp |
| `HeaderProfile::RFC` | `RateLimit-*` (IETF draft) | delta-seconds |
| `HeaderProfile::STRIPE` / `HeaderProfile::AWS` | `Retry-After` only | — |

| Method | Description |
|---|---|
| `parse(headers)` | Parse using the system clock for absolute resets. |
| `parse_at(headers, now_unix_secs)` | Parse against an explicit now (testable, deterministic). |

```rust
use throttle_net::provider::HeaderProfile;

let headers = [
    ("x-ratelimit-limit-requests", "5000"),
    ("x-ratelimit-remaining-requests", "4999"),
    ("x-ratelimit-reset-requests", "6m0s"),
];
let info = HeaderProfile::OPENAI.parse(&headers);
let requests = info.requests.unwrap();
assert_eq!(requests.remaining, Some(4999));
```

### Synchronization

```rust
impl RateLimitInfo {
    pub fn sync_requests<C>(&self, throttle: &Throttle<C>) -> u32;
    pub fn sync_tokens<C>(&self, throttle: &Throttle<C>) -> u32;
}
```

Reconcile a [`Throttle`](#throttle) with the server's reported remaining count by
draining the local budget down to it. It **only ever reduces** — never adds — so
synchronization corrects client/server drift without ever raising the throttle
above its hard limit. Returns the number of tokens drained.

```rust
use throttle_net::Throttle;
use throttle_net::provider::{RateLimitInfo, Window};

let throttle = Throttle::per_second(100); // locally believes 100 are free
let info = RateLimitInfo {
    requests: Some(Window { remaining: Some(10), ..Window::default() }),
    ..RateLimitInfo::default()
};
assert_eq!(info.sync_requests(&throttle), 90); // drained
assert_eq!(throttle.available(), 10);          // now matches the server
```

### LLM presets &mdash; `presets` (feature `provider-llm`)

Ready-made [`MultiLimiter`](#multilimiter) tier configurations, pre-wiring the
requests / input-tokens / output-tokens dimensions. The numbers are illustrative
starting points — verify against current provider docs.

| Preset | |
|---|---|
| `presets::anthropic::{tier_1, tier_2, tier_4}` | Anthropic tiers |
| `presets::openai::{tier_1, tier_2}` | OpenAI tiers |

```rust
use throttle_net::presets;

let limiter = presets::anthropic::tier_2();
assert!(limiter.try_acquire_costs(&[
    ("requests", 1),
    ("input_tokens", 1500),
    ("output_tokens", 200),
]));
```

---

## Observability

There is no public API surface here — observability is emitted automatically by
the limiters when the `metrics` and/or `tracing` features are enabled, and
compiles to nothing (inputs not even evaluated) when they are off. Wire up any
`metrics` recorder / `tracing` subscriber in your application to collect it.

### Metrics (feature `metrics`)

Emitted through the [`metrics`](https://crates.io/crates/metrics) facade:

| Metric | Type | Emitted when |
|---|---|---|
| `throttle_acquired_total` | counter (label `limiter`) | a waiting `acquire` is granted |
| `throttle_wait_duration` | histogram, seconds (label `limiter`) | a waiting `acquire` completes |
| `throttle_queue_depth` | gauge | the queue's waiter count changes |
| `throttle_circuit_state` | gauge (0 closed, 1 half-open, 2 open) | a circuit-breaker transition |
| `throttle_rate_current` | gauge | an adaptive limit changes |

### Tracing events (feature `tracing`)

Emitted through the [`tracing`](https://crates.io/crates/tracing) facade under the
`throttle_net` target:

| Event | Fields | When |
|---|---|---|
| `acquire` (debug) | `limiter`, `cost`, `granted`, `wait_secs` | a waiting acquire completes |
| `circuit breaker transition` (info) | `from`, `to` | the breaker changes state |
| `adaptive limit changed` (debug) | `old`, `new` | the adaptive limit moves |
| `queue overflow` (warn) | `policy` | a waiter is rejected or evicted |
| `queue waiter deadline exceeded` (warn) | — | a waiter's deadline passes |

Both are feature-gated and **zero-cost when disabled**: the instrumentation hooks
compile to empty inlined functions, and the wait timer is zero-sized when neither
feature is on.

---

## Clock seam

throttle-net re-exports the time abstraction so the `with_clock` methods are usable without depending on `clock-lib` directly:

| Re-export | Description |
|---|---|
| `Clock` | The trait a time source implements. |
| `SystemClock` | The OS monotonic clock (the default). |
| `ManualClock` | A clock you advance by hand, for deterministic, sleep-free tests. |

```rust
use std::sync::Arc;
use std::time::Duration;
use throttle_net::{ManualClock, Throttle};

let clock = Arc::new(ManualClock::new());
let throttle = Throttle::per_second(1).with_clock(clock.clone());

assert!(throttle.try_acquire());
assert!(!throttle.try_acquire());
clock.advance(Duration::from_secs(1)); // no real sleeping
assert!(throttle.try_acquire());
```

---

## Runtime backends

The waiting `acquire` surface and the [`Queue`](#queue) need an async runtime to
drive their timers and wake-ups. The code that does the waiting is
runtime-agnostic — it parks on an `event-listener` notification and races a
wake-up against a timeout with `futures-lite` — so the **same** async methods run
unchanged on either backend. You choose one by feature:

| Backend | Feature | Notes |
|---|---|---|
| tokio | `tokio` (default) | The default. Pulls only tokio's `time` feature. |
| smol | `smol` | An alternative timer backend. Enable with `default-features = false`. |
| async-std | — | Unsupported: async-std is discontinued (RUSTSEC-2025-0052). |

Selecting the waiting surface without a backend is a clear compile error rather
than a confusing one — enable exactly one of `tokio` or `smol`.

Run on tokio (the default — nothing extra to do):

```toml
throttle-net = "0.8"
```

Run on smol instead:

```toml
throttle-net = { version = "0.8", default-features = false, features = ["smol"] }
```

The call site is identical either way:

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::Throttle;

let throttle = Throttle::per_second(100);
throttle.acquire().await?; // same code on tokio or smol
# Ok(())
# }
```

The synchronous surface (`try_acquire`, `peek`, `available`, `capacity`) needs no
runtime at all — only `std`.

---

## `no_std` core

With `std` off, throttle-net compiles as a `no_std` crate exposing the pure
algorithm types — no clock, no allocator, no async runtime:

| Item | Available `no_std` |
|---|---|
| [`Decision`](#decision) | yes |
| [`Backoff`](#backoff), [`BackoffIter`](#backoff), [`Jitter`](#backoff) | yes |
| [`VERSION`](#feature-flags) | yes |
| Everything else (limiters, clock seam, `ThrottleError`, retry, provider, …) | no — needs `std` |

```toml
# Algorithm core only, no standard library:
throttle-net = { version = "0.8", default-features = false }
```

```rust
// Compiles and runs under `no_std`.
use core::time::Duration;
use throttle_net::{Backoff, Decision};

// Deterministic backoff sequence — no system clock needed.
let mut delays = Backoff::exponential(Duration::from_millis(50), 2.0).iter_seeded(1);
let first = delays.next_delay();
assert!(first >= Duration::from_millis(50));

// Reason about an outcome without any runtime.
assert!(Decision::Acquired.is_acquired());
```

Under `no_std`, `Backoff::iter()` seeds its jitter from a monotonic counter
instead of system entropy; `iter_seeded(seed)` is fully deterministic on both.

---

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `std` | yes | Standard library. Gates the limiter surface, the clock seam, and `ThrottleError`. With it off the crate is `no_std` and exposes the [algorithm core](#no_std-core) ([`Backoff`](#backoff), [`Jitter`](#backoff), [`Decision`](#decision)) plus `VERSION`. |
| `tokio` | yes | tokio timer backend for the waiting `acquire` surface and the [`Queue`](#queue). Implies `std` (and the internal `runtime` marker). |
| `smol` | no | smol timer backend, as an alternative to `tokio`. Implies `std`. See [Runtime backends](#runtime-backends). |
| `adaptive` | no | The [`AdaptiveLimiter`](#adaptivelimiter) (AIMD + Vegas). Implies `std`; its waiting `acquire` additionally needs a runtime (`tokio` or `smol`). |
| `circuit-breaker` | no | The [`CircuitBreaker`](#circuitbreaker) state machine. Implies `std`. |
| `provider-headers` | no | The [`provider`](#provider-integration) module: rate-limit header parsing + sync. Implies `std`. |
| `provider-llm` | no | The [`presets`](#provider-integration) module: LLM tier presets. Implies `provider-headers`. |
| `metrics` | no | [Metrics](#observability) via the `metrics` facade. Zero-cost when off. |
| `tracing` | no | [Tracing events](#observability) via the `tracing` facade. Zero-cost when off. |
| `serde` | no | Serializable limiter configs. _(planned)_ |

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
