# throttle-net &mdash; API Reference

> Complete reference for every public item in `throttle-net`, with examples.
>
> **Status: pre-1.0.** This document tracks the API surface as it lands across the 0.x series. Sections marked _(planned)_ describe an intended surface that is not yet shipped. Everything else is available as of the version in [`CHANGELOG.md`](../CHANGELOG.md).

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
- [Clock seam](#clock-seam) &mdash; deterministic time
- [Feature flags](#feature-flags)

---

## Overview

throttle-net is an outbound throttling library: it protects the services *you call* from being overwhelmed, and protects you from being banned by them. The defining operation is to **wait** for capacity rather than reject the caller &mdash; you pace your own requests.

It does not reimplement token-bucket accounting; it consumes [`better-bucket`](https://crates.io/crates/better-bucket) for that and reads time from [`clock-lib`](https://crates.io/crates/clock-lib), then builds the waiting, cost-aware, composable surface on top.

Every limiter exposes the same shape:

- a **waiting** acquire (`acquire().await`) that paces the caller &mdash; requires the `tokio` feature;
- a **non-blocking** attempt (`try_acquire()`) that returns a `bool` immediately;
- a **non-consuming** check (`peek()`) that reports what *would* happen.

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

### Waiting acquire (requires `tokio`)

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

The synchronous outcome of an attempt. `Acquired` means the tokens were granted and deducted; `Retry { after }` means they will be available after `after`; `Impossible` means the cost exceeds capacity and no wait would ever satisfy it.

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
| `acquire(&self).await` _(tokio)_ | `Result<(), ThrottleError>` | Waiting. |
| `acquire_with_cost(&self, cost).await` _(tokio)_ | `Result<(), ThrottleError>` | Waiting, cost-aware. |
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
| `acquire_costs(&self, costs).await` _(tokio)_ | `Result<(), ThrottleError>` | Waiting. |
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
| `acquire(&self, key).await` _(tokio)_ | `Result<(), ThrottleError>` | Waiting. |
| `acquire_with_cost(&self, key, cost).await` _(tokio)_ | `Result<(), ThrottleError>` | Waiting, cost-aware. |
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
| `acquire(&self, key, endpoint).await` _(tokio)_ | `Result<(), ThrottleError>` | Waiting. |
| `acquire_with_cost(&self, key, endpoint, cost).await` _(tokio)_ | `Result<(), ThrottleError>` | Waiting, weighted. |
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

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `std` | yes | Standard library. Gates the entire limiter surface. With it off the crate is `no_std` and exposes only `VERSION`. |
| `tokio` | yes | The waiting `acquire` surface, driven by tokio's timer. Implies `std`. |
| `adaptive` | no | AIMD + latency-based adaptive limiters. _(planned: 0.5)_ |
| `circuit-breaker` | no | Circuit breaker state machine. _(planned: 0.4)_ |
| `provider-headers` | no | HTTP rate-limit header parsing. _(planned: 0.6)_ |
| `provider-llm` | no | LLM provider presets. _(planned: 0.6)_ |
| `metrics` | no | Metrics counters/histograms. _(planned: 0.7)_ |
| `tracing` | no | Tracing spans around `acquire()`. _(planned: 0.7)_ |
| `serde` | no | Serializable limiter configs. _(planned)_ |

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
