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
- [`SlidingWindowLog`](#slidingwindowlog) &mdash; exact window limiter
- [`Backoff`](#backoff) &mdash; retry delays with jitter
- [`Retry`](#retry) &mdash; the retry policy
- [`parse_retry_after`](#parse_retry_after) &mdash; `Retry-After` parsing
- [`CircuitBreaker`](#circuitbreaker) &mdash; fail fast on failures
- [`Queue`](#queue) &mdash; bounded, deadline-aware waiting
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
| `acquire().await` / `acquire_with_cost(n).await` _(tokio)_ | Waiting. |
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
| `async fn run(op, classify)` _(tokio)_ | Run `op`, retrying per `classify` until it succeeds, the classifier gives up, or attempts run out. |

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
| `acquire().await` _(tokio)_ | Fail fast if open; otherwise pace on the limiter. Returns a [`Permit`]. |
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
pub struct Queue<L, K = (), C = SystemClock> { /* ... */ } // feature = "tokio"

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
| `tokio` | yes | The waiting `acquire` surface and the [`Queue`](#queue), driven by tokio's timer/sync. Implies `std`. |
| `adaptive` | no | AIMD + latency-based adaptive limiters. _(planned: 0.5)_ |
| `circuit-breaker` | no | The [`CircuitBreaker`](#circuitbreaker) state machine. Implies `std`. |
| `provider-headers` | no | HTTP rate-limit header parsing. _(planned: 0.6)_ |
| `provider-llm` | no | LLM provider presets. _(planned: 0.6)_ |
| `metrics` | no | Metrics counters/histograms. _(planned: 0.7)_ |
| `tracing` | no | Tracing spans around `acquire()`. _(planned: 0.7)_ |
| `serde` | no | Serializable limiter configs. _(planned)_ |

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
