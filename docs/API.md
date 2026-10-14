# throttle-net &mdash; API Reference

> Every public item in `throttle-net`, with examples. Every Rust example in this file is compiled and run as a doctest (`cargo test --all-features --doc`).
>
> **Status: stable (2.x).** The surface follows Semantic Versioning. Coming from 1.x? See [`MIGRATION_1_TO_2.md`](./MIGRATION_1_TO_2.md); [`CHANGELOG.md`](../CHANGELOG.md) lists what changed in each release.
>
> **Runtime backends.** Methods marked _(runtime)_ wait asynchronously and need a timer backend: enable `tokio` (the default) or `smol`. If both are enabled, tokio's timer is used. async-std is not supported (it is discontinued, RUSTSEC-2025-0052).
>
> **`no_std`.** With `std` off, the pure algorithm core &mdash; [`Backoff`](#backoff), `BackoffIter`, `Jitter`, and [`Decision`](#decision) &mdash; compiles without the standard library (including on targets without 64-bit atomics). Everything else needs `std`.

## Table of Contents

- [Overview](#overview)
- [Waiting, timeouts, and cancellation](#waiting-timeouts-and-cancellation)
- [`Throttle`](#throttle) &mdash; the token bucket
- [`Decision`](#decision) &mdash; the outcome of an attempt
- [`ThrottleError`](#throttleerror) &mdash; the domain error
- [`Limiter`](#limiter) &mdash; the composition trait
- [`Hybrid`](#hybrid) &mdash; must pass every constituent
- [`MultiLimiter`](#multilimiter) &mdash; named budgets, aliases, settle
- [`PerKey`](#perkey) &mdash; independent state per key
- [`Eviction`](#eviction) &mdash; per-key memory policy
- [`Layered`](#layered) &mdash; global / per-key / per-endpoint scopes
- [`SlidingWindowLog`](#slidingwindowlog) &mdash; the exact window
- [`Backoff`](#backoff) &mdash; delays with jitter
- [`Retry`](#retry) and [`RetryBudget`](#retrybudget) &mdash; guarded retries
- [`parse_retry_after`](#parse_retry_after)
- [`CircuitBreaker`](#circuitbreaker) &mdash; fail fast
- [`Bulkhead`](#bulkhead) &mdash; fixed concurrency
- [`Queue`](#queue) &mdash; bounded, deadline-aware waiting
- [`AdaptiveLimiter`](#adaptivelimiter) &mdash; learn the limit
- [`resilience`](#resilience) &mdash; `timeout`, `fallback`, `hedge`
- [Provider integration](#provider-integration) &mdash; headers, reconciliation, presets
- [`tower` integration](#tower-integration)
- [Observability](#observability)
- [Clocks](#clocks) &mdash; `ManualClock`, `TokioClock`
- [Feature flags](#feature-flags)

---

## Overview

throttle-net protects the services *you call* from being overwhelmed, and protects you from being banned by them. The defining operation is to **wait** for capacity, pacing your own requests, rather than reject a caller.

Token-bucket accounting comes from [`better-bucket`](https://crates.io/crates/better-bucket) (one atomic word; a stale or backward clock reading can only deny more) and time from [`clock-lib`](https://crates.io/crates/clock-lib). Every limiter has the same three-part shape:

- a **waiting** acquire &mdash; `acquire().await` _(runtime)_, plus a `*_timeout` variant;
- a **non-blocking** attempt &mdash; `try_acquire()` returning `bool`;
- a **non-consuming** check &mdash; `peek()` returning a [`Decision`](#decision).

Over any window, a token bucket's grants never exceed its capacity plus what its rate accrues in that window, under any number of concurrent callers. Composites hold that bound for each constituent, both for what is charged and for when the request is released.

---

## Waiting, timeouts, and cancellation

Every waiting acquire runs on one engine:

- **Reservations.** Token-bucket limiters *reserve* the units at once and sleep exactly until they have accrued. Later callers queue behind earlier reservations, so a large request is never starved by a stream of small ones. Limiters without native reservations (the sliding log, your own [`Limiter`](#limiter)) sleep for their reported wait and try again.
- **Composites queue on their bottleneck.** A `Hybrid`, `MultiLimiter`, or `Layered` waiter peeks every constituent, reserves on the one with the longest wait, and takes the others only when that reservation matures. If another constituent then needs a longer wait, the matured units are given back and the waiter moves to it &mdash; unless that wait is a few milliseconds, or the bottleneck has already moved four times, in which case it reserves the rest too. So a constituent shared with other callers (a `Layered` global scope, a `HybridBuilder::shared` limiter) is never charged long before the request can go, and one caller's backlog on its own slow scope cannot starve everyone else. (The four-switch fallback, which guarantees progress when two contended constituents keep overtaking each other, lets that one waiter hold at most its own cost in the matured constituent while it waits for the rest.) Arrival-order fairness is per limiter: a composite waiter competing with direct waiters that saturate *every* constituent progresses through that fallback and can wait long &mdash; measured over 20 s, 22 composite waiters on two saturated 50/s buckets got 44 grants (worst wait 9.2 s) while direct cost-3 waiters took 1,989 tokens.
- **Timeouts.** `acquire_timeout` / `acquire_with_cost_timeout` (and `acquire_owned_timeout`, `call_timeout`) take a deadline that bounds the *whole* wait, whatever the constituents answer. A token bucket returns [`ThrottleError::DeadlineExceeded`](#throttleerror) *immediately*, reserving nothing, when the wait is known up front to exceed the timeout. (A bulkhead's timeout reports `BulkheadFull`.)
- **Pauses.** A waiter sleeps out a limiter's pause &mdash; also when the pause is set after it reserved: it keeps the reservation and goes when the pause ends (failing with `DeadlineExceeded` at once if the pause outlasts its deadline). `resume()` wakes paused waiters immediately. The resume signal is process-wide, so any `resume()` also makes a waiter that is between its check and its sleep look once more (harmless; avoid calling `resume` in a hot loop).
- **Cancel safety.** Dropping a waiting future (a losing `select!` branch, an outer `tokio::time::timeout`) returns whatever it held &mdash; a reservation, a composite's bottleneck units, a circuit breaker's half-open trial slot (also when the breaker is a composite's constituent) &mdash; and holds no bulkhead or adaptive slot.

```rust
# async fn run() {
use std::time::Duration;
use throttle_net::{Throttle, ThrottleError};

// One token per hour, already spent.
let throttle = Throttle::per_duration(1, Duration::from_secs(3600));
assert!(throttle.try_acquire());

// Known up front not to fit a one-second budget: fails at once.
let err = throttle.acquire_timeout(Duration::from_secs(1)).await.unwrap_err();
assert_eq!(err, ThrottleError::DeadlineExceeded);

// Cancelling a waiter (here by an outer timeout) returns its reservation.
let _ = tokio::time::timeout(Duration::from_millis(5), throttle.acquire()).await;
assert_eq!(throttle.reserve(1, Duration::MAX).map(|w| w > Duration::from_secs(3590)), Some(true));
# }
```

---

## `Throttle`

A single token bucket. `Throttle<C: Clock = SystemClock>`.

### Constructors

| Item | Meaning |
|------|---------|
| `Throttle::per_second(rate)` | `rate` per second, burst `rate`. `0` grants nothing. |
| `Throttle::per_minute(rate)` | `rate` per minute, burst `rate`. |
| `Throttle::per_duration(amount, period)` | `amount` per `period`, burst `amount`. A zero amount or period grants nothing. |
| `Throttle::smooth(amount, period)` | GCRA-style pacing: burst of one, grants spaced `period / amount` apart. |
| `Throttle::builder()` | A [`ThrottleBuilder`](#throttlebuilder). |
| `.with_clock(clock)` | The same configuration on another [`Clock`](#clocks) (rebuilt with its initial fill). |
| `.with_name(name)` / `.name()` | The instance name used to label [metrics and events](#observability). |

```rust
use std::time::Duration;
use throttle_net::Throttle;

assert_eq!(Throttle::per_second(50).capacity(), 50);
assert_eq!(Throttle::per_minute(600).capacity(), 600);
assert_eq!(Throttle::per_duration(5, Duration::from_millis(100)).capacity(), 5);

let smooth = Throttle::smooth(10, Duration::from_secs(1)); // one every 100ms
assert!(smooth.try_acquire());
assert!(!smooth.try_acquire());
```

### `ThrottleBuilder`

`rate(amount, period)`, `per_second(amount)`, `per_minute(amount)`, `burst(capacity)`, `initial(tokens)`, `name(name)`, `clock(clock)`, then `try_build()` (returns [`ThrottleError::InvalidConfig`](#throttleerror) for a zero rate, period, or burst) or `build()` (an unusable configuration grants nothing).

```rust
use std::sync::Arc;
use std::time::Duration;
use throttle_net::{ManualClock, Throttle, ThrottleError};

let clock = Arc::new(ManualClock::new());
let throttle = Throttle::builder()
    .per_minute(1_000) // sustained 1000/min
    .burst(100)        // never more than 100 at once
    .initial(0)        // start empty
    .clock(clock.clone())
    .try_build()?;
assert_eq!(throttle.available(), 0);
clock.advance(Duration::from_secs(6));
assert_eq!(throttle.available(), 100);

assert!(matches!(
    Throttle::builder().per_second(5).burst(0).try_build(),
    Err(ThrottleError::InvalidConfig { .. })
));
# Ok::<(), ThrottleError>(())
```

### Waiting _(runtime)_

| Method | Returns |
|--------|---------|
| `acquire()` / `acquire_with_cost(cost)` | `Result<(), ThrottleError>` once the tokens are reserved and accrued. |
| `acquire_timeout(t)` / `acquire_with_cost_timeout(cost, t)` | The same, or `DeadlineExceeded`. |

Errors: `CostExceedsCapacity` when `cost` exceeds the capacity (it can never be granted); `DeadlineExceeded` for the timeout variants.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::Throttle;

let throttle = Throttle::per_second(100);
throttle.acquire().await?;
throttle.acquire_with_cost(25).await?;
# Ok(())
# }
```

### Non-blocking, reservations, and reconciliation

| Method | Meaning |
|--------|---------|
| `try_acquire()` / `try_acquire_with_cost(cost)` | Take now or not at all (`bool`). |
| `peek(cost)` | What a grant would do now, consuming nothing; the `Retry` wait is exact (to the millisecond). |
| `available()` / `capacity()` | Tokens now (`0` while paused) / the burst. |
| `reserve(cost, max_wait)` | Take now, ahead of accrual; `Some(wait)` before use, or `None` taking nothing. |
| `refund(cost)` | Give back units that were never used (also pays down a reservation). |
| `drain_to(remaining)` | Take tokens until at most `remaining` are left; never adds. |
| `pause_for(d)` / `resume()` / `paused_for()` | Grant nothing for `d` (tokens keep accruing); lift early; time left. |

```rust
use std::sync::Arc;
use std::time::Duration;
use throttle_net::{Decision, ManualClock, Throttle};

let clock = Arc::new(ManualClock::new());
let throttle = Throttle::per_second(10).with_clock(clock.clone());

assert!(throttle.try_acquire_with_cost(10));
assert_eq!(throttle.peek(5), Decision::Retry { after: Duration::from_millis(500) });

// Reserve the next 5 tokens; a later caller queues behind them.
assert_eq!(throttle.reserve(5, Duration::from_secs(1)), Some(Duration::from_millis(500)));
assert_eq!(throttle.reserve(5, Duration::from_secs(1)), Some(Duration::from_secs(1)));
throttle.refund(5); // the second one was cancelled

throttle.pause_for(Duration::from_secs(30)); // the server said "come back in 30s"
assert!(!throttle.try_acquire());
clock.advance(Duration::from_secs(30));
assert!(throttle.try_acquire());
```

---

## `Decision`

```text
#[non_exhaustive]
pub enum Decision {
    Acquired,                    // granted (or would be, for a peek)
    Retry { after: Duration },   // not now; the wait until it could be
    Impossible,                  // never: the cost exceeds every capacity it must fit
}
```

Helpers: `is_acquired()`, `retry_after()`.

```rust
use std::time::Duration;
use throttle_net::Decision;

let d = Decision::Retry { after: Duration::from_millis(20) };
assert_eq!(d.retry_after(), Some(Duration::from_millis(20)));
assert!(Decision::Acquired.is_acquired());
assert!(!Decision::Impossible.is_acquired());
```

---

## `ThrottleError`

A `Copy`, `#[non_exhaustive]` enum implementing `std::error::Error` and `error_forge::ForgeError` (kind and retryability).

| Variant | Meaning | Retryable |
|---------|---------|-----------|
| `CostExceedsCapacity { cost, capacity }` | The cost can never be granted. | no |
| `CircuitOpen { retry_after }` | A breaker shed the request. | yes |
| `QueueFull` | A full queue rejected or evicted the waiter. | yes |
| `DeadlineExceeded` | The timeout or deadline passed (or would have) first. | no |
| `BulkheadFull` | No bulkhead slot within the timeout. | yes |
| `InvalidConfig { reason }` | A builder's `try_build` rejected the configuration. | no |
| `UnknownDimension { index }` | A strict `MultiLimiter` was charged an unknown name. | no |
| `NotReady` | A `tower` service was called before `poll_ready` reported it ready; the request was not sent. | no |

```rust
# async fn run() {
use error_forge::ForgeError;
use throttle_net::{Throttle, ThrottleError};

let err = Throttle::per_second(5).acquire_with_cost(9).await.unwrap_err();
assert_eq!(err, ThrottleError::CostExceedsCapacity { cost: 9, capacity: 5 });
assert!(!err.is_retryable());
# }
```

---

## `Limiter`

The object-safe composition trait (`Send + Sync`), implemented by `Throttle`, `SlidingWindowLog`, `Hybrid`, `CircuitBreaker`, and `AdaptiveLimiter` &mdash; and by `&L`, `Box<L>`, and `Arc<L>` for any limiter `L` (sized or `dyn`), forwarding every method, so a shared limiter is passed as is.

```text
pub trait Limiter: Send + Sync {
    fn peek(&self, cost: u32) -> Decision;          // non-consuming
    fn acquire_cost(&self, cost: u32) -> Decision;  // consuming, never blocks
    fn available(&self) -> u32;
    fn capacity(&self) -> u32;
    // Provided, with conservative defaults:
    fn refund(&self, cost: u32) {}                                        // default: keep units spent
    fn reserve(&self, cost: u32, max_wait: Duration) -> Option<Duration>; // default: only what is there now
    fn pause_for(&self, duration: Duration);                              // default: drain what is available
    fn paused_for(&self) -> Duration;                                     // default: ZERO
    fn resume(&self) {}                                                   // default: nothing
    fn charge(&self, cost: u32) -> u32;                                   // units already spent; returns the uncharged rest
}
```

Composites peek every constituent and commit only when all would grant; a constituent that loses a race between the peek and its commit causes the ones already charged to be refunded. `charge` records units that were already spent downstream even when they do not fit and even while paused: a token bucket takes them as debt that later callers wait behind.

```rust
use throttle_net::{Decision, Limiter, Throttle};

fn drain(limiter: &dyn Limiter) -> u32 {
    let mut granted = 0;
    while limiter.acquire_cost(1) == Decision::Acquired {
        granted += 1;
    }
    granted
}
assert_eq!(drain(&Throttle::per_second(8)), 8);
```

---

## `Hybrid`

Several limiters a request must *all* satisfy. Build with `Hybrid::builder().limiter(l).shared(arc).name(n).build()`.

| Method | Meaning |
|--------|---------|
| `try_acquire()` / `try_acquire_with_cost(cost)` | All-or-nothing across constituents. |
| `peek(cost)` | The longest constituent wait, or `Impossible`. |
| `acquire*` / `acquire_with_cost_timeout` _(runtime)_ | Queue on the bottleneck constituent, take the rest when it matures; cancel-safe. |
| `pause_for(d)` / `resume()` / `paused_for()` | Pause every constituent / lift every pause / the longest pause left. |
| `len()` / `is_empty()` / `name()` | |
| `Limiter` impl | `capacity`/`available` report the binding constituent; an empty hybrid is unbounded. |

```rust
use std::time::Duration;
use throttle_net::{Hybrid, SlidingWindowLog, Throttle};

// 10 per second, and no more than 100 per minute, exactly.
let hybrid = Hybrid::builder()
    .limiter(Throttle::per_second(10))
    .limiter(SlidingWindowLog::new(100, Duration::from_secs(60)))
    .build();
assert!(hybrid.try_acquire());
```

---

## `MultiLimiter`

Named, independently metered dimensions; a call is admitted only when every dimension can afford its share.

Builder: `dimension(name, limiter)`, `shared(name, arc)`, `alias(alias, target)`, `strict(bool)` (default `true`), `name(n)`, then `build()` (keeps mistakes: duplicate names are charged independently, a dangling alias never matches) or `try_build()` (rejects them with `InvalidConfig`).

| Method | Meaning |
|--------|---------|
| `try_acquire_costs(&[(name, cost)])` | All-or-nothing. Repeated names and aliases are **summed** into their dimension. |
| `peek_costs(costs)` | Non-consuming. |
| `acquire_costs(costs)` / `acquire_costs_timeout(costs, t)` _(runtime)_ | Waiting, cancel-safe. |
| `settle_costs(estimated, actual)` | Refund an underspend, charge an overspend as debt (LLM output tokens), even while paused. |
| `refund_costs(costs)` | Give back a call that was charged but never sent. |
| `drain_to(name, remaining)` / `pause_for(d)` / `resume()` / `paused_for()` | Reconcile with a downstream. |
| `available(name)` / `dimension(name)` / `dimensions()` | Inspection (aliases resolve to their dimension). |
| `with_strict(bool)` / `is_strict()` / `with_name(n)` / `name()` | |

Unknown names are refused by default (`Decision::Impossible`, or `ThrottleError::UnknownDimension { index }` from the waiting methods), so a typo cannot charge nothing and slip through. `strict(false)` restores the 1.x behaviour of ignoring them.

```rust
use std::sync::Arc;
use throttle_net::{ManualClock, MultiLimiter, Throttle};

let clock = Arc::new(ManualClock::new());
// OpenAI-style: one combined token budget, charged as input or output tokens.
let limiter = MultiLimiter::builder()
    .dimension("requests", Throttle::per_minute(500).with_clock(clock.clone()))
    .dimension("tokens", Throttle::per_minute(30_000).with_clock(clock.clone()))
    .alias("input_tokens", "tokens")
    .alias("output_tokens", "tokens")
    .try_build()?;

// Admit on the max_tokens ceiling, then settle with the reported usage.
let estimate = [("requests", 1), ("input_tokens", 1_200), ("output_tokens", 1_000)];
assert!(limiter.try_acquire_costs(&estimate));
limiter.settle_costs(&estimate, &[("requests", 1), ("input_tokens", 1_200), ("output_tokens", 340)]);
assert_eq!(limiter.available("tokens"), Some(30_000 - 1_540));

assert!(!limiter.try_acquire_costs(&[("reqeusts", 1)])); // strict: a typo is refused
# Ok::<(), throttle_net::ThrottleError>(())
```

---

## `PerKey`

`PerKey<K, C = SystemClock>`: one token bucket per key (`K: Eq + Hash + Clone + Send + Sync + 'static`) in a sharded store with bounded memory.

| Item | Meaning |
|------|---------|
| `per_second(rate)` / `per_minute(rate)` / `per_duration(amount, period)` | Same rate for every key. |
| `PerKey::builder()` | `rate`/`per_second`/`per_minute`, `burst`, `initial`, `eviction`, `shards`, `name`, `clock`, `build`/`try_build`. |
| `with_clock` / `with_eviction` / `with_shards` | Rebuild the store (empty). |
| `try_acquire(&key)` / `try_acquire_with_cost(&key, cost)` | Creates the key's bucket on first sight. |
| `peek(&key, cost)` / `available(&key)` | Non-consuming; an unseen key is judged as a fresh bucket (carrying any debt it left in the eviction ledger) and is not created. |
| `reserve(&key, cost, max_wait)` / `refund(&key, cost)` | As on `Throttle`. |
| `reset(&key)` / `remove(&key)` / `clear()` | Restore a key's allowance / forget it / forget all. |
| `acquire*` / `acquire_with_cost_timeout` _(runtime)_ | Waiting, reservation-ordered per key, cancel-safe. |
| `capacity()` / `len()` / `is_empty()` / `shard_count()` / `eviction()` / `name()` | |
| `MAX_SHARDS` | 65,536, the most shards a store creates. |

An existing key's acquire takes a shard read lock, one clock read, and one atomic compare-and-swap.

```rust
use std::sync::Arc;
use std::time::Duration;
use throttle_net::{ManualClock, PerKey};

let clock = Arc::new(ManualClock::new());
let limiter = PerKey::<&str>::per_second(2).with_clock(clock.clone());
assert!(limiter.try_acquire(&"a"));
assert!(limiter.try_acquire(&"a"));
assert!(!limiter.try_acquire(&"a"));
assert!(limiter.try_acquire(&"b")); // independent
clock.advance(Duration::from_secs(1));
assert!(limiter.try_acquire(&"a"));
```

---

## `Eviction`

How a `PerKey` store bounds its memory. `Eviction::capacity(max_keys)`, `idle(ttl)` (keeps the default cap), `new(max_keys, ttl)`, `unbounded()`, `with_capacity`, `with_idle`, `without_capacity`, `with_when_full(WhenFull)`, `max_keys()`, `idle_ttl()`, `when_full()`; the default is a cap of `DEFAULT_MAX_KEYS` (1,048,576), no TTL, and `WhenFull::RefuseNew`.

- **A limited key never regains a fresh allowance.** A key whose bucket is full again is forgotten freely (that loses nothing).
- **Capacity: CLOCK, recovered keys first, then the debt ledger.** A new key in a full shard evicts a recovered key found within a 32-slot scan, preferring one not used recently. If none of the scanned keys has recovered, a limited key is evicted but its debt (when it will be full again) goes into the shard's ledger &mdash; 16 bytes keyed by the key's hash &mdash; and the key is re-created in that debt if it returns. The ledger holds as many debts as the shard holds keys.
- **When keys and ledger are both full of limited keys**, `WhenFull` decides: `RefuseNew` (default) refuses the new key (its `try_*` returns `false`; its waiting acquire waits until room is expected), so every limit holds; `ForgetDebt` admits it by forgetting the debt that settles soonest (a bounded bypass, traded for availability).
- **Idle TTL: incremental, recovered keys only.** Each insert checks the next 8 slots for keys idle past the TTL *whose bucket has recovered*; a key in debt (behind its own reservations) is kept until it is full again. The effective TTL is never shorter than a bucket's time to refill completely.
- **Bounded work.** Every insert does a fixed amount of work however large the store; shards are sized so none holds more than 16,384 keys of the cap.

Locking newcomers out takes a flood that keeps every key slot *and* every ledger slot filled with drained keys: about `2 × max_keys / T` new keys per second, where `T` is how long a drained key takes to recover. At the default cap that is ~2.1 M keys/s for `per_minute(60)` keys drained of one token, ~35 k keys/s for `per_minute(100)` keys drained of all 100, and ~580 keys/s for `per_duration(1000, 1h)` keys drained of all 1,000 &mdash; the exposed case; size the cap above your real key cardinality, or choose `ForgetDebt`.

```rust
use std::time::Duration;
use throttle_net::{DEFAULT_MAX_KEYS, Eviction, PerKey};

let policy = Eviction::capacity(100_000).with_idle(Duration::from_secs(300));
let limiter: PerKey<String> = PerKey::per_second(100).with_eviction(policy);
assert_eq!(limiter.eviction().max_keys(), Some(100_000));
assert_eq!(Eviction::default().max_keys(), Some(DEFAULT_MAX_KEYS));
```

---

## `Layered`

`Layered<K, E = K>`: an optional global [`Limiter`](#limiter), an optional per-key `PerKey<K>`, and an optional per-endpoint `PerKey<E>`, all of which a request must clear. Builder: `global(l)`, `shared_global(arc)`, `per_key(perkey)`, `per_endpoint(perkey)`, `name(n)`.

Methods: `try_acquire(&key, &endpoint)`, `try_acquire_with_cost`, `peek`, `refund`, `pause_for` / `resume` / `paused_for` (the global scope), `capacity`, `name`; _(runtime)_ `acquire`, `acquire_with_cost`, `acquire_with_cost_timeout`. A waiter queues on its tightest scope (usually its own per-key budget) and charges the global scope only when it can go, so one tenant's backlog never drains the global budget the others share.

```rust
use throttle_net::{Layered, PerKey, Throttle};

let layered = Layered::<&str>::builder()
    .global(Throttle::per_second(2))
    .per_key(PerKey::per_second(1))
    .build();
assert!(layered.try_acquire(&"a", &"/x"));
assert!(!layered.try_acquire(&"a", &"/x")); // a's own scope is empty
assert!(layered.try_acquire(&"b", &"/x"));
```

---

## `SlidingWindowLog`

At most `limit` units in any trailing `window`, exactly (no boundary burst). Time is tracked to the nanosecond, so sub-millisecond windows are exact; a zero window is a per-instant limit.

`new(limit, window)`, `per_second(rate)`, `per_minute(rate)`, `with_clock`, `with_name`; `try_acquire`, `try_acquire_with_cost`, `peek`, `available`, `capacity`, `window`, `refund` (removes the oldest units, never more permissive than the exact refund), `pause_for`, `resume` (wakes paused waiters), `paused_for`; _(runtime)_ `acquire`, `acquire_with_cost`, `acquire_with_cost_timeout`. It does not reserve, so under sustained contention a large cost can wait behind smaller ones; in a composite it is charged when the request goes, not while it waits on a slower constituent.

```rust
use std::sync::Arc;
use std::time::Duration;
use throttle_net::{ManualClock, SlidingWindowLog};

let clock = Arc::new(ManualClock::new());
let limiter = SlidingWindowLog::new(2, Duration::from_micros(500)).with_clock(clock.clone());
assert!(limiter.try_acquire());
assert!(limiter.try_acquire());
assert!(!limiter.try_acquire());
clock.advance(Duration::from_micros(500));
assert!(limiter.try_acquire());
```

---

## `Backoff`

A delay policy: `constant(d)`, `linear(initial, increment)`, `exponential(initial, factor)`, then `with_max(ceiling)` (default 30s; `Duration::MAX` allowed), `with_jitter(Jitter)`, `with_seed(entropy)`. `iter()` / `&backoff` (`IntoIterator`) start an independent `BackoffIter`; `iter_seeded(seed)` a reproducible one. `Backoff::default()` is exponential from 100ms, doubling, capped at 30s, with decorrelated jitter. No configuration (NaN or negative factors, an unbounded ceiling) can make it panic or exceed its ceiling.

| `Jitter` | Delay |
|----------|-------|
| `None` | The capped curve. |
| `Full` | Uniform in `[0, delay]`. |
| `Equal` | `delay/2 + rand(0, delay/2)`. |
| `Decorrelated` (default) | `min(max, rand(base, previous * 3))`; uses only the base and the ceiling, not the curve kind or factor. |

```rust
use std::time::Duration;
use throttle_net::{Backoff, Jitter};

let backoff = Backoff::exponential(Duration::from_millis(100), 2.0)
    .with_max(Duration::from_secs(5))
    .with_jitter(Jitter::Full);
for delay in (&backoff).into_iter().take(10) {
    assert!(delay <= Duration::from_secs(5));
}
```

---

## `Retry`

`Retry::new(backoff)` then `max_attempts(n)` (default 5, counting the first), `respect_retry_after(bool)` (default on), `max_retry_after(cap)` (default 5 minutes), `max_elapsed(budget)` (off by default), `name(impl Into<Cow<'static, str>>)` (`Retry` is `Clone`). Accessors: `attempts()`, `backoff()`, `retry_after_cap()`, `elapsed_budget()`.

_(runtime)_ `run(operation, classify)` and `run_with_budget(&budget, operation, classify)` call `operation` until it succeeds or a guardrail stops them, returning the last error. `classify` returns a `RetryAction`: `Retry`, `RetryAfter(delay)`, or `GiveUp`. A `RetryAfter` beyond `max_retry_after` ends the loop instead of sleeping; a wait that would cross `max_elapsed` is not taken. `retry_if_retryable` classifies an `error_forge::ForgeError` by its own retryability.

```rust
# async fn run() {
use std::time::Duration;
use throttle_net::{Backoff, Retry, RetryAction, parse_retry_after};

struct Rejected { retry_after: Option<&'static str> }

let retry = Retry::new(Backoff::default())
    .max_attempts(4)
    .max_retry_after(Duration::from_secs(60))
    .max_elapsed(Duration::from_secs(30));
let result: Result<(), Rejected> = retry
    .run(
        || async { Err(Rejected { retry_after: Some("1") }) },
        |err: &Rejected| match err.retry_after.and_then(parse_retry_after) {
            Some(after) => RetryAction::RetryAfter(after),
            None => RetryAction::Retry,
        },
    )
    .await;
assert!(result.is_err());
# }
```

### `RetryBudget`

A retry allowance shared by many retry loops (finagle/tower style): each request earns `ratio` retries, the budget also accrues `min_per_second`, and each retry spends one. `RetryBudget::new(ratio, min_per_second)`, `with_max_balance(retries)`, `with_clock(clock)`, `deposit()`, `try_withdraw()`, `balance()`.

```rust
use throttle_net::RetryBudget;

let budget = RetryBudget::new(0.5, 0); // one retry per two requests, no floor
budget.deposit();
budget.deposit();
assert!(budget.try_withdraw());
assert!(!budget.try_withdraw());
```

---

## `parse_retry_after`

`parse_retry_after(value)` / `parse_retry_after_at(value, now_unix_secs)`: the seconds form and the three HTTP-date forms (RFC 9110), as a delay from now; a past date is `Duration::ZERO`, malformed input `None`.

```rust
use std::time::Duration;
use throttle_net::{parse_retry_after, parse_retry_after_at};

assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(120)));
let when = "Thu, 01 Jan 2026 00:00:00 GMT";
assert_eq!(parse_retry_after_at(when, 1_767_225_540), Some(Duration::from_secs(60)));
assert_eq!(parse_retry_after("soon"), None);
```

---

## `CircuitBreaker`

`CircuitBreaker<L: Limiter, C = SystemClock>` (feature `circuit-breaker`). Builder: `trip(Trip)`, `cooldown(d)`, `half_open(trials, required)`, `name(impl Into<Cow<'static, str>>)`, `build(limiter)`.

| `Trip` | Opens when |
|--------|-----------|
| `Consecutive(n)` | `n` failures in a row. |
| `Ratio { window, ratio, min_calls }` | The failure ratio over the last `window` calls reaches `ratio`, after `min_calls`. |
| `Windowed { failures, period }` | `failures` failures within a rolling `period`. |

Out-of-range parameters are normalised (zero counts become one; `ratio` is clamped, NaN meaning "every call failed").

| Method | Meaning |
|--------|---------|
| `try_acquire()` | `Ok(Some(Permit))`, `Ok(None)` (rate-limited), or `Err(CircuitOpen \| CostExceedsCapacity)`. |
| `acquire()` / `acquire_timeout(t)` _(runtime)_ | Shed at once while open; otherwise wait on the limiter. A waiter whose queued wait outlives the state it was admitted in is shed (`CircuitOpen`) if the breaker has opened meanwhile. Cancel-safe. |
| `try_acquire_owned()` / `acquire_owned()` / `acquire_owned_timeout(t)` | `OwnedPermit`, which can move into a spawned task (on `Arc<CircuitBreaker>`). |
| `call(operation)` / `call_timeout(t, operation)` _(runtime)_ | Admit (waiting at most `t` for `call_timeout`), run, settle from the `Result`: `Err(CallError::Rejected(e))` or `Err(CallError::Failed(e))`. |
| `record_success()` / `record_failure()` | Report a call made without a permit. While half-open, frees a trial slot only if one is held by a trial admitted through the `Limiter` trait. |
| `reset()` / `state()` / `inner()` / `with_clock` / `with_name` / `name()` | |
| `Limiter` impl | Composes in a `Hybrid` or as a `Layered` global scope. A half-open trial taken through the trait is released by `refund` (a composite's rollback or cancelled wait), so the breaker never wedges half-open. A refund carries no identity: one for units admitted while the breaker was closed, arriving during a later half-open period, frees a live trial's slot, so one extra trial may run until that trial settles. Permits are exact. |

Settle a `Permit` with `success()` or `failure()`, or give it back with `cancel()` when the call never reached the downstream (no outcome is recorded; the trial slot and the limiter's unit are returned). Dropping it unsettled counts as a **failure** &mdash; so hedged or `select!`ed calls should `cancel()` their losers. Permits carry the breaker's state generation, so a slow call admitted before a transition never counts toward the state after it.

```rust
# async fn run() {
use std::time::Duration;
use throttle_net::{CallError, CircuitBreaker, Throttle, Trip};

let breaker = CircuitBreaker::builder()
    .trip(Trip::Consecutive(1))
    .cooldown(Duration::from_secs(10))
    .build(Throttle::per_second(100));

let failed: Result<(), CallError<&str>> = breaker.call(|| async { Err("503") }).await;
assert!(matches!(failed, Err(CallError::Failed("503"))));
let shed: Result<(), CallError<&str>> = breaker.call(|| async { Ok(()) }).await;
assert!(matches!(shed, Err(CallError::Rejected(_))));
# }
```

---

## `Bulkhead`

A fixed cap on concurrent calls. `Bulkhead::new(limit)`, `with_name`, `limit()`, `in_use()`, `available()`, `try_acquire()` / `try_acquire_owned()` (a slot as a `BulkheadPermit` / `OwnedBulkheadPermit`, released on drop); _(runtime)_ `acquire()`, `acquire_timeout(t)` (`BulkheadFull` on timeout), `acquire_owned()`, `acquire_owned_timeout(t)`, `call(operation)`, `call_timeout(t, operation)` (the timeout bounds the wait for a slot), `try_call(operation)`. Every freed slot wakes one more waiter.

```rust
use throttle_net::Bulkhead;

let bulkhead = Bulkhead::new(2);
let a = bulkhead.try_acquire().expect("slot");
let _b = bulkhead.try_acquire().expect("slot");
assert!(bulkhead.try_acquire().is_none());
drop(a);
assert_eq!(bulkhead.available(), 1);
```

---

## `Queue`

`Queue<L: Limiter, K = (), C = SystemClock>` _(runtime)_: up to `capacity` waiters, served by priority, then least-recently-served key, then arrival. Builder: `capacity(n)` (default 1024), `overflow(Overflow)`, `name(impl Into<Cow<'static, str>>)`, `build(limiter)`. `Overflow::{Reject, DropOldest, DropLowestPriority}`.

`acquire(key, priority, deadline)` / `acquire_with_cost(key, priority, deadline, cost)` return `QueueFull`, `DeadlineExceeded`, or `CostExceedsCapacity`. Also `len`, `is_empty`, `capacity`, `overflow`, `inner`, `with_clock`, `with_name`, `name`. Scheduling is O(log n), only the new turn-holder is woken, and per-key fairness state is bounded by the capacity.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use std::time::Duration;
use throttle_net::{Overflow, Queue, Throttle};

let queue: Queue<Throttle, &str> = Queue::builder()
    .capacity(100)
    .overflow(Overflow::DropOldest)
    .build(Throttle::per_second(50));
queue.acquire("tenant:1", 0, Some(Duration::from_secs(2))).await?;
# Ok(())
# }
```

---

## `AdaptiveLimiter`

`AdaptiveLimiter<S: AdaptiveStrategy, C = SystemClock>` (feature `adaptive`): an in-flight limit between a floor and a ceiling, moved by outcomes. Builder: `floor`, `ceiling`, `initial`, `name(impl Into<Cow<'static, str>>)`, `build(strategy)`. Strategies: `Aimd::new(increase, decrease)` and `Vegas::new(alpha, beta).with_probe_interval(samples)`, or your own `AdaptiveStrategy`.

Every update is a compare-and-swap, a failure lowers the limit at most once per congestion event (only if its request started after the last decrease), and Vegas re-probes its no-load latency.

| Method | Meaning |
|--------|---------|
| `try_acquire()` / `try_acquire_owned()` | An `AdaptivePermit` / `OwnedAdaptivePermit`; settle with `success()` / `failure()` (drop = failure), or `cancel()` to free the slot with no outcome. |
| `acquire()` / `acquire_timeout(t)` / `acquire_owned()` / `acquire_owned_timeout(t)` _(runtime)_ | Wait for a slot; cancel-safe. Every freed slot (and every slot the limit grows by) wakes one more waiter. |
| `release(outcome)` | Settle a slot taken through the `Limiter` trait. |
| `current_limit()` / `in_flight()` / `floor()` / `ceiling()` / `with_clock` / `with_name` / `name()` | |

```rust
use throttle_net::{AdaptiveLimiter, Aimd};

let limiter = AdaptiveLimiter::builder()
    .floor(2)
    .ceiling(50)
    .initial(10)
    .build(Aimd::default());
let permit = limiter.try_acquire().expect("a slot");
permit.failure();
assert_eq!(limiter.current_limit(), 5);
```

---

## `resilience`

Runtime-agnostic combinators _(runtime)_:

- `timeout(duration, future) -> Result<T, Elapsed>` &mdash; `Elapsed` converts into `ThrottleError::DeadlineExceeded`.
- `fallback(primary, |err| async { .. }) -> T` &mdash; a degraded answer on error.
- `hedge(delay, || attempt) -> Result<T, E>` &mdash; a second attempt if the first is slow; the first success wins. The loser is dropped: through a circuit breaker, `cancel()` its permit rather than let the drop count as a failure.

```rust
# async fn run() {
use std::time::Duration;
use throttle_net::resilience::{fallback, timeout};

assert_eq!(timeout(Duration::from_secs(1), async { 1 }).await, Ok(1));
let value = fallback(async { Err::<u32, &str>("down") }, |_| async { 0 }).await;
assert_eq!(value, 0);
# }
```

---

## Provider integration

### Header parsing (feature `provider-headers`)

`HeaderProfile::{OPENAI, ANTHROPIC, GITHUB, RFC, STRIPE, AWS}` parse `&[(&str, &str)]` headers (case-insensitive) with `parse(headers)` or `parse_at(headers, now_unix_secs)` into a `RateLimitInfo { requests, tokens, retry_after }` of `Window { limit, remaining, reset }`. Reset formats: delta seconds, Go durations (`1s`, `6m0s`, `1.5s`, `20ms`), Unix timestamps, and RFC 3339 instants with `Z` or a numeric offset. Malformed values are dropped, never a panic.

### Reconciliation

- `Window::apply(&limiter)` drains any `Limiter` to `remaining` and, when `remaining` is `0` and a reset is known, pauses it until the reset.
- `RateLimitInfo::sync_requests(&limiter)` / `sync_tokens(&limiter)` apply one window and pause for a `Retry-After`. Any `Limiter` works, including an `Arc<Throttle>` passed as `&arc`.
- `RateLimitInfo::sync_multi(&multi)` reconciles a whole `MultiLimiter`: requests to `requests`, tokens to `tokens` (or each of `input_tokens` / `output_tokens`), `Retry-After` to every dimension.

Reconciliation only ever reduces the local budget. Every pause it sets comes from the server, so it is clamped to `provider::DEFAULT_MAX_PAUSE` (five minutes): a hostile `Retry-After: 999999999` pauses for five minutes, not 31 years. `Window::apply_capped`, `sync_requests_capped`, `sync_tokens_capped`, and `sync_multi_capped` take your own ceiling (`Duration::ZERO` drains without pausing).

```rust
use std::sync::Arc;
use std::time::Duration;
use throttle_net::provider::HeaderProfile;
use throttle_net::{ManualClock, Throttle};

let clock = Arc::new(ManualClock::new());
let throttle = Throttle::per_minute(60).with_clock(clock.clone());
let headers = [
    ("x-ratelimit-remaining-requests", "0"),
    ("x-ratelimit-reset-requests", "20s"),
];
let info = HeaderProfile::OPENAI.parse(&headers);
assert_eq!(info.sync_requests(&throttle), 60);
assert_eq!(throttle.paused_for(), Duration::from_secs(20));
```

### LLM presets (feature `provider-llm`)

`presets::anthropic::{tier_1, tier_2, tier_4}` (separate `input_tokens` / `output_tokens`) and `presets::openai::{tier_1, tier_2}` (one combined `tokens` budget with `input_tokens` / `output_tokens` aliases). The figures are illustrative; confirm against your account and keep in step with `sync_multi`.

```rust
use throttle_net::presets;

let limiter = presets::openai::tier_1();
assert!(limiter.try_acquire_costs(&[("requests", 1), ("input_tokens", 1_000), ("output_tokens", 500)]));
assert_eq!(limiter.available("tokens"), Some(28_500));
```

---

## `tower` integration

Feature `tower` (only `tower-layer` and `tower-service`). `ThrottleLayer::new(limiter)` / `shared(arc)` / `with_cost(n)` paces calls; `CircuitBreakerLayer::new(Arc<CircuitBreaker>)` sheds while open and settles each call from its `Result`; `AdaptiveLayer::new(Arc<AdaptiveLimiter>)` caps in-flight calls and feeds outcomes (and the call's latency) back. Each has `with_timeout(t)`.

Each service acquires its admission in **`poll_ready`** &mdash; then polls the inner service's readiness &mdash; and spends it in `call`, like tower's `RateLimit`: `load_shed`, `buffer`, and balancers see real backpressure, and an inner layer whose readiness reserves capacity (a `ConcurrencyLimit` permit, a pooled connection) is not held through the wait. A service dropped (or cloned) while ready gives its admission back; one called without being ready fails with `ThrottleError::NotReady`. Errors are boxed (`tower::BoxError`).

```rust
# async fn run() -> Result<(), throttle_net::tower::BoxError> {
use tower::{ServiceBuilder, ServiceExt, service_fn};
use throttle_net::Throttle;
use throttle_net::tower::ThrottleLayer;

let service = ServiceBuilder::new()
    .layer(ThrottleLayer::new(Throttle::per_second(100)))
    .service(service_fn(|n: u32| async move { Ok::<_, std::convert::Infallible>(n + 1) }));
assert_eq!(service.oneshot(1).await?, 2);
# Ok(())
# }
```

---

## Observability

With `metrics` (the 0.24 facade) and/or `tracing`, every public acquire path and state transition reports, labelled with the instance `name` (`"default"` when unset) and, where several kinds share a metric, the `limiter` kind. Both are zero-cost when off.

| Metric | Kind | Labels | Meaning |
|--------|------|--------|---------|
| `throttle_acquired_total` | counter | `limiter`, `name` | Grants (waiting and non-waiting). |
| `throttle_denied_total` | counter | `limiter`, `name` | Non-waiting attempts refused. |
| `throttle_timeouts_total` | counter | `limiter`, `name` | Waiting acquires that hit their timeout. |
| `throttle_wait_duration` | histogram (s) | `limiter`, `name` | Time spent in a waiting acquire. |
| `throttle_shed_total` | counter | `limiter`, `name` | Calls a breaker or bulkhead turned away. |
| `throttle_circuit_state` | gauge | `name` | 0 closed, 1 half-open, 2 open; set at construction. |
| `throttle_queue_depth` | gauge | `name` | Waiters in a queue. |
| `throttle_queue_rejected_total` | counter | `name`, `policy` | Waiters a full queue rejected or evicted. |
| `throttle_rate_current` | gauge | `name` | An adaptive limiter's limit; set at construction. |
| `throttle_retries_total` | counter | `name` | Retries scheduled. |
| `throttle_retry_budget_exhausted_total` | counter | `name` | Retries a budget refused. |

Composites report for themselves, not for their constituents. Tracing events use the target `throttle_net`: grants and denials at `TRACE`, waiting acquires and sheds at `DEBUG`, breaker transitions at `INFO`, queue overflows and exhausted retry budgets at `WARN`.

---

## Clocks

Every limiter reads an injectable `Clock` (re-exported from `clock-lib`): `SystemClock` by default, `ManualClock` for deterministic tests (advance it by hand), and `TokioClock` (feature `tokio`), which follows tokio's time &mdash; under `#[tokio::test(start_paused = true)]` a limiter on a `TokioClock` sees exactly the virtual time its waits sleep.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::{Throttle, TokioClock};

let throttle = Throttle::per_second(10).with_clock(TokioClock::new());
throttle.acquire_with_cost(10).await?;
# Ok(())
# }
```

---

## Feature flags

| Feature | Default | Enables |
|---------|---------|---------|
| `std` | yes | The limiter surface. Off: the `no_std` core (`Backoff`, `BackoffIter`, `Jitter`, `Decision`, `VERSION`). |
| `tokio` | yes | The tokio timer for every _(runtime)_ method, and `TokioClock`. |
| `smol` | no | The smol timer instead. |
| `circuit-breaker` | no | `CircuitBreaker` and its permits. |
| `adaptive` | no | `AdaptiveLimiter`, `Aimd`, `Vegas`. |
| `provider-headers` | no | The `provider` module. |
| `provider-llm` | no | The `presets` module (implies `provider-headers`). |
| `metrics` | no | Metrics through the `metrics` 0.24 facade. |
| `tracing` | no | `tracing` events. |
| `tower` | no | The `tower` module (needs `tokio` or `smol`). |
