# Migrating from throttle-net 1.x to 2.0

throttle-net 2.0 fixes a critical over-admission bug in 1.0 (its token buckets
admitted 25–180 times their configured rate under ordinary multi-threaded use)
and makes the defaults fail safe. Most programs upgrade by changing the version
in `Cargo.toml` and recompiling: the API that 1.0 shipped still exists, with the
same names and the same call shapes. What changes is **behaviour you may rely
on**, a handful of trait and derive details, and two dependency majors on the
observability side.

This guide lists every change from 1.0 to 2.0 that can affect a 1.x program,
each with what changed, why, and the code to keep (or restore) the 1.0
behaviour. Blocks marked `1.x` show the old code; the others compile against
2.0 (they run as doctests).

- [1. Update `Cargo.toml`](#1-update-cargotoml)
- [2. `Retry` caps a server's `Retry-After`](#2-retry-caps-a-servers-retry-after)
- [3. A full `PerKey` store never resets a limited key](#3-a-full-perkey-store-never-resets-a-limited-key)
- [4. Provider reconciliation pauses, within a cap](#4-provider-reconciliation-pauses-within-a-cap)
- [5. `MultiLimiter` refuses unknown dimension names](#5-multilimiter-refuses-unknown-dimension-names)
- [6. Waiting acquires reserve](#6-waiting-acquires-reserve)
- [7. Composites queue on their bottleneck](#7-composites-queue-on-their-bottleneck)
- [8. Pauses hold every waiter; `resume` wakes them](#8-pauses-hold-every-waiter-resume-wakes-them)
- [9. Timeouts are deadlines](#9-timeouts-are-deadlines)
- [10. Builders and `Retry` are `Clone`, not `Copy`](#10-builders-and-retry-are-clone-not-copy)
- [11. The `Limiter` trait grew](#11-the-limiter-trait-grew)
- [12. `ThrottleError` has new variants](#12-throttleerror-has-new-variants)
- [13. Adaptive limits fall once per congestion event](#13-adaptive-limits-fall-once-per-congestion-event)
- [14. Metrics: the 0.24 facade and a `name` label](#14-metrics-the-024-facade-and-a-name-label)
- [15. Smaller behaviour changes](#15-smaller-behaviour-changes)
- [New in 2.0 (no migration needed)](#new-in-20-no-migration-needed)

---

## 1. Update `Cargo.toml`

```toml
# 1.x
throttle-net = "1"

# 2.0
throttle-net = "2"
```

The MSRV is unchanged (Rust 1.85). The dependency floors rose: better-bucket
1.1 (the over-admission fix), clock-lib 1.1, event-listener 5.4.2, metrics
0.24.6, tracing 0.1.40.

---

## 2. `Retry` caps a server's `Retry-After`

**What changed.** 1.0 slept for any `RetryAction::RetryAfter` delay. 2.0 honours
one of at most five minutes; a longer one ends the loop and returns the error.

**Why.** A hostile or broken `Retry-After` (one of 999999999 seconds is 31
years) parked a 1.0 retry loop forever.

**Migration.** Nothing, if five minutes is enough. To choose the ceiling, or to
restore 1.0's unlimited waits:

```rust
use std::time::Duration;
use throttle_net::{Backoff, Retry};

let patient = Retry::new(Backoff::default()).max_retry_after(Duration::from_secs(900));
let like_1_0 = Retry::new(Backoff::default()).max_retry_after(Duration::MAX);
assert_eq!(like_1_0.retry_after_cap(), Duration::MAX);
# let _ = patient;
```

`max_elapsed(budget)` (new) bounds the total time of a retry loop as well.

---

## 3. A full `PerKey` store never resets a limited key

**What changed.** In 1.0, inserting a new key into a full store evicted the
least-recently-seen key, whatever its state. A key forgotten while it was
still limited came back with a **fresh allowance**, so a flood of unique keys
could reset anyone's limit. In 2.0:

- a key whose bucket is full again is evicted freely (that loses nothing);
- a key that is still limited is evicted only into a compact per-shard **debt
  ledger** (16 bytes, keyed by the key's hash), and comes back in its debt;
- when both the key slots and the ledger are full of limited keys, the new key
  is **refused** until one recovers — its `try_acquire` returns `false`, and its
  waiting `acquire` waits;
- an idle TTL reclaims only keys whose bucket has recovered.

**Why.** Correctness: the per-key limit must hold however many keys an attacker
invents.

**Migration.** Usually nothing: size `Eviction::capacity` above your real key
cardinality, as before. If admitting new keys matters more to you than exact
limits for the keys closest to recovery, opt into forgetting debt when the
store is full:

```rust
use throttle_net::{Eviction, PerKey, WhenFull};

let limiter: PerKey<String> = PerKey::per_minute(100)
    .with_eviction(Eviction::capacity(100_000).with_when_full(WhenFull::ForgetDebt));
# let _ = limiter;
```

The [`Eviction`](./API.md#eviction) docs give the flood rates a lockout needs.
The store also now raises its shard count so that no shard holds more than
16,384 keys of the cap (this bounds every insert's work).

---

## 4. Provider reconciliation pauses, within a cap

**What changed.** In 1.0, `RateLimitInfo::sync_requests` / `sync_tokens` only
drained the limiter to the server's remaining count. In 2.0 they also pause it:
until the reset when the server reports the window exhausted, and for a
`Retry-After`. Every pause is clamped to `provider::DEFAULT_MAX_PAUSE` (five
minutes), because the values come from the server. The functions also accept
any `Limiter` now, not just a `Throttle`.

**Why.** Draining alone let the limiter refill and send into a window the server
had said was exhausted.

**Migration.** Nothing, to get the safer behaviour. To restore 1.0's drain-only
reconciliation, pass a zero ceiling; to choose another ceiling, pass it:

```rust
use std::time::Duration;
use throttle_net::Throttle;
use throttle_net::provider::{RateLimitInfo, Window};

let throttle = Throttle::per_second(100);
let info = RateLimitInfo {
    requests: Some(Window { remaining: Some(0), reset: Some(Duration::from_secs(30)), ..Window::default() }),
    ..RateLimitInfo::default()
};

// 1.0 behaviour: drain only.
info.sync_requests_capped(&throttle, Duration::ZERO);
assert_eq!(throttle.paused_for(), Duration::ZERO);

// A ceiling of your choosing.
info.sync_requests_capped(&throttle, Duration::from_secs(10));
assert!(throttle.paused_for() <= Duration::from_secs(10));
```

`Window::apply_capped` and `RateLimitInfo::sync_multi_capped` are the same for a
single window and for a whole `MultiLimiter`.

---

## 5. `MultiLimiter` refuses unknown dimension names

**What changed.** 1.0 ignored a cost naming a dimension the limiter does not
have, so a typo charged nothing and the call went through. 2.0 refuses it:
`try_acquire_costs` returns `false`, `peek_costs` returns
`Decision::Impossible`, and the waiting methods return
`ThrottleError::UnknownDimension { index }`. The LLM presets follow the same
default.

**Why.** A misspelt dimension silently bypassed that budget.

**Migration.** Fix the names, or — if you deliberately pass one cost list to
limiters with different dimensions — restore 1.0's leniency:

```rust,ignore
// 1.x: "input_token" was silently ignored.
let limiter = MultiLimiter::builder()
    .dimension("input_tokens", Throttle::per_minute(100_000))
    .build();
assert!(limiter.try_acquire_costs(&[("input_token", 500)]));
```

```rust
use throttle_net::{MultiLimiter, Throttle};

let strict = MultiLimiter::builder()
    .dimension("input_tokens", Throttle::per_minute(100_000))
    .build();
assert!(!strict.try_acquire_costs(&[("input_token", 500)])); // refused

let lenient = MultiLimiter::builder()
    .dimension("input_tokens", Throttle::per_minute(100_000))
    .strict(false)
    .build();
assert!(lenient.try_acquire_costs(&[("input_token", 500)])); // 1.0 behaviour
```

Several entries for the same dimension in one call are now **summed** (1.0
used only the first).

---

## 6. Waiting acquires reserve

**What changed.** In 1.0 a waiting `acquire` slept for the reported wait and
then raced every other waiter for the refilled tokens. In 2.0 a token-bucket
waiter **reserves** its tokens at once and sleeps exactly until they accrue,
so waiters are served in arrival order and a large cost is never starved.
Two things follow:

- `try_acquire`, `peek`, and `available()` see the queued waiters' debt: while
  waiters are queued, a non-waiting attempt is refused even if tokens have
  accrued (they are promised to the queue).
- Under a `ManualClock`, a waiting acquire sleeps its reservation in real
  (runtime) time and returns, rather than looping until the test advances the
  clock. Tests that drove waits by advancing a `ManualClock` should run on
  tokio's paused time with a `TokioClock` instead:

```rust
# #[tokio::main(flavor = "current_thread", start_paused = true)]
# async fn main() -> Result<(), throttle_net::ThrottleError> {
use std::time::Duration;
use throttle_net::{Throttle, TokioClock};

let throttle = Throttle::per_second(10).with_clock(TokioClock::new());
assert!(throttle.try_acquire_with_cost(10));
let start = tokio::time::Instant::now();
throttle.acquire_with_cost(5).await?; // sleeps 500ms of virtual time
assert_eq!(start.elapsed(), Duration::from_millis(500));
# Ok(())
# }
```

**Cancel-safety** is unchanged in spirit and now exact: dropping a waiting
future returns its reservation.

---

## 7. Composites queue on their bottleneck

**What changed.** A `Hybrid`, `MultiLimiter`, or `Layered` waiting acquire now
reserves on its *bottleneck* constituent (the one with the longest wait) and
takes the others only when that reservation matures. The request is admitted
when every constituent has granted, as in 1.0; what changes is **when** the
fast constituents are charged: when the request can go, not while it waits.

**Why.** Charging every constituent up front let one tenant with a backlog on
its own slow per-key scope drain a shared global scope ahead of time, starving
every other tenant.

**Migration.** None. If you implement `Limiter` yourself, see
[§ 11](#11-the-limiter-trait-grew).

---

## 8. Pauses hold every waiter; `resume` wakes them

**What changed.** A pause (`pause_for`, or provider reconciliation) now holds
waiters that had *already* reserved before it began — they keep their
reservation and go when the pause ends — and `resume()` wakes paused waiters
immediately. `Hybrid`, `MultiLimiter`, and `Layered` gained `resume()` and
`paused_for()`. A paused limiter reports `available() == 0`.

**Migration.** None.

---

## 9. Timeouts are deadlines

**What changed.** Every `*_timeout` bounds the whole wait, however the limiter
answers. A token bucket still fails at once when the wait is known to be too
long. 2.0 adds timeouts where 1.0 had none:
`acquire_owned_timeout` on `CircuitBreaker`, `AdaptiveLimiter`, and `Bulkhead`,
and `Bulkhead::call_timeout`.

**Migration.** None; replace an outer `tokio::time::timeout` with the built-in
variant where you like.

---

## 10. Builders and `Retry` are `Clone`, not `Copy`

**What changed.** `CircuitBreakerBuilder`, `AdaptiveLimiterBuilder`,
`QueueBuilder`, and `Retry` no longer implement `Copy`, because their `name`
setters now accept an owned name (`impl Into<Cow<'static, str>>`), like every
other builder.

**Migration.** Where you used a value after copying it, clone it:

```rust,ignore
// 1.x: `Retry` was `Copy`.
let retry = Retry::new(Backoff::default());
let a = retry;
let b = retry;
```

```rust
use throttle_net::{Backoff, CircuitBreaker, Retry, Throttle};

let retry = Retry::new(Backoff::default()).name(format!("retry-{}", 7));
let a = retry.clone();
let b = retry;
# let _ = (a, b);

let builder = CircuitBreaker::builder().name(String::from("payments"));
let first = builder.clone().build(Throttle::per_second(10));
let second = builder.build(Throttle::per_second(10));
assert_eq!(first.name(), second.name());
```

---

## 11. The `Limiter` trait grew

**What changed.** `Limiter` keeps its four required methods and gained provided
methods with conservative defaults: `refund`, `reserve`, `pause_for`,
`paused_for`, `resume`, and `charge`. It is also implemented for `&L`,
`Box<L>`, and `Arc<L>` whenever `L: Limiter` (including `dyn Limiter`).

**Migration.**

- A custom limiter compiles unchanged. Override `reserve` / `refund` to queue
  fairly inside composites, and `pause_for` / `paused_for` / `resume` if it can
  pause exactly.
- Remove any wrapper you wrote to pass an `Arc<Throttle>` where a `Limiter` was
  expected — it is one now:

```rust
use std::sync::Arc;
use throttle_net::{Hybrid, Throttle};
use throttle_net::provider::RateLimitInfo;

let shared = Arc::new(Throttle::per_second(10));
let info = RateLimitInfo::default();
info.sync_requests(&shared);
let hybrid = Hybrid::builder().limiter(Arc::clone(&shared)).build();
assert!(hybrid.try_acquire());
```

- A blanket `impl<T: YourTrait> Limiter for T` in your crate may now overlap
  these impls; implement `Limiter` for your concrete types instead.

---

## 12. `ThrottleError` has new variants

`BulkheadFull`, `InvalidConfig { reason }`, `UnknownDimension { index }`, and
`NotReady`. The enum is `#[non_exhaustive]`, so a `match` already has a
wildcard arm; add arms where you want to handle them.

---

## 13. Adaptive limits fall once per congestion event

**What changed.** A failure lowers an `AdaptiveLimiter`'s limit only if its
request started after the latest decrease, so a burst of correlated failures
halves an AIMD limit once instead of collapsing it to the floor. This also
applies to custom `AdaptiveStrategy` implementations: `adjust` is not called
for a failure from an already-answered congestion event.

**Migration.** None, unless a custom strategy counted every failure; it now
sees one per congestion event.

---

## 14. Metrics: the 0.24 facade and a `name` label

**What changed.** With the `metrics` feature, 2.0 emits through the `metrics`
**0.24** facade (1.0 used 0.23; one global recorder exists per facade major, so
a 0.23 recorder sees nothing from 2.0). Every series carries a `name` label
(the instance name, `"default"` when unset), and more paths emit: denials,
timeouts, sheds, queue rejections, retries, and retry-budget exhaustion. Gauges
are set at construction.

**Migration.** Install a recorder built for the `metrics` 0.24 facade. Name
your instances so their series stay
apart, and update dashboards and alerts that match on exact label sets:

```rust
use throttle_net::Throttle;

let search = Throttle::per_second(100).with_name("search-api");
assert_eq!(search.name(), Some("search-api"));
```

---

## 15. Smaller behaviour changes

- **OpenAI presets** meter one combined `tokens` budget; `input_tokens` and
  `output_tokens` are aliases for it (1.0 metered them separately and admitted
  up to twice the real budget). Costs and lookups naming `input_tokens` or
  `output_tokens` resolve to the combined budget.
- **Sliding windows** below a millisecond, and zero windows, are enforced (a
  zero window is a per-instant limit); 1.0 treated them as unlimited.
- **Capacities above ~4.29 M tokens** are exact (1.0 clamped them silently).
- **Very slow rates** refill, and `peek` agrees with `acquire`.
- **`Trip` and `Aimd` parameters** out of range are normalised (zero counts
  become one; ratios are clamped).
- **Circuit-breaker permits** carry the state generation they were admitted
  under: a slow call admitted before a transition no longer counts toward the
  state after it.
- **`tower` layers** (new in 2.0) wait in `poll_ready`, not in `call`.

---

## New in 2.0 (no migration needed)

`Throttle::builder` (independent burst and initial fill), `smooth`,
`reserve` / `refund` / `drain_to`, `pause_for` / `resume`; `RetryBudget`;
`MultiLimiter` aliases and `settle_costs`; `PerKey` builder, `reserve`,
`refund`, `reset`; `Bulkhead`; `resilience::{timeout, fallback, hedge}`;
owned permits and `cancel()` for the breaker and adaptive limiter;
`CircuitBreaker::call`; `TokioClock`; `Window::apply` and `sync_multi`; the
`tower` layers. See the [CHANGELOG](../CHANGELOG.md) and the
[API reference](./API.md).

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
