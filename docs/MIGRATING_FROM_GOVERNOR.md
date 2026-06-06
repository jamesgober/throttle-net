# Migrating from `governor`

> A guide for moving an existing [`governor`](https://crates.io/crates/governor)
> setup to throttle-net. For task recipes see [`COOKBOOK.md`](./COOKBOOK.md); for
> the full reference see [`API.md`](./API.md).

## Why move

`governor` is an excellent inbound rate limiter. throttle-net targets the
*outbound* problem &mdash; pacing the calls *you* make so you do not overwhelm or
get banned by a downstream &mdash; and ships the pieces an outbound caller needs
that `governor` does not: a **waiting** acquire that paces instead of rejecting,
**cost-aware** acquisition, **multi-dimensional** budgets (the LLM case),
**layered** scopes, and the resilience layer (retry/backoff, circuit breaker,
adaptive concurrency, deadline-aware queue, provider-header sync).

If all you need is to reject inbound traffic over a fixed rate, `governor` is a
fine choice and you may not need to move at all. The case for moving is when you
are calling *out* and want to wait, weight, compose, or adapt.

## The one conceptual shift: wait vs. reject

`governor`'s primary question is "may I proceed *right now*?" &mdash; `check()`
returns `Ok`/`Err`. throttle-net's primary verb is **wait**: `acquire().await`
returns as soon as a token is free, so the caller is paced rather than dropped.
The non-blocking question is still available as `try_acquire()`.

| You want to... | `governor` | throttle-net |
|---|---|---|
| Proceed or be told no, now | `limiter.check().is_ok()` | `throttle.try_acquire()` |
| Wait until allowed | manual sleep loop on `check()` | `throttle.acquire().await?` |
| Inspect without consuming | `check()` (consumes on success) | `throttle.peek(1)` (consumes nothing) |

## API mapping

| `governor` | throttle-net |
|---|---|
| `RateLimiter::direct(Quota::per_second(n))` | `Throttle::per_second(n)` |
| `Quota::per_second(n)` / `Quota::with_period(d)` | `Throttle::per_second(n)` / `Throttle::per_duration(amount, period)` |
| `Quota::...burst_size(b)` | size the bucket over a longer period: `Throttle::per_duration(b, period)` |
| `limiter.check()` | `throttle.try_acquire()` (`bool`) |
| `limiter.check_n(n)` | `throttle.try_acquire_with_cost(n)` |
| `limiter.until_ready().await` | `throttle.acquire().await?` |
| `limiter.until_n_ready(n).await` | `throttle.acquire_with_cost(n).await?` |
| `RateLimiter::keyed(quota)` | `PerKey::<K>::per_second(n)` |
| `keyed.check_key(&k)` | `perkey.try_acquire(&k)` |
| `keyed.until_key_ready(&k).await` | `perkey.acquire(&k).await?` |
| `GCRA` algorithm | token bucket (default) or exact `SlidingWindowLog` |
| keyed limiter memory growth | bounded by default via [`Eviction`](./API.md#eviction) |

throttle-net has no direct equivalent to `governor`'s `Jitter` *on the limiter*
because the limiter waits exactly as long as needed; jitter lives where it belongs,
in [`Backoff`](./API.md#backoff) for retries.

## Before and after

### Direct limiter, non-blocking

```rust
// governor
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;

let limiter = RateLimiter::direct(Quota::per_second(NonZeroU32::new(100).unwrap()));
if limiter.check().is_ok() {
    // proceed
}
```

```rust
// throttle-net
use throttle_net::Throttle;

let throttle = Throttle::per_second(100);
if throttle.try_acquire() {
    // proceed
}
```

### Waiting until ready

```rust
// governor
# async fn run() {
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;

let limiter = RateLimiter::direct(Quota::per_second(NonZeroU32::new(100).unwrap()));
limiter.until_ready().await;
// proceed
# }
```

```rust
// throttle-net
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::Throttle;

let throttle = Throttle::per_second(100);
throttle.acquire().await?;
// proceed
# Ok(())
# }
```

### Keyed (per-tenant) limiter

```rust
// governor
# async fn run() {
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;

let limiter = RateLimiter::keyed(Quota::per_second(NonZeroU32::new(100).unwrap()));
limiter.until_key_ready(&"tenant:42").await;
# }
```

```rust
// throttle-net
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::PerKey;

let limiter: PerKey<String> = PerKey::per_second(100);
limiter.acquire(&"tenant:42".to_string()).await?;
# Ok(())
# }
```

Note that throttle-net's keyed store is **bounded by default**: a flood of unique
keys is capped and idle keys are reclaimed (see [`Eviction`](./API.md#eviction)),
whereas a `governor` keyed limiter grows with the key space unless you shrink it
yourself.

## Things `governor` does not have

These have no `governor` equivalent; they are the reason to adopt throttle-net:

- **Cost-aware, multi-dimensional budgets** &mdash; charge requests *and* input
  tokens *and* output tokens atomically with [`MultiLimiter`](./API.md#multilimiter).
- **Layered scopes** &mdash; global / per-key / per-endpoint in one limiter with
  [`Layered`](./API.md#layered).
- **Retry + backoff** with `Retry-After` honoring ([`Retry`](./API.md#retry),
  [`Backoff`](./API.md#backoff)).
- **Circuit breaker** ([`CircuitBreaker`](./API.md#circuitbreaker)).
- **Adaptive concurrency** ([`AdaptiveLimiter`](./API.md#adaptivelimiter)).
- **Deadline-aware priority queue** ([`Queue`](./API.md#queue)).
- **Provider-header parsing and sync** ([`provider`](./API.md#provider-integration)).

## Runtime and clock

`governor` uses its own clock abstraction; throttle-net uses
[`clock-lib`](https://crates.io/crates/clock-lib), re-exported, so `with_clock`
takes a `ManualClock` for deterministic tests. The waiting surface runs on tokio
(default) or smol &mdash; choose with a feature, no code change. See
[Runtime backends](./API.md#runtime-backends).

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
