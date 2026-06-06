# throttle-net &mdash; Cookbook

> Task-oriented recipes for common outbound throttling and resilience problems.
> Each recipe is self-contained. For the exhaustive per-item reference see
> [`API.md`](./API.md); for moving off `governor` see
> [`MIGRATING_FROM_GOVERNOR.md`](./MIGRATING_FROM_GOVERNOR.md).

## Contents

- [Pace outbound calls to a fixed rate](#pace-outbound-calls-to-a-fixed-rate)
- [Allow a burst, then sustain a rate](#allow-a-burst-then-sustain-a-rate)
- [Shed instead of wait](#shed-instead-of-wait)
- [Weight requests by cost](#weight-requests-by-cost)
- [Budget an LLM across several limits](#budget-an-llm-across-several-limits)
- [Throttle per tenant](#throttle-per-tenant)
- [Stack global, per-tenant, and per-endpoint caps](#stack-global-per-tenant-and-per-endpoint-caps)
- [Forbid a boundary burst](#forbid-a-boundary-burst)
- [Retry with jittered backoff and `Retry-After`](#retry-with-jittered-backoff-and-retry-after)
- [Fail fast when a downstream is unhealthy](#fail-fast-when-a-downstream-is-unhealthy)
- [Find the right concurrency without configuring it](#find-the-right-concurrency-without-configuring-it)
- [Queue with deadlines and priority](#queue-with-deadlines-and-priority)
- [Stay in sync with a provider's headers](#stay-in-sync-with-a-providers-headers)
- [Choose a runtime, or go `no_std`](#choose-a-runtime-or-go-no_std)
- [Test limiter logic deterministically](#test-limiter-logic-deterministically)
- [Collect metrics and traces](#collect-metrics-and-traces)

---

## Pace outbound calls to a fixed rate

The outbound default: `acquire().await` returns as soon as a token is free, so you
pace yourself rather than dropping work.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::Throttle;

let throttle = Throttle::per_second(100); // 100 requests/second

for _ in 0..1_000 {
    throttle.acquire().await?; // waits just long enough to stay under the rate
    // ... call the downstream ...
}
# Ok(())
# }
```

For a non-second period, use `per_duration`:

```rust
use std::time::Duration;
use throttle_net::Throttle;

let throttle = Throttle::per_duration(60, Duration::from_secs(60)); // 60/minute
# let _ = throttle;
```

---

## Allow a burst, then sustain a rate

A token bucket starts full, so the capacity *is* the burst allowance, and the
refill rate is the sustained rate. `Throttle::per_second(n)` bursts up to `n` then
settles at `n`/second. To allow a larger burst than the per-second rate, size the
bucket over a longer period:

```rust
use std::time::Duration;
use throttle_net::Throttle;

// Burst up to 500 at once, then sustain 100/second (500 per 5 seconds).
let throttle = Throttle::per_duration(500, Duration::from_secs(5));
assert_eq!(throttle.capacity(), 500);
```

---

## Shed instead of wait

When the right behavior is to drop the request rather than slow down, use the
non-blocking `try_acquire` &mdash; it returns immediately and needs no runtime.

```rust
use throttle_net::Throttle;

let throttle = Throttle::per_second(100);
if throttle.try_acquire() {
    // a token was free: send now
} else {
    // over budget: shed this request (return 429, drop, sample, ...)
}
```

`peek` answers the same question without consuming a token:

```rust
use throttle_net::Throttle;

let throttle = Throttle::per_second(100);
if throttle.peek(10).is_acquired() {
    // 10 tokens are available right now (nothing was taken)
}
# let _ = throttle;
```

---

## Weight requests by cost

Not every call weighs one unit. `acquire_with_cost(n)` (and the `try_`/`peek`
variants) spend `n` at once, all-or-nothing.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::Throttle;

let throttle = Throttle::per_second(1_000);
let payload_units = 250;
throttle.acquire_with_cost(payload_units).await?;
# Ok(())
# }
```

A cost larger than the bucket capacity can never succeed, so the waiting form
returns `ThrottleError::CostExceedsCapacity` immediately instead of waiting
forever.

---

## Budget an LLM across several limits

Providers meter requests *and* input tokens *and* output tokens, each with its own
ceiling. A `MultiLimiter` charges all dimensions atomically &mdash; a call is
admitted only when every budget can afford its share.

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
    .acquire_costs(&[("requests", 1), ("input_tokens", 1_500), ("output_tokens", 200)])
    .await?;
# Ok(())
# }
```

See [`examples/llm_budget.rs`](../examples/llm_budget.rs) for the full flow.

---

## Throttle per tenant

`PerKey` keeps independent state per key, so one noisy tenant cannot spend
another's budget. State is sharded and memory is bounded by default.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::PerKey;

let limiter: PerKey<String> = PerKey::per_second(100); // 100/s per tenant
limiter.acquire(&"tenant:42".to_string()).await?;
# Ok(())
# }
```

Cap the memory a flood of unique keys can occupy:

```rust
use std::time::Duration;
use throttle_net::{Eviction, PerKey};

let limiter: PerKey<String> = PerKey::per_second(100)
    .with_eviction(Eviction::capacity(50_000).with_idle(Duration::from_secs(300)));
# let _ = limiter;
```

---

## Stack global, per-tenant, and per-endpoint caps

A `Layered` limiter applies several scopes in order; a request must clear every
one. The classic shape is a global ceiling over a per-tenant share over a
per-endpoint cap.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::{Layered, PerKey, Throttle};

let limiter = Layered::<String>::builder()
    .global(Throttle::per_second(1_000))   // whole-service ceiling
    .per_key(PerKey::per_second(100))      // per tenant
    .per_endpoint(PerKey::per_second(50))  // per route
    .build();

limiter
    .acquire(&"tenant:42".to_string(), &"/v1/chat".to_string())
    .await?;
# Ok(())
# }
```

For per-tenant quotas under a shared cap with no endpoint scope, omit
`per_endpoint` &mdash; see [`examples/per_tenant_quotas.rs`](../examples/per_tenant_quotas.rs).

---

## Forbid a boundary burst

A token bucket permits a full burst at any instant. When you need an *exact* "no
more than N in any trailing window", use `SlidingWindowLog`. It implements the
same `Limiter` trait, so it composes everywhere the bucket does.

```rust
use std::time::Duration;
use throttle_net::SlidingWindowLog;

let limiter = SlidingWindowLog::new(5, Duration::from_secs(1));
for _ in 0..5 {
    assert!(limiter.try_acquire());
}
assert!(!limiter.try_acquire()); // the 6th in this window is refused
```

---

## Retry with jittered backoff and `Retry-After`

`Retry` wraps any fallible async operation, classifying each error with a closure.
Decorrelated jitter (the default) breaks up a thundering herd; a server
`Retry-After` can override the computed delay.

```rust
# async fn run() {
use std::time::Duration;
use throttle_net::{Backoff, Retry, RetryAction, parse_retry_after};

struct Rejected { retry_after: Option<String> }

let retry = Retry::new(Backoff::default().with_max(Duration::from_secs(5)))
    .max_attempts(5);

let result: Result<&str, Rejected> = retry
    .run(
        || async { Err(Rejected { retry_after: None }) }, // your call
        |err: &Rejected| match err.retry_after.as_deref().and_then(parse_retry_after) {
            Some(after) => RetryAction::RetryAfter(after), // honor the server
            None => RetryAction::Retry,                    // else use the backoff
        },
    )
    .await;
let _ = result;
# }
```

To drive your own loop instead, call `Backoff::iter()` and read `next_delay()`.

---

## Fail fast when a downstream is unhealthy

A limiter paces requests; a `CircuitBreaker` *stops* them. After enough failures
it opens and sheds immediately &mdash; without consuming the wrapped limiter
&mdash; then tests recovery through half-open. Needs the `circuit-breaker` feature.

```rust
# async fn run() {
use std::time::Duration;
use throttle_net::{CircuitBreaker, Throttle, Trip};

let breaker = CircuitBreaker::builder()
    .trip(Trip::Consecutive(5))         // open after 5 failures in a row
    .cooldown(Duration::from_secs(10))
    .build(Throttle::per_second(100));

match breaker.acquire().await {
    Ok(permit) => {
        let ok = true; // ... call the downstream ...
        if ok { permit.success() } else { permit.failure() }
    }
    Err(_shed) => { /* breaker open: fail fast */ }
}
# }
```

Dropping a permit unsettled counts as a failure, so an early return or panic is
treated conservatively.

---

## Find the right concurrency without configuring it

When you do not know the downstream's safe concurrency, let an `AdaptiveLimiter`
discover it from outcomes: it grows the in-flight limit while requests succeed and
pulls back when they fail or slow, bounded by a floor and a hard ceiling. Needs
the `adaptive` feature.

```rust
# async fn run() {
use throttle_net::{AdaptiveLimiter, Aimd};

let limiter = AdaptiveLimiter::builder()
    .floor(2)
    .ceiling(50)   // never exceeded
    .initial(10)
    .build(Aimd::default());

if let Some(permit) = limiter.try_acquire() {
    let ok = true; // ... call the downstream ...
    if ok { permit.success() } else { permit.failure() }
}
# }
```

`Vegas` is the latency-based alternative; both implement `AdaptiveStrategy`, as can
your own.

---

## Queue with deadlines and priority

When a limiter is saturated, a `Queue` lets callers wait in an orderly way: bounded
size, served by priority (and fairly across keys at equal priority), dropping any
waiter whose deadline has passed. Needs a runtime feature.

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

## Stay in sync with a provider's headers

Parse a response's rate-limit headers and reconcile your limiter with the server's
view, so client and server do not drift. Start from a tier preset where one exists.
Needs the `provider-llm` feature (or `provider-headers` for parsing alone).

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::presets;
use throttle_net::provider::HeaderProfile;

let limiter = presets::anthropic::tier_2();

// After a response, reconcile with what the server reported:
let headers = [
    ("anthropic-ratelimit-requests-remaining", "12"),
    ("anthropic-ratelimit-tokens-remaining", "40000"),
];
let info = HeaderProfile::ANTHROPIC.parse(&headers);
let _ = info; // info.sync_requests(&throttle) drains a Throttle to the server's count

limiter.acquire_costs(&[("requests", 1), ("input_tokens", 1_500)]).await?;
# Ok(())
# }
```

Synchronization only ever *reduces* the local budget, so it cannot raise a limiter
above its hard limit.

---

## Choose a runtime, or go `no_std`

The waiting surface runs on either tokio (default) or smol; the call-site code is
identical. Pick the backend in `Cargo.toml`:

```toml
# tokio (default):
throttle-net = "0.8"

# smol instead:
throttle-net = { version = "0.8", default-features = false, features = ["smol"] }
```

For a `no_std` build, take the pure algorithm core only &mdash; `Backoff`, `Jitter`,
and `Decision` &mdash; with the standard library off:

```toml
throttle-net = { version = "0.8", default-features = false }
```

```rust
use core::time::Duration;
use throttle_net::Backoff;

// A deterministic backoff sequence with no clock and no allocator.
let mut delays = Backoff::exponential(Duration::from_millis(50), 2.0).iter_seeded(1);
let _first = delays.next_delay();
```

---

## Test limiter logic deterministically

Inject a `ManualClock` to drive refill by hand &mdash; no sleeping, fully
deterministic.

```rust
use std::sync::Arc;
use std::time::Duration;
use throttle_net::{ManualClock, Throttle};

let clock = Arc::new(ManualClock::new());
let throttle = Throttle::per_second(2).with_clock(clock.clone());

assert!(throttle.try_acquire());
assert!(throttle.try_acquire());
assert!(!throttle.try_acquire());      // drained
clock.advance(Duration::from_secs(1)); // a full period refills it
assert!(throttle.try_acquire());
```

Do not mix a `ManualClock` with a real async sleep: the limiter would read the
manual clock while the waiter sleeps on the runtime's, and the two desynchronize.
Test the synchronous logic with `try_acquire`, or use real (small) durations for
the waiting path.

---

## Collect metrics and traces

Enable the `metrics` and/or `tracing` features and install any recorder/subscriber
in your application; the limiters emit automatically, and the instrumentation is
zero-cost (inputs not even evaluated) when the features are off.

```toml
throttle-net = { version = "0.8", features = ["metrics", "tracing"] }
```

The emitted metrics (`throttle_acquired_total`, `throttle_wait_duration`,
`throttle_queue_depth`, `throttle_circuit_state`, `throttle_rate_current`) and
tracing events are documented in [`API.md`](./API.md#observability).

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
