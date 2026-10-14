# throttle-net &mdash; Cookbook

> Task-oriented recipes for common outbound throttling and resilience problems.
> Each recipe is self-contained. For the exhaustive per-item reference see
> [`API.md`](./API.md); for moving off `governor` see
> [`MIGRATING_FROM_GOVERNOR.md`](./MIGRATING_FROM_GOVERNOR.md).

## Contents

- [Pace outbound calls to a fixed rate](#pace-outbound-calls-to-a-fixed-rate)
- [Allow a burst, then sustain a rate](#allow-a-burst-then-sustain-a-rate)
- [Shed instead of wait](#shed-instead-of-wait)
- [Give up instead of waiting too long](#give-up-instead-of-waiting-too-long)
- [Weight requests by cost](#weight-requests-by-cost)
- [Budget an LLM across several limits](#budget-an-llm-across-several-limits)
- [Admit an LLM call on an estimate, then settle](#admit-an-llm-call-on-an-estimate-then-settle)
- [Throttle per tenant](#throttle-per-tenant)
- [Stack global, per-tenant, and per-endpoint caps](#stack-global-per-tenant-and-per-endpoint-caps)
- [Forbid a boundary burst](#forbid-a-boundary-burst)
- [Retry with jittered backoff and `Retry-After`](#retry-with-jittered-backoff-and-retry-after)
- [Keep retries from becoming a storm](#keep-retries-from-becoming-a-storm)
- [Fail fast when a downstream is unhealthy](#fail-fast-when-a-downstream-is-unhealthy)
- [Isolate a slow dependency](#isolate-a-slow-dependency)
- [Hedge a slow call](#hedge-a-slow-call)
- [Find the right concurrency without configuring it](#find-the-right-concurrency-without-configuring-it)
- [Queue with deadlines and priority](#queue-with-deadlines-and-priority)
- [Stay in sync with a provider's headers](#stay-in-sync-with-a-providers-headers)
- [Throttle a `tower` client](#throttle-a-tower-client)
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

A token bucket's capacity *is* the burst allowance and its refill rate is the
sustained rate. `Throttle::per_second(n)` bursts up to `n`; the builder sets the
burst independently, and can start the bucket empty so a restart does not fire a
burst at a downstream that just recovered:

```rust
use throttle_net::Throttle;

// Sustain 100/second, burst up to 500 at once, start with nothing banked.
let throttle = Throttle::builder()
    .per_second(100)
    .burst(500)
    .initial(0)
    .try_build()?;
assert_eq!(throttle.capacity(), 500);
assert_eq!(throttle.available(), 0);
# Ok::<(), throttle_net::ThrottleError>(())
```

For the opposite &mdash; perfectly even spacing with no burst at all, as GCRA or a
leaky bucket gives &mdash; use `Throttle::smooth(amount, period)`.

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

## Give up instead of waiting too long

Every waiting acquire has a `*_timeout` variant. Waits are reservations with a
known end, so when the budget cannot be met the call fails **at once** with
`DeadlineExceeded`, reserving nothing, rather than sleeping to the deadline.
Dropping a waiting future instead (an outer `tokio::time::timeout`, a `select!`)
is also safe: its reservation goes back.

```rust
# async fn run() {
use std::time::Duration;
use throttle_net::{Throttle, ThrottleError};

let throttle = Throttle::per_duration(1, Duration::from_secs(60));
assert!(throttle.try_acquire());
let err = throttle.acquire_timeout(Duration::from_millis(250)).await.unwrap_err();
assert_eq!(err, ThrottleError::DeadlineExceeded);
# }
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

## Admit an LLM call on an estimate, then settle

Output tokens are unknown until the response arrives. Admit the call on its
`max_tokens` ceiling, then settle with the usage the provider reports: the
unused budget is refunded, and an overspend (an under-estimated prompt) is
charged as debt that later calls wait behind.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use throttle_net::presets;

let limiter = presets::anthropic::tier_1();
let estimate = [("requests", 1), ("input_tokens", 1_200), ("output_tokens", 1_024)];
limiter.acquire_costs(&estimate).await?;

// ... send the request; the response says it used 230 output tokens ...
let usage = [("requests", 1), ("input_tokens", 1_200), ("output_tokens", 230)];
limiter.settle_costs(&estimate, &usage);
# Ok(())
# }
```

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

## Keep retries from becoming a storm

When a downstream fails wholesale, every client retrying multiplies the load
it is failing under. Bound each loop (`max_retry_after` caps a server's
`Retry-After`; `max_elapsed` caps the total time), and share a `RetryBudget`
so retries can add at most a fixed fraction to the request rate.

```rust
# async fn run() {
use std::time::Duration;
use throttle_net::{Backoff, Retry, RetryAction, RetryBudget};

// Retries may add 10% to traffic, with a floor of one per second.
let budget = RetryBudget::new(0.1, 1);
let retry = Retry::new(Backoff::default())
    .max_retry_after(Duration::from_secs(30))
    .max_elapsed(Duration::from_secs(10));

let result: Result<(), &str> = retry
    .run_with_budget(&budget, || async { Err("503") }, |_| RetryAction::Retry)
    .await;
assert!(result.is_err());
# }
```

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

## Isolate a slow dependency

A `Bulkhead` caps how many calls into one dependency may be in flight, so a
slow dependency ties up at most its own slots. `CircuitBreaker::call` settles the
breaker from the call's result for you.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
use std::time::Duration;
use throttle_net::resilience::timeout;
use throttle_net::{Bulkhead, CircuitBreaker, Throttle, Trip};

let bulkhead = Bulkhead::new(8);
let breaker = CircuitBreaker::builder()
    .trip(Trip::Consecutive(5))
    .build(Throttle::per_second(50));

let result = bulkhead
    .call(|| breaker.call(|| async {
        timeout(Duration::from_secs(2), async { Ok::<_, &str>("response") })
            .await
            .map_err(|_| "timed out")?
    }))
    .await?;
assert!(result.is_ok());
# Ok(())
# }
```

---

## Hedge a slow call

For idempotent reads with a long latency tail, start a second attempt if the
first has not answered within a delay; the first success wins and the loser is
cancelled.

```rust
# async fn run() {
use std::time::Duration;
use throttle_net::resilience::hedge;

let value: Result<&str, ()> = hedge(Duration::from_millis(50), || async { Ok("replica") }).await;
assert_eq!(value, Ok("replica"));
# }
```

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
view, so client and server do not drift. When the server reports a window
exhausted, the limiter pauses until the reported reset; a `Retry-After` on a 429
pauses it too. Start from a tier preset where one exists. Needs the
`provider-llm` feature (or `provider-headers` for parsing and `Window::apply`).

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
let drained = info.sync_multi(&limiter);
assert!(drained > 0);
assert_eq!(limiter.available("requests"), Some(12));

limiter.acquire_costs(&[("requests", 1), ("input_tokens", 1_500)]).await?;
# Ok(())
# }
```

Reconciliation only ever *reduces* the local budget, so it cannot raise a limiter
above its configured capacity. For a single `Throttle`, use
`info.sync_requests(&throttle)` or `info.sync_tokens(&throttle)`.

---

## Throttle a `tower` client

With the `tower` feature, put a limiter, a breaker, or an adaptive concurrency
limit in front of any `tower::Service` stack.

```rust
# async fn run() -> Result<(), throttle_net::tower::BoxError> {
use tower::{ServiceBuilder, ServiceExt, service_fn};
use throttle_net::Throttle;
use throttle_net::tower::ThrottleLayer;

let client = ServiceBuilder::new()
    .layer(ThrottleLayer::new(Throttle::per_second(20)))
    .service(service_fn(|path: &'static str| async move {
        Ok::<_, std::convert::Infallible>(path.len())
    }));
assert_eq!(client.oneshot("/v1/models").await?, 10);
# Ok(())
# }
```

---

## Choose a runtime, or go `no_std`

The waiting surface runs on either tokio (default) or smol; the call-site code is
identical. Pick the backend in `Cargo.toml`:

```toml
# tokio (default):
throttle-net = "1"

# smol instead:
throttle-net = { version = "1", default-features = false, features = ["smol"] }
```

For a `no_std` build, take the pure algorithm core only &mdash; `Backoff`, `Jitter`,
and `Decision` &mdash; with the standard library off:

```toml
throttle-net = { version = "1", default-features = false }
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

To test the *waiting* path deterministically, drive the limiter with
`TokioClock` under tokio's paused time: the limiter then reads the same virtual
clock its waits sleep on, and a minute of refill takes no real time.

```rust
# async fn run() -> Result<(), throttle_net::ThrottleError> {
// In a test: #[tokio::test(start_paused = true)]
use std::time::Duration;
use throttle_net::{Throttle, TokioClock};

let throttle = Throttle::per_minute(1).with_clock(TokioClock::new());
throttle.acquire().await?;
let start = tokio::time::Instant::now();
throttle.acquire().await?; // a full minute of virtual time, instantly
assert!(start.elapsed() >= Duration::from_secs(59));
# Ok(())
# }
```

---

## Collect metrics and traces

Enable the `metrics` and/or `tracing` features and install any recorder or
subscriber in your application; every limiter emits automatically, and the hooks
compile to nothing when the features are off. Name each instance so two of a kind
get separate series:

```toml
throttle-net = { version = "1", features = ["metrics", "tracing"] }
```

```rust
use throttle_net::Throttle;

let github = Throttle::per_minute(5_000).with_name("github");
let stripe = Throttle::per_second(25).with_name("stripe");
# let _ = (github, stripe);
```

The metrics (grants, denials, timeouts, sheds, waits, breaker state, queue
depth, adaptive limit, retries) and tracing events are listed in
[`API.md`](./API.md#observability).

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
