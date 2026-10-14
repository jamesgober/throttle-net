//! # throttle-net
//!
//! Outbound throttling and resilience. Where `rate-net` protects your service
//! from being overwhelmed (inbound), throttle-net protects your service from
//! *overwhelming* the downstreams it calls — and from being banned by them. The
//! defining operation is therefore to **wait**, not to reject: you pace your own
//! outbound work rather than dropping someone else's request.
//!
//! throttle-net does not reimplement token-bucket accounting. It consumes
//! [`better-bucket`](https://crates.io/crates/better-bucket) for that and reads
//! time from [`clock-lib`](https://crates.io/crates/clock-lib), then builds the
//! waiting, cost-aware, composable surface on top. It is the outbound companion
//! to [`rate-net`](https://crates.io/crates/rate-net).
//!
//! ## What is here
//!
//! - **Limiters** — the [`Throttle`] token bucket (burst, rate, and initial fill
//!   set independently; [`Throttle::smooth`] for GCRA-style pacing) and the
//!   exact [`SlidingWindowLog`], both behind the [`Limiter`] trait.
//! - **Composites** — [`Hybrid`] (must pass all), [`MultiLimiter`] (named
//!   budgets such as requests + tokens, with aliases and a strict mode),
//!   [`PerKey`] (independent per-key state with bounded memory), and
//!   [`Layered`] (global / per-key / per-endpoint scopes).
//! - **Waiting** — every limiter has a waiting `acquire` that reserves its units
//!   and sleeps exactly until they accrue (fair: later callers queue behind it),
//!   a `*_timeout` variant that gives up at once when the wait is known to be
//!   too long, and is **cancel-safe**: dropping the future returns what it
//!   reserved.
//! - **Resilience** — [`Retry`] with jittered [`Backoff`], a `Retry-After` cap, a
//!   total time budget, and a shared [`RetryBudget`]; a `CircuitBreaker`
//!   (`circuit-breaker` feature) with owned permits and
//!   `call(future)`; a [`Bulkhead`]; a deadline-aware priority [`Queue`]; and
//!   [`resilience`] helpers — `timeout`, `fallback`, and `hedge`.
//! - **Adaptive concurrency** — an `AdaptiveLimiter` (`adaptive` feature) that
//!   learns the right in-flight limit with AIMD or Vegas.
//! - **Providers** — rate-limit header parsing for OpenAI, Anthropic, GitHub,
//!   the IETF draft, and others, with reconciliation that drains limiters to the
//!   server's count and pauses them until a reset (`provider-headers`), plus LLM
//!   tier presets (`provider-llm`).
//! - **Observability** — per-instance names on every metric and event,
//!   grant / denial / timeout / shed counters, wait histograms, and state
//!   gauges (`metrics`, `tracing`); zero cost when off.
//! - **Integrations** — `tower` layers (`tower` feature) and a `TokioClock`
//!   that follows tokio's pausable test time (`tokio` feature).
//!
//! ```
//! # #[cfg(feature = "runtime")]
//! # async fn run() -> Result<(), throttle_net::ThrottleError> {
//! use throttle_net::Throttle;
//!
//! // 100 requests per second, bursting up to 100.
//! let throttle = Throttle::per_second(100);
//!
//! // Pace an outbound call: returns as soon as a token is free.
//! throttle.acquire().await?;
//! // ... call the downstream ...
//! # Ok(())
//! # }
//! ```
//!
//! When you would rather not wait, ask without blocking:
//!
//! ```
//! # #[cfg(feature = "std")] {
//! use throttle_net::Throttle;
//!
//! let throttle = Throttle::per_second(100);
//! if throttle.try_acquire() {
//!     // a token was free — send now
//! }
//! # }
//! ```
//!
//! ## Guarantees
//!
//! - **No over-admission under concurrency.** Every grant is one atomic
//!   compare-and-swap in better-bucket's single-instant engine, where a stale or
//!   backward clock reading can only deny more. Over any window, a token
//!   bucket's grants never exceed its capacity plus what its rate accrues, under
//!   any number of threads; composites hold the same bound per constituent.
//!   This is checked by real-clock multi-threaded stress tests here and loom
//!   models in better-bucket.
//! - **Bounded memory.** Per-key state, queue bookkeeping, and sliding logs are
//!   all bounded, with O(1) or O(log n) work per operation.
//! - **No panics** on any input: hostile headers, extreme durations, rates, and
//!   costs are all handled with checked or saturating arithmetic.
//!
//! ## Feature flags
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `std` | yes | The limiter surface. With it off the crate is `no_std` and exposes the pure algorithm core: [`Backoff`], [`BackoffIter`], [`Jitter`], [`Decision`], and [`VERSION`]. |
//! | `tokio` | yes | tokio timer backend for every waiting `acquire`, plus `TokioClock`. Implies `std`. |
//! | `smol` | no | smol timer backend, as an alternative to `tokio`. If both are enabled, tokio's timer is used. (async-std is unsupported: it is discontinued.) |
//! | `circuit-breaker` | no | `CircuitBreaker`, `Permit`, `OwnedPermit`, `Trip`, `BreakerState`. |
//! | `adaptive` | no | `AdaptiveLimiter` with the `Aimd` and `Vegas` strategies. Its waiting `acquire` also needs `tokio` or `smol`. |
//! | `provider-headers` | no | The `provider` module: rate-limit header parsing and limiter reconciliation. |
//! | `provider-llm` | no | The `presets` module: LLM provider tier presets. Implies `provider-headers`. |
//! | `metrics` | no | Emit metrics through the [`metrics`](https://crates.io/crates/metrics) 0.24 facade. |
//! | `tracing` | no | Emit [`tracing`](https://crates.io/crates/tracing) events. |
//! | `tower` | no | The `tower` module: `tower::Layer` adapters. Needs `tokio` or `smol`. |

// `no_std` for the library build when `std` is off, but always link `std` under
// `test` so the unit-test harness and dev-dependencies have what they need.
#![cfg_attr(all(not(feature = "std"), not(test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![deny(warnings)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(unused_must_use)]
#![deny(unused_results)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::unreachable)]
#![deny(clippy::undocumented_unsafe_blocks)]
#![deny(clippy::missing_safety_doc)]

#[cfg(feature = "adaptive")]
mod adaptive;
// `backoff` and `decision` are the `no_std` algorithm core: pure types and
// math, no clock or async. Everything else requires `std`.
mod backoff;
#[cfg(feature = "std")]
mod bulkhead;
#[cfg(feature = "circuit-breaker")]
mod circuit;
mod decision;
#[cfg(feature = "std")]
mod error;
#[cfg(feature = "std")]
mod eviction;
#[cfg(feature = "std")]
mod hold;
#[cfg(feature = "std")]
mod hybrid;
#[cfg(feature = "std")]
mod layered;
#[cfg(feature = "std")]
mod limiter;
#[cfg(feature = "std")]
mod multi;
#[cfg(feature = "std")]
mod obs;
#[cfg(feature = "std")]
mod perkey;
#[cfg(feature = "provider-llm")]
#[cfg_attr(docsrs, doc(cfg(feature = "provider-llm")))]
pub mod presets;
#[cfg(feature = "provider-headers")]
#[cfg_attr(docsrs, doc(cfg(feature = "provider-headers")))]
pub mod provider;
#[cfg(feature = "runtime")]
mod queue;
#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
pub mod resilience;
#[cfg(feature = "std")]
mod retry;
#[cfg(feature = "std")]
mod retry_after;
#[cfg(feature = "runtime")]
mod rt;
#[cfg(feature = "std")]
mod sliding;
#[cfg(feature = "std")]
mod store;
// Loom-aware atomics indirection for the adaptive limiter's and the bulkhead's
// lock-free slot accounting.
#[cfg(feature = "std")]
mod sync;
#[cfg(feature = "std")]
mod throttle;
#[cfg(feature = "std")]
mod timeutil;
#[cfg(feature = "tower")]
#[cfg_attr(docsrs, doc(cfg(feature = "tower")))]
pub mod tower;
#[cfg(feature = "runtime")]
mod wait;

#[cfg(feature = "adaptive")]
#[cfg_attr(docsrs, doc(cfg(feature = "adaptive")))]
pub use crate::adaptive::{
    AdaptiveLimiter, AdaptiveLimiterBuilder, AdaptivePermit, AdaptiveStrategy, Aimd, Outcome,
    OwnedAdaptivePermit, Vegas,
};
pub use crate::backoff::{Backoff, BackoffIter, Jitter};
#[cfg(feature = "std")]
pub use crate::bulkhead::{Bulkhead, BulkheadPermit, OwnedBulkheadPermit};
#[cfg(feature = "circuit-breaker")]
#[cfg_attr(docsrs, doc(cfg(feature = "circuit-breaker")))]
pub use crate::circuit::{
    BreakerState, CallError, CircuitBreaker, CircuitBreakerBuilder, OwnedPermit, Permit, Trip,
};
pub use crate::decision::Decision;
#[cfg(feature = "std")]
pub use crate::error::ThrottleError;
#[cfg(feature = "std")]
pub use crate::eviction::{DEFAULT_MAX_KEYS, Eviction};
#[cfg(feature = "std")]
pub use crate::hybrid::{Hybrid, HybridBuilder};
#[cfg(feature = "std")]
pub use crate::layered::{Layered, LayeredBuilder};
#[cfg(feature = "std")]
pub use crate::limiter::Limiter;
#[cfg(feature = "std")]
pub use crate::multi::{MultiLimiter, MultiLimiterBuilder};
#[cfg(feature = "std")]
pub use crate::perkey::{PerKey, PerKeyBuilder};
#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
pub use crate::queue::{Overflow, Queue, QueueBuilder};
#[cfg(feature = "std")]
pub use crate::retry::{Retry, RetryAction, RetryBudget, retry_if_retryable};
#[cfg(feature = "std")]
pub use crate::retry_after::{parse_retry_after, parse_retry_after_at};
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use crate::rt::TokioClock;
#[cfg(feature = "std")]
pub use crate::sliding::SlidingWindowLog;
#[cfg(feature = "std")]
pub use crate::store::MAX_SHARDS;
#[cfg(feature = "std")]
pub use crate::throttle::{Throttle, ThrottleBuilder};

// The clock seam is part of the public API: `with_clock` and the builders' `clock`
// methods take any [`Clock`], and tests drive a [`ManualClock`]. Re-exported so
// callers need not depend on `clock-lib` directly.
#[cfg(feature = "std")]
pub use clock_lib::{Clock, ManualClock, SystemClock};

/// The version of this crate, from `Cargo.toml`.
///
/// # Examples
///
/// ```
/// assert!(throttle_net::VERSION.starts_with("1."));
/// ```
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// Compile and run the Rust examples in the README and the reference docs as
// doctests, so the documentation cannot drift from the code. They exercise
// every feature, so they run under `--all-features`.
#[cfg(all(
    doctest,
    feature = "tokio",
    feature = "circuit-breaker",
    feature = "adaptive",
    feature = "provider-llm"
))]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

#[cfg(all(
    doctest,
    feature = "tokio",
    feature = "circuit-breaker",
    feature = "adaptive",
    feature = "provider-llm"
))]
#[doc = include_str!("../docs/API.md")]
struct ApiReferenceDoctests;

#[cfg(all(
    doctest,
    feature = "tokio",
    feature = "circuit-breaker",
    feature = "adaptive",
    feature = "provider-llm"
))]
#[doc = include_str!("../docs/COOKBOOK.md")]
struct CookbookDoctests;

#[cfg(all(
    doctest,
    feature = "tokio",
    feature = "circuit-breaker",
    feature = "adaptive",
    feature = "provider-llm"
))]
#[doc = include_str!("../docs/MIGRATING_FROM_GOVERNOR.md")]
struct MigratingDoctests;
