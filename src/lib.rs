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
//! ## Status
//!
//! **Pre-1.0 (v0.8 — public API frozen).** The limiter and resilience surface:
//! the [`Limiter`] trait, the [`Throttle`] token bucket and the exact
//! [`SlidingWindowLog`], each with a waiting cost-aware
//! [`acquire`](Throttle::acquire); the composites — [`Hybrid`] (must pass all),
//! [`MultiLimiter`] (multi-dimensional budgets), [`PerKey`] (independent per-key
//! state, bounded memory), and [`Layered`] (global / per-key / per-endpoint
//! scopes); standalone [`Retry`]/[`Backoff`] with jittered backoff and
//! `Retry-After` parsing; the resilience layer — a `CircuitBreaker` that wraps
//! any limiter and fails fast (`circuit-breaker` feature), and a deadline-aware,
//! priority [`Queue`]; adaptive concurrency — an `AdaptiveLimiter` that discovers
//! the right in-flight limit from outcome feedback (`adaptive` feature); provider
//! integration — response-header parsers with limiter sync (`provider-headers`
//! feature) and LLM tier `presets` (`provider-llm` feature); and observability —
//! metrics and tracing events, feature-gated and zero-cost when off (`metrics`,
//! `tracing` features).
//!
//! The waiting surface runs on either [`tokio`](crate#feature-flags) or
//! [`smol`](crate#feature-flags) — the async code is runtime-agnostic, and you
//! choose the timer backend by feature. With `std` off, the pure algorithm core
//! ([`Backoff`], [`Jitter`], [`Decision`]) compiles `no_std`. The public API is
//! now frozen and will not change incompatibly before 1.0.
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
//! ## Design goals
//!
//! - **Wait by default.** The Tier-1 [`acquire`](Throttle::acquire) paces the
//!   caller; [`try_acquire`](Throttle::try_acquire) is there when you need the
//!   non-blocking answer.
//! - **Cost-aware.** Not every request weighs one unit. `acquire_with_cost(n)`
//!   takes `n` tokens at once — the basis for the multi-dimensional LLM budgets
//!   that arrive with the rest of v0.2.
//! - **Lock-free accounting.** Each acquire is a single atomic compare-and-swap
//!   in `better-bucket`; no lock sits on the path.
//! - **Runtime-free core, lazy refill.** Tokens accrue from a monotonic clock on
//!   access; there is no background timer thread, and the synchronous core has no
//!   async-runtime dependency.
//! - **Composable.** Every limiter is one [`Limiter`]; composites combine them
//!   without the call site changing.
//!
//! ## Feature flags
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `std`   | yes | Standard library — the limiter surface. With it off the crate is `no_std` and exposes the pure algorithm core ([`Backoff`], [`Jitter`], [`Decision`]) plus [`VERSION`]. |
//! | `tokio` | yes | tokio timer backend for the waiting [`acquire`](Throttle::acquire) surface. Implies `std`. |
//! | `smol`  | no  | smol timer backend, as an alternative to `tokio`. (async-std is unsupported — it is discontinued.) |
//!
//! The `circuit-breaker`, `adaptive`, `provider-headers` / `provider-llm`, and
//! `metrics` / `tracing` features are documented in `docs/API.md`, which carries
//! the full feature matrix.

// `no_std` for the library build when `std` is off, but always link `std` under
// `test` so the unit-test harness and dev-dependencies have what they need.
#![cfg_attr(all(not(feature = "std"), not(test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]
#![forbid(unsafe_code)]
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

// The limiter surface requires the standard library (the clock-driven token
// bucket and the domain error type). With `std` off the crate is `no_std` and
// exposes only `VERSION`.
#[cfg(feature = "adaptive")]
mod adaptive;
// `backoff` and `decision` are the `no_std`-capable algorithm core: pure types
// and math, no clock or async. The rest of the surface requires `std`.
mod backoff;
#[cfg(feature = "circuit-breaker")]
mod circuit;
mod decision;
#[cfg(feature = "std")]
mod error;
#[cfg(feature = "std")]
mod eviction;
#[cfg(feature = "std")]
mod hybrid;
#[cfg(feature = "std")]
mod layered;
#[cfg(feature = "std")]
mod limiter;
#[cfg(feature = "std")]
mod multi;
#[cfg(any(feature = "runtime", feature = "circuit-breaker", feature = "adaptive"))]
mod obs;
#[cfg(feature = "std")]
mod perkey;
#[cfg(feature = "provider-llm")]
pub mod presets;
#[cfg(feature = "provider-headers")]
pub mod provider;
#[cfg(feature = "runtime")]
mod queue;
#[cfg(feature = "std")]
mod retry;
#[cfg(feature = "std")]
mod retry_after;
#[cfg(feature = "runtime")]
mod rt;
#[cfg(feature = "std")]
mod sliding;
#[cfg(feature = "std")]
mod throttle;
#[cfg(feature = "std")]
mod timeutil;

#[cfg(feature = "adaptive")]
pub use crate::adaptive::{
    AdaptiveLimiter, AdaptiveLimiterBuilder, AdaptivePermit, AdaptiveStrategy, Aimd, Outcome, Vegas,
};
pub use crate::backoff::{Backoff, BackoffIter, Jitter};
#[cfg(feature = "circuit-breaker")]
pub use crate::circuit::{BreakerState, CircuitBreaker, CircuitBreakerBuilder, Permit, Trip};
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
pub use crate::perkey::PerKey;
#[cfg(feature = "runtime")]
pub use crate::queue::{Overflow, Queue, QueueBuilder};
#[cfg(feature = "std")]
pub use crate::retry::{Retry, RetryAction, retry_if_retryable};
#[cfg(feature = "std")]
pub use crate::retry_after::{parse_retry_after, parse_retry_after_at};
#[cfg(feature = "std")]
pub use crate::sliding::SlidingWindowLog;
#[cfg(feature = "std")]
pub use crate::throttle::Throttle;

// The clock seam is part of the public API: [`Throttle::with_clock`] and the
// per-key/composite `with_clock` methods take any [`Clock`], and tests drive a
// [`ManualClock`]. Re-exported so callers need not depend on `clock-lib` directly.
#[cfg(feature = "std")]
pub use clock_lib::{Clock, ManualClock, SystemClock};

/// The version of this crate, from `Cargo.toml`.
///
/// # Examples
///
/// ```
/// assert!(!throttle_net::VERSION.is_empty());
/// ```
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
