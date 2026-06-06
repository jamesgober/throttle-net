//! Atomics indirection for `loom` model checking.
//!
//! Under `--cfg throttle_loom` the limiter's own atomics become loom's
//! instrumented versions, so
//! [`tests/loom_throttle.rs`](../../tests/loom_throttle.rs) can exhaustively
//! explore their interleavings; in every normal build they are the standard
//! library's, with zero overhead and no behavioral difference. Only throttle-net's
//! own concurrency state routes through here — the token-bucket accounting lives in
//! `better-bucket`, which carries its own tests.
//!
//! A crate-private cfg name (`throttle_loom`, not the bare `loom`) is deliberate:
//! a global `--cfg loom` would also switch on the `cfg(loom)` paths inside
//! transitive dependencies (`concurrent-queue`, …) that expect the `loom` crate in
//! *their* graph, breaking the build. Scoping the cfg to this crate keeps the model
//! check to throttle-net's own atomics.

#[cfg(not(throttle_loom))]
pub(crate) use core::sync::atomic::AtomicU32;
#[cfg(throttle_loom)]
pub(crate) use loom::sync::atomic::AtomicU32;
