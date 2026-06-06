//! Runtime abstraction: one async `sleep` over the selected backend.
//!
//! Notifications use `event-listener` and races use `futures-lite`, both
//! runtime-agnostic, so the only runtime-specific operation is the timer.
//! Exactly one of the `tokio` (default) or `smol` features provides it; the rest
//! of the waiting surface is identical on either, and the same async code runs on
//! whichever executor the application drives it with.

use core::time::Duration;

#[cfg(all(feature = "runtime", not(any(feature = "tokio", feature = "smol"))))]
compile_error!(
    "throttle-net: the async `acquire` surface needs a runtime timer — enable the `tokio` (default) or `smol` feature"
);

/// Sleeps for `duration` using tokio's timer.
#[cfg(feature = "tokio")]
#[inline]
pub(crate) async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

/// Sleeps for `duration` using smol's timer.
#[cfg(all(feature = "smol", not(feature = "tokio")))]
#[inline]
pub(crate) async fn sleep(duration: Duration) {
    let _ = smol::Timer::after(duration).await;
}
