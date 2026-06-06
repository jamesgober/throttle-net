//! Internal observability hooks: metrics (the `metrics` feature) and tracing
//! events (the `tracing` feature).
//!
//! Every hook is a small `#[inline]` function that expands to its
//! metrics/tracing call when the feature is on and to nothing when it is off —
//! the off branch only binds the arguments to `_`, so an empty function is
//! inlined away and the hot path pays nothing. Inputs are passed as already-cheap
//! values (`&'static str`, integers, a [`Timer`] that is zero-sized when both
//! observability features are off), so they cost nothing to supply either.
//!
//! Each hook is additionally gated to the feature of the limiter that calls it
//! (the waiting `tokio` surface, the `circuit-breaker`, the `adaptive` limiter),
//! so no hook is dead code in any build.
//!
//! Metric names match the documented set: `throttle_acquired_total`,
//! `throttle_wait_duration`, `throttle_queue_depth`, `throttle_circuit_state`,
//! `throttle_rate_current`.

/// Measures wait time for the metrics histogram and the tracing event. Holds an
/// `Instant` only when at least one observability feature is enabled; otherwise
/// it is zero-sized and `start` is a no-op.
#[cfg(feature = "tokio")]
pub(crate) struct Timer {
    #[cfg(any(feature = "metrics", feature = "tracing"))]
    start: std::time::Instant,
}

#[cfg(feature = "tokio")]
impl Timer {
    /// Begins timing (a no-op unless an observability feature is enabled).
    #[inline]
    pub(crate) fn start() -> Self {
        Self {
            #[cfg(any(feature = "metrics", feature = "tracing"))]
            start: std::time::Instant::now(),
        }
    }

    /// Elapsed seconds since [`start`](Self::start).
    #[cfg(any(feature = "metrics", feature = "tracing"))]
    #[inline]
    fn secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }
}

/// Records a granted acquisition (`throttle_acquired_total`).
#[cfg(feature = "tokio")]
#[inline]
pub(crate) fn acquired(limiter: &'static str) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("throttle_acquired_total", "limiter" => limiter).increment(1);
    #[cfg(not(feature = "metrics"))]
    let _ = limiter;
}

/// Records how long an acquisition waited (`throttle_wait_duration`, seconds).
#[cfg(feature = "tokio")]
#[inline]
pub(crate) fn wait(limiter: &'static str, timer: &Timer) {
    #[cfg(feature = "metrics")]
    ::metrics::histogram!("throttle_wait_duration", "limiter" => limiter).record(timer.secs());
    #[cfg(not(feature = "metrics"))]
    let _ = (limiter, timer);
}

/// Emits a tracing event describing a completed acquisition.
#[cfg(feature = "tokio")]
#[inline]
pub(crate) fn trace_acquire(limiter: &'static str, cost: u32, granted: bool, timer: &Timer) {
    #[cfg(feature = "tracing")]
    ::tracing::debug!(
        target: "throttle_net",
        limiter,
        cost,
        granted,
        wait_secs = timer.secs(),
        "acquire",
    );
    #[cfg(not(feature = "tracing"))]
    let _ = (limiter, cost, granted, timer);
}

/// Sets the current queue depth gauge (`throttle_queue_depth`).
#[cfg(feature = "tokio")]
#[inline]
pub(crate) fn queue_depth(depth: usize) {
    #[cfg(feature = "metrics")]
    #[allow(clippy::cast_precision_loss)]
    ::metrics::gauge!("throttle_queue_depth").set(depth as f64);
    #[cfg(not(feature = "metrics"))]
    let _ = depth;
}

/// Emits a structured event for a queue overflow (a waiter rejected or evicted).
#[cfg(feature = "tokio")]
#[inline]
pub(crate) fn queue_overflow(policy: &'static str) {
    #[cfg(feature = "tracing")]
    ::tracing::warn!(target: "throttle_net", policy, "queue overflow");
    #[cfg(not(feature = "tracing"))]
    let _ = policy;
}

/// Emits a structured event for a waiter dropped because its deadline passed.
#[cfg(feature = "tokio")]
#[inline]
pub(crate) fn deadline_exceeded() {
    #[cfg(feature = "tracing")]
    ::tracing::warn!(target: "throttle_net", "queue waiter deadline exceeded");
}

/// Records a circuit-breaker state change: the gauge (`throttle_circuit_state`:
/// 0 closed, 1 half-open, 2 open) plus a transition event.
#[cfg(feature = "circuit-breaker")]
#[inline]
pub(crate) fn circuit_transition(from: &'static str, to: &'static str, state: u8) {
    #[cfg(feature = "metrics")]
    ::metrics::gauge!("throttle_circuit_state").set(f64::from(state));
    #[cfg(feature = "tracing")]
    ::tracing::info!(target: "throttle_net", from, to, "circuit breaker transition");
    #[cfg(not(any(feature = "metrics", feature = "tracing")))]
    let _ = (from, to, state);
    #[cfg(all(feature = "metrics", not(feature = "tracing")))]
    let _ = (from, to);
    #[cfg(all(feature = "tracing", not(feature = "metrics")))]
    let _ = state;
}

/// Records an adaptive-limiter limit change: the gauge (`throttle_rate_current`)
/// plus a change event. Only called when the limit actually moves.
#[cfg(feature = "adaptive")]
#[inline]
pub(crate) fn rate_change(old: u32, new: u32) {
    #[cfg(feature = "metrics")]
    ::metrics::gauge!("throttle_rate_current").set(f64::from(new));
    #[cfg(feature = "tracing")]
    ::tracing::debug!(target: "throttle_net", old, new, "adaptive limit changed");
    #[cfg(not(any(feature = "metrics", feature = "tracing")))]
    let _ = (old, new);
    #[cfg(all(feature = "metrics", not(feature = "tracing")))]
    let _ = old;
}

#[cfg(all(
    test,
    feature = "tokio",
    not(any(feature = "metrics", feature = "tracing"))
))]
mod tests {
    use super::Timer;

    #[test]
    fn test_timer_is_zero_sized_when_observability_is_off() {
        // The zero-cost guarantee, made concrete: with neither feature, the
        // timer carries no state.
        assert_eq!(core::mem::size_of::<Timer>(), 0);
    }
}
