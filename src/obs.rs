//! Observability: per-instance names, metrics (the `metrics` feature), and
//! tracing events (the `tracing` feature).
//!
//! Every emitting hook is a small `#[inline]` function whose body is compiled
//! only when the matching feature is on; with both features off the hooks are
//! empty, inline away, and the acquire paths pay nothing. Arguments are cheap
//! values the caller already holds (`&'static str` kinds, integers, a
//! [`Label`] reference, a [`Timer`] that is zero-sized without either feature),
//! so supplying them costs nothing either.
//!
//! Each hook is compiled only in the builds whose limiters call it, so none is
//! dead code in any feature combination.
//!
//! # Metric set
//!
//! Every metric carries a `name` label (the instance name set with a
//! `with_name` / `name` builder method, `"default"` when unset) and, where more
//! than one limiter kind can emit it, a `limiter` label naming the kind.
//!
//! | Metric | Kind | Labels | Meaning |
//! |---|---|---|---|
//! | `throttle_acquired_total` | counter | `limiter`, `name` | Grants, from every public acquire path (waiting and non-waiting). |
//! | `throttle_denied_total` | counter | `limiter`, `name` | Non-waiting attempts refused (`try_*` returning `false`/`None`). |
//! | `throttle_timeouts_total` | counter | `limiter`, `name` | Waiting acquires that gave up at their timeout or deadline. |
//! | `throttle_wait_duration` | histogram (s) | `limiter`, `name` | Time a waiting acquire spent before it was granted or gave up. |
//! | `throttle_shed_total` | counter | `limiter`, `name` | Requests a circuit breaker or bulkhead turned away without waiting. |
//! | `throttle_circuit_state` | gauge | `name` | Breaker state: 0 closed, 1 half-open, 2 open. Set at construction and on every transition. |
//! | `throttle_queue_depth` | gauge | `name` | Waiters currently in a queue. |
//! | `throttle_queue_rejected_total` | counter | `name`, `policy` | Waiters a full queue turned away or evicted. |
//! | `throttle_rate_current` | gauge | `name` | An adaptive limiter's current concurrency limit. Set at construction and on every change. |
//! | `throttle_retries_total` | counter | `name` | Retries a [`Retry`](crate::Retry) policy scheduled. |
//! | `throttle_retry_budget_exhausted_total` | counter | `name` | Retries a shared [`RetryBudget`](crate::RetryBudget) refused. |

use std::borrow::Cow;
use std::sync::Arc;

/// The name label used when an instance was never named.
#[cfg(any(feature = "metrics", feature = "tracing"))]
const DEFAULT_NAME: &str = "default";

/// An instance name: a static string or a shared, reference-counted one.
#[derive(Clone)]
enum Value {
    Static(&'static str),
    Shared(Arc<str>),
}

/// An optional instance name, attached to every metric and event an instance
/// emits so two limiters of the same kind do not overwrite each other's gauges.
///
/// Cloning is cheap: a pointer copy or one reference-count increment.
#[derive(Clone, Default)]
pub(crate) struct Label {
    value: Option<Value>,
}

impl Label {
    /// An unnamed label (reported as `"default"`).
    #[inline]
    pub(crate) const fn none() -> Self {
        Self { value: None }
    }

    /// A label for `name`. A `&'static str` is stored as-is; an owned string is
    /// moved into one shared allocation, made once here rather than per emission.
    pub(crate) fn new(name: impl Into<Cow<'static, str>>) -> Self {
        let value = match name.into() {
            Cow::Borrowed(name) => Value::Static(name),
            Cow::Owned(name) => Value::Shared(Arc::from(name)),
        };
        Self { value: Some(value) }
    }

    /// The configured name, if any.
    #[inline]
    pub(crate) fn get(&self) -> Option<&str> {
        match &self.value {
            None => None,
            Some(Value::Static(name)) => Some(name),
            Some(Value::Shared(name)) => Some(name),
        }
    }

    /// The name as reported in metrics and events.
    #[cfg(feature = "tracing")]
    #[inline]
    fn as_str(&self) -> &str {
        self.get().unwrap_or(DEFAULT_NAME)
    }

    /// The name as a metrics label value, without allocating.
    #[cfg(feature = "metrics")]
    #[inline]
    fn shared(&self) -> metrics::SharedString {
        match &self.value {
            None => metrics::SharedString::const_str(DEFAULT_NAME),
            Some(Value::Static(name)) => metrics::SharedString::const_str(name),
            Some(Value::Shared(name)) => metrics::SharedString::from(Arc::clone(name)),
        }
    }
}

impl core::fmt::Debug for Label {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.get() {
            Some(name) => core::fmt::Debug::fmt(name, f),
            None => f.write_str("None"),
        }
    }
}

/// Measures how long a waiting acquire took, for the histogram and the event.
/// Holds an `Instant` only when an observability feature is on; otherwise it is
/// zero-sized and [`start`](Self::start) is a no-op.
#[cfg(feature = "runtime")]
pub(crate) struct Timer {
    #[cfg(any(feature = "metrics", feature = "tracing"))]
    start: std::time::Instant,
}

#[cfg(feature = "runtime")]
impl Timer {
    /// Begins timing (a no-op unless an observability feature is on).
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

/// Records a grant of `cost` units (`throttle_acquired_total`).
#[inline]
pub(crate) fn granted(kind: &'static str, label: &Label, cost: u32) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("throttle_acquired_total", "limiter" => kind, "name" => label.shared())
        .increment(1);
    #[cfg(feature = "tracing")]
    ::tracing::trace!(target: "throttle_net", limiter = kind, name = label.as_str(), cost, "granted");
    #[cfg(not(any(feature = "metrics", feature = "tracing")))]
    let _ = (kind, label, cost);
    #[cfg(all(feature = "metrics", not(feature = "tracing")))]
    let _ = cost;
}

/// Records a refused non-waiting attempt (`throttle_denied_total`).
#[inline]
pub(crate) fn denied(kind: &'static str, label: &Label) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("throttle_denied_total", "limiter" => kind, "name" => label.shared())
        .increment(1);
    #[cfg(feature = "tracing")]
    ::tracing::trace!(target: "throttle_net", limiter = kind, name = label.as_str(), "denied");
    #[cfg(not(any(feature = "metrics", feature = "tracing")))]
    let _ = (kind, label);
}

/// Records a non-waiting attempt's outcome: [`granted`] or [`denied`].
#[inline]
pub(crate) fn attempt(kind: &'static str, label: &Label, cost: u32, ok: bool) {
    if ok {
        granted(kind, label, cost);
    } else {
        denied(kind, label);
    }
}

/// How a waiting acquire ended.
#[cfg(feature = "runtime")]
#[derive(Clone, Copy)]
pub(crate) enum Waited {
    /// The request was granted.
    Granted,
    /// The request gave up at its timeout or deadline.
    TimedOut,
    /// The request failed for another reason (impossible cost, shed, full).
    Refused,
}

#[cfg(feature = "runtime")]
impl Waited {
    /// The outcome as an event field.
    #[cfg(feature = "tracing")]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Granted => "granted",
            Self::TimedOut => "timed_out",
            Self::Refused => "refused",
        }
    }

    /// Classifies a waiting acquire's result.
    #[inline]
    pub(crate) fn of<T>(result: &Result<T, crate::error::ThrottleError>) -> Self {
        match result {
            Ok(_) => Self::Granted,
            Err(crate::error::ThrottleError::DeadlineExceeded) => Self::TimedOut,
            Err(_) => Self::Refused,
        }
    }
}

/// Records the end of a waiting acquire: the wait histogram, a grant or a
/// timeout counter, and a debug event.
#[cfg(feature = "runtime")]
#[inline]
pub(crate) fn waited(kind: &'static str, label: &Label, timer: &Timer, cost: u32, outcome: Waited) {
    #[cfg(feature = "metrics")]
    {
        ::metrics::histogram!("throttle_wait_duration", "limiter" => kind, "name" => label.shared())
            .record(timer.secs());
        match outcome {
            Waited::Granted => {
                ::metrics::counter!("throttle_acquired_total", "limiter" => kind, "name" => label.shared())
                    .increment(1);
            }
            Waited::TimedOut => {
                ::metrics::counter!("throttle_timeouts_total", "limiter" => kind, "name" => label.shared())
                    .increment(1);
            }
            Waited::Refused => {}
        }
    }
    #[cfg(feature = "tracing")]
    ::tracing::debug!(
        target: "throttle_net",
        limiter = kind,
        name = label.as_str(),
        cost,
        outcome = outcome.as_str(),
        wait_secs = timer.secs(),
        "acquire",
    );
    #[cfg(not(any(feature = "metrics", feature = "tracing")))]
    let _ = (kind, label, timer, cost, outcome);
    #[cfg(all(feature = "metrics", not(feature = "tracing")))]
    let _ = cost;
}

/// Records a request shed without waiting by a breaker or bulkhead
/// (`throttle_shed_total`).
#[cfg(feature = "circuit-breaker")]
#[inline]
pub(crate) fn shed(kind: &'static str, label: &Label) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("throttle_shed_total", "limiter" => kind, "name" => label.shared())
        .increment(1);
    #[cfg(feature = "tracing")]
    ::tracing::debug!(target: "throttle_net", limiter = kind, name = label.as_str(), "shed");
    #[cfg(not(any(feature = "metrics", feature = "tracing")))]
    let _ = (kind, label);
}

/// Records a circuit-breaker state (`throttle_circuit_state`: 0 closed, 1
/// half-open, 2 open) and, for a transition, an info event naming both ends.
#[cfg(feature = "circuit-breaker")]
#[inline]
pub(crate) fn circuit_state(label: &Label, from: Option<&'static str>, to: &'static str, code: u8) {
    #[cfg(feature = "metrics")]
    ::metrics::gauge!("throttle_circuit_state", "name" => label.shared()).set(f64::from(code));
    #[cfg(feature = "tracing")]
    if let Some(from) = from {
        ::tracing::info!(target: "throttle_net", name = label.as_str(), from, to, "circuit breaker transition");
    }
    #[cfg(not(any(feature = "metrics", feature = "tracing")))]
    let _ = (label, from, to, code);
    #[cfg(all(feature = "metrics", not(feature = "tracing")))]
    let _ = (from, to);
    #[cfg(all(feature = "tracing", not(feature = "metrics")))]
    let _ = code;
}

/// Sets a queue's depth gauge (`throttle_queue_depth`).
#[cfg(feature = "runtime")]
#[inline]
pub(crate) fn queue_depth(label: &Label, depth: usize) {
    #[cfg(feature = "metrics")]
    {
        // A gauge is an approximate reading by nature, and f64 represents every
        // count a real queue reaches exactly.
        #[allow(clippy::cast_precision_loss)]
        let depth = depth as f64;
        ::metrics::gauge!("throttle_queue_depth", "name" => label.shared()).set(depth);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = (label, depth);
}

/// Records a waiter a full queue turned away or evicted
/// (`throttle_queue_rejected_total`).
#[cfg(feature = "runtime")]
#[inline]
pub(crate) fn queue_rejected(label: &Label, policy: &'static str) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("throttle_queue_rejected_total", "name" => label.shared(), "policy" => policy)
        .increment(1);
    #[cfg(feature = "tracing")]
    ::tracing::warn!(target: "throttle_net", name = label.as_str(), policy, "queue overflow");
    #[cfg(not(any(feature = "metrics", feature = "tracing")))]
    let _ = (label, policy);
}

/// Records an adaptive limiter's limit (`throttle_rate_current`) and, when it
/// moved, a debug event.
#[cfg(feature = "adaptive")]
#[inline]
pub(crate) fn adaptive_limit(label: &Label, old: u32, new: u32) {
    #[cfg(feature = "metrics")]
    ::metrics::gauge!("throttle_rate_current", "name" => label.shared()).set(f64::from(new));
    #[cfg(feature = "tracing")]
    if old != new {
        ::tracing::debug!(target: "throttle_net", name = label.as_str(), old, new, "adaptive limit changed");
    }
    #[cfg(not(any(feature = "metrics", feature = "tracing")))]
    let _ = (label, old, new);
    #[cfg(all(feature = "metrics", not(feature = "tracing")))]
    let _ = old;
}

/// Records a scheduled retry (`throttle_retries_total`).
#[cfg(feature = "runtime")]
#[inline]
pub(crate) fn retried(label: &Label, attempt: u32, delay: core::time::Duration) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("throttle_retries_total", "name" => label.shared()).increment(1);
    #[cfg(feature = "tracing")]
    ::tracing::debug!(
        target: "throttle_net",
        name = label.as_str(),
        attempt,
        delay_secs = delay.as_secs_f64(),
        "retrying",
    );
    #[cfg(not(any(feature = "metrics", feature = "tracing")))]
    let _ = (label, attempt, delay);
    #[cfg(all(feature = "metrics", not(feature = "tracing")))]
    let _ = (attempt, delay);
}

/// Records a retry a shared budget refused
/// (`throttle_retry_budget_exhausted_total`).
#[cfg(feature = "runtime")]
#[inline]
pub(crate) fn retry_budget_exhausted(label: &Label) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("throttle_retry_budget_exhausted_total", "name" => label.shared())
        .increment(1);
    #[cfg(feature = "tracing")]
    ::tracing::warn!(target: "throttle_net", name = label.as_str(), "retry budget exhausted");
    #[cfg(not(any(feature = "metrics", feature = "tracing")))]
    let _ = label;
}

#[cfg(test)]
mod tests {
    use super::Label;

    #[test]
    fn test_label_reports_static_and_owned_names() {
        assert_eq!(Label::none().get(), None);
        assert_eq!(Label::new("openai").get(), Some("openai"));
        assert_eq!(Label::new(String::from("tenant-7")).get(), Some("tenant-7"));
        assert_eq!(format!("{:?}", Label::new("x")), "\"x\"");
        assert_eq!(format!("{:?}", Label::none()), "None");
        assert_eq!(Label::default().get(), None);
    }

    #[cfg(all(
        feature = "runtime",
        not(any(feature = "metrics", feature = "tracing"))
    ))]
    #[test]
    fn test_timer_is_zero_sized_when_observability_is_off() {
        // The zero-cost guarantee, made concrete: with neither feature the
        // timer carries no state.
        assert_eq!(core::mem::size_of::<super::Timer>(), 0);
    }
}
