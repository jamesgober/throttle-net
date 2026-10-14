//! The domain error type.
//!
//! The non-waiting surface reports outcomes as a [`Decision`](crate::Decision)
//! or a `bool`; the waiting surface either succeeds once the request is
//! granted or returns a [`ThrottleError`] naming why it never will (or not in
//! time): a cost larger than the limiter can ever hold, a tripped circuit
//! breaker, a full queue or bulkhead, a passed deadline, an invalid
//! configuration, an unknown dimension name in strict mode, or a tower service
//! called before it was ready.
//!
//! [`ThrottleError`] implements [`error_forge::ForgeError`], so it carries the
//! same kind/retryability metadata as every other domain error in the portfolio
//! stack.

use core::fmt;
use core::time::Duration;

use error_forge::ForgeError;

/// Why an acquisition did not (or will not) complete.
///
/// The enum is `#[non_exhaustive]`: new failure modes may be added in minor
/// releases, so a `match` on it must include a wildcard arm. It is `Copy` and
/// carries no heap data, so it is cheap to return from hot paths.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "runtime")]
/// # async fn run() {
/// use throttle_net::{Throttle, ThrottleError};
///
/// // Capacity is 5; asking for 9 can never be satisfied.
/// let throttle = Throttle::per_second(5);
/// let err = throttle.acquire_with_cost(9).await.unwrap_err();
/// assert!(matches!(err, ThrottleError::CostExceedsCapacity { cost: 9, capacity: 5 }));
/// # }
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThrottleError {
    /// The requested cost is larger than the limiter's capacity (or the limiter
    /// grants nothing at all), so no amount of waiting can satisfy it. Reduce
    /// the cost or raise the capacity. This is a configuration mismatch, not a
    /// transient condition, so it is **not** retryable.
    CostExceedsCapacity {
        /// The number of units the caller asked for.
        cost: u32,
        /// The limiter's maximum capacity, which `cost` exceeded.
        capacity: u32,
    },
    /// A circuit breaker is open and shed this request without touching the
    /// wrapped limiter. The breaker is failing fast to give the downstream time
    /// to recover; retry once it has had a chance to close again. This **is**
    /// retryable, after the suggested wait.
    CircuitOpen {
        /// How long until the breaker is expected to admit a trial request
        /// (move to half-open). [`Duration::ZERO`] means it is half-open now but
        /// all trial slots are taken; try again shortly.
        retry_after: Duration,
    },
    /// A bounded queue was full and its overflow policy rejected (or evicted)
    /// this request. Transient — capacity may free up — so it is retryable.
    QueueFull,
    /// The request's deadline or timeout passed before it could be granted —
    /// or, for a reservation-backed limiter, it was clear up front that the
    /// earliest grant lay beyond the deadline, so the request gave up at once
    /// instead of waiting to fail. Not retryable as-is: the deadline is spent;
    /// a fresh attempt needs a fresh deadline.
    DeadlineExceeded,
    /// A bulkhead had no free slot and the request chose not to wait (or its
    /// wait timed out). Transient, so it is retryable.
    BulkheadFull,
    /// A builder was given a configuration that cannot work. `reason` names
    /// the violated constraint (for example `"rate must be non-zero"`). Fix the
    /// configuration; this is **not** retryable.
    InvalidConfig {
        /// The constraint the configuration violated.
        reason: &'static str,
    },
    /// A strict [`MultiLimiter`](crate::MultiLimiter) was charged against a
    /// dimension name it does not have. `index` is the position of the
    /// offending `(name, cost)` pair in the costs slice. This is a programming
    /// error (usually a typo), so it is **not** retryable.
    UnknownDimension {
        /// The index of the unknown name in the costs slice.
        index: usize,
    },
    /// A [`tower`](crate::tower) service was called without `poll_ready`
    /// having first reported it ready, so it held no admission for the request
    /// and did not send it. This breaks the `tower::Service` contract — a
    /// programming error — so it is **not** retryable.
    NotReady,
}

impl fmt::Display for ThrottleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CostExceedsCapacity { cost, capacity } => write!(
                f,
                "requested cost {cost} exceeds limiter capacity {capacity}; it can never be granted"
            ),
            Self::CircuitOpen { retry_after } => write!(
                f,
                "circuit breaker is open; request shed, retry in {retry_after:?}"
            ),
            Self::QueueFull => {
                f.write_str("queue is full; request rejected by the overflow policy")
            }
            Self::DeadlineExceeded => {
                f.write_str("deadline passed before the request could be granted")
            }
            Self::BulkheadFull => f.write_str("bulkhead is full; no concurrency slot is free"),
            Self::InvalidConfig { reason } => write!(f, "invalid limiter configuration: {reason}"),
            Self::UnknownDimension { index } => write!(
                f,
                "cost entry {index} names a dimension this strict multi-limiter does not have"
            ),
            Self::NotReady => f.write_str(
                "service called before poll_ready reported it ready; the request was not sent",
            ),
        }
    }
}

impl std::error::Error for ThrottleError {}

impl ForgeError for ThrottleError {
    fn kind(&self) -> &'static str {
        match self {
            Self::CostExceedsCapacity { .. } => "CostExceedsCapacity",
            Self::CircuitOpen { .. } => "CircuitOpen",
            Self::QueueFull => "QueueFull",
            Self::DeadlineExceeded => "DeadlineExceeded",
            Self::BulkheadFull => "BulkheadFull",
            Self::InvalidConfig { .. } => "InvalidConfig",
            Self::UnknownDimension { .. } => "UnknownDimension",
            Self::NotReady => "NotReady",
        }
    }

    fn caption(&self) -> &'static str {
        "Throttle acquisition error"
    }

    fn is_retryable(&self) -> bool {
        match self {
            // The downstream may recover, or capacity may free up.
            Self::CircuitOpen { .. } | Self::QueueFull | Self::BulkheadFull => true,
            // A configuration mismatch or a programming error will not fix
            // itself, and a spent deadline stays spent.
            Self::CostExceedsCapacity { .. }
            | Self::DeadlineExceeded
            | Self::InvalidConfig { .. }
            | Self::UnknownDimension { .. }
            | Self::NotReady => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ThrottleError;
    use core::time::Duration;
    use error_forge::ForgeError;

    fn all() -> [ThrottleError; 8] {
        [
            ThrottleError::CostExceedsCapacity {
                cost: 9,
                capacity: 5,
            },
            ThrottleError::CircuitOpen {
                retry_after: Duration::from_secs(3),
            },
            ThrottleError::QueueFull,
            ThrottleError::DeadlineExceeded,
            ThrottleError::BulkheadFull,
            ThrottleError::InvalidConfig {
                reason: "rate must be non-zero",
            },
            ThrottleError::UnknownDimension { index: 2 },
            ThrottleError::NotReady,
        ]
    }

    #[test]
    fn test_display_names_both_values() {
        let msg = ThrottleError::CostExceedsCapacity {
            cost: 9,
            capacity: 5,
        }
        .to_string();
        assert!(msg.contains('9'));
        assert!(msg.contains('5'));
    }

    #[test]
    fn test_every_variant_displays_actionable_text() {
        for err in all() {
            let msg = err.to_string();
            assert!(msg.len() > 10, "{err:?} rendered as {msg:?}");
        }
        assert!(
            ThrottleError::InvalidConfig { reason: "burst" }
                .to_string()
                .contains("burst")
        );
        assert!(
            ThrottleError::UnknownDimension { index: 4 }
                .to_string()
                .contains('4')
        );
    }

    #[test]
    fn test_forge_kind_matches_variant() {
        let kinds: Vec<&str> = all().iter().map(ForgeError::kind).collect();
        assert_eq!(
            kinds,
            [
                "CostExceedsCapacity",
                "CircuitOpen",
                "QueueFull",
                "DeadlineExceeded",
                "BulkheadFull",
                "InvalidConfig",
                "UnknownDimension",
                "NotReady",
            ]
        );
        assert!(
            all()
                .iter()
                .all(|e| e.caption() == "Throttle acquisition error")
        );
    }

    #[test]
    fn test_retryability_by_variant() {
        let retryable: Vec<bool> = all().iter().map(ForgeError::is_retryable).collect();
        assert_eq!(
            retryable,
            [false, true, true, false, true, false, false, false]
        );
    }

    #[test]
    fn test_is_a_std_error() {
        fn takes(_: &dyn std::error::Error) {}
        takes(&ThrottleError::QueueFull);
    }
}
