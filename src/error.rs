//! The domain error type.
//!
//! The acquire path is mostly infallible: it returns a [`Decision`](crate::Decision)
//! or, for the waiting surface, simply succeeds once tokens are free. The one
//! failure that no amount of waiting can fix is a request whose cost exceeds the
//! limiter's capacity — that is reported as a [`ThrottleError`] rather than left
//! to spin forever.
//!
//! [`ThrottleError`] implements [`error_forge::ForgeError`], so it carries the
//! same kind/retryability metadata as every other domain error in the portfolio
//! stack.

use core::fmt;

use error_forge::ForgeError;

/// An acquisition that cannot complete.
///
/// The enum is `#[non_exhaustive]`: later phases introduce new failure modes
/// (deadlines, a tripped circuit breaker, a closed limiter), so a `match` on it
/// must include a wildcard arm.
///
/// # Examples
///
/// ```
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThrottleError {
    /// The requested cost is larger than the limiter's capacity, so the bucket
    /// can never hold enough tokens to grant it. Reduce the cost or raise the
    /// limiter's capacity. This is a configuration mismatch, not a transient
    /// condition, so it is **not** retryable.
    CostExceedsCapacity {
        /// The number of tokens the caller asked for.
        cost: u32,
        /// The limiter's maximum capacity, which `cost` exceeded.
        capacity: u32,
    },
}

impl fmt::Display for ThrottleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CostExceedsCapacity { cost, capacity } => write!(
                f,
                "requested cost {cost} exceeds limiter capacity {capacity}; it can never be granted"
            ),
        }
    }
}

impl std::error::Error for ThrottleError {}

impl ForgeError for ThrottleError {
    fn kind(&self) -> &'static str {
        match self {
            Self::CostExceedsCapacity { .. } => "CostExceedsCapacity",
        }
    }

    fn caption(&self) -> &'static str {
        "Throttle acquisition error"
    }
}

#[cfg(test)]
mod tests {
    use super::ThrottleError;
    use error_forge::ForgeError;

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
    fn test_forge_kind_matches_variant() {
        let err = ThrottleError::CostExceedsCapacity {
            cost: 1,
            capacity: 0,
        };
        assert_eq!(err.kind(), "CostExceedsCapacity");
    }

    #[test]
    fn test_capacity_mismatch_is_not_retryable() {
        // Retrying the same oversized cost on the same limiter never succeeds.
        let err = ThrottleError::CostExceedsCapacity {
            cost: 9,
            capacity: 5,
        };
        assert!(!err.is_retryable());
    }
}
