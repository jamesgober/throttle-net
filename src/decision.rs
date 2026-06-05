//! The outcome of a non-blocking acquisition attempt.

use core::time::Duration;

/// What happened when a limiter was asked for tokens without waiting.
///
/// This is the synchronous core that the waiting
/// [`acquire`](crate::Throttle::acquire) surface is built on, and the value the
/// [`Limiter`](crate::Limiter) trait returns so composite limiters can reason
/// about an outcome before deciding whether to wait. The waiting layer maps it
/// to either a return, a sleep, or a [`ThrottleError`](crate::ThrottleError).
///
/// `#[non_exhaustive]`: later phases add outcomes (for example, a deadline or a
/// circuit-open signal), so a `match` on it must include a wildcard arm.
///
/// # Examples
///
/// ```
/// use throttle_net::{Decision, Limiter, Throttle};
///
/// let throttle = Throttle::per_second(1);
/// // The bucket starts full, so the first unit is granted immediately.
/// assert_eq!(throttle.acquire_cost(1), Decision::Acquired);
/// // The next unit must wait for the bucket to refill.
/// assert!(matches!(throttle.acquire_cost(1), Decision::Retry { .. }));
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The tokens were granted and have been deducted.
    Acquired,
    /// The request was refused for now. The bucket will hold enough tokens
    /// again after `after` has elapsed, assuming no competing acquisitions.
    Retry {
        /// The minimum wait until a retry of the same cost can succeed.
        after: Duration,
    },
    /// The request can never succeed: the cost exceeds the limiter's capacity,
    /// so no amount of waiting will satisfy it.
    Impossible,
}

impl Decision {
    /// Returns `true` if the tokens were granted.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Decision;
    ///
    /// assert!(Decision::Acquired.is_acquired());
    /// assert!(!Decision::Impossible.is_acquired());
    /// ```
    #[inline]
    #[must_use]
    pub const fn is_acquired(&self) -> bool {
        matches!(self, Self::Acquired)
    }

    /// Returns the wait before a retry can succeed, or `None` when the request
    /// was granted or is impossible.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::Decision;
    ///
    /// let d = Decision::Retry { after: Duration::from_millis(20) };
    /// assert_eq!(d.retry_after(), Some(Duration::from_millis(20)));
    /// assert_eq!(Decision::Acquired.retry_after(), None);
    /// ```
    #[inline]
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Retry { after } => Some(*after),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Decision;
    use core::time::Duration;

    #[test]
    fn test_is_acquired_only_for_acquired() {
        assert!(Decision::Acquired.is_acquired());
        assert!(
            !Decision::Retry {
                after: Duration::ZERO
            }
            .is_acquired()
        );
        assert!(!Decision::Impossible.is_acquired());
    }

    #[test]
    fn test_retry_after_returns_wait_only_for_retry() {
        let wait = Duration::from_millis(5);
        assert_eq!(Decision::Retry { after: wait }.retry_after(), Some(wait));
        assert_eq!(Decision::Acquired.retry_after(), None);
        assert_eq!(Decision::Impossible.retry_after(), None);
    }
}
