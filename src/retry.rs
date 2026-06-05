//! Retry policy: drive a fallible async operation with a [`Backoff`].

use crate::backoff::Backoff;

/// The default number of attempts a [`Retry`] makes before giving up.
const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// What to do with an error a retried operation returned.
///
/// A classifier (see [`Retry::run`]) inspects each error and returns one of
/// these. `#[non_exhaustive]` so future actions do not break callers.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryAction {
    /// Retry, waiting the policy's computed backoff delay.
    Retry,
    /// Retry, but wait at least this long — a server-supplied `Retry-After`
    /// override, honored when [`Retry::respect_retry_after`] is set.
    RetryAfter(core::time::Duration),
    /// Stop and return the error to the caller.
    GiveUp,
}

/// A retry policy: a [`Backoff`], an attempt ceiling, and whether to honor a
/// server's `Retry-After`.
///
/// The policy is independent of the limiters — retry any fallible async
/// operation, or wrap a limiter's `acquire` call. The error is classified per
/// attempt by a closure you supply, so retry works with any error type; for
/// errors that implement [`error_forge::ForgeError`], the
/// [`retry_if_retryable`] helper classifies by the error's own retryability.
///
/// # Examples
///
/// ```
/// # async fn run() {
/// use throttle_net::{Backoff, Retry, RetryAction};
///
/// let retry = Retry::new(Backoff::default()).max_attempts(4);
///
/// let result: Result<u32, &str> = retry
///     .run(
///         || async { Err("transient") },
///         |_err| RetryAction::Retry,
///     )
///     .await;
/// assert_eq!(result, Err("transient")); // gave up after 4 attempts
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Retry {
    backoff: Backoff,
    max_attempts: u32,
    respect_retry_after: bool,
}

impl Retry {
    /// Creates a retry policy with the given backoff, a default of five
    /// attempts, and `Retry-After` honored.
    #[must_use]
    pub fn new(backoff: Backoff) -> Self {
        Self {
            backoff,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            respect_retry_after: true,
        }
    }

    /// Sets the maximum number of attempts (including the first). A value of `1`
    /// disables retrying; `0` is treated as `1`.
    #[must_use]
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// Sets whether a [`RetryAction::RetryAfter`] delay overrides the computed
    /// backoff. On by default.
    #[must_use]
    pub fn respect_retry_after(mut self, yes: bool) -> Self {
        self.respect_retry_after = yes;
        self
    }

    /// The configured attempt ceiling.
    #[must_use]
    pub const fn attempts(&self) -> u32 {
        self.max_attempts
    }

    /// The configured backoff policy.
    #[must_use]
    pub const fn backoff(&self) -> &Backoff {
        &self.backoff
    }
}

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
impl Retry {
    /// Runs `operation`, retrying on failure per the policy until it succeeds,
    /// the classifier says to stop, or the attempt ceiling is reached.
    ///
    /// `operation` is called once per attempt. `classify` inspects each error and
    /// returns a [`RetryAction`]: retry with the backoff delay, retry honoring a
    /// `Retry-After`, or give up. The last error is returned when attempts run
    /// out or the classifier gives up.
    ///
    /// # Examples
    ///
    /// Retry on a `Retry-After` the server sent, parsed with
    /// [`parse_retry_after`](crate::parse_retry_after):
    ///
    /// ```
    /// # async fn run() {
    /// use std::time::Duration;
    /// use throttle_net::{Backoff, Retry, RetryAction};
    ///
    /// struct Rejected { retry_after: Option<Duration> }
    ///
    /// let retry = Retry::new(Backoff::default()).respect_retry_after(true);
    /// let result: Result<(), &str> = retry
    ///     .run(
    ///         || async { Err::<(), _>(Rejected { retry_after: Some(Duration::from_millis(10)) }) },
    ///         |err: &Rejected| match err.retry_after {
    ///             Some(after) => RetryAction::RetryAfter(after),
    ///             None => RetryAction::Retry,
    ///         },
    ///     )
    ///     .await
    ///     .map(|_| ())
    ///     .map_err(|_| "exhausted");
    /// assert_eq!(result, Err("exhausted"));
    /// # }
    /// ```
    pub async fn run<F, Fut, T, E, C>(&self, mut operation: F, classify: C) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: core::future::Future<Output = Result<T, E>>,
        C: Fn(&E) -> RetryAction,
    {
        let mut delays = self.backoff.iter();
        let mut attempt = 1u32;
        loop {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    if attempt >= self.max_attempts {
                        return Err(error);
                    }
                    let delay = match classify(&error) {
                        RetryAction::GiveUp => return Err(error),
                        RetryAction::Retry => delays.next_delay(),
                        RetryAction::RetryAfter(after) => {
                            // Always advance the backoff so its state (and
                            // decorrelated jitter) keeps progressing, even when
                            // the server's hint overrides the chosen delay.
                            let computed = delays.next_delay();
                            if self.respect_retry_after {
                                after
                            } else {
                                computed
                            }
                        }
                    };
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }
}

/// Classifies an [`error_forge::ForgeError`] by its own retryability: retry when
/// [`is_retryable`](error_forge::ForgeError::is_retryable) is `true`, otherwise
/// give up. A convenient `classify` argument for [`Retry::run`].
///
/// # Examples
///
/// ```
/// # async fn run() {
/// use throttle_net::{Backoff, Retry, retry_if_retryable};
/// # use error_forge::AppError;
///
/// let retry = Retry::new(Backoff::default());
/// let result = retry
///     .run(
///         || async { Err::<(), _>(AppError::network("api.example", None)) },
///         retry_if_retryable,
///     )
///     .await;
/// assert!(result.is_err());
/// # }
/// ```
#[must_use]
pub fn retry_if_retryable<E: error_forge::ForgeError>(error: &E) -> RetryAction {
    if error.is_retryable() {
        RetryAction::Retry
    } else {
        RetryAction::GiveUp
    }
}

#[cfg(test)]
mod tests {
    use super::{Retry, RetryAction, retry_if_retryable};
    use crate::backoff::Backoff;
    use core::time::Duration;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn fast_policy() -> Retry {
        // Tiny, deterministic delays so paused-time tests stay exact.
        Retry::new(Backoff::constant(Duration::from_millis(10)))
    }

    #[test]
    fn test_max_attempts_floor_is_one() {
        assert_eq!(fast_policy().max_attempts(0).attempts(), 1);
        assert_eq!(fast_policy().max_attempts(7).attempts(), 7);
    }

    #[tokio::test(start_paused = true)]
    async fn test_succeeds_after_transient_failures() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let result: Result<u32, &str> = fast_policy()
            .max_attempts(5)
            .run(
                move || {
                    let c = c.clone();
                    async move {
                        let n = c.fetch_add(1, Ordering::Relaxed) + 1;
                        if n < 3 { Err("transient") } else { Ok(n) }
                    }
                },
                |_| RetryAction::Retry,
            )
            .await;
        assert_eq!(result, Ok(3));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn test_gives_up_after_max_attempts() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let result: Result<(), &str> = fast_policy()
            .max_attempts(3)
            .run(
                move || {
                    let c = c.clone();
                    async move {
                        let _ = c.fetch_add(1, Ordering::Relaxed);
                        Err("always")
                    }
                },
                |_| RetryAction::Retry,
            )
            .await;
        assert_eq!(result, Err("always"));
        assert_eq!(
            calls.load(Ordering::Relaxed),
            3,
            "operation runs exactly max_attempts times"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_give_up_classification_stops_immediately() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let result: Result<(), &str> = fast_policy()
            .max_attempts(10)
            .run(
                move || {
                    let c = c.clone();
                    async move {
                        let _ = c.fetch_add(1, Ordering::Relaxed);
                        Err("fatal")
                    }
                },
                |_| RetryAction::GiveUp,
            )
            .await;
        assert_eq!(result, Err("fatal"));
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "GiveUp stops after the first attempt"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_after_is_honored_when_enabled() {
        let start = tokio::time::Instant::now();
        let policy = Retry::new(Backoff::constant(Duration::from_secs(1)))
            .max_attempts(2)
            .respect_retry_after(true);
        let _: Result<(), &str> = policy
            .run(
                || async { Err("rejected") },
                |_| RetryAction::RetryAfter(Duration::from_secs(30)),
            )
            .await;
        // One retry, waiting the 30s Retry-After rather than the 1s backoff.
        assert_eq!(start.elapsed(), Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_after_is_ignored_when_disabled() {
        let start = tokio::time::Instant::now();
        let policy = Retry::new(Backoff::constant(Duration::from_secs(1)))
            .max_attempts(2)
            .respect_retry_after(false);
        let _: Result<(), &str> = policy
            .run(
                || async { Err("rejected") },
                |_| RetryAction::RetryAfter(Duration::from_secs(30)),
            )
            .await;
        // The 30s hint is ignored; the 1s computed backoff is used instead.
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_if_retryable_helper() {
        use error_forge::AppError;

        // A non-retryable error gives up immediately.
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let result: Result<(), AppError> = fast_policy()
            .max_attempts(5)
            .run(
                move || {
                    let c = c.clone();
                    async move {
                        let _ = c.fetch_add(1, Ordering::Relaxed);
                        Err(AppError::config("bad"))
                    }
                },
                retry_if_retryable,
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "config errors are not retryable"
        );
    }
}
