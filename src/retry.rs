//! Retry policy: drive a fallible async operation with a [`Backoff`], inside
//! guardrails — an attempt ceiling, a cap on server-requested waits, a total
//! time budget, and an optional retry budget shared across callers.

use core::time::Duration;

use better_bucket::{Bucket, BucketConfig};
use clock_lib::{Clock, SystemClock};

use crate::backoff::Backoff;

/// The default number of attempts a [`Retry`] makes before giving up.
const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// The default longest server-requested (`Retry-After`) wait a [`Retry`]
/// honours before giving up instead: five minutes.
const DEFAULT_MAX_RETRY_AFTER: Duration = Duration::from_secs(300);

/// What to do with an error a retried operation returned.
///
/// A classifier (see [`Retry::run`]) inspects each error and returns one of
/// these. `#[non_exhaustive]` so future actions do not break callers.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetryAction {
    /// Retry, waiting the policy's computed backoff delay.
    Retry,
    /// Retry after this server-supplied delay (a `Retry-After`) instead of the
    /// computed backoff — honored when [`Retry::respect_retry_after`] is set
    /// (the default) and the delay is within [`Retry::max_retry_after`];
    /// a longer delay ends the loop with the error.
    RetryAfter(Duration),
    /// Stop and return the error to the caller.
    GiveUp,
}

/// A retry policy: a [`Backoff`], an attempt ceiling, and the guardrails that
/// keep a retry loop from stalling or amplifying load.
///
/// - **Attempts** — [`max_attempts`](Self::max_attempts) (default 5, counting
///   the first).
/// - **Server-requested waits** — a `Retry-After` is honored
///   ([`respect_retry_after`](Self::respect_retry_after), on by default), but
///   only up to [`max_retry_after`](Self::max_retry_after) (default 5 minutes).
///   A longer request ends the loop with the error instead of parking the task
///   for hours, or for the decades a hostile or broken header can ask for.
/// - **Total time** — [`max_elapsed`](Self::max_elapsed) (off by default)
///   bounds the operation's run time plus the waits between attempts: a retry
///   whose wait would cross it is not attempted.
/// - **Fleet-wide retries** — [`run_with_budget`](Self::run_with_budget) draws
///   every retry from a shared [`RetryBudget`], so a downstream outage cannot
///   multiply your traffic into a retry storm.
///
/// The policy is independent of the limiters — retry any fallible async
/// operation, or wrap a limiter's `acquire`. Errors are classified per attempt
/// by a closure you supply, so retry works with any error type; for errors that
/// implement [`error_forge::ForgeError`], [`retry_if_retryable`] classifies by
/// the error's own retryability.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "runtime")]
/// # async fn run() {
/// use std::time::Duration;
/// use throttle_net::{Backoff, Retry, RetryAction};
///
/// let retry = Retry::new(Backoff::default())
///     .max_attempts(4)
///     .max_elapsed(Duration::from_secs(10));
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
    max_retry_after: Duration,
    max_elapsed: Option<Duration>,
    name: Option<&'static str>,
}

impl Retry {
    /// Creates a retry policy with the given backoff, five attempts,
    /// `Retry-After` honored up to five minutes, and no total time budget.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Backoff, Retry};
    ///
    /// let retry = Retry::new(Backoff::default());
    /// assert_eq!(retry.attempts(), 5);
    /// ```
    #[must_use]
    pub fn new(backoff: Backoff) -> Self {
        Self {
            backoff,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            respect_retry_after: true,
            max_retry_after: DEFAULT_MAX_RETRY_AFTER,
            max_elapsed: None,
            name: None,
        }
    }

    /// Sets the maximum number of attempts (including the first). A value of `1`
    /// disables retrying; `0` is treated as `1`.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::{Backoff, Retry};
    ///
    /// assert_eq!(Retry::new(Backoff::default()).max_attempts(0).attempts(), 1);
    /// ```
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

    /// Sets the longest server-requested wait to honor. A
    /// [`RetryAction::RetryAfter`] beyond it ends the loop with the error
    /// rather than sleeping that long. Defaults to five minutes;
    /// `Duration::MAX` honors any value (the 1.0 behavior).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use throttle_net::{Backoff, Retry};
    ///
    /// let retry = Retry::new(Backoff::default()).max_retry_after(Duration::from_secs(60));
    /// assert_eq!(retry.retry_after_cap(), Duration::from_secs(60));
    /// ```
    #[must_use]
    pub fn max_retry_after(mut self, cap: Duration) -> Self {
        self.max_retry_after = cap;
        self
    }

    /// Sets a total time budget: the operation's run time plus the waits
    /// between attempts. A retry whose wait would cross the budget is not
    /// attempted; the last error is returned. Off by default.
    ///
    /// Run time is measured with the monotonic clock and waits are counted as
    /// scheduled, so the budget is exact under a paused test runtime too.
    #[must_use]
    pub fn max_elapsed(mut self, budget: Duration) -> Self {
        self.max_elapsed = Some(budget);
        self
    }

    /// Names the policy for metrics and events (`throttle_retries_total`).
    #[must_use]
    pub fn name(mut self, name: &'static str) -> Self {
        self.name = Some(name);
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

    /// The longest server-requested wait honored.
    #[must_use]
    pub const fn retry_after_cap(&self) -> Duration {
        self.max_retry_after
    }

    /// The total time budget, if one is set.
    #[must_use]
    pub const fn elapsed_budget(&self) -> Option<Duration> {
        self.max_elapsed
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl Retry {
    /// Runs `operation`, retrying on failure per the policy until it succeeds,
    /// the classifier says to stop, a guardrail is reached, or the attempt
    /// ceiling runs out — whichever comes first. The last error is returned.
    ///
    /// `operation` is called once per attempt. `classify` inspects each error
    /// and returns a [`RetryAction`]: retry with the backoff delay, retry after
    /// a server's `Retry-After`, or give up.
    ///
    /// Cancel-safe: dropping the future mid-wait simply stops retrying.
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
    pub async fn run<F, Fut, T, E, C>(&self, operation: F, classify: C) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: core::future::Future<Output = Result<T, E>>,
        C: Fn(&E) -> RetryAction,
    {
        self.drive::<F, Fut, T, E, C, SystemClock>(operation, classify, None)
            .await
    }

    /// Runs `operation` like [`run`](Self::run), drawing every retry from a
    /// shared [`RetryBudget`]: the call deposits into the budget once, and each
    /// retry withdraws one; when the budget is empty the loop stops and returns
    /// the error instead of retrying.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() {
    /// use throttle_net::{Backoff, Retry, RetryAction, RetryBudget};
    ///
    /// // Retries may add at most 20% to the request rate, with a floor of one
    /// // retry per second.
    /// let budget = RetryBudget::new(0.2, 1);
    /// let retry = Retry::new(Backoff::default());
    ///
    /// let result: Result<(), &str> = retry
    ///     .run_with_budget(&budget, || async { Err("down") }, |_| RetryAction::Retry)
    ///     .await;
    /// assert!(result.is_err());
    /// # }
    /// ```
    pub async fn run_with_budget<F, Fut, T, E, C, B>(
        &self,
        budget: &RetryBudget<B>,
        operation: F,
        classify: C,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: core::future::Future<Output = Result<T, E>>,
        C: Fn(&E) -> RetryAction,
        B: Clock,
    {
        budget.deposit();
        self.drive(operation, classify, Some(budget)).await
    }

    /// The retry loop shared by [`run`](Self::run) and
    /// [`run_with_budget`](Self::run_with_budget).
    async fn drive<F, Fut, T, E, C, B>(
        &self,
        mut operation: F,
        classify: C,
        budget: Option<&RetryBudget<B>>,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: core::future::Future<Output = Result<T, E>>,
        C: Fn(&E) -> RetryAction,
        B: Clock,
    {
        let label = self
            .name
            .map_or_else(crate::obs::Label::none, crate::obs::Label::new);
        let mut delays = self.backoff.iter();
        let mut attempt = 1u32;
        let mut elapsed = Duration::ZERO;
        loop {
            let started = std::time::Instant::now();
            let outcome = operation().await;
            elapsed = elapsed.saturating_add(started.elapsed());
            let error = match outcome {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            if attempt >= self.max_attempts {
                return Err(error);
            }
            let delay = match classify(&error) {
                RetryAction::GiveUp => return Err(error),
                RetryAction::Retry => delays.next_delay(),
                RetryAction::RetryAfter(after) => {
                    // Always advance the backoff so its state (and decorrelated
                    // jitter) keeps progressing, even when the server's hint
                    // overrides the chosen delay.
                    let computed = delays.next_delay();
                    if !self.respect_retry_after {
                        computed
                    } else if after > self.max_retry_after {
                        return Err(error);
                    } else {
                        after
                    }
                }
            };
            if self
                .max_elapsed
                .is_some_and(|max| elapsed.saturating_add(delay) > max)
            {
                return Err(error);
            }
            if let Some(budget) = budget {
                if !budget.try_withdraw() {
                    crate::obs::retry_budget_exhausted(&label);
                    return Err(error);
                }
            }
            crate::obs::retried(&label, attempt, delay);
            crate::rt::sleep(delay).await;
            elapsed = elapsed.saturating_add(delay);
            attempt += 1;
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
/// # #[cfg(feature = "runtime")]
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

/// Milli-retries per retry: the budget's internal unit, so a fractional ratio
/// (0.1 retries per request) is exact in integer arithmetic.
const MILLI: u32 = 1_000;

/// A retry allowance shared by many retry loops, so retries can add at most a
/// bounded fraction to the request rate.
///
/// Each logical request [deposits](Self::deposit) `ratio` retries (for example
/// `0.2`: one retry per five requests), and the budget also accrues a floor of
/// `min_per_second` retries per second so a quiet service can still retry.
/// Each retry [withdraws](Self::try_withdraw) one. When a downstream fails
/// wholesale, the budget empties after a bounded number of retries and further
/// failures are returned at once instead of being retried — the retry-storm
/// protection that finagle and tower call a retry budget. The balance is capped
/// at ten seconds' worth of the floor (at least ten retries) unless set with
/// [`with_max_balance`](Self::with_max_balance).
///
/// The balance is a lock-free token bucket (better-bucket) in milli-retries;
/// share one budget across tasks behind an [`Arc`](std::sync::Arc) or a
/// `static`.
///
/// # Examples
///
/// ```
/// use throttle_net::RetryBudget;
///
/// let budget = RetryBudget::new(0.5, 0); // one retry per two requests, no floor
/// assert!(!budget.try_withdraw());       // nothing deposited yet
/// budget.deposit();
/// budget.deposit();
/// assert!(budget.try_withdraw());        // two requests bought one retry
/// assert!(!budget.try_withdraw());
/// ```
pub struct RetryBudget<C: Clock = SystemClock> {
    balance: Bucket<C>,
    /// Milli-retries each request earns.
    deposit: u32,
    /// Retries per second accrued regardless of traffic.
    floor: u32,
    /// The most whole retries the balance can hold.
    max_retries: u32,
}

impl RetryBudget<SystemClock> {
    /// Creates a budget that earns `ratio` retries per request (clamped to
    /// `0.0..=1000.0`; NaN counts as zero) plus `min_per_second` retries per
    /// second, starting with one second's floor in hand.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::RetryBudget;
    ///
    /// let budget = RetryBudget::new(0.1, 5);
    /// assert_eq!(budget.balance(), 5); // the first second's floor
    /// ```
    #[must_use]
    pub fn new(ratio: f64, min_per_second: u32) -> Self {
        let ratio = if ratio.is_nan() {
            0.0
        } else {
            ratio.clamp(0.0, 1_000.0)
        };
        // At most 1e6 milli-retries: fits a u32 exactly.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let deposit = (ratio * f64::from(MILLI)).round() as u32;
        let max_retries = min_per_second.saturating_mul(10).max(10);
        Self::assemble(deposit, min_per_second, max_retries, SystemClock::new())
    }

    /// Caps the banked balance at `retries` (at least one; the default is ten
    /// seconds' worth of the floor, and at least ten). The balance restarts.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::RetryBudget;
    ///
    /// let budget = RetryBudget::new(1.0, 0).with_max_balance(2);
    /// for _ in 0..10 {
    ///     budget.deposit();
    /// }
    /// assert_eq!(budget.balance(), 2);
    /// ```
    #[must_use]
    pub fn with_max_balance(self, retries: u32) -> Self {
        Self::assemble(self.deposit, self.floor, retries, SystemClock::new())
    }
}

impl<C: Clock> RetryBudget<C> {
    fn assemble(deposit: u32, floor: u32, max_retries: u32, clock: C) -> Self {
        let max_retries = max_retries.clamp(1, u32::MAX / MILLI);
        let capacity = max_retries * MILLI;
        // A floor of zero still needs a non-zero refill for a valid bucket: one
        // milli-retry per hour (one retry per ~42 days) is no floor at all.
        let (amount, period) = if floor == 0 {
            (1, Duration::from_secs(3_600))
        } else {
            (
                floor.saturating_mul(MILLI).min(capacity),
                Duration::from_secs(1),
            )
        };
        let initial = floor.saturating_mul(MILLI).min(capacity);
        let balance = match BucketConfig::new(capacity, amount, period, initial) {
            Ok(config) => Bucket::with_config_and_clock(config, clock),
            // Unreachable: every argument above is non-zero and in range. A
            // closed bucket (no retries) is the safe fallback.
            Err(_) => Bucket::per_second(0).with_clock(clock),
        };
        Self {
            balance,
            deposit,
            floor,
            max_retries,
        }
    }

    /// Replaces the clock (for deterministic tests). The balance restarts.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use throttle_net::{ManualClock, RetryBudget};
    ///
    /// let clock = Arc::new(ManualClock::new());
    /// let budget = RetryBudget::new(0.0, 2).with_clock(clock.clone());
    /// assert!(budget.try_withdraw());
    /// assert!(budget.try_withdraw());
    /// assert!(!budget.try_withdraw());
    /// clock.advance(Duration::from_secs(1)); // the floor accrues again
    /// assert!(budget.try_withdraw());
    /// ```
    #[must_use]
    pub fn with_clock<C2: Clock>(self, clock: C2) -> RetryBudget<C2> {
        RetryBudget::assemble(self.deposit, self.floor, self.max_retries, clock)
    }

    /// Records one logical request, earning `ratio` retries.
    pub fn deposit(&self) {
        // A deposit is a refund of units that were never taken: exactly the
        // "earn credit" operation, capped at the balance ceiling.
        self.balance.refund(self.deposit);
    }

    /// Takes one retry from the budget, returning whether one was available.
    #[must_use]
    pub fn try_withdraw(&self) -> bool {
        self.balance.try_acquire(MILLI)
    }

    /// Whole retries available right now.
    #[must_use]
    pub fn balance(&self) -> u32 {
        self.balance.available() / MILLI
    }
}

impl<C: Clock> core::fmt::Debug for RetryBudget<C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RetryBudget")
            .field("ratio", &(f64::from(self.deposit) / f64::from(MILLI)))
            .field("min_per_second", &self.floor)
            .field("max_balance", &self.max_retries)
            .field("balance", &self.balance())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::{Retry, RetryBudget};
    use crate::backoff::Backoff;
    use clock_lib::ManualClock;
    use core::time::Duration;
    use std::sync::Arc;

    fn fast_policy() -> Retry {
        // Tiny, deterministic delays so paused-time tests stay exact.
        Retry::new(Backoff::constant(Duration::from_millis(10)))
    }

    #[test]
    fn test_builder_accessors() {
        let retry = fast_policy()
            .max_attempts(0)
            .max_retry_after(Duration::from_secs(9))
            .max_elapsed(Duration::from_secs(30))
            .respect_retry_after(false)
            .name("payments");
        assert_eq!(retry.attempts(), 1);
        assert_eq!(retry.retry_after_cap(), Duration::from_secs(9));
        assert_eq!(retry.elapsed_budget(), Some(Duration::from_secs(30)));
        assert_eq!(retry.backoff().max(), Duration::from_millis(10));
        assert_eq!(
            Retry::new(Backoff::default()).retry_after_cap(),
            Duration::from_secs(300)
        );
        assert_eq!(Retry::new(Backoff::default()).elapsed_budget(), None);
    }

    #[test]
    fn test_budget_earns_by_ratio_and_caps_the_balance() {
        let budget = RetryBudget::new(0.25, 0);
        assert_eq!(budget.balance(), 0);
        for _ in 0..4 {
            budget.deposit();
        }
        assert!(budget.try_withdraw(), "four requests at 0.25 buy one retry");
        assert!(!budget.try_withdraw());
        let capped = RetryBudget::new(1.0, 0).with_max_balance(3);
        for _ in 0..100 {
            capped.deposit();
        }
        assert_eq!(capped.balance(), 3);
        assert!(format!("{capped:?}").contains("RetryBudget"));
    }

    #[test]
    fn test_budget_floor_accrues_over_time_and_ratio_is_sanitised() {
        let clock = Arc::new(ManualClock::new());
        let budget = RetryBudget::new(f64::NAN, 3).with_clock(clock.clone());
        assert_eq!(budget.balance(), 3);
        budget.deposit(); // a NaN ratio earns nothing
        for _ in 0..3 {
            assert!(budget.try_withdraw());
        }
        assert!(!budget.try_withdraw());
        clock.advance(Duration::from_secs(1));
        assert_eq!(budget.balance(), 3);
        clock.advance(Duration::from_secs(60));
        assert_eq!(budget.balance(), 30, "capped at ten seconds of floor");
        // Ratios are clamped, never negative.
        let negative = RetryBudget::new(-5.0, 0);
        negative.deposit();
        assert_eq!(negative.balance(), 0);
    }

    #[cfg(feature = "runtime")]
    mod runtime {
        #[cfg(feature = "tokio")]
        use super::Retry;
        use super::{RetryBudget, fast_policy};
        use crate::retry::RetryAction;
        #[cfg(feature = "tokio")]
        use core::time::Duration;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        type Outcome = core::future::Ready<Result<u32, &'static str>>;

        fn counting(calls: &Arc<AtomicU32>) -> impl FnMut() -> Outcome {
            let calls = Arc::clone(calls);
            move || {
                let _ = calls.fetch_add(1, Ordering::Relaxed);
                core::future::ready(Err("down"))
            }
        }

        #[tokio::test(start_paused = true)]
        async fn test_succeeds_after_transient_failures() {
            let calls = Arc::new(AtomicU32::new(0));
            let c = calls.clone();
            let result: Result<u32, &str> = fast_policy()
                .max_attempts(5)
                .run(
                    move || {
                        let n = c.fetch_add(1, Ordering::Relaxed) + 1;
                        async move { if n < 3 { Err("transient") } else { Ok(n) } }
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
            let result = fast_policy()
                .max_attempts(3)
                .run(counting(&calls), |_| RetryAction::Retry)
                .await;
            assert_eq!(result, Err("down"));
            assert_eq!(calls.load(Ordering::Relaxed), 3);
        }

        #[tokio::test(start_paused = true)]
        async fn test_single_attempt_never_retries() {
            let calls = Arc::new(AtomicU32::new(0));
            let result = fast_policy()
                .max_attempts(1)
                .run(counting(&calls), |_| RetryAction::Retry)
                .await;
            assert_eq!(result, Err("down"));
            assert_eq!(calls.load(Ordering::Relaxed), 1);
        }

        #[tokio::test(start_paused = true)]
        async fn test_give_up_classification_stops_immediately() {
            let calls = Arc::new(AtomicU32::new(0));
            let result = fast_policy()
                .max_attempts(10)
                .run(counting(&calls), |_| RetryAction::GiveUp)
                .await;
            assert_eq!(result, Err("down"));
            assert_eq!(calls.load(Ordering::Relaxed), 1);
        }

        // The tests below assert exact elapsed time on tokio's paused clock,
        // which needs the tokio timer.
        #[cfg(feature = "tokio")]
        #[tokio::test(start_paused = true)]
        async fn test_retry_after_is_honored_when_enabled() {
            let start = tokio::time::Instant::now();
            let policy = Retry::new(crate::Backoff::constant(Duration::from_secs(1)))
                .max_attempts(2)
                .respect_retry_after(true);
            let _: Result<(), &str> = policy
                .run(
                    || async { Err("rejected") },
                    |_| RetryAction::RetryAfter(Duration::from_secs(30)),
                )
                .await;
            assert_eq!(start.elapsed(), Duration::from_secs(30));
        }

        #[cfg(feature = "tokio")]
        #[tokio::test(start_paused = true)]
        async fn test_retry_after_is_ignored_when_disabled() {
            let start = tokio::time::Instant::now();
            let policy = Retry::new(crate::Backoff::constant(Duration::from_secs(1)))
                .max_attempts(2)
                .respect_retry_after(false);
            let _: Result<(), &str> = policy
                .run(
                    || async { Err("rejected") },
                    |_| RetryAction::RetryAfter(Duration::from_secs(3_000)),
                )
                .await;
            assert_eq!(start.elapsed(), Duration::from_secs(1));
        }

        #[cfg(feature = "tokio")]
        #[tokio::test(start_paused = true)]
        async fn test_huge_retry_after_gives_up_instead_of_stalling() {
            // Regression (TN-013): `Retry-After: 999999999` parked the task for
            // ~31.7 years. Over the cap, the loop returns the error at once.
            let calls = Arc::new(AtomicU32::new(0));
            let start = tokio::time::Instant::now();
            let result = fast_policy()
                .max_attempts(5)
                .run(counting(&calls), |_| {
                    RetryAction::RetryAfter(Duration::from_secs(999_999_999))
                })
                .await;
            assert_eq!(result, Err("down"));
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            assert_eq!(start.elapsed(), Duration::ZERO);

            // Raising the cap honors it (the 1.0 behaviour, opt-in).
            let start = tokio::time::Instant::now();
            let _ = fast_policy()
                .max_attempts(2)
                .max_retry_after(Duration::MAX)
                .run(counting(&calls), |_| {
                    RetryAction::RetryAfter(Duration::from_secs(600))
                })
                .await;
            assert_eq!(start.elapsed(), Duration::from_secs(600));
        }

        #[cfg(feature = "tokio")]
        #[tokio::test(start_paused = true)]
        async fn test_max_elapsed_bounds_the_total_wait() {
            let calls = Arc::new(AtomicU32::new(0));
            let start = tokio::time::Instant::now();
            let result = Retry::new(crate::Backoff::constant(Duration::from_secs(4)))
                .max_attempts(100)
                .max_elapsed(Duration::from_secs(10))
                .run(counting(&calls), |_| RetryAction::Retry)
                .await;
            assert_eq!(result, Err("down"));
            // 4s + 4s fit; a third 4s wait would cross 10s.
            assert_eq!(calls.load(Ordering::Relaxed), 3);
            assert_eq!(start.elapsed(), Duration::from_secs(8));
        }

        #[tokio::test(start_paused = true)]
        async fn test_budget_stops_a_retry_storm() {
            // No floor, one retry per ten requests: ten failing calls together
            // may retry once, not forty times.
            let budget = RetryBudget::new(0.1, 0);
            let calls = Arc::new(AtomicU32::new(0));
            for _ in 0..10 {
                let _ = fast_policy()
                    .max_attempts(5)
                    .run_with_budget(&budget, counting(&calls), |_| RetryAction::Retry)
                    .await;
            }
            assert_eq!(calls.load(Ordering::Relaxed), 10 + 1);
        }

        #[tokio::test(start_paused = true)]
        async fn test_retry_if_retryable_helper() {
            use error_forge::AppError;
            let calls = Arc::new(AtomicU32::new(0));
            let c = calls.clone();
            let result: Result<(), AppError> = fast_policy()
                .max_attempts(5)
                .run(
                    move || {
                        let _ = c.fetch_add(1, Ordering::Relaxed);
                        async { Err(AppError::config("bad")) }
                    },
                    crate::retry::retry_if_retryable,
                )
                .await;
            assert!(result.is_err());
            assert_eq!(
                calls.load(Ordering::Relaxed),
                1,
                "config errors are not retryable"
            );
        }

        #[cfg(feature = "tokio")]
        #[tokio::test(start_paused = true)]
        async fn test_cancelling_mid_wait_stops_retrying() {
            let calls = Arc::new(AtomicU32::new(0));
            let policy =
                Retry::new(crate::Backoff::constant(Duration::from_secs(60))).max_attempts(10);
            let run = policy.run(counting(&calls), |_| RetryAction::Retry);
            let cancelled = tokio::time::timeout(Duration::from_secs(1), run).await;
            assert!(cancelled.is_err());
            assert_eq!(calls.load(Ordering::Relaxed), 1);
        }
    }
}
