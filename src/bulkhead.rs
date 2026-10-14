//! A bulkhead: a fixed cap on concurrent calls into one dependency.

use core::sync::atomic::Ordering;
#[cfg(feature = "runtime")]
use core::time::Duration;
use std::borrow::Cow;
use std::sync::Arc;

#[cfg(feature = "runtime")]
use event_listener::Event;

#[cfg(feature = "runtime")]
use crate::error::ThrottleError;
use crate::obs::{self, Label};
use crate::sync::AtomicU32;

/// The limiter kind reported in metrics and events.
const KIND: &str = "bulkhead";

/// A fixed limit on how many calls may be in flight at once.
///
/// The bulkhead pattern isolates failure domains: give each downstream its own
/// bulkhead, and a slow dependency can tie up at most its own slots rather than
/// every task in the process. Unlike a rate limiter it bounds *concurrency*, not
/// throughput; unlike an [`AdaptiveLimiter`](crate::AdaptiveLimiter) the limit
/// is fixed.
///
/// A slot is held by a [`BulkheadPermit`] (or an [`OwnedBulkheadPermit`] that can
/// move into a spawned task) and released when the permit drops. Slot
/// accounting is one lock-free compare-and-swap.
///
/// # Examples
///
/// ```
/// use throttle_net::Bulkhead;
///
/// let bulkhead = Bulkhead::new(2);
/// let a = bulkhead.try_acquire().expect("slot 1");
/// let b = bulkhead.try_acquire().expect("slot 2");
/// assert!(bulkhead.try_acquire().is_none()); // full: fail fast
/// drop(a);
/// assert!(bulkhead.try_acquire().is_some()); // a slot freed
/// # drop(b);
/// ```
pub struct Bulkhead {
    limit: u32,
    in_use: AtomicU32,
    #[cfg(feature = "runtime")]
    notify: Event,
    label: Label,
}

impl Bulkhead {
    /// Creates a bulkhead admitting at most `limit` concurrent calls. A `limit`
    /// of `0` admits nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::Bulkhead;
    ///
    /// let bulkhead = Bulkhead::new(16);
    /// assert_eq!(bulkhead.limit(), 16);
    /// assert_eq!(bulkhead.available(), 16);
    /// ```
    #[must_use]
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            in_use: AtomicU32::new(0),
            #[cfg(feature = "runtime")]
            notify: Event::new(),
            label: Label::none(),
        }
    }

    /// Names this instance for metrics and events.
    #[must_use]
    pub fn with_name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.label = Label::new(name);
        self
    }

    /// The instance name, if one was set.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.label.get()
    }

    /// The concurrency limit.
    #[must_use]
    pub fn limit(&self) -> u32 {
        self.limit
    }

    /// Slots currently held.
    #[must_use]
    pub fn in_use(&self) -> u32 {
        self.in_use.load(Ordering::Acquire)
    }

    /// Slots currently free.
    #[must_use]
    pub fn available(&self) -> u32 {
        self.limit.saturating_sub(self.in_use())
    }

    /// Takes a slot if one is free.
    fn try_take(&self) -> bool {
        let mut in_use = self.in_use.load(Ordering::Acquire);
        loop {
            if in_use >= self.limit {
                return false;
            }
            match self.in_use.compare_exchange_weak(
                in_use,
                in_use + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => in_use = actual,
            }
        }
    }

    /// Returns a slot and wakes one waiter.
    fn release(&self) {
        let _released = self
            .in_use
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_sub(1))
            });
        // A waiter cancelled after being notified passes the wake-up on.
        #[cfg(feature = "runtime")]
        let _woken = self.notify.notify(1);
    }

    /// Takes a slot without waiting, or returns `None` (counted as shed) when
    /// the bulkhead is full.
    #[must_use]
    pub fn try_acquire(&self) -> Option<BulkheadPermit<'_>> {
        let taken = self.try_take();
        self.report(taken);
        // `then`, not `then_some`: building a permit eagerly and dropping it on
        // the refused path would release a slot that was never taken.
        taken.then(|| BulkheadPermit { bulkhead: self })
    }

    /// Like [`try_acquire`](Self::try_acquire), returning a permit that keeps
    /// the bulkhead alive through an [`Arc`] so it can move into a spawned task.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use throttle_net::Bulkhead;
    ///
    /// let bulkhead = Arc::new(Bulkhead::new(1));
    /// let permit = bulkhead.try_acquire_owned().expect("the slot");
    /// std::thread::spawn(move || drop(permit)).join().unwrap();
    /// assert_eq!(bulkhead.in_use(), 0);
    /// ```
    #[must_use]
    pub fn try_acquire_owned(self: &Arc<Self>) -> Option<OwnedBulkheadPermit> {
        let taken = self.try_take();
        self.report(taken);
        taken.then(|| OwnedBulkheadPermit {
            bulkhead: Arc::clone(self),
        })
    }

    /// Records a non-waiting admission.
    fn report(&self, taken: bool) {
        if taken {
            obs::granted(KIND, &self.label, 1);
        } else {
            obs::shed(KIND, &self.label);
        }
    }
}

#[cfg(feature = "runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "runtime")))]
impl Bulkhead {
    /// Takes a slot, waiting until one frees. **Cancel-safe:** a slot is taken
    /// only at the moment the future completes.
    ///
    /// # Errors
    ///
    /// [`ThrottleError::CostExceedsCapacity`] when the limit is `0` (no slot
    /// will ever free).
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use throttle_net::Bulkhead;
    ///
    /// let bulkhead = Bulkhead::new(4);
    /// let _permit = bulkhead.acquire().await?;
    /// // ... call the dependency; the slot frees when `_permit` drops ...
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire(&self) -> Result<BulkheadPermit<'_>, ThrottleError> {
        self.wait(None).await?;
        Ok(BulkheadPermit { bulkhead: self })
    }

    /// Takes a slot, waiting at most `timeout`.
    ///
    /// # Errors
    ///
    /// [`ThrottleError::BulkheadFull`] if no slot frees within `timeout`;
    /// [`ThrottleError::CostExceedsCapacity`] when the limit is `0`.
    pub async fn acquire_timeout(
        &self,
        timeout: Duration,
    ) -> Result<BulkheadPermit<'_>, ThrottleError> {
        self.wait(Some(timeout)).await?;
        Ok(BulkheadPermit { bulkhead: self })
    }

    /// Like [`acquire`](Self::acquire), returning an [`OwnedBulkheadPermit`].
    ///
    /// # Errors
    ///
    /// As [`acquire`](Self::acquire).
    pub async fn acquire_owned(self: &Arc<Self>) -> Result<OwnedBulkheadPermit, ThrottleError> {
        self.wait(None).await?;
        Ok(OwnedBulkheadPermit {
            bulkhead: Arc::clone(self),
        })
    }

    /// Runs `operation` inside the bulkhead: waits for a slot, awaits the
    /// operation, and frees the slot (also if the returned future is dropped).
    ///
    /// # Errors
    ///
    /// [`ThrottleError::CostExceedsCapacity`] when the limit is `0`; the
    /// operation's own output is returned as `Ok`.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn run() -> Result<(), throttle_net::ThrottleError> {
    /// use throttle_net::Bulkhead;
    ///
    /// let bulkhead = Bulkhead::new(8);
    /// let answer = bulkhead.call(|| async { 42 }).await?;
    /// assert_eq!(answer, 42);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call<F, Fut>(&self, operation: F) -> Result<Fut::Output, ThrottleError>
    where
        F: FnOnce() -> Fut,
        Fut: core::future::Future,
    {
        let _permit = self.acquire().await?;
        Ok(operation().await)
    }

    /// Runs `operation` only if a slot is free right now.
    ///
    /// # Errors
    ///
    /// [`ThrottleError::BulkheadFull`] when every slot is taken.
    pub async fn try_call<F, Fut>(&self, operation: F) -> Result<Fut::Output, ThrottleError>
    where
        F: FnOnce() -> Fut,
        Fut: core::future::Future,
    {
        let _permit = self.try_acquire().ok_or(ThrottleError::BulkheadFull)?;
        Ok(operation().await)
    }

    /// Waits until a slot is taken, or the timeout passes.
    async fn wait(&self, timeout: Option<Duration>) -> Result<(), ThrottleError> {
        if self.limit == 0 {
            return Err(ThrottleError::CostExceedsCapacity {
                cost: 1,
                capacity: 0,
            });
        }
        let timer = obs::Timer::start();
        let waiting = async {
            loop {
                if self.try_take() {
                    return;
                }
                // `listen` registers immediately, so a release between the
                // check above and the await is still delivered.
                let listener = self.notify.listen();
                if self.try_take() {
                    return;
                }
                listener.await;
            }
        };
        let result = match timeout {
            None => {
                waiting.await;
                Ok(())
            }
            Some(timeout) => crate::resilience::timeout(timeout, waiting)
                .await
                .map_err(|_| ThrottleError::BulkheadFull),
        };
        obs::waited(KIND, &self.label, &timer, 1, obs::Waited::of(&result));
        result
    }
}

impl core::fmt::Debug for Bulkhead {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Bulkhead")
            .field("name", &self.label)
            .field("limit", &self.limit)
            .field("in_use", &self.in_use())
            .finish_non_exhaustive()
    }
}

/// A held bulkhead slot, released when dropped.
#[must_use = "the slot is released as soon as the permit is dropped"]
pub struct BulkheadPermit<'a> {
    bulkhead: &'a Bulkhead,
}

impl Drop for BulkheadPermit<'_> {
    fn drop(&mut self) {
        self.bulkhead.release();
    }
}

impl core::fmt::Debug for BulkheadPermit<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BulkheadPermit").finish_non_exhaustive()
    }
}

/// A held bulkhead slot that owns a reference to its bulkhead, so it can move
/// into a spawned task. Released when dropped.
#[must_use = "the slot is released as soon as the permit is dropped"]
pub struct OwnedBulkheadPermit {
    bulkhead: Arc<Bulkhead>,
}

impl Drop for OwnedBulkheadPermit {
    fn drop(&mut self) {
        self.bulkhead.release();
    }
}

impl core::fmt::Debug for OwnedBulkheadPermit {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("OwnedBulkheadPermit")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::Bulkhead;
    use std::sync::Arc;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_bulkhead_types_are_send_sync() {
        assert_send_sync::<Bulkhead>();
        assert_send_sync::<super::OwnedBulkheadPermit>();
    }

    #[test]
    fn test_limit_is_enforced_and_slots_return_on_drop() {
        let bulkhead = Bulkhead::new(2).with_name("db");
        let a = bulkhead.try_acquire().unwrap();
        let b = bulkhead.try_acquire().unwrap();
        assert!(bulkhead.try_acquire().is_none());
        assert_eq!((bulkhead.in_use(), bulkhead.available()), (2, 0));
        drop(a);
        assert_eq!(bulkhead.available(), 1);
        drop(b);
        assert_eq!(bulkhead.in_use(), 0);
        assert_eq!(bulkhead.name(), Some("db"));
        assert!(format!("{bulkhead:?}").contains("Bulkhead"));
    }

    #[test]
    fn test_zero_limit_admits_nothing() {
        let bulkhead = Arc::new(Bulkhead::new(0));
        assert!(bulkhead.try_acquire().is_none());
        assert!(bulkhead.try_acquire_owned().is_none());
    }

    #[test]
    fn test_concurrent_holders_never_exceed_the_limit() {
        let bulkhead = Arc::new(Bulkhead::new(3));
        let peak = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let bulkhead = Arc::clone(&bulkhead);
                let peak = Arc::clone(&peak);
                std::thread::spawn(move || {
                    for _ in 0..2_000 {
                        if let Some(permit) = bulkhead.try_acquire_owned() {
                            let _ = peak
                                .fetch_max(bulkhead.in_use(), std::sync::atomic::Ordering::Relaxed);
                            drop(permit);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert!(peak.load(std::sync::atomic::Ordering::Relaxed) <= 3);
        assert_eq!(bulkhead.in_use(), 0);
    }

    #[cfg(feature = "tokio")]
    mod waiting {
        use super::Bulkhead;
        use crate::error::ThrottleError;
        use core::time::Duration;
        use std::sync::Arc;

        #[tokio::test(start_paused = true)]
        async fn test_acquire_waits_times_out_and_calls() {
            let bulkhead = Arc::new(Bulkhead::new(1));
            let held = bulkhead.acquire().await.unwrap();
            assert_eq!(
                bulkhead
                    .acquire_timeout(Duration::from_millis(5))
                    .await
                    .err(),
                Some(ThrottleError::BulkheadFull)
            );
            assert_eq!(
                bulkhead.try_call(|| async { 1 }).await,
                Err(ThrottleError::BulkheadFull)
            );
            let b = Arc::clone(&bulkhead);
            let waiter = tokio::spawn(async move { b.call(|| async { 5 }).await });
            tokio::task::yield_now().await;
            drop(held);
            assert_eq!(waiter.await.unwrap(), Ok(5));
            assert_eq!(bulkhead.in_use(), 0);
            let owned = bulkhead.acquire_owned().await.unwrap();
            assert!(format!("{owned:?}").contains("OwnedBulkheadPermit"));
            drop(owned);
            assert_eq!(bulkhead.try_call(|| async { 2 }).await, Ok(2));
            let permit = bulkhead
                .acquire_timeout(Duration::from_millis(5))
                .await
                .unwrap();
            assert!(format!("{permit:?}").contains("BulkheadPermit"));
        }

        #[tokio::test(start_paused = true)]
        async fn test_cancelled_waiter_holds_no_slot() {
            let bulkhead = Bulkhead::new(1);
            let held = bulkhead.try_acquire().unwrap();
            let cancelled =
                tokio::time::timeout(Duration::from_millis(5), bulkhead.acquire()).await;
            assert!(cancelled.is_err());
            drop(held);
            assert_eq!(bulkhead.in_use(), 0);
            assert!(bulkhead.acquire().await.is_ok());
        }

        #[tokio::test(start_paused = true)]
        async fn test_zero_limit_errors_instead_of_hanging() {
            let bulkhead = Bulkhead::new(0);
            assert_eq!(
                bulkhead.acquire().await.err(),
                Some(ThrottleError::CostExceedsCapacity {
                    cost: 1,
                    capacity: 0
                })
            );
        }
    }
}
