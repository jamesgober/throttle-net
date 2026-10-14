//! [`tower`](https://docs.rs/tower) integration: layers that put a limiter, a
//! circuit breaker, or an adaptive concurrency limit in front of any
//! `tower::Service` — an HTTP client stack built on hyper, reqwest's
//! `tower`-compatible clients, tonic channels, and so on.
//!
//! - [`ThrottleLayer`] paces calls through any [`Limiter`] (waiting, not
//!   rejecting — the outbound default).
//! - [`CircuitBreakerLayer`] sheds calls while a [`CircuitBreaker`] is open
//!   and settles each call's permit from its `Result` (`circuit-breaker`
//!   feature).
//! - [`AdaptiveLayer`] caps in-flight calls with an [`AdaptiveLimiter`] and
//!   feeds each call's outcome and latency back to it (`adaptive` feature).
//!
//! # Readiness and backpressure
//!
//! Each service waits in **`poll_ready`**, the way tower's own `RateLimit` and
//! `ConcurrencyLimit` do: it acquires its admission (the limiter's units, the
//! breaker's or adaptive limiter's permit) first, then polls the inner
//! service's readiness, and reports ready only when both are held. `call`
//! spends the admission it holds. So:
//!
//! - **Backpressure works.** A throttled service reports `Pending` while it
//!   waits, so `tower::load_shed` sheds, `tower::buffer` queues, and a
//!   balancer routes elsewhere — instead of seeing "ready" and then sleeping
//!   inside every call.
//! - **The inner service's readiness is not held during the wait.** Layers
//!   whose readiness reserves capacity (a `ConcurrencyLimit` permit, a
//!   `Buffer` slot, a pooled connection) are polled only once the admission is
//!   in hand.
//! - **Nothing leaks.** A service dropped (or cloned — a clone starts unready)
//!   while it holds an admission gives it back: units are refunded, permits are
//!   cancelled without counting an outcome.
//!
//! Each layer has a `with_timeout` option: `poll_ready` then fails with a boxed
//! [`ThrottleError`] if the admission cannot be had in time, instead of
//! waiting indefinitely. A service called without first being ready (a breach
//! of the `Service` contract) does not send the request; it fails with
//! [`ThrottleError::NotReady`].
//!
//! Errors are boxed (`BoxError`), the tower convention, so layers stack; a
//! limiter's refusal surfaces as a boxed [`ThrottleError`].
//!
//! Behind the `tower` feature (which needs `tokio` or `smol` for the waiting).

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::sync::Arc;

use tower_layer::Layer;
use tower_service::Service;

#[cfg(feature = "adaptive")]
use crate::adaptive::{AdaptiveLimiter, AdaptiveStrategy, OwnedAdaptivePermit};
#[cfg(feature = "circuit-breaker")]
use crate::circuit::{CircuitBreaker, OwnedPermit};
use crate::error::ThrottleError;
use crate::limiter::Limiter;

/// The boxed error every service in this module returns.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The boxed future every service in this module returns.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = Result<T, BoxError>> + Send>>;

/// An admission being acquired in `poll_ready`. `Sync` so the services stay
/// `Sync`, as frameworks such as axum require.
type Acquiring<T> = Pin<Box<dyn Future<Output = Result<T, ThrottleError>> + Send + Sync>>;

/// A service's admission for its next call: none yet, being acquired, or held.
enum Admission<T> {
    Idle,
    Acquiring(Acquiring<T>),
    Held(T),
}

impl<T> Admission<T> {
    /// Drives the admission to `Held`, starting an acquisition with `start` if
    /// none is in progress. An acquisition that fails resets to `Idle`.
    fn poll_acquire(
        &mut self,
        cx: &mut Context<'_>,
        start: impl FnOnce() -> Acquiring<T>,
    ) -> Poll<Result<(), ThrottleError>> {
        let mut start = Some(start);
        loop {
            match self {
                Self::Held(_) => return Poll::Ready(Ok(())),
                Self::Idle => {
                    let Some(start) = start.take() else {
                        return Poll::Ready(Err(ThrottleError::NotReady));
                    };
                    *self = Self::Acquiring(start());
                }
                Self::Acquiring(acquiring) => match acquiring.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(held)) => *self = Self::Held(held),
                    Poll::Ready(Err(error)) => {
                        *self = Self::Idle;
                        return Poll::Ready(Err(error));
                    }
                },
            }
        }
    }

    /// Takes the held admission for a call, leaving `Idle`.
    fn take(&mut self) -> Option<T> {
        match core::mem::replace(self, Self::Idle) {
            Self::Held(held) => Some(held),
            other => {
                *self = other;
                None
            }
        }
    }
}

/// Units taken from a limiter for a call that has not been made yet: refunded
/// if dropped unused.
struct Units {
    limiter: Arc<dyn Limiter>,
    cost: u32,
    spent: bool,
}

impl Units {
    /// The call went out: the units stay taken.
    fn spend(mut self) {
        self.spent = true;
    }
}

impl Drop for Units {
    fn drop(&mut self) {
        if !self.spent {
            self.limiter.refund(self.cost);
        }
    }
}

/// A [`Layer`] that paces every call through a [`Limiter`].
///
/// # Examples
///
/// ```
/// # async fn run() -> Result<(), throttle_net::tower::BoxError> {
/// use tower::{ServiceBuilder, ServiceExt, service_fn};
/// use throttle_net::Throttle;
/// use throttle_net::tower::ThrottleLayer;
///
/// let service = ServiceBuilder::new()
///     .layer(ThrottleLayer::new(Throttle::per_second(100)))
///     .service(service_fn(|request: u32| async move {
///         Ok::<_, std::convert::Infallible>(request * 2)
///     }));
///
/// assert_eq!(service.oneshot(21).await?, 42);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ThrottleLayer {
    limiter: Arc<dyn Limiter>,
    cost: u32,
    timeout: Option<Duration>,
}

impl ThrottleLayer {
    /// Paces calls through `limiter`, one unit each.
    #[must_use]
    pub fn new(limiter: impl Limiter + 'static) -> Self {
        Self::shared(Arc::new(limiter))
    }

    /// Paces calls through a limiter shared with other code (for example the
    /// same [`Throttle`](crate::Throttle) across several clients).
    #[must_use]
    pub fn shared(limiter: Arc<dyn Limiter>) -> Self {
        Self {
            limiter,
            cost: 1,
            timeout: None,
        }
    }

    /// Charges `cost` units per call instead of one.
    #[must_use]
    pub fn with_cost(mut self, cost: u32) -> Self {
        self.cost = cost;
        self
    }

    /// Waits at most `timeout` in `poll_ready` for the units; past it,
    /// `poll_ready` fails with a boxed [`ThrottleError::DeadlineExceeded`]
    /// (at once, if the units are known not to come in time).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl<S> Layer<S> for ThrottleLayer {
    type Service = Throttled<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Throttled {
            inner,
            limiter: Arc::clone(&self.limiter),
            cost: self.cost,
            timeout: self.timeout,
            admission: Admission::Idle,
        }
    }
}

impl core::fmt::Debug for ThrottleLayer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ThrottleLayer")
            .field("capacity", &self.limiter.capacity())
            .field("cost", &self.cost)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// The service [`ThrottleLayer`] produces.
pub struct Throttled<S> {
    inner: S,
    limiter: Arc<dyn Limiter>,
    cost: u32,
    timeout: Option<Duration>,
    admission: Admission<Units>,
}

impl<S: Clone> Clone for Throttled<S> {
    /// The clone shares the limiter but holds no admission: it must be polled
    /// ready itself.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            limiter: Arc::clone(&self.limiter),
            cost: self.cost,
            timeout: self.timeout,
            admission: Admission::Idle,
        }
    }
}

impl<S, Request> Service<Request> for Throttled<S>
where
    S: Service<Request>,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = BoxFuture<S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let limiter = &self.limiter;
        let (cost, timeout) = (self.cost, self.timeout);
        match self.admission.poll_acquire(cx, || {
            let limiter = Arc::clone(limiter);
            Box::pin(async move {
                wait(limiter.as_ref(), cost, timeout).await?;
                Ok(Units {
                    limiter,
                    cost,
                    spent: false,
                })
            })
        }) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
            Poll::Ready(Ok(())) => {}
        }
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let Some(units) = self.admission.take() else {
            return Box::pin(async { Err(ThrottleError::NotReady.into()) });
        };
        let response = self.inner.call(request);
        units.spend();
        Box::pin(async move { response.await.map_err(Into::into) })
    }
}

impl<S> core::fmt::Debug for Throttled<S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Throttled")
            .field("cost", &self.cost)
            .field("ready", &matches!(self.admission, Admission::Held(_)))
            .finish_non_exhaustive()
    }
}

/// Waits on a dynamically typed limiter (queueing on its bottleneck,
/// cancel-safe).
async fn wait(
    limiter: &dyn Limiter,
    cost: u32,
    timeout: Option<Duration>,
) -> Result<(), ThrottleError> {
    use crate::limiter::{Progress, paused_all, release_all, step_all};
    use crate::wait::{Refusal, wait_with};
    let item = || core::iter::once((limiter, cost));
    wait_with(
        Progress::new(),
        |progress, budget| step_all(item(), budget, progress),
        |progress| release_all(item(), progress),
        || paused_all(item()),
        timeout,
    )
    .await
    .map_err(|refusal| match refusal {
        Refusal::Never => ThrottleError::CostExceedsCapacity {
            cost,
            capacity: limiter.capacity(),
        },
        Refusal::TimedOut => ThrottleError::DeadlineExceeded,
    })
}

/// A [`Layer`] that guards every call with a [`CircuitBreaker`]: calls are shed
/// (with a boxed [`ThrottleError::CircuitOpen`]) while it is open, paced by its
/// wrapped limiter otherwise, and each call's permit is settled from its result
/// — `Ok` as a success, `Err` as a failure.
///
/// The permit is taken in `poll_ready`, so an open breaker fails `poll_ready`
/// itself (a `tower::load_shed` or retry layer above sees it there).
///
/// # Examples
///
/// ```
/// # async fn run() {
/// use std::sync::Arc;
/// use tower::{ServiceBuilder, ServiceExt, service_fn};
/// use throttle_net::{CircuitBreaker, Throttle, Trip};
/// use throttle_net::tower::CircuitBreakerLayer;
///
/// let breaker = Arc::new(
///     CircuitBreaker::builder().trip(Trip::Consecutive(1)).build(Throttle::per_second(10)),
/// );
/// let service = ServiceBuilder::new()
///     .layer(CircuitBreakerLayer::new(Arc::clone(&breaker)))
///     .service(service_fn(|_: ()| async { Err::<(), _>(std::io::Error::other("down")) }));
///
/// assert!(service.clone().oneshot(()).await.is_err()); // fails, trips the breaker
/// let shed = service.oneshot(()).await.unwrap_err();   // shed without calling
/// assert!(shed.is::<throttle_net::ThrottleError>());
/// # }
/// ```
#[cfg(feature = "circuit-breaker")]
#[cfg_attr(docsrs, doc(cfg(feature = "circuit-breaker")))]
pub struct CircuitBreakerLayer<L, C = clock_lib::SystemClock>
where
    C: clock_lib::Clock,
{
    breaker: Arc<CircuitBreaker<L, C>>,
    timeout: Option<Duration>,
}

#[cfg(feature = "circuit-breaker")]
impl<L, C> CircuitBreakerLayer<L, C>
where
    C: clock_lib::Clock,
{
    /// Guards calls with `breaker`, shared with any other code that holds it.
    #[must_use]
    pub fn new(breaker: Arc<CircuitBreaker<L, C>>) -> Self {
        Self {
            breaker,
            timeout: None,
        }
    }

    /// Waits at most `timeout` in `poll_ready` on the breaker's wrapped
    /// limiter; past it, `poll_ready` fails with a boxed
    /// [`ThrottleError::DeadlineExceeded`].
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

#[cfg(feature = "circuit-breaker")]
impl<L, C> Clone for CircuitBreakerLayer<L, C>
where
    C: clock_lib::Clock,
{
    fn clone(&self) -> Self {
        Self {
            breaker: Arc::clone(&self.breaker),
            timeout: self.timeout,
        }
    }
}

#[cfg(feature = "circuit-breaker")]
impl<S, L, C> Layer<S> for CircuitBreakerLayer<L, C>
where
    L: Limiter,
    C: clock_lib::Clock,
{
    type Service = Guarded<S, L, C>;

    fn layer(&self, inner: S) -> Self::Service {
        Guarded {
            inner,
            breaker: Arc::clone(&self.breaker),
            timeout: self.timeout,
            admission: Admission::Idle,
        }
    }
}

#[cfg(feature = "circuit-breaker")]
impl<L, C> core::fmt::Debug for CircuitBreakerLayer<L, C>
where
    L: Limiter,
    C: clock_lib::Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CircuitBreakerLayer")
            .field("breaker", &self.breaker)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// A breaker permit taken in `poll_ready` and not yet used: cancelled (no
/// outcome recorded) if dropped.
#[cfg(feature = "circuit-breaker")]
struct HeldPermit<L, C>(Option<OwnedPermit<L, C>>)
where
    L: Limiter,
    C: clock_lib::Clock;

#[cfg(feature = "circuit-breaker")]
impl<L, C> Drop for HeldPermit<L, C>
where
    L: Limiter,
    C: clock_lib::Clock,
{
    fn drop(&mut self) {
        if let Some(permit) = self.0.take() {
            permit.cancel();
        }
    }
}

/// The service [`CircuitBreakerLayer`] produces.
#[cfg(feature = "circuit-breaker")]
#[cfg_attr(docsrs, doc(cfg(feature = "circuit-breaker")))]
pub struct Guarded<S, L, C = clock_lib::SystemClock>
where
    L: Limiter,
    C: clock_lib::Clock,
{
    inner: S,
    breaker: Arc<CircuitBreaker<L, C>>,
    timeout: Option<Duration>,
    admission: Admission<HeldPermit<L, C>>,
}

#[cfg(feature = "circuit-breaker")]
impl<S: Clone, L, C> Clone for Guarded<S, L, C>
where
    L: Limiter,
    C: clock_lib::Clock,
{
    /// The clone shares the breaker but holds no permit.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            breaker: Arc::clone(&self.breaker),
            timeout: self.timeout,
            admission: Admission::Idle,
        }
    }
}

#[cfg(feature = "circuit-breaker")]
impl<S, L, C, Request> Service<Request> for Guarded<S, L, C>
where
    S: Service<Request>,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    L: Limiter + 'static,
    C: clock_lib::Clock + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = BoxFuture<S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let breaker = &self.breaker;
        let timeout = self.timeout;
        match self.admission.poll_acquire(cx, || {
            let breaker = Arc::clone(breaker);
            Box::pin(async move {
                let permit = match timeout {
                    None => breaker.acquire_owned().await?,
                    Some(timeout) => breaker.acquire_owned_timeout(timeout).await?,
                };
                Ok(HeldPermit(Some(permit)))
            })
        }) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
            Poll::Ready(Ok(())) => {}
        }
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let Some(permit) = self.admission.take().and_then(|mut held| held.0.take()) else {
            return Box::pin(async { Err(ThrottleError::NotReady.into()) });
        };
        let response = self.inner.call(request);
        Box::pin(async move {
            match response.await {
                Ok(response) => {
                    permit.success();
                    Ok(response)
                }
                Err(error) => {
                    permit.failure();
                    Err(error.into())
                }
            }
        })
    }
}

#[cfg(feature = "circuit-breaker")]
impl<S, L, C> core::fmt::Debug for Guarded<S, L, C>
where
    L: Limiter,
    C: clock_lib::Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Guarded")
            .field("breaker", &self.breaker)
            .field("ready", &matches!(self.admission, Admission::Held(_)))
            .finish_non_exhaustive()
    }
}

/// A [`Layer`] that caps in-flight calls with an [`AdaptiveLimiter`], waiting
/// (in `poll_ready`) for a slot, and feeds each call's outcome (with its
/// latency, measured from `call`) back to it — `Ok` as a success, `Err` as a
/// failure.
///
/// # Examples
///
/// ```
/// # async fn run() -> Result<(), throttle_net::tower::BoxError> {
/// use std::sync::Arc;
/// use tower::{ServiceBuilder, ServiceExt, service_fn};
/// use throttle_net::{AdaptiveLimiter, Aimd};
/// use throttle_net::tower::AdaptiveLayer;
///
/// let limiter = Arc::new(AdaptiveLimiter::builder().initial(8).build(Aimd::default()));
/// let service = ServiceBuilder::new()
///     .layer(AdaptiveLayer::new(Arc::clone(&limiter)))
///     .service(service_fn(|n: u8| async move { Ok::<_, std::convert::Infallible>(n) }));
/// assert_eq!(service.oneshot(5).await?, 5);
/// assert_eq!(limiter.in_flight(), 0);
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "adaptive")]
#[cfg_attr(docsrs, doc(cfg(feature = "adaptive")))]
pub struct AdaptiveLayer<St, C = clock_lib::SystemClock>
where
    C: clock_lib::Clock,
{
    limiter: Arc<AdaptiveLimiter<St, C>>,
    timeout: Option<Duration>,
}

#[cfg(feature = "adaptive")]
impl<St, C> AdaptiveLayer<St, C>
where
    C: clock_lib::Clock,
{
    /// Caps calls with `limiter`, shared with any other code that holds it.
    #[must_use]
    pub fn new(limiter: Arc<AdaptiveLimiter<St, C>>) -> Self {
        Self {
            limiter,
            timeout: None,
        }
    }

    /// Waits at most `timeout` in `poll_ready` for a slot; past it,
    /// `poll_ready` fails with a boxed [`ThrottleError::DeadlineExceeded`].
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

#[cfg(feature = "adaptive")]
impl<St, C> Clone for AdaptiveLayer<St, C>
where
    C: clock_lib::Clock,
{
    fn clone(&self) -> Self {
        Self {
            limiter: Arc::clone(&self.limiter),
            timeout: self.timeout,
        }
    }
}

#[cfg(feature = "adaptive")]
impl<S, St, C> Layer<S> for AdaptiveLayer<St, C>
where
    St: AdaptiveStrategy,
    C: clock_lib::Clock,
{
    type Service = Adaptive<S, St, C>;

    fn layer(&self, inner: S) -> Self::Service {
        Adaptive {
            inner,
            limiter: Arc::clone(&self.limiter),
            timeout: self.timeout,
            admission: Admission::Idle,
        }
    }
}

#[cfg(feature = "adaptive")]
impl<St, C> core::fmt::Debug for AdaptiveLayer<St, C>
where
    C: clock_lib::Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AdaptiveLayer")
            .field("limiter", &self.limiter)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// An adaptive slot taken in `poll_ready` and not yet used: freed without an
/// outcome if dropped.
#[cfg(feature = "adaptive")]
struct HeldSlot<St, C>(Option<OwnedAdaptivePermit<St, C>>)
where
    St: AdaptiveStrategy,
    C: clock_lib::Clock;

#[cfg(feature = "adaptive")]
impl<St, C> Drop for HeldSlot<St, C>
where
    St: AdaptiveStrategy,
    C: clock_lib::Clock,
{
    fn drop(&mut self) {
        if let Some(permit) = self.0.take() {
            permit.cancel();
        }
    }
}

/// The service [`AdaptiveLayer`] produces.
#[cfg(feature = "adaptive")]
#[cfg_attr(docsrs, doc(cfg(feature = "adaptive")))]
pub struct Adaptive<S, St, C = clock_lib::SystemClock>
where
    St: AdaptiveStrategy,
    C: clock_lib::Clock,
{
    inner: S,
    limiter: Arc<AdaptiveLimiter<St, C>>,
    timeout: Option<Duration>,
    admission: Admission<HeldSlot<St, C>>,
}

#[cfg(feature = "adaptive")]
impl<S: Clone, St, C> Clone for Adaptive<S, St, C>
where
    St: AdaptiveStrategy,
    C: clock_lib::Clock,
{
    /// The clone shares the limiter but holds no slot.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            limiter: Arc::clone(&self.limiter),
            timeout: self.timeout,
            admission: Admission::Idle,
        }
    }
}

#[cfg(feature = "adaptive")]
impl<S, St, C, Request> Service<Request> for Adaptive<S, St, C>
where
    S: Service<Request>,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    St: AdaptiveStrategy + 'static,
    C: clock_lib::Clock + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = BoxFuture<S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let limiter = &self.limiter;
        let timeout = self.timeout;
        match self.admission.poll_acquire(cx, || {
            let limiter = Arc::clone(limiter);
            Box::pin(async move {
                let permit = match timeout {
                    None => limiter.acquire_owned().await,
                    Some(timeout) => limiter.acquire_owned_timeout(timeout).await?,
                };
                Ok(HeldSlot(Some(permit)))
            })
        }) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
            Poll::Ready(Ok(())) => {}
        }
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let Some(mut permit) = self.admission.take().and_then(|mut held| held.0.take()) else {
            return Box::pin(async { Err(ThrottleError::NotReady.into()) });
        };
        // Latency is the call's, not the time the slot sat ready.
        permit.restart();
        let response = self.inner.call(request);
        Box::pin(async move {
            match response.await {
                Ok(response) => {
                    permit.success();
                    Ok(response)
                }
                Err(error) => {
                    permit.failure();
                    Err(error.into())
                }
            }
        })
    }
}

#[cfg(feature = "adaptive")]
impl<S, St, C> core::fmt::Debug for Adaptive<S, St, C>
where
    St: AdaptiveStrategy,
    C: clock_lib::Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Adaptive")
            .field("limiter", &self.limiter)
            .field("ready", &matches!(self.admission, Admission::Held(_)))
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::ThrottleLayer;
    use crate::error::ThrottleError;
    use crate::throttle::Throttle;
    use core::time::Duration;
    use std::sync::Arc;
    use tower::{ServiceBuilder, ServiceExt, service_fn};

    fn double() -> impl tower::Service<
        u32,
        Response = u32,
        Error = std::convert::Infallible,
        Future = impl core::future::Future<Output = Result<u32, std::convert::Infallible>> + Send,
    > + Clone {
        service_fn(|n: u32| async move { Ok(n * 2) })
    }

    #[tokio::test(start_paused = true)]
    async fn test_throttle_layer_paces_calls() {
        let throttle = Arc::new(Throttle::per_second(10));
        let layer = ThrottleLayer::shared(throttle.clone()).with_cost(5);
        assert!(format!("{layer:?}").contains("ThrottleLayer"));
        let service = ServiceBuilder::new().layer(layer).service(double());
        assert!(format!("{service:?}").contains("Throttled"));
        let start = tokio::time::Instant::now();
        for n in 0..4 {
            assert_eq!(service.clone().oneshot(n).await.unwrap(), n * 2);
        }
        // 20 units at 10/s with a burst of 10: the third call waited ~0.5s for
        // its reservation and the fourth ~1s (virtual time).
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_millis(1_400) && waited <= Duration::from_millis(1_500),
            "{waited:?}"
        );
        assert_eq!(throttle.available(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_throttle_layer_reports_an_impossible_cost() {
        let service = ServiceBuilder::new()
            .layer(ThrottleLayer::new(Throttle::per_second(1)).with_cost(2))
            .service(double());
        let err = service.oneshot(1).await.unwrap_err();
        let err = err.downcast::<ThrottleError>().unwrap();
        assert_eq!(
            *err,
            ThrottleError::CostExceedsCapacity {
                cost: 2,
                capacity: 1
            }
        );
    }

    #[cfg(feature = "circuit-breaker")]
    #[tokio::test(start_paused = true)]
    async fn test_breaker_layer_settles_and_sheds() {
        use super::CircuitBreakerLayer;
        use crate::circuit::{BreakerState, CircuitBreaker, Trip};

        let breaker = Arc::new(
            CircuitBreaker::builder()
                .trip(Trip::Consecutive(2))
                .cooldown(Duration::from_secs(5))
                .build(Throttle::per_second(100)),
        );
        let layer = CircuitBreakerLayer::new(Arc::clone(&breaker));
        assert!(format!("{:?}", layer.clone()).contains("CircuitBreakerLayer"));
        let failing = ServiceBuilder::new()
            .layer(layer.clone())
            .service(service_fn(|(): ()| async {
                Err::<(), _>(std::io::Error::other("down"))
            }));
        assert!(format!("{failing:?}").contains("Guarded"));
        let ok = ServiceBuilder::new().layer(layer).service(double());
        assert_eq!(ok.clone().oneshot(2).await.unwrap(), 4);
        assert!(failing.clone().oneshot(()).await.is_err());
        assert!(failing.clone().oneshot(()).await.is_err());
        assert_eq!(breaker.state(), BreakerState::Open);
        let shed = ok.oneshot(1).await.unwrap_err();
        assert!(matches!(
            *shed.downcast::<ThrottleError>().unwrap(),
            ThrottleError::CircuitOpen { .. }
        ));
    }

    #[cfg(feature = "adaptive")]
    #[tokio::test(start_paused = true)]
    async fn test_adaptive_layer_feeds_outcomes_back() {
        use super::AdaptiveLayer;
        use crate::adaptive::{AdaptiveLimiter, Aimd};

        let limiter = Arc::new(
            AdaptiveLimiter::builder()
                .floor(1)
                .ceiling(16)
                .initial(8)
                .build(Aimd::new(1, 0.5)),
        );
        let layer = AdaptiveLayer::new(Arc::clone(&limiter));
        assert!(format!("{:?}", layer.clone()).contains("AdaptiveLayer"));
        let failing = ServiceBuilder::new()
            .layer(layer.clone())
            .service(service_fn(|(): ()| async {
                Err::<(), _>(std::io::Error::other("overloaded"))
            }));
        assert!(format!("{failing:?}").contains("Adaptive"));
        assert!(failing.oneshot(()).await.is_err());
        assert_eq!(limiter.current_limit(), 4, "a failure halves the limit");
        let ok = ServiceBuilder::new().layer(layer).service(double());
        assert_eq!(ok.oneshot(3).await.unwrap(), 6);
        assert_eq!(limiter.in_flight(), 0);
    }
}
