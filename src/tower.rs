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
//! Each service waits inside the future it returns from `call`, after taking
//! the inner service (which `poll_ready` made ready) and leaving a clone in its
//! place — so the inner service must be `Clone`, as most tower stacks are.
//! Errors are boxed (`BoxError`), the tower convention, so layers stack; a
//! limiter's refusal surfaces as a boxed [`ThrottleError`].
//!
//! Behind the `tower` feature (which needs `tokio` or `smol` for the waiting).

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::Arc;

use tower_layer::Layer;
use tower_service::Service;

#[cfg(feature = "adaptive")]
use crate::adaptive::{AdaptiveLimiter, AdaptiveStrategy};
#[cfg(feature = "circuit-breaker")]
use crate::circuit::CircuitBreaker;
use crate::error::ThrottleError;
use crate::limiter::Limiter;

/// The boxed error every service in this module returns.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The boxed future every service in this module returns.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = Result<T, BoxError>> + Send>>;

/// Takes the ready inner service, leaving a clone in its place (the standard
/// tower pattern for using a service inside a `'static` future).
fn take_ready<S: Clone>(inner: &mut S) -> S {
    let clone = inner.clone();
    core::mem::replace(inner, clone)
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
        Self { limiter, cost: 1 }
    }

    /// Charges `cost` units per call instead of one.
    #[must_use]
    pub fn with_cost(mut self, cost: u32) -> Self {
        self.cost = cost;
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
        }
    }
}

impl core::fmt::Debug for ThrottleLayer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ThrottleLayer")
            .field("capacity", &self.limiter.capacity())
            .field("cost", &self.cost)
            .finish()
    }
}

/// The service [`ThrottleLayer`] produces.
#[derive(Clone)]
pub struct Throttled<S> {
    inner: S,
    limiter: Arc<dyn Limiter>,
    cost: u32,
}

impl<S, Request> Service<Request> for Throttled<S>
where
    S: Service<Request> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: Into<BoxError>,
    Request: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = BoxFuture<S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let mut inner = take_ready(&mut self.inner);
        let limiter = Arc::clone(&self.limiter);
        let cost = self.cost;
        Box::pin(async move {
            wait(limiter.as_ref(), cost).await?;
            inner.call(request).await.map_err(Into::into)
        })
    }
}

impl<S> core::fmt::Debug for Throttled<S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Throttled")
            .field("cost", &self.cost)
            .finish_non_exhaustive()
    }
}

/// Waits on a dynamically typed limiter (reserving where it can, cancel-safe).
async fn wait(limiter: &dyn Limiter, cost: u32) -> Result<(), ThrottleError> {
    use crate::wait::{Refusal, wait_for};
    let item = || core::iter::once((limiter, cost));
    wait_for(
        |budget| crate::limiter::step_all(item(), budget),
        || limiter.refund(cost),
        None,
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
}

#[cfg(feature = "circuit-breaker")]
impl<L, C> CircuitBreakerLayer<L, C>
where
    C: clock_lib::Clock,
{
    /// Guards calls with `breaker`, shared with any other code that holds it.
    #[must_use]
    pub fn new(breaker: Arc<CircuitBreaker<L, C>>) -> Self {
        Self { breaker }
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
        }
    }
}

#[cfg(feature = "circuit-breaker")]
impl<S, L, C> Layer<S> for CircuitBreakerLayer<L, C>
where
    C: clock_lib::Clock,
{
    type Service = Guarded<S, L, C>;

    fn layer(&self, inner: S) -> Self::Service {
        Guarded {
            inner,
            breaker: Arc::clone(&self.breaker),
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
            .finish()
    }
}

/// The service [`CircuitBreakerLayer`] produces.
#[cfg(feature = "circuit-breaker")]
#[cfg_attr(docsrs, doc(cfg(feature = "circuit-breaker")))]
pub struct Guarded<S, L, C = clock_lib::SystemClock>
where
    C: clock_lib::Clock,
{
    inner: S,
    breaker: Arc<CircuitBreaker<L, C>>,
}

#[cfg(feature = "circuit-breaker")]
impl<S: Clone, L, C> Clone for Guarded<S, L, C>
where
    C: clock_lib::Clock,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            breaker: Arc::clone(&self.breaker),
        }
    }
}

#[cfg(feature = "circuit-breaker")]
impl<S, L, C, Request> Service<Request> for Guarded<S, L, C>
where
    S: Service<Request> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: Into<BoxError>,
    L: Limiter + 'static,
    C: clock_lib::Clock + 'static,
    Request: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = BoxFuture<S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let mut inner = take_ready(&mut self.inner);
        let breaker = Arc::clone(&self.breaker);
        Box::pin(async move {
            let permit = breaker.acquire_owned().await?;
            match inner.call(request).await {
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
            .finish_non_exhaustive()
    }
}

/// A [`Layer`] that caps in-flight calls with an [`AdaptiveLimiter`], waiting
/// for a slot, and feeds each call's outcome (with its latency) back to it —
/// `Ok` as a success, `Err` as a failure.
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
}

#[cfg(feature = "adaptive")]
impl<St, C> AdaptiveLayer<St, C>
where
    C: clock_lib::Clock,
{
    /// Caps calls with `limiter`, shared with any other code that holds it.
    #[must_use]
    pub fn new(limiter: Arc<AdaptiveLimiter<St, C>>) -> Self {
        Self { limiter }
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
        }
    }
}

#[cfg(feature = "adaptive")]
impl<S, St, C> Layer<S> for AdaptiveLayer<St, C>
where
    C: clock_lib::Clock,
{
    type Service = Adaptive<S, St, C>;

    fn layer(&self, inner: S) -> Self::Service {
        Adaptive {
            inner,
            limiter: Arc::clone(&self.limiter),
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
            .finish()
    }
}

/// The service [`AdaptiveLayer`] produces.
#[cfg(feature = "adaptive")]
#[cfg_attr(docsrs, doc(cfg(feature = "adaptive")))]
pub struct Adaptive<S, St, C = clock_lib::SystemClock>
where
    C: clock_lib::Clock,
{
    inner: S,
    limiter: Arc<AdaptiveLimiter<St, C>>,
}

#[cfg(feature = "adaptive")]
impl<S: Clone, St, C> Clone for Adaptive<S, St, C>
where
    C: clock_lib::Clock,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            limiter: Arc::clone(&self.limiter),
        }
    }
}

#[cfg(feature = "adaptive")]
impl<S, St, C, Request> Service<Request> for Adaptive<S, St, C>
where
    S: Service<Request> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: Into<BoxError>,
    St: AdaptiveStrategy + 'static,
    C: clock_lib::Clock + 'static,
    Request: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = BoxFuture<S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let mut inner = take_ready(&mut self.inner);
        let limiter = Arc::clone(&self.limiter);
        Box::pin(async move {
            let permit = limiter.acquire_owned().await;
            match inner.call(request).await {
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
    C: clock_lib::Clock,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Adaptive")
            .field("limiter", &self.limiter)
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
