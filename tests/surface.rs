//! The accessor, timeout, and cancellation surface added in 2.0, end to end:
//! owned-permit timeouts and `cancel`, composite pause/resume, the tower
//! layers' readiness edge cases, and — with `tracing` — every event field.

#![cfg(all(
    feature = "tokio",
    feature = "circuit-breaker",
    feature = "adaptive",
    feature = "tower"
))]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use throttle_net::{
    AdaptiveLimiter, Aimd, BreakerState, Bulkhead, CircuitBreaker, Hybrid, Layered, Limiter,
    MultiLimiter, PerKey, Queue, Throttle, ThrottleError, TokioClock, Trip,
};

#[tokio::test(start_paused = true)]
async fn owned_permit_timeouts_and_call_timeout() {
    let bulkhead = Arc::new(Bulkhead::new(1).with_name("pool"));
    assert_eq!(bulkhead.name(), Some("pool"));
    assert_eq!(bulkhead.limit(), 1);
    let held = bulkhead
        .acquire_owned_timeout(Duration::from_millis(5))
        .await
        .unwrap();
    assert_eq!(
        bulkhead
            .acquire_owned_timeout(Duration::from_millis(5))
            .await
            .unwrap_err(),
        ThrottleError::BulkheadFull
    );
    assert_eq!(
        bulkhead
            .call_timeout(Duration::from_millis(5), || async { 1 })
            .await
            .unwrap_err(),
        ThrottleError::BulkheadFull
    );
    drop(held);
    assert_eq!(
        bulkhead
            .call_timeout(Duration::from_millis(5), || async { 7 })
            .await,
        Ok(7)
    );

    let adaptive = Arc::new(
        AdaptiveLimiter::builder()
            .floor(1)
            .ceiling(1)
            .initial(1)
            .name(String::from("search"))
            .build(Aimd::default()),
    );
    assert_eq!(adaptive.floor(), 1);
    assert_eq!(adaptive.ceiling(), 1);
    let permit = adaptive
        .acquire_owned_timeout(Duration::from_millis(5))
        .await
        .unwrap();
    assert_eq!(
        adaptive
            .acquire_owned_timeout(Duration::from_millis(5))
            .await
            .unwrap_err(),
        ThrottleError::DeadlineExceeded
    );
    permit.cancel();
    assert_eq!(adaptive.in_flight(), 0);
    assert_eq!(adaptive.current_limit(), 1, "a cancel teaches nothing");
    adaptive.try_acquire().unwrap().cancel();
    assert_eq!(adaptive.in_flight(), 0);

    let breaker = Arc::new(
        CircuitBreaker::builder()
            .trip(Trip::Consecutive(1))
            .build(Throttle::per_second(1).with_clock(TokioClock::new())),
    );
    let owned = breaker
        .acquire_owned_timeout(Duration::from_millis(5))
        .await
        .unwrap();
    owned.cancel(); // refunds the unit, records nothing
    assert_eq!(breaker.state(), BreakerState::Closed);
    let again = breaker
        .acquire_owned_timeout(Duration::from_millis(5))
        .await
        .unwrap();
    assert_eq!(
        breaker
            .acquire_owned_timeout(Duration::from_millis(5))
            .await
            .unwrap_err(),
        ThrottleError::DeadlineExceeded
    );
    again.success();
}

#[tokio::test(start_paused = true)]
async fn composites_pause_resume_and_report() {
    let shared: Arc<dyn Limiter> = Arc::new(Throttle::per_second(10).with_clock(TokioClock::new()));
    let hybrid = Hybrid::builder()
        .shared(Arc::clone(&shared))
        .name("edge")
        .build();
    assert_eq!(hybrid.paused_for(), Duration::ZERO);
    hybrid.pause_for(Duration::from_secs(5));
    assert_eq!(hybrid.paused_for(), Duration::from_secs(5));
    assert_eq!(Limiter::paused_for(&hybrid), Duration::from_secs(5));
    Limiter::resume(&hybrid);
    assert_eq!(hybrid.paused_for(), Duration::ZERO);
    assert_eq!(Limiter::charge(&hybrid, 0), 0);

    let multi = MultiLimiter::builder()
        .dimension(
            "requests",
            Throttle::per_second(10).with_clock(TokioClock::new()),
        )
        .build();
    multi.pause_for(Duration::from_secs(2));
    assert_eq!(multi.paused_for(), Duration::from_secs(2));
    multi.resume();
    assert_eq!(multi.paused_for(), Duration::ZERO);

    let layered = Layered::<&str>::builder()
        .global(Throttle::per_second(10).with_clock(TokioClock::new()))
        .per_key(
            PerKey::builder()
                .per_second(1)
                .clock(TokioClock::new())
                .build(),
        )
        .build();
    layered.pause_for(Duration::from_secs(3));
    assert_eq!(layered.paused_for(), Duration::from_secs(3));
    layered.resume();
    assert_eq!(layered.paused_for(), Duration::ZERO);
    let bare = Layered::<&str>::builder().build();
    bare.resume();
    assert_eq!(bare.paused_for(), Duration::ZERO);

    // A paused composite waiter holds; a resume releases it.
    let waiter_hybrid = Arc::new(
        Hybrid::builder()
            .limiter(Throttle::per_second(10).with_clock(TokioClock::new()))
            .build(),
    );
    waiter_hybrid.pause_for(Duration::from_secs(3600));
    let h = Arc::clone(&waiter_hybrid);
    let start = tokio::time::Instant::now();
    let waiter =
        tokio::spawn(async move { h.acquire_with_cost_timeout(1, Duration::from_secs(1)).await });
    assert_eq!(waiter.await.unwrap(), Err(ThrottleError::DeadlineExceeded));
    assert_eq!(
        start.elapsed(),
        Duration::ZERO,
        "the pause outlasts the deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn queue_names_and_accessors() {
    let queue: Queue<Throttle, &str> = Queue::builder()
        .name(String::from("crawler"))
        .capacity(4)
        .build(Throttle::per_second(10));
    assert_eq!(queue.name(), Some("crawler"));
    assert_eq!(queue.capacity(), 4);
    assert!(queue.is_empty());
    queue.acquire("a", 0, None).await.unwrap();
}

mod tower_layers {
    use super::*;
    use core::task::{Context, Poll};
    use throttle_net::tower::{AdaptiveLayer, CircuitBreakerLayer, ThrottleLayer};
    use tower::{Service, ServiceBuilder, ServiceExt, service_fn};

    fn echo() -> impl Service<
        u32,
        Response = u32,
        Error = std::convert::Infallible,
        Future = impl core::future::Future<Output = Result<u32, std::convert::Infallible>> + Send,
    > + Clone {
        service_fn(|n: u32| async move { Ok(n) })
    }

    fn not_ready_error(error: throttle_net::tower::BoxError) -> ThrottleError {
        *error.downcast::<ThrottleError>().unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn calling_an_unready_service_sends_nothing() {
        let throttle = Arc::new(Throttle::per_second(5).with_clock(TokioClock::new()));
        let mut throttled = ServiceBuilder::new()
            .layer(ThrottleLayer::shared(throttle.clone()))
            .service(echo());
        assert_eq!(
            not_ready_error(throttled.call(1).await.unwrap_err()),
            ThrottleError::NotReady
        );
        assert_eq!(throttle.available(), 5, "nothing was charged");

        let breaker = Arc::new(CircuitBreaker::builder().build(Throttle::per_second(5)));
        let mut guarded = ServiceBuilder::new()
            .layer(CircuitBreakerLayer::new(Arc::clone(&breaker)))
            .service(echo());
        assert_eq!(
            not_ready_error(guarded.call(1).await.unwrap_err()),
            ThrottleError::NotReady
        );

        let adaptive = Arc::new(AdaptiveLimiter::builder().initial(2).build(Aimd::default()));
        let mut capped = ServiceBuilder::new()
            .layer(AdaptiveLayer::new(Arc::clone(&adaptive)))
            .service(echo());
        assert_eq!(
            not_ready_error(capped.call(1).await.unwrap_err()),
            ThrottleError::NotReady
        );
    }

    #[tokio::test(start_paused = true)]
    async fn ready_services_give_their_admission_back_when_dropped() {
        let breaker = Arc::new(
            CircuitBreaker::builder()
                .trip(Trip::Consecutive(1))
                .build(Throttle::per_second(5)),
        );
        let layer =
            CircuitBreakerLayer::new(Arc::clone(&breaker)).with_timeout(Duration::from_secs(1));
        assert!(format!("{layer:?}").contains("timeout"));
        let mut guarded = ServiceBuilder::new().layer(layer).service(echo());
        let _ready = guarded.ready().await.unwrap();
        assert!(format!("{guarded:?}").contains("ready: true"));
        let clone = guarded.clone();
        assert!(
            format!("{clone:?}").contains("ready: false"),
            "a clone starts unready"
        );
        drop(guarded);
        assert_eq!(
            breaker.state(),
            BreakerState::Closed,
            "a cancel, not a failure"
        );

        let adaptive = Arc::new(
            AdaptiveLimiter::builder()
                .initial(1)
                .ceiling(1)
                .build(Aimd::default()),
        );
        let layer =
            AdaptiveLayer::new(Arc::clone(&adaptive)).with_timeout(Duration::from_millis(5));
        assert!(format!("{layer:?}").contains("timeout"));
        let mut capped = ServiceBuilder::new().layer(layer.clone()).service(echo());
        let _ready = capped.ready().await.unwrap();
        assert_eq!(adaptive.in_flight(), 1);
        assert!(format!("{:?}", capped.clone()).contains("Adaptive"));
        // A second service cannot get the only slot in time.
        let mut second = ServiceBuilder::new().layer(layer).service(echo());
        let refused = second.ready().await.err().unwrap();
        assert_eq!(not_ready_error(refused), ThrottleError::DeadlineExceeded);
        drop(capped);
        assert_eq!(
            adaptive.in_flight(),
            0,
            "the slot was freed without an outcome"
        );
        assert_eq!(adaptive.current_limit(), 1);
        assert_eq!(second.ready().await.unwrap().call(3).await.unwrap(), 3);
        assert_eq!(adaptive.in_flight(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_layer_timeout_fails_poll_ready() {
        let throttle = Arc::new(
            Throttle::per_duration(1, Duration::from_secs(60)).with_clock(TokioClock::new()),
        );
        let layer = ThrottleLayer::shared(throttle.clone()).with_timeout(Duration::from_millis(10));
        assert!(format!("{layer:?}").contains("timeout"));
        let mut service = ServiceBuilder::new().layer(layer).service(echo());
        assert_eq!(service.ready().await.unwrap().call(1).await.unwrap(), 1);
        let refused = core::future::poll_fn(|cx: &mut Context<'_>| match service.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(result),
        })
        .await;
        assert_eq!(
            not_ready_error(refused.unwrap_err()),
            ThrottleError::DeadlineExceeded
        );
    }

    #[tokio::test(start_paused = true)]
    async fn breaker_layer_settles_failures() {
        let breaker = Arc::new(
            CircuitBreaker::builder()
                .trip(Trip::Consecutive(1))
                .build(Throttle::per_second(5)),
        );
        let failing = ServiceBuilder::new()
            .layer(CircuitBreakerLayer::new(Arc::clone(&breaker)))
            .service(service_fn(|(): ()| async {
                Err::<(), _>(std::io::Error::other("down"))
            }));
        assert!(failing.clone().oneshot(()).await.is_err());
        assert_eq!(breaker.state(), BreakerState::Open);
        let shed = failing.oneshot(()).await.unwrap_err();
        assert!(matches!(
            not_ready_error(shed),
            ThrottleError::CircuitOpen { .. }
        ));
    }
}

#[cfg(feature = "tracing")]
mod events {
    use super::*;
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    /// Enabled for everything, records nothing: makes every event's fields
    /// evaluate.
    struct Everything;

    impl Subscriber for Everything {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, event: &Event<'_>) {
            struct Visit;
            impl tracing::field::Visit for Visit {
                fn record_debug(
                    &mut self,
                    _: &tracing::field::Field,
                    value: &dyn core::fmt::Debug,
                ) {
                    let _ = format!("{value:?}");
                }
            }
            event.record(&mut Visit);
        }
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}
    }

    #[test]
    fn every_event_formats() {
        let _guard = tracing::subscriber::set_default(Everything);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            let throttle = Throttle::per_second(1).with_name("t");
            assert!(throttle.try_acquire());
            assert!(!throttle.try_acquire());
            assert!(
                throttle
                    .acquire_timeout(Duration::from_millis(1))
                    .await
                    .is_err()
            );
            assert!(throttle.acquire_with_cost(2).await.is_err());

            let breaker = CircuitBreaker::builder()
                .trip(Trip::Consecutive(1))
                .name("b")
                .build(Throttle::per_second(10));
            breaker.record_failure();
            assert!(breaker.try_acquire().is_err());

            let adaptive = AdaptiveLimiter::builder().initial(4).build(Aimd::default());
            adaptive.try_acquire().unwrap().failure();

            let bulkhead = Bulkhead::new(0);
            assert!(bulkhead.try_acquire().is_none());

            let queue: Queue<Throttle> = Queue::builder()
                .capacity(1)
                .build(Throttle::per_second(100));
            queue.acquire((), 0, None).await.unwrap();

            let retry =
                throttle_net::Retry::new(throttle_net::Backoff::constant(Duration::from_millis(1)))
                    .max_attempts(2)
                    .name("r");
            let result: Result<(), ()> = retry
                .run(|| async { Err(()) }, |()| throttle_net::RetryAction::Retry)
                .await;
            assert!(result.is_err());
            let budget = throttle_net::RetryBudget::new(0.0, 0);
            let budgeted: Result<(), ()> = retry
                .run_with_budget(
                    &budget,
                    || async { Err(()) },
                    |()| throttle_net::RetryAction::Retry,
                )
                .await;
            assert!(budgeted.is_err());
        });
    }
}
