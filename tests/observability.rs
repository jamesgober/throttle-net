//! Verifies that the observability hooks emit the documented metrics, with
//! per-instance `name` labels, from every limiter kind.
//!
//! A minimal capturing [`Recorder`] is installed for the scope of each test via
//! `metrics::with_local_recorder`; every series is keyed by its name plus its
//! sorted labels, so two instances of one kind are told apart. Runs only with
//! the `metrics` feature and the limiters it exercises (`--all-features`).

#![cfg(all(
    feature = "metrics",
    feature = "circuit-breaker",
    feature = "adaptive",
    feature = "tokio"
))]
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use throttle_net::{
    AdaptiveLimiter, Aimd, Bulkhead, CircuitBreaker, Hybrid, Queue, Throttle, Trip,
};

/// Captured values, keyed by `name{label=value,...}`.
#[derive(Default)]
struct Store {
    gauges: Mutex<HashMap<String, f64>>,
    counters: Mutex<HashMap<String, u64>>,
    histograms: Mutex<HashMap<String, usize>>,
}

impl Store {
    fn counter(&self, series: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .get(series)
            .copied()
            .unwrap_or(0)
    }

    fn gauge(&self, series: &str) -> Option<f64> {
        self.gauges.lock().unwrap().get(series).copied()
    }

    fn samples(&self, series: &str) -> usize {
        self.histograms
            .lock()
            .unwrap()
            .get(series)
            .copied()
            .unwrap_or(0)
    }
}

/// `name{a=1,b=2}` with labels sorted, so lookups do not depend on order.
fn series(key: &Key) -> String {
    let mut labels: Vec<String> = key
        .labels()
        .map(|l| format!("{}={}", l.key(), l.value()))
        .collect();
    labels.sort();
    format!("{}{{{}}}", key.name(), labels.join(","))
}

struct Handle {
    series: String,
    store: Arc<Store>,
}

impl GaugeFn for Handle {
    fn increment(&self, value: f64) {
        *self
            .store
            .gauges
            .lock()
            .unwrap()
            .entry(self.series.clone())
            .or_default() += value;
    }
    fn decrement(&self, value: f64) {
        *self
            .store
            .gauges
            .lock()
            .unwrap()
            .entry(self.series.clone())
            .or_default() -= value;
    }
    fn set(&self, value: f64) {
        let _ = self
            .store
            .gauges
            .lock()
            .unwrap()
            .insert(self.series.clone(), value);
    }
}

impl CounterFn for Handle {
    fn increment(&self, value: u64) {
        *self
            .store
            .counters
            .lock()
            .unwrap()
            .entry(self.series.clone())
            .or_default() += value;
    }
    fn absolute(&self, value: u64) {
        let _ = self
            .store
            .counters
            .lock()
            .unwrap()
            .insert(self.series.clone(), value);
    }
}

impl HistogramFn for Handle {
    fn record(&self, _value: f64) {
        *self
            .store
            .histograms
            .lock()
            .unwrap()
            .entry(self.series.clone())
            .or_default() += 1;
    }
}

struct Capturing {
    store: Arc<Store>,
}

impl Capturing {
    fn handle(&self, key: &Key) -> Arc<Handle> {
        Arc::new(Handle {
            series: series(key),
            store: Arc::clone(&self.store),
        })
    }
}

impl Recorder for Capturing {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(self.handle(key))
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(self.handle(key))
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(self.handle(key))
    }
}

/// Runs `body` with a fresh capturing recorder and returns what it captured.
fn capture(body: impl FnOnce()) -> Arc<Store> {
    let store = Arc::new(Store::default());
    let recorder = Capturing {
        store: Arc::clone(&store),
    };
    metrics::with_local_recorder(&recorder, body);
    store
}

#[test]
fn circuit_state_gauge_is_set_at_construction_and_per_instance() {
    // Regression (TN-014): the state gauge had no instance label (two breakers
    // overwrote each other) and was only set on a transition.
    let store = capture(|| {
        let a = CircuitBreaker::builder()
            .trip(Trip::Consecutive(1))
            .name("payments")
            .build(Throttle::per_second(10));
        let _b = CircuitBreaker::builder()
            .name("search")
            .build(Throttle::per_second(10));
        a.record_failure(); // payments: Closed -> Open
        assert!(a.try_acquire().is_err()); // shed
    });
    assert_eq!(
        store.gauge("throttle_circuit_state{name=payments}"),
        Some(2.0)
    );
    assert_eq!(
        store.gauge("throttle_circuit_state{name=search}"),
        Some(0.0)
    );
    assert_eq!(
        store.counter("throttle_shed_total{limiter=circuit_breaker,name=payments}"),
        1
    );
}

#[test]
fn non_waiting_acquires_count_grants_and_denials() {
    let store = capture(|| {
        let throttle = Throttle::per_minute(2).with_name("api");
        assert!(throttle.try_acquire());
        assert!(throttle.try_acquire());
        assert!(!throttle.try_acquire());
    });
    assert_eq!(
        store.counter("throttle_acquired_total{limiter=throttle,name=api}"),
        2
    );
    assert_eq!(
        store.counter("throttle_denied_total{limiter=throttle,name=api}"),
        1
    );
}

#[test]
fn composites_emit_for_themselves_not_their_constituents() {
    let store = capture(|| {
        let hybrid = Hybrid::builder()
            .limiter(Throttle::per_second(5).with_name("inner"))
            .name("outer")
            .build();
        assert!(hybrid.try_acquire());
    });
    assert_eq!(
        store.counter("throttle_acquired_total{limiter=hybrid,name=outer}"),
        1
    );
    assert_eq!(
        store.counter("throttle_acquired_total{limiter=throttle,name=inner}"),
        0
    );
}

#[test]
fn unnamed_instances_report_the_default_name() {
    let store = capture(|| {
        assert!(Throttle::per_second(1).try_acquire());
    });
    assert_eq!(
        store.counter("throttle_acquired_total{limiter=throttle,name=default}"),
        1
    );
}

#[test]
fn waiting_acquires_record_waits_and_timeouts() {
    let store = capture(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            let throttle = Throttle::per_duration(1, Duration::from_secs(3600)).with_name("slow");
            throttle.acquire().await.unwrap();
            let timed_out = throttle.acquire_timeout(Duration::from_millis(1)).await;
            assert!(timed_out.is_err());
        });
    });
    assert_eq!(
        store.samples("throttle_wait_duration{limiter=throttle,name=slow}"),
        2
    );
    assert_eq!(
        store.counter("throttle_acquired_total{limiter=throttle,name=slow}"),
        1
    );
    assert_eq!(
        store.counter("throttle_timeouts_total{limiter=throttle,name=slow}"),
        1
    );
}

#[test]
fn queue_adaptive_and_bulkhead_gauges_and_counters() {
    let store = capture(|| {
        let _queue: Queue<Throttle> = Queue::builder()
            .name("crawl")
            .build(Throttle::per_second(1));
        let limiter = AdaptiveLimiter::builder()
            .initial(8)
            .ceiling(8)
            .name("db")
            .build(Aimd::new(1, 0.5));
        limiter.try_acquire().unwrap().failure();
        let bulkhead = Bulkhead::new(0).with_name("pool");
        assert!(bulkhead.try_acquire().is_none());
    });
    assert_eq!(store.gauge("throttle_queue_depth{name=crawl}"), Some(0.0));
    assert_eq!(store.gauge("throttle_rate_current{name=db}"), Some(4.0));
    assert_eq!(
        store.counter("throttle_shed_total{limiter=bulkhead,name=pool}"),
        1
    );
}
