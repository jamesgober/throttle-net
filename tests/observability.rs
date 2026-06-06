//! Verifies that observability hooks actually emit when the features are on.
//!
//! Uses a minimal capturing [`Recorder`] installed for the scope of the test via
//! `metrics::with_local_recorder`, then asserts the documented metric fired on a
//! state transition. Runs only with both the `metrics` and `circuit-breaker`
//! features (so it is exercised under `--all-features` in CI).

#![cfg(all(feature = "metrics", feature = "circuit-breaker"))]
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use metrics::{
    Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use throttle_net::{CircuitBreaker, Throttle, Trip};

/// Captured metric values, keyed by metric name.
#[derive(Default)]
struct Store {
    gauges: Mutex<HashMap<String, f64>>,
    counters: Mutex<HashMap<String, u64>>,
}

/// A gauge handle that records `set` into the shared store.
struct CapturingGauge {
    name: String,
    store: Arc<Store>,
}

impl GaugeFn for CapturingGauge {
    fn increment(&self, value: f64) {
        let mut g = self.store.gauges.lock().unwrap();
        *g.entry(self.name.clone()).or_default() += value;
    }
    fn decrement(&self, value: f64) {
        let mut g = self.store.gauges.lock().unwrap();
        *g.entry(self.name.clone()).or_default() -= value;
    }
    fn set(&self, value: f64) {
        let _ = self
            .store
            .gauges
            .lock()
            .unwrap()
            .insert(self.name.clone(), value);
    }
}

/// A counter handle that accumulates increments into the shared store.
struct CapturingCounter {
    name: String,
    store: Arc<Store>,
}

impl metrics::CounterFn for CapturingCounter {
    fn increment(&self, value: u64) {
        let mut c = self.store.counters.lock().unwrap();
        *c.entry(self.name.clone()).or_default() += value;
    }
    fn absolute(&self, value: u64) {
        let _ = self
            .store
            .counters
            .lock()
            .unwrap()
            .insert(self.name.clone(), value);
    }
}

/// A recorder that captures counters and gauges; histograms are ignored.
struct Capturing {
    store: Arc<Store>,
}

impl Recorder for Capturing {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(CapturingCounter {
            name: key.name().to_string(),
            store: Arc::clone(&self.store),
        }))
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(CapturingGauge {
            name: key.name().to_string(),
            store: Arc::clone(&self.store),
        }))
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

#[test]
fn circuit_state_gauge_fires_on_transition() {
    let store = Arc::new(Store::default());
    let recorder = Capturing {
        store: Arc::clone(&store),
    };

    metrics::with_local_recorder(&recorder, || {
        let breaker = CircuitBreaker::builder()
            .trip(Trip::Consecutive(1))
            .build(Throttle::per_second(10));
        breaker.record_failure(); // trips Closed -> Open
    });

    // The documented gauge fired with the "open" value (2).
    let gauges = store.gauges.lock().unwrap();
    assert_eq!(
        gauges.get("throttle_circuit_state").copied(),
        Some(2.0),
        "circuit state gauge should report open after a trip; captured: {gauges:?}"
    );
}
