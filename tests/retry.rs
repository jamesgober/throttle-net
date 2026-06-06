//! Integration tests for the retry/backoff layer: the thundering-herd property
//! of decorrelated jitter, and an end-to-end `Retry-After` parse-and-honor path.

#![cfg(feature = "std")]

use std::collections::HashSet;
use std::time::Duration;

use throttle_net::{Backoff, Jitter};

/// Decorrelated jitter must scatter a fleet of clients that all failed at the
/// same instant, so their retries do not land together (the thundering herd).
#[test]
fn decorrelated_jitter_scatters_a_thundering_herd() {
    const CLIENTS: u64 = 1_000;
    let base = Duration::from_millis(100);

    let jittered = Backoff::exponential(base, 2.0).with_jitter(Jitter::Decorrelated);
    // Each client is a distinct seed: its first retry delay after a shared failure.
    let delays: Vec<Duration> = (0..CLIENTS)
        .map(|client| jittered.iter_seeded(client).next_delay())
        .collect();

    // Every delay sits in the decorrelated first-step window [base, 3*base].
    let lo = base;
    let hi = base * 3;
    for d in &delays {
        assert!(*d >= lo && *d <= hi, "{d:?} outside [{lo:?}, {hi:?}]");
    }

    // The delays are well spread: almost all are distinct (nanosecond grid) and
    // the observed range covers most of the window — no single instant attracts
    // the herd.
    let distinct = delays.iter().collect::<HashSet<_>>().len() as u64;
    assert!(
        distinct > CLIENTS * 9 / 10,
        "only {distinct}/{CLIENTS} distinct delays"
    );

    let min = delays.iter().min().copied().unwrap_or_default();
    let max = delays.iter().max().copied().unwrap_or_default();
    assert!(
        max - min > Duration::from_millis(150),
        "spread too narrow: {min:?}..{max:?}"
    );

    // Contrast: without jitter, every client retries at the identical delay —
    // the very pile-up jitter exists to prevent.
    let lockstep = Backoff::exponential(base, 2.0).with_jitter(Jitter::None);
    let lockstep_distinct = (0..CLIENTS)
        .map(|client| lockstep.iter_seeded(client).next_delay())
        .collect::<HashSet<_>>()
        .len();
    assert_eq!(
        lockstep_distinct, 1,
        "no-jitter backoff is a thundering herd"
    );
}

/// A server's `Retry-After` header is parsed and honored over the computed
/// backoff when the policy opts in.
///
/// Asserts *exact* elapsed time via tokio's paused virtual clock, which only
/// advances when the wait is on the tokio timer; under a real (smol) timer the
/// wait is real, so this is tokio-specific. The parse-and-honor logic itself is
/// runtime-agnostic and is exercised by the unit tests.
#[cfg(feature = "tokio")]
#[tokio::test(start_paused = true)]
async fn retry_after_header_is_parsed_and_honored() {
    use throttle_net::{Retry, RetryAction, parse_retry_after};

    /// A stand-in for a rejected response carrying a `Retry-After` header.
    struct Rejected {
        retry_after: &'static str,
    }

    let start = tokio::time::Instant::now();
    let retry = Retry::new(Backoff::constant(Duration::from_secs(1)))
        .max_attempts(2)
        .respect_retry_after(true);

    let result: Result<(), &str> = retry
        .run(
            || async { Err::<(), _>(Rejected { retry_after: "5" }) },
            |err: &Rejected| match parse_retry_after(err.retry_after) {
                Some(after) => RetryAction::RetryAfter(after),
                None => RetryAction::Retry,
            },
        )
        .await
        .map_err(|_| "exhausted");

    assert_eq!(result, Err("exhausted"));
    // The parsed 5s header was honored instead of the 1s computed backoff.
    assert_eq!(start.elapsed(), Duration::from_secs(5));
}
