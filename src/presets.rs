//! Ready-made [`MultiLimiter`] configurations for common LLM provider tiers.
//!
//! These pre-wire the requests / input-tokens / output-tokens dimensions an LLM
//! API meters, so you can start with `presets::anthropic::tier_2()` instead of
//! hand-building the [`MultiLimiter`]. The numbers are **illustrative starting
//! points** — providers change tier limits and meter per model — so confirm them
//! against the current provider documentation and adjust.
//!
//! Behind the `provider-llm` feature.

use core::time::Duration;

use crate::multi::MultiLimiter;
use crate::throttle::Throttle;

/// One minute, the window LLM providers meter against.
const MINUTE: Duration = Duration::from_secs(60);

/// Builds a per-minute request/input-token/output-token [`MultiLimiter`].
fn per_minute(requests: u32, input_tokens: u32, output_tokens: u32) -> MultiLimiter {
    MultiLimiter::builder()
        .dimension("requests", Throttle::per_duration(requests, MINUTE))
        .dimension("input_tokens", Throttle::per_duration(input_tokens, MINUTE))
        .dimension(
            "output_tokens",
            Throttle::per_duration(output_tokens, MINUTE),
        )
        .build()
}

/// Anthropic API tier presets (illustrative; verify against current docs).
pub mod anthropic {
    use super::{MultiLimiter, per_minute};

    /// Tier 1 — modest limits for a new account.
    ///
    /// Requests/min, input-tokens/min, output-tokens/min: `50 / 40_000 / 8_000`.
    ///
    /// # Examples
    ///
    /// ```
    /// use throttle_net::presets;
    ///
    /// let limiter = presets::anthropic::tier_1();
    /// // Charge a call against all three budgets at once (non-blocking here;
    /// // `acquire_costs(...).await` waits, with the `tokio` feature).
    /// assert!(limiter.try_acquire_costs(&[
    ///     ("requests", 1),
    ///     ("input_tokens", 1500),
    ///     ("output_tokens", 200),
    /// ]));
    /// ```
    #[must_use]
    pub fn tier_1() -> MultiLimiter {
        per_minute(50, 40_000, 8_000)
    }

    /// Tier 2 — raised limits after initial usage.
    ///
    /// Requests/min, input-tokens/min, output-tokens/min: `1_000 / 80_000 / 16_000`.
    #[must_use]
    pub fn tier_2() -> MultiLimiter {
        per_minute(1_000, 80_000, 16_000)
    }

    /// Tier 4 — high-volume limits.
    ///
    /// Requests/min, input-tokens/min, output-tokens/min: `4_000 / 400_000 / 80_000`.
    #[must_use]
    pub fn tier_4() -> MultiLimiter {
        per_minute(4_000, 400_000, 80_000)
    }
}

/// OpenAI API tier presets (illustrative; verify against current docs).
pub mod openai {
    use super::{MultiLimiter, per_minute};

    /// Tier 1 — modest limits for a new account.
    ///
    /// Requests/min, input-tokens/min, output-tokens/min: `500 / 30_000 / 30_000`.
    #[must_use]
    pub fn tier_1() -> MultiLimiter {
        per_minute(500, 30_000, 30_000)
    }

    /// Tier 2 — raised limits after initial usage.
    ///
    /// Requests/min, input-tokens/min, output-tokens/min: `5_000 / 450_000 / 450_000`.
    #[must_use]
    pub fn tier_2() -> MultiLimiter {
        per_minute(5_000, 450_000, 450_000)
    }
}

#[cfg(test)]
mod tests {
    use super::{anthropic, openai};

    #[test]
    fn test_anthropic_tiers_have_the_three_dimensions() {
        let limiter = anthropic::tier_2();
        assert_eq!(limiter.available("requests"), Some(1_000));
        assert_eq!(limiter.available("input_tokens"), Some(80_000));
        assert_eq!(limiter.available("output_tokens"), Some(16_000));
    }

    #[test]
    fn test_tiers_are_monotonic() {
        // Higher tiers grant at least as much as lower ones.
        assert!(
            anthropic::tier_2().available("requests") >= anthropic::tier_1().available("requests")
        );
        assert!(
            anthropic::tier_4().available("requests") >= anthropic::tier_2().available("requests")
        );
        assert!(openai::tier_2().available("requests") >= openai::tier_1().available("requests"));
    }

    #[test]
    fn test_preset_admits_a_typical_call() {
        let limiter = openai::tier_1();
        assert!(limiter.try_acquire_costs(&[
            ("requests", 1),
            ("input_tokens", 1_000),
            ("output_tokens", 500),
        ]));
    }
}
