//! Ready-made [`MultiLimiter`] configurations for common LLM provider tiers.
//!
//! These pre-wire the budgets an LLM API meters, so you can start with
//! `presets::anthropic::tier_2()` instead of hand-building the
//! [`MultiLimiter`]. Every preset has a `requests` dimension; the token
//! dimensions follow how the provider meters:
//!
//! - **Anthropic** meters input and output tokens separately (ITPM and OTPM),
//!   so its presets have independent `input_tokens` and `output_tokens`
//!   dimensions.
//! - **OpenAI** meters one combined token budget per minute (TPM), so its
//!   presets have a single `tokens` dimension, with `input_tokens` and
//!   `output_tokens` as aliases that charge it — code written against either
//!   naming spends the same budget, and input plus output can never exceed the
//!   real TPM.
//!
//! The figures are **illustrative starting points**: providers change tier
//! limits, and meter per model. Confirm them against your account's current
//! limits, and keep the limiter in step with what the server reports using
//! [`RateLimitInfo::sync_multi`](crate::provider::RateLimitInfo::sync_multi).
//!
//! Like any [`MultiLimiter`], the presets refuse unknown dimension names, so a
//! misspelt name is not charged nothing; call
//! [`with_strict(false)`](MultiLimiter::with_strict) to ignore unknown names
//! instead.
//!
//! Behind the `provider-llm` feature.

use core::time::Duration;

use crate::multi::MultiLimiter;
use crate::throttle::Throttle;

/// One minute, the window LLM providers meter against.
const MINUTE: Duration = Duration::from_secs(60);

/// A per-minute requests / input-tokens / output-tokens limiter (separate
/// token budgets).
fn separate(requests: u32, input_tokens: u32, output_tokens: u32) -> MultiLimiter {
    MultiLimiter::builder()
        .dimension("requests", Throttle::per_duration(requests, MINUTE))
        .dimension("input_tokens", Throttle::per_duration(input_tokens, MINUTE))
        .dimension(
            "output_tokens",
            Throttle::per_duration(output_tokens, MINUTE),
        )
        .build()
}

/// A per-minute requests / tokens limiter (one combined token budget, charged
/// through `tokens`, `input_tokens`, or `output_tokens`).
fn combined(requests: u32, tokens: u32) -> MultiLimiter {
    MultiLimiter::builder()
        .dimension("requests", Throttle::per_duration(requests, MINUTE))
        .dimension("tokens", Throttle::per_duration(tokens, MINUTE))
        .alias("input_tokens", "tokens")
        .alias("output_tokens", "tokens")
        .build()
}

/// Anthropic API tier presets (illustrative; verify against current docs).
pub mod anthropic {
    use super::{MultiLimiter, separate};

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
        separate(50, 40_000, 8_000)
    }

    /// Tier 2 — raised limits after initial usage.
    ///
    /// Requests/min, input-tokens/min, output-tokens/min: `1_000 / 80_000 / 16_000`.
    ///
    /// # Examples
    ///
    /// ```
    /// let limiter = throttle_net::presets::anthropic::tier_2();
    /// assert_eq!(limiter.available("output_tokens"), Some(16_000));
    /// ```
    #[must_use]
    pub fn tier_2() -> MultiLimiter {
        separate(1_000, 80_000, 16_000)
    }

    /// Tier 4 — high-volume limits.
    ///
    /// Requests/min, input-tokens/min, output-tokens/min: `4_000 / 400_000 / 80_000`.
    ///
    /// # Examples
    ///
    /// ```
    /// let limiter = throttle_net::presets::anthropic::tier_4();
    /// assert_eq!(limiter.available("requests"), Some(4_000));
    /// ```
    #[must_use]
    pub fn tier_4() -> MultiLimiter {
        separate(4_000, 400_000, 80_000)
    }
}

/// OpenAI API tier presets (illustrative; verify against current docs).
///
/// OpenAI meters one combined token budget, so these presets have a single
/// `tokens` dimension; `input_tokens` and `output_tokens` are aliases for it.
pub mod openai {
    use super::{MultiLimiter, combined};

    /// Tier 1 — modest limits for a new account.
    ///
    /// Requests/min, tokens/min: `500 / 30_000`.
    ///
    /// # Examples
    ///
    /// ```
    /// let limiter = throttle_net::presets::openai::tier_1();
    /// // Input and output tokens draw on one combined budget.
    /// assert!(limiter.try_acquire_costs(&[
    ///     ("requests", 1),
    ///     ("input_tokens", 20_000),
    ///     ("output_tokens", 10_000),
    /// ]));
    /// assert_eq!(limiter.available("tokens"), Some(0));
    /// ```
    #[must_use]
    pub fn tier_1() -> MultiLimiter {
        combined(500, 30_000)
    }

    /// Tier 2 — raised limits after initial usage.
    ///
    /// Requests/min, tokens/min: `5_000 / 450_000`.
    ///
    /// # Examples
    ///
    /// ```
    /// let limiter = throttle_net::presets::openai::tier_2();
    /// assert_eq!(limiter.available("tokens"), Some(450_000));
    /// ```
    #[must_use]
    pub fn tier_2() -> MultiLimiter {
        combined(5_000, 450_000)
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
        assert_eq!(limiter.dimensions().count(), 3);
    }

    #[test]
    fn test_tiers_are_monotonic() {
        assert!(
            anthropic::tier_2().available("requests") >= anthropic::tier_1().available("requests")
        );
        assert!(
            anthropic::tier_4().available("requests") >= anthropic::tier_2().available("requests")
        );
        assert!(openai::tier_2().available("tokens") >= openai::tier_1().available("tokens"));
    }

    #[test]
    fn test_openai_tokens_are_one_combined_budget() {
        // Regression (TN-033): separate input and output budgets at the full
        // TPM each admitted up to twice the real token budget.
        let limiter = openai::tier_1();
        assert!(limiter.try_acquire_costs(&[("input_tokens", 20_000), ("output_tokens", 10_000)]));
        assert!(
            !limiter.try_acquire_costs(&[("output_tokens", 1)]),
            "the 30k TPM is spent"
        );
        assert_eq!(
            limiter.dimensions().collect::<Vec<_>>(),
            ["requests", "tokens"]
        );
    }

    #[test]
    fn test_preset_admits_a_typical_call_and_is_strict() {
        let limiter = openai::tier_1();
        assert!(limiter.is_strict());
        assert!(limiter.try_acquire_costs(&[
            ("requests", 1),
            ("input_tokens", 1_000),
            ("output_tokens", 500),
        ]));
        assert!(
            !limiter.try_acquire_costs(&[("request", 1)]),
            "typo refused"
        );
    }
}
