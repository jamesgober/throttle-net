# throttle-net &mdash; API Reference

> Complete reference for every public item in `throttle-net`, with examples.
> Format mirrors the portfolio standard.
>
> **Status: pre-1.0.** This document tracks the API surface as it lands across the 0.x series. Sections marked _(planned)_ describe the intended surface.

## Table of Contents

- [Overview](#overview)
- [Tier 1 &mdash; the lazy path](#tier-1--the-lazy-path) _(planned: 0.2)_
- [Tier 2 &mdash; the configured path](#tier-2--the-configured-path) _(planned: 0.3)_
- [Tier 3 &mdash; the power path](#tier-3--the-power-path) _(traits)_
- [Errors](#errors) _(planned: 0.2)_
- [Feature flags](#feature-flags)

---

## Overview

throttle-net is a general-purpose outbound throttling and resilience library. Where `rate-net` protects your service from being overwhelmed (inbound), `throttle-net` protects your service from overwhelming downstream APIs and from being banned by them (outbound).

The common case is a small Tier-1 surface; advanced use is a builder (Tier 2); the full surface is the trait seams (Tier 3) that let you swap the backend/transport/store.

---

## Tier 1 &mdash; the lazy path

_Documented in full as the 0.2 foundation release lands._

---

## Tier 2 &mdash; the configured path

_Documented at 0.3 when the configured/builder surface stabilises._

---

## Tier 3 &mdash; the power path

_The trait seams custom backends plug into. Documented as the traits stabilise across 0.x._

---

## Errors

_Domain error type built on `error-forge` (`#[non_exhaustive]`). Variants documented at 0.2._

---

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `std` | yes | Standard library. |
| `tokio` | yes | Tokio async runtime integration (default). |
| `adaptive` | no | AIMD + latency-based adaptive limiters. |
| `circuit-breaker` | no | Circuit breaker state machine. |
| `provider-headers` | no | HTTP rate-limit header parsing. |
| `provider-llm` | no | LLM provider presets (Anthropic, OpenAI, ...). |
| `metrics` | no | Metrics counters/histograms. |
| `tracing` | no | Tracing spans around acquire(). |
| `serde` | no | Serializable limiter configs. |

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
