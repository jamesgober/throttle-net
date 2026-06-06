//! Per-tenant quotas: every tenant gets its own budget, under a shared global
//! ceiling, so one noisy tenant cannot starve the others or the service.
//!
//! A [`Layered`] limiter stacks a global cap over a per-tenant [`PerKey`] cap:
//! a request must clear both. Per-tenant state is sharded and bounded, so a flood
//! of unique tenants cannot grow memory without limit.
//!
//! Run with: `cargo run --example per_tenant_quotas`

use throttle_net::{Layered, PerKey, Throttle};

#[tokio::main]
async fn main() -> Result<(), throttle_net::ThrottleError> {
    // 1000 requests/second overall, but at most 10/second for any one tenant.
    let limiter = Layered::<String>::builder()
        .global(Throttle::per_second(1000))
        .per_key(PerKey::per_second(10))
        .build();

    let endpoint = "/v1/api".to_string();

    // Two tenants each fire 15 requests at once. Each is capped at its own 10,
    // independently — and both are well under the 1000 global.
    for tenant in ["acme", "globex"] {
        let key = tenant.to_string();
        let mut admitted = 0;
        for _ in 0..15 {
            if limiter.try_acquire(&key, &endpoint) {
                admitted += 1;
            }
        }
        println!("tenant {tenant:>7}: {admitted:>2}/15 admitted now (per-tenant cap 10)");
    }

    // The waiting form paces a throttled tenant instead of dropping it: this
    // returns once "acme" has refilled a token, rather than failing.
    limiter.acquire(&"acme".to_string(), &endpoint).await?;
    println!("tenant    acme: one more admitted after a brief wait for refill");

    Ok(())
}
