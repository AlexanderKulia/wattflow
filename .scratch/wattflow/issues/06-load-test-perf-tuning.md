# 06 — Load test + perf tuning

**What to build:** Load-test the pipeline with vegeta or k6 to find its actual throughput ceiling under the block backpressure policy (ADR-0002) — not an assumed number. Make and measure at least one documented before/after performance change (e.g. worker pool sizing, batch write size from ticket 04), with before/after numbers recorded.

**Blocked by:** 05

**Status:** ready-for-agent

- [ ] Load test run with vegeta or k6, throughput ceiling measured and recorded
- [ ] At least one performance change made and measured before/after, with numbers documented
