# 09 — Stretch: TimescaleDB continuous aggregate over raw Readings

**What to build:** A TimescaleDB continuous aggregate (materialized view, `time_bucket()`) over the raw Reading hypertable (ticket 08), bucketed at the same interval as the app-computed Bucket, as a read-path comparison against the app aggregate — not a source for the correctness harness (ADR-0008). Exists to show explicit awareness of Timescale's native bucketing and a deliberate, documented reason for not relying on it for the correctness invariant.

**Blocked by:** 08

**Status:** needs-triage

- [ ] Continuous aggregate defined over raw Reading table, same bucket interval as app aggregation
- [ ] Documented (design doc, ticket 07) as a read-optimization/comparison layer, explicitly not the correctness source of truth
- [ ] Spot-check: continuous aggregate output matches app-computed Bucket for a sample device/interval
