# 09 — Stretch: TimescaleDB continuous aggregate over raw Readings

**What to build:** A TimescaleDB continuous aggregate (materialized view, `time_bucket()`) over the raw Reading hypertable (ticket 08), bucketed at the same interval as the app-computed Bucket, as a read-path comparison against the app aggregate — not a source for the correctness harness (ADR-0008). Exists to show explicit awareness of Timescale's native bucketing and a deliberate, documented reason for not relying on it for the correctness invariant.

**Blocked by:** 08

**Status:** done

- [x] Continuous aggregate defined over raw Reading table, same bucket interval as app aggregation
- [x] Documented (design doc, ticket 07) as a read-optimization/comparison layer, explicitly not the correctness source of truth
- [x] Spot-check: continuous aggregate output matches app-computed Bucket for a sample device/interval

View: `reading_bucket_totals`, `internal/storage/migrations/0001_init.up.sql`. `WITH NO DATA` (Timescale continuous aggregates can't materialize inside the migration's transaction) — real-time aggregation covers query-time reads, but a fresh continuous aggregate's materialization threshold starts at creation time, not `-infinity`, so pre-existing rows need one `refresh_continuous_aggregate` call to become visible. Wired into test setup, not a manual step: `TestContinuousAggregateMatchesAppBucket`, `internal/storage/storage_test.go`.
