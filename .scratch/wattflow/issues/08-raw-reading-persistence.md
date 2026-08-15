# 08 — Raw Reading persistence (Timescale hypertable)

**What to build:** Alongside the existing Bucket upsert (ticket 04, ADR-0007), batch-write each deduped Reading to a new Timescale hypertable partitioned on timestamp (ADR-0008). Raw Reading inserts are `ON CONFLICT (dedup_key, timestamp) DO NOTHING` — a Reading is an immutable fact, so a retried/replayed batch write must not duplicate rows, but also must never overwrite one (unlike the Bucket upsert, which replaces). `dedup_key` reuses ingestion's dedup identity rule (ADR-0005), not `(device_id, reading_id)`, since `reading_id` can be empty under the producer's unreliable-ID knob; `timestamp` is appended because Timescale requires the partitioning column in any unique constraint on a hypertable.

**Blocked by:** 04

**Status:** done

- [x] Raw Reading table is a Timescale hypertable, partitioned on timestamp
- [x] Deduped Readings written batched, not per-event
- [x] Insert is `ON CONFLICT (dedup_key, timestamp) DO NOTHING`
- [x] Test demonstrates a retried/replayed batch write does not duplicate raw rows
- [x] Test demonstrates the correctness replay harness (ticket 03) is unaffected — raw-table writes are not in the aggregate's input path
