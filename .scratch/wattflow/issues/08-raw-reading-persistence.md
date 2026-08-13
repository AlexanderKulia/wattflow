# 08 — Raw Reading persistence (Timescale hypertable)

**What to build:** Alongside the existing Bucket upsert (ticket 04, ADR-0007), batch-write each deduped Reading to a new Timescale hypertable partitioned on timestamp (ADR-0008). Raw Reading inserts are `ON CONFLICT (device_id, reading_id) DO NOTHING` — a Reading is an immutable fact, so a retried/replayed batch write must not duplicate rows, but also must never overwrite one (unlike the Bucket upsert, which replaces).

**Deviation:** conflict target built as `ON CONFLICT (dedup_key, timestamp) DO NOTHING`, not `(device_id, reading_id)`. `reading_id` can be `""` (producer's unreliable-ID knob) — two distinct real Readings with unreliable IDs would collide under `(device_id, reading_id)`, silently dropping the second. `dedup_key` (`producer.Reading.DedupKey()`) is the same identity rule ingestion already uses for dedup: real ID when present, hash of device+timestamp+kwh when not — so storage's notion of "same Reading" matches ingestion's exactly, one method, not two independently-maintained rules. `timestamp` is appended because Timescale requires any unique/PK constraint on a hypertable to include the partitioning column.

**Blocked by:** 04

**Status:** done

- [x] Raw Reading table is a Timescale hypertable, partitioned on timestamp
- [x] Deduped Readings written batched, not per-event
- [x] Insert is `ON CONFLICT (dedup_key, timestamp) DO NOTHING` (see Deviation)
- [x] Test demonstrates a retried/replayed batch write does not duplicate raw rows
- [x] Test demonstrates the correctness replay harness (ticket 03) is unaffected — raw-table writes are not in the aggregate's input path
