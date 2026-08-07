# 03 — Aggregation correctness + replay harness

**What to build:** Aggregation sums kWh per `device_id` per calendar-aligned Bucket (e.g. 15-minute intervals). A Bucket is emitted exactly once, gated on that device's watermark passing it (ADR-0004) — never re-emitted or updated afterward. This is the project's core correctness invariant: build the automated replay test harness (under `test/correctness`) that replays a fixed input set through the pipeline under multiple delivery patterns (in-order, reordered, duplicated, mixed) and asserts the final aggregate output is bitwise-identical across all of them. Wire the harness into CI (not a one-off manual check). This is the single highest-priority deliverable for the whole project.

**Blocked by:** 02

**Status:** done

- [x] Aggregation buckets are calendar-aligned (e.g. `:00/:15/:30/:45`), independent of arrival order
- [x] A Bucket is emitted exactly once, only after its device's watermark has passed it
- [x] Replay harness replays a fixed input set under in-order, reordered, duplicated, and mixed delivery patterns
- [x] Harness asserts bitwise-identical final aggregate output across all patterns
- [x] Harness runs in CI
