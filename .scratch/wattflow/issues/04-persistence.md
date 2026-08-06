# 04 — Persistence

**What to build:** Batched (not per-event) writes of finalized Buckets to Postgres/TimescaleDB, flushed on a size-or-time trigger (whichever hits first). Writes are upserts on `(device_id, bucket)` that **replace** the kWh value on conflict, never increment it (ADR-0007) — this is what keeps a retried or replayed batch write from double-counting a Bucket's total.

**Blocked by:** 03

**Status:** ready-for-agent

- [ ] Writes are batched, flushed on size-or-time trigger
- [ ] Upsert on `(device_id, bucket)` uses replace-on-conflict, not increment-on-conflict
- [ ] Test demonstrates a retried/replayed batch write for the same Bucket does not change the persisted total
