# 02 — Ingestion correctness: dedup + per-device watermark

**What to build:** Ingestion deduplicates on the `(device_id, reading_id)` tuple (ADR-0005), falling back to a content hash of `device_id + timestamp + kWh` when `reading_id` is absent/unreliable. Each device gets its own watermark (ADR-0004) with a bounded, configurable allowed-lateness window. A Reading is checked for lateness first, then dedup (our Q10 resolution) — so a Reading that's both late and a duplicate is classified `late`, never double-counted as a drop. Dropped Readings are logged and counted by reason (`late` or `duplicate`), never silently discarded. Dedup state for a device is evicted once that device's watermark passes it (ADR-0003).

**Blocked by:** 01

**Status:** done

- [x] Producer gets a knob to emit Readings with absent/unreliable `reading_id`, so the content-hash fallback path is actually exercised, not dead code
- [x] Dedup key is `(device_id, reading_id)`, with content-hash fallback when reading_id is absent/unreliable
- [x] Watermark tracked independently per `device_id`
- [x] Lateness window is bounded and configurable
- [x] Lateness checked before dedup; late-and-duplicate Readings classified `late` only
- [x] Dropped Readings logged and counted per reason (`late`, `duplicate`)
- [x] Dedup state for a device evicted once outside that device's lateness window