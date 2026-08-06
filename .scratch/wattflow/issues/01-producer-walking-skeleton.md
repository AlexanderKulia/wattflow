# 01 — Producer + walking skeleton

**What to build:** A producer that emits synthetic meter Readings (`device_id`, `timestamp`, `reading_id`, `kWh`) onto a channel (ADR-0001), configurable to inject out-of-order delivery, duplicate events, delayed events, and burst load. Readings flow through a pass-through ingestion stage and a naive (non-deduping, non-watermarked) aggregation stage to stdout/log output. No correctness guarantees yet — this ticket proves the pipeline's shape and wiring end-to-end.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] Producer emits Readings with all four required fields
- [ ] Producer is configurable to inject: out-of-order delivery, duplicates, delayed events, burst load
- [ ] Readings flow producer → ingestion (pass-through) → aggregation (naive sum) → output, via bounded channels
- [ ] Running `go run ./cmd/wattflow` demonstrates the full path with visible output
