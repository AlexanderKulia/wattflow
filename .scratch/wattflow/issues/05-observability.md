# 05 — Observability

**What to build:** OpenTelemetry tracing across every pipeline stage. Each Reading is wrapped in a pipeline envelope carrying its trace/span context through the channels (ADR-0006) — Reading itself stays pure domain data. Each stage extracts the context, starts its own span (giving per-stage latency), and re-attaches before forwarding. A single Reading's journey from ingest to persistence must be traceable end-to-end.

**Blocked by:** 04

**Status:** done

- [x] Pipeline envelope carries trace/span context alongside each Reading through every channel
- [x] Each stage emits its own span; per-stage latency is visible
- [x] A single Reading's journey is correlatable end-to-end from ingest through persistence — real parent-child spans within `produce → ingest → aggregate`; span Links (not shared trace_id) bridge the fan-in points `aggregate → bucket` and `bucket → flush_buckets`/`flush_readings`, since a batching span has many contributors and picking one as "the" parent would be order-dependent, conflicting with the bitwise-identical-regardless-of-order invariant. Not a single continuous waterfall — three linked traces per Reading.
