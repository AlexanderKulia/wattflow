# 05 — Observability

**What to build:** OpenTelemetry tracing across every pipeline stage. Each Reading is wrapped in a pipeline envelope carrying its trace/span context through the channels (ADR-0006) — Reading itself stays pure domain data. Each stage extracts the context, starts its own span (giving per-stage latency), and re-attaches before forwarding. A single Reading's journey from ingest to persistence must be traceable end-to-end.

**Blocked by:** 04

**Status:** ready-for-agent

- [ ] Pipeline envelope carries trace/span context alongside each Reading through every channel
- [ ] Each stage emits its own span; per-stage latency is visible
- [ ] A single Reading's trace is followable end-to-end from ingest through persistence
