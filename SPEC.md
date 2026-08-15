# Project Requirements: Energy Telemetry Ingestion Pipeline

## Context

Personal project simulating an energy telemetry ingestion pipeline. Domain modeled on real-world energy time-series/topology systems, using synthetic data and no proprietary code. Goal is defensible engineering decisions (dedup strategy, backpressure policy, correctness guarantees under adversarial data delivery), not a tutorial-style demo.

## Goals

- Demonstrate correct handling of out-of-order, duplicate, and bursty event delivery — the core hard problem in real telemetry/streaming systems.
- Demonstrate deliberate backpressure and load-handling decisions under Go's concurrency model (goroutines, channels).
- Demonstrate observability instrumentation (OpenTelemetry) across a multi-stage pipeline.
- Produce a correctness proof (automated test harness), not just a working demo.

## Non-goals

- Not a production system; no auth, multi-tenancy, or UI.
- Not attempting to replicate a full stream-processing framework (Kafka/Flink) — building a focused subset of the same problem.
- Distributed/multi-node partitioning is a stretch goal, not a requirement for v1.

## Functional Requirements

### 1. Event producer (simulator)
- Emits synthetic meter readings: `device_id`, `timestamp`, `reading_id`, `kWh`.
- Configurable to inject: out-of-order delivery, duplicate events, delayed events (seconds to minutes), burst load.

### 2. Ingestion service
- Accepts events from the producer over an in-process Go channel.
- Deduplicates events by `(device_id, reading_id)` (or content hash of device+timestamp+kWh if `reading_id` is absent/unreliable). Keyed on the tuple, not bare `reading_id`, since reading IDs are only guaranteed unique per device, not globally.
- Handles out-of-order arrival via a per-`device_id` watermark with a bounded, configurable "allowed lateness" window.
- Events arriving after the lateness window: logged and counted as dropped (not silently discarded).
- Dedup state held in memory, bounded by watermark-driven eviction (a reading ID older than the lateness window can never legally recur).

### 3. Aggregation
- Computes total kWh per `device_id` per fixed time interval (e.g., 15-minute buckets).
- Aggregate must be **identical** regardless of delivery order or duplication, for a fixed input set — this is the core correctness invariant.

### 4. Persistence
- Aggregate Buckets written to TimescaleDB, upserted (replace-on-conflict) on `(device_id, bucket)` — a retried/replayed batch write is a safe no-op.
- Deduped raw Readings also written to a separate TimescaleDB hypertable partitioned on timestamp, as an audit trail alongside the aggregate — insert is `ON CONFLICT DO NOTHING`, since a Reading is an immutable fact, never replaced.
- Writes are batched, not per-event, for realistic throughput.

### 5. Backpressure
- Bounded channels between pipeline stages (ingest → dedupe → aggregate → persist).
- Policy for a downstream stage falling behind: block. Upstream stages block on the bounded channel send; dropping or spilling would either break the correctness invariant or solve a durability problem out of scope for a single-process v1.

### 6. Observability
- OpenTelemetry tracing across all pipeline stages.
- Each event's journey from ingest to persistence should be traceable end-to-end, with latency visible per stage.

## Correctness Requirements (highest priority)

- Automated test harness that replays the same event set through the pipeline under multiple delivery patterns (in-order, reordered, duplicated, mixed).
- Assertion: final aggregate output is bitwise-identical across all replay patterns.
- Test harness must be part of CI, not a one-off manual check.

## Performance Requirements

- Load-tested to find the actual throughput ceiling of the pipeline as built (not an assumed number).
- At least one documented before/after performance change (e.g., worker pool sizing, batch write size) with measured impact.

## Stretch Goals (only after core requirements are solid)

- Partition ingestion by `device_id` across multiple nodes.
- Shared dedup store across nodes (Redis with TTL, or custom).
- Optional: minimal leader-election mechanism for partition ownership.

## Deliverables

- Go source repository with clear module boundaries (producer, ingestion, aggregation, persistence, observability).
- Test suite including the correctness harness.
- A short written design doc explaining the trade-off decisions made (dedup strategy, lateness window sizing, backpressure policy) — the artifact that documents the "why," not just the code.

## Design Decisions

Resolved during design phase; full rationale in `docs/adr/` and `DESIGN.md`.

- Producer → ingestion boundary: in-process Go channel (ADR-0001).
- Backpressure policy: block (ADR-0002).
- Dedup state: in-memory, watermark-evicted (ADR-0003).