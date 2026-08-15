# Project Requirements: Energy Telemetry Ingestion Pipeline

## Context

Personal project simulating an energy telemetry ingestion pipeline. Domain draws on prior production experience in energy-sector time-series/topology modeling, without reusing any proprietary code or data. Goal is defensible engineering decisions (dedup strategy, backpressure policy, correctness guarantees under adversarial data delivery), not a tutorial-style demo.

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
- Accepts events from the producer (in-process channel or HTTP endpoint — TBD in design phase).
- Deduplicates events by `reading_id` (or content hash if `reading_id` is absent/unreliable).
- Handles out-of-order arrival via a watermark with a bounded, configurable "allowed lateness" window.
- Events arriving after the lateness window: logged and counted as dropped (not silently discarded).

### 3. Aggregation
- Computes total kWh per `device_id` per fixed time interval (e.g., 15-minute buckets).
- Aggregate must be **identical** regardless of delivery order or duplication, for a fixed input set — this is the core correctness invariant.

### 4. Persistence
- Aggregates written to Postgres or TimescaleDB.
- Writes are batched, not per-event, for realistic throughput.

### 5. Backpressure
- Bounded channels between pipeline stages (ingest → dedupe → aggregate → persist).
- Explicit, documented policy for what happens when a downstream stage falls behind (block / drop-with-metric / spill-to-disk) — one policy chosen and justified, not arbitrary.

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
- A short written design doc explaining the trade-off decisions made (dedup strategy, lateness window sizing, backpressure policy) — this is what gets referenced in interviews, not just the code.

## Explicitly Open Design Decisions (to resolve during design phase, not now)

- In-process channels vs. HTTP/gRPC ingestion endpoint for the producer → ingestion boundary.
- Exact backpressure policy (block / drop / spill) — pick one and document rationale.
- Whether Redis or in-memory store is sufficient for dedup state in v1.