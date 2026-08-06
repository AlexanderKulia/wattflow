# Wattflow

A Go pipeline simulating energy telemetry ingestion: correctness under out-of-order/duplicate/bursty event delivery, a deliberate backpressure policy, and OpenTelemetry observability across pipeline stages.

Not a production system (no auth/multi-tenancy/UI) and not a Kafka/Flink clone — a focused subset of the same problem, built to be defensible in depth rather than broad.

## Status

Early scaffolding. Module boundaries exist (`cmd/wattflow`, `internal/{producer,ingestion,aggregation,storage,observability}`, `test/correctness`); most are still empty. See [SPEC.md](SPEC.md) for the full requirements/design doc.

## Correctness invariant

For a fixed input set of events, the final per-`device_id`/per-interval kWh aggregate is bitwise-identical regardless of delivery order or duplication. Covered by an automated replay harness (in-order, reordered, duplicated, mixed patterns) intended to run in CI — see [SPEC.md](SPEC.md#correctness-requirements-highest-priority).

## Architecture

Pipeline stages connected by bounded channels, each its own module under `internal/`:

1. **producer** — synthetic meter-reading generator, configurable for out-of-order/duplicate/delayed/burst delivery.
2. **ingestion** — dedup by `reading_id`, watermark with bounded allowed-lateness window.
3. **aggregation** — total kWh per device per fixed time bucket, order/duplication-independent.
4. **storage** — batched writes to Postgres/TimescaleDB.
5. **observability** — OpenTelemetry tracing, per-stage latency, end-to-end per event.

Backpressure: bounded channels between every stage pair, with one explicit, documented policy for what happens when a downstream stage falls behind. See [SPEC.md](SPEC.md) for the full rationale.

## Commands

```
go build ./...
go run ./cmd/wattflow
go test ./...
go test ./test/correctness/...   # correctness/replay harness
go vet ./...
```