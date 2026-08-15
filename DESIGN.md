# Design doc — trade-off decisions

Short version of the "why" behind the pipeline's core decisions. Full rationale lives in `docs/adr/`; this doc ties them together with final measured numbers from ticket 06.

## Dedup strategy

Key: `(device_id, reading_id)` tuple, not bare `reading_id` ([ADR-0005](docs/adr/0005-dedup-key-is-device-and-reading-id.md)). Nothing guarantees `reading_id` global uniqueness — per-device sequence counters would collide across devices, silently dropping one device's real reading as a false duplicate.

Fallback: content hash (device+timestamp+kWh) when `reading_id` unreliable/empty. Same identity rule reused for raw-Reading storage conflict target (`dedup_key`, [ADR-0008](docs/adr/0008-persist-raw-readings-alongside-aggregate-buckets.md)) — one method, not two independently-maintained "same reading" definitions.

State: in-memory map, not Redis ([ADR-0003](docs/adr/0003-in-memory-dedup-bounded-by-watermark.md)). No cross-process state to share — multi-node is stretch goal, not v1. Eviction tied to watermark: once a reading's timestamp falls outside the lateness window, its ID can never legally reappear, safe to drop. Bounded memory, no separate TTL/LRU needed. Sits behind an interface so Redis swap-in stays possible later.

## Lateness window sizing

15 minutes, matching `BucketSize`. Watermark tracked **per device**, not globally ([ADR-0004](docs/adr/0004-per-device-watermark.md)) — a global watermark would let one delayed/bursty device stall bucket finalization for every other device. Per-device watermark isolates each device's out-of-order tolerance and aligns with the aggregation key (`device_id` + interval): a bucket only closes once its own device's watermark passes it.

Consequence observed in ticket 06 load test: 50k readings for 1 device span ~1.4s wall-clock, so the watermark never advances 15 minutes mid-run — no bucket closes until channel drain. Window sizing trades faster bucket finalization against tolerance for real-world delayed telemetry; 15min chosen to match the bucket granularity itself, not tuned against synthetic load.

## Backpressure policy

Block, not drop-with-metric or spill-to-disk ([ADR-0002](docs/adr/0002-block-backpressure-policy.md)). Correctness invariant (bitwise-identical aggregate for fixed input) is non-negotiable — dropping events under load changes the input set and breaks it. Spill-to-disk solves a durability problem out of scope for single-process v1. Blocking never sacrifices correctness for throughput.

Mechanism: bounded Go channels between every stage, plain `ch <- x` sends — no `select`/`default` escape hatch. This is backpressure with zero extra code, just the absence of a bypass.

Ticket 06 measured where it actually engages: bucket-storage path near-idle in a short high-volume run (watermark logic above means it barely fires), reading-storage path is the real bottleneck. `~88 bytes/reading`, batch-size-byte threshold trips, synchronous `pool.Exec` blocks `RunReadings` recv, fills `readingStorageCh`, blocks ingestion send, fills `ingestCh`, blocks producer send. One stalled write, backpressure propagates end to end — exactly as ADR-0002 predicts.

## Tuned values (ticket 06)

Channel buffers: swept in the low hundreds to low thousands per stage — not sized to run volume; a buffer sized to swallow the whole run means block policy never engages at any load, defeats the point of measuring it.

Reading-storage batch size: swept in the tens to low hundreds of KB (~500–3,000 rows/batch), the range real-world multi-row `INSERT` batching sits in. Bigger batch means bigger single blocking write, more data at risk if the process dies mid-batch — a deliberate corner cut, not free.

Transport: in-process channels ([ADR-0001](docs/adr/0001-in-process-channel-transport.md)), not HTTP — load test is a Go benchmark (`test/loadtest/throughput_test.go`, `BenchmarkPipelineThroughputMatrix`) against real pipeline + real TimescaleDB, not an HTTP tool.

Full matrix results, sweet spot, and bottleneck trace: `.scratch/wattflow/issues/06-load-test-perf-tuning.md`.

## Continuous aggregate as read-path comparison, not correctness source

`reading_bucket_totals` ([ADR-0008](docs/adr/0008-persist-raw-readings-alongside-aggregate-buckets.md)) is a TimescaleDB continuous aggregate (`time_bucket()`) over the raw `readings` hypertable, same 15min interval as app aggregation. It exists to show explicit awareness of Timescale's native bucketing, and to spot-check the app-computed `bucket_totals` against a second, independent computation of the same numbers.

It is not part of the correctness invariant's input path. The aggregation module — dedup, watermark, order-independent summation — is this project's core correctness demonstration; delegating bucketing to the database would remove the reason that module exists. `bucket_totals` (app-computed) stays the source of truth; `reading_bucket_totals` is comparison-only.

Spot-checked in `TestContinuousAggregateMatchesAppBucket` (`internal/storage/storage_test.go`): same device/bucket, both paths produce identical kWh.
