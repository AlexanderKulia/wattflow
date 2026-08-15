# 06 — Load test + perf tuning

**What to build:** Load-test the pipeline to find its actual throughput ceiling under the block backpressure policy (ADR-0002) — not an assumed number. Make and measure at least one documented before/after performance change (e.g. worker pool sizing, batch write size from ticket 04), with before/after numbers recorded.

**Blocked by:** 05

**Status:** done

- [x] Load test run, throughput ceiling measured and recorded
- [x] At least one performance change made and measured before/after, with numbers documented

## Results

Transport in-process channels (ADR-0001), not HTTP. Load test = Go
benchmark: `test/loadtest/throughput_test.go`,
`BenchmarkPipelineThroughput`. Real pipeline, real TimescaleDB via
testcontainers, 50k events, 1 device.

Channel buffers fixed at 256 (was `producerCfg.Count`, buffer swallowed
whole run, block policy never engaged at any volume).

**Ceiling:** 36,615 events/sec (`BatchSizeBytes: 2MB`).

Bottleneck trace: 1 device, readings timestamped within ~1.4s wall-clock,
15min lateness watermark never advances enough to close bucket mid-run.
Bucket-storage path (`bucketStorageCh`, buffer 2) near-idle, flushes once at
drain. Real bottleneck: reading-storage path. ~88 bytes/reading, 2MB
threshold trips ~every 24k readings, synchronous `pool.Exec` blocks
`RunReadings` recv loop. Blocked recv fills `readingStorageCh`, blocks
ingestion send, fills `ingestCh`, blocks producer send. One stalled write,
backpressure propagates end to end.

**Change:** `readingStorageCfg.BatchSizeBytes` 2MB → 8MB (holds ~95k
readings, above 50k run total, threshold never trips mid-run).

| | events/sec |
|---|---|
| before (2MB batch) | 36,615 |
| after (8MB batch) | 86,114 |

+135%. Trade-off: bigger batch, bigger single blocking write, more data at
risk if process dies mid-batch.
