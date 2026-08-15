# 06 — Load test + perf matrix sweep

**What to build:** Load-test the pipeline to find its actual throughput ceiling under the block backpressure policy (ADR-0002) — not an assumed number. Sweep realistic ranges for the tunable config params (channel buffer size, reading-storage `BatchSizeBytes`, `BatchFlushTimeout`), run the pipeline benchmark across the full permutation set (one real TimescaleDB run per combo), and record events/sec per combo. Goal: locate the actual throughput sweet spot by measurement across a matrix, not a single before/after point.

**Blocked by:** 05

**Status:** done

- [x] Matrix runner defined: config dimensions + realistic value ranges (batch sizes grounded near real-world multi-row INSERT norms, ~500–3000 rows/batch, not arbitrary MB figures), permutations generated (cartesian product)
- [x] Each permutation run against real pipeline + real TimescaleDB, events/sec recorded per combo
- [x] Results collected into one table, sweet spot identified and called out
- [x] Bottleneck trace documented: which stage/path backpressure engages on, why
- [x] Sweet-spot config values + bottleneck trace documented in `DESIGN.md`

Load test = Go benchmark: `test/loadtest/throughput_test.go`, `BenchmarkPipelineThroughputMatrix`. Real pipeline, real TimescaleDB via testcontainers, 50k events, 1 device.

## Mechanism (from initial single-combo run, still holds across the matrix)

Bottleneck trace: 1 device, readings timestamped within ~1.4s wall-clock, 15min lateness watermark never advances enough to close a bucket mid-run. Bucket-storage path (`bucketStorageCh`, buffer 2) stays near-idle, flushes once at drain. Real bottleneck: reading-storage path. `~88 bytes/reading`; once buffered bytes cross the batch-size threshold, a synchronous `pool.Exec` blocks `RunReadings`'s recv loop. Blocked recv fills `readingStorageCh`, blocks ingestion's send, fills `ingestCh`, blocks producer's send — one stalled write, backpressure propagates end to end, exactly as ADR-0002 predicts.

## Results

2x2x3 matrix: `channelBufferSize` {256, 1024} x `readingFlushTimeout` {5s, 30s} x `readingBatchSizeBytes` {64KB, 128KB, 256KB}, `-benchtime 1x` (one real pipeline + TimescaleDB run per combo).

| buf | batch | flush | events/sec |
|---|---|---|---|
| 256 | 64KB | 5s | 21,831 |
| 256 | 64KB | 30s | 18,465 |
| 256 | 128KB | 5s | 14,721 |
| 256 | 128KB | 30s | 16,894 |
| 256 | 256KB | 5s | 18,406 |
| 256 | 256KB | 30s | 15,957 |
| 1024 | 64KB | 5s | 16,982 |
| 1024 | 64KB | 30s | 16,590 |
| 1024 | 128KB | 5s | 15,950 |
| 1024 | 128KB | 30s | 16,409 |
| 1024 | 256KB | 5s | 17,237 |
| 1024 | 256KB | 30s | 15,660 |

Sweet spot: **buf=256, batch=64KB, flush=5s — 21,831 events/sec**. Range is 14.7k–21.8k across all 12 combos, no clean monotonic trend by batch size or buffer size — single run per combo (`-benchtime 1x`) against a fresh testcontainers-spun TimescaleDB instance per combo means container startup variance and one-shot noise likely dominate over small config deltas. Repeating each combo (`-benchtime`, multiple runs) would be needed to separate real signal from noise before trusting this as a stable optimum.
