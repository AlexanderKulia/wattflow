# 10 — CPU and memory profiling

**What to build:** Add `pprof` profiling to the pipeline benchmark (`test/loadtest`), capture CPU and heap profiles during a real run against TimescaleDB, and use `go tool pprof` to identify actual hot paths and allocation sources. Goal: get hands-on with Go's profiling tooling and find real bottlenecks by measurement, not guesswork — currently ticket 06's matrix shows throughput per config combo but not *why*.

**Blocked by:** none

**Status:** done

- [x] Profiling wired via `go test -bench`'s built-in `-cpuprofile`/`-memprofile`/`-blockprofile` flags — no `runtime/pprof`/`net/http/pprof` code needed, stdlib test tooling already covers it (see README.md § Profiling for the exact commands)
- [x] CPU, heap, and block profiles captured from a real run at the ticket-06 sweet-spot config (`buf=256/batch=64KB/flush=5s`)
- [x] `go tool pprof -top`/`-list` used to find hot functions and allocation sites
- [x] Findings documented below; matches ticket 06 partially, with one correction and two concrete fixes applied

## Findings

**Coverage caveat:** CPU profile only samples on-CPU time. First run showed `Total samples = 370ms (4.68%)` of a 7.91s wall-clock duration — the process spends ~95% of wall time off-CPU, blocked on network I/O (DB round-trip, docker API), not spinning. This means a CPU profile alone can't see the actual backpressure wait from ticket 06 — it only shows the compute wrapped around that wait.

**Block profile correction:** Go's block profiler tracks channel/mutex/select contention only, not blocked syscalls. `pool.Exec`'s own blocking cost in `block.prof` was ~2ms — the real DB-write wait (a socket read) is invisible to this profile type. Block profile is still useful for pipeline-stage channel contention (`ingestion.Run`, `aggregation.Run`, `storage.RunBuckets` each ~0.6-0.7s of blocking), but at this run's volume that signal was swamped by test-harness plumbing (testcontainers reaper heartbeat, pgxpool health-check ticker) — ~70% of total block time.

**Real hot path (CPU + heap agree):** `storage.flushReadings` — 25-27% of CPU samples, 60.75% of heap allocations (164.78MB/271.23MB) in the captured run. Two concrete, fixable causes inside it, not the DB write itself:

1. **squirrel's builder clones its internal tree on every `.Values()` call** (`github.com/lann/ps.(*tree).clone`, `lann/builder.Set`/`Extend`) — O(n) allocations for an n-row batch just to build the INSERT string.
2. **`reading_id`/`device_id` passed as Go `string` into `UUID` columns forced pgx's text-format encode fallback** (`pgtype.encodePlanStringToAnyTextFormat.Encode`, 30MB flat — second-largest single allocator in the whole profile) instead of its binary encode plan.

## Fix applied

`internal/storage/storage.go` — `flushReadings` and `flushBuckets` rewritten to hand-build the query via `strings.Builder` (drops squirrel entirely — `github.com/Masterminds/squirrel` removed from `go.mod`/`go.sum`, was otherwise unused) and pass UUID columns as `pgtype.UUID{Bytes: [16]byte, Valid: bool}` instead of raw strings, giving pgx its binary encode plan.

Before/after, same sweet-spot config, single run each:

| | before | after |
|---|---|---|
| `flushReadings` cum alloc | 164.78MB (60.75% of run) | 99.34MB (48.01% of run) |
| total run allocations | 271.23MB | 206.90MB |
| throughput | 21,831 events/sec | 22,398 events/sec |

`lann`/`squirrel` no longer appear anywhere in the post-fix heap profile's top allocators; `encodePlanUUIDCodecBinaryUUIDValuer` (the binary path) now appears in their place. Throughput unchanged within the noise band ticket 06 already flagged for single-run measurements — the win is allocation/GC pressure, not raw events/sec at this scale.
