# 10 — CPU and memory profiling

**What to build:** Add `pprof` profiling to the pipeline benchmark (`test/loadtest`), capture CPU and heap profiles during a real run against TimescaleDB, and use `go tool pprof` to identify actual hot paths and allocation sources. Goal: get hands-on with Go's profiling tooling and find real bottlenecks by measurement, not guesswork — currently ticket 06's matrix shows throughput per config combo but not *why*.

**Blocked by:** none

**Status:** ready-for-agent

- [ ] `runtime/pprof` (or `net/http/pprof` if run as a live server) wired into the load test, CPU + heap profiles written to file per run
- [ ] At least one CPU profile and one heap profile captured from a real high-volume run
- [ ] `go tool pprof` used to find top hot functions / allocation sites, findings written down
- [ ] Findings documented in `DESIGN.md` or the ticket itself: what's hot, whether it matches the backpressure bottleneck already found in ticket 06 (reading-storage batch write), any surprises
