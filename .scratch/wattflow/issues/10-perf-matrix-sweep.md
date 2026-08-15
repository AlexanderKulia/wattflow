# 10 — Perf matrix sweep across config ranges

**What to build:** Extend the ticket 06 load-test benchmark into a matrix sweep: define ranges for the tunable config params (channel buffer size, reading/bucket `BatchSizeBytes`, `BatchFlushTimeout`) and run the pipeline benchmark across the full permutation set, one real TimescaleDB run per combo, recording events/sec for each. Goal: find the actual throughput sweet spot by measurement, not a single before/after point like ticket 06.

Ticket 06 only swept one param (`readingStorageCfg.BatchSizeBytes`, 2 values). This generalizes that into N params x M values each, run automatically, results collected into one comparison table.

**Blocked by:** 06

**Status:** ready-for-agent

- [ ] Matrix runner defined: list of config dimensions and their value ranges, permutations generated (cartesian product)
- [ ] Each permutation run against a real pipeline + real TimescaleDB, events/sec recorded per combo
- [ ] Results collected into one table/report, sweet spot identified and called out
- [ ] Sweet-spot config values documented in `DESIGN.md` / ticket 06, replacing or supplementing the single before/after datapoint
