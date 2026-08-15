# Block (not drop or spill) as the backpressure policy

When a downstream stage falls behind, upstream stages block on the bounded channel send rather than dropping events or spilling to disk. The project's non-negotiable invariant is a bitwise-identical aggregate for a fixed input set — dropping events under load changes the input set and breaks that guarantee. Spill-to-disk solves a durability/crash-recovery problem out of scope for a single-process v1. Blocking is the only policy that never sacrifices correctness for throughput; the resulting throughput ceiling is exactly what the load-testing requirement is meant to measure and report, not avoid.

Lateness-window drops (events arriving after the allowed-lateness watermark) are a separate, unrelated mechanism — those are deliberately and visibly discarded by design, not a backpressure response.
