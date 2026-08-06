# In-process channel for producer → ingestion, not HTTP/gRPC

Producer feeds ingestion via a Go channel, not a network endpoint. SPEC.md explicitly deferred this choice and rules out multi-node as a v1 requirement — the project's hard problems (dedup, watermark/lateness, backpressure, correctness under replay) all live downstream of this boundary, not in it. A channel also makes the replay test harness direct: feed events straight in, no server to stand up. HTTP/gRPC stays a possible stretch-goal wrapper, not the core boundary.
