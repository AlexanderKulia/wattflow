# Watermark tracked per device, not globally

Ingestion keeps one watermark per `device_id` rather than a single pipeline-wide watermark. A global watermark would let one delayed or bursty device stall lateness-window closure — and therefore bucket finalization — for every other device. Per-device watermarks isolate each device's out-of-order tolerance and align with the aggregation key (`device_id` + interval): a bucket only closes once its own device's watermark has passed it. Cost is a small map of watermarks instead of one value, not a real scaling concern at this project's size.
