# Wattflow

Simulates an energy telemetry ingestion pipeline: correctness under out-of-order/duplicate/bursty delivery, deliberate backpressure, and per-stage observability.

## Language

**Reading**:
A single meter measurement emitted by the producer: `device_id`, `timestamp`, `reading_id`, `kWh`. The unit of data flowing through every pipeline stage.
_Avoid_: Event, record, data point

**Reading ID**:
The identifier attached to a Reading, not assumed globally unique across devices. The actual dedup key is always the tuple `(device_id, Reading ID)`. Falls back to a content hash when absent/unreliable.
_Avoid_: Event ID, dedup key (dedup key is the mechanism; Reading ID is one half of the value used for it)

**Watermark**:
Per-device marker of how far event-time delivery is considered complete, tracked independently for each `device_id`. Readings timestamped before that device's watermark minus the lateness window are treated as unrecoverably late.
_Avoid_: Cutoff, checkpoint, global watermark

**Lateness window**:
The bounded, configurable duration past the watermark during which a late Reading is still admitted. Readings arriving after it are logged and counted dropped as `late`, never silently discarded. Lateness is checked before dedup, so a Reading that's both late and a duplicate is classified `late` — dedup state may have already evicted the entry anyway.
_Avoid_: Grace period, delay tolerance

**Bucket**:
A fixed, calendar-aligned time interval (e.g. `10:30–10:45`) that aggregation sums a device's kWh into. A Reading's bucket is a pure function of its timestamp, independent of arrival order.
_Avoid_: Interval, window (window is used for lateness, not aggregation)

**Dedup state**:
The store tracking which Reading IDs have already been ingested, scoped to the lateness window — a Reading ID older than the window can never legitimately reappear, so it's safe to evict.
_Avoid_: Dedup cache, seen-set
