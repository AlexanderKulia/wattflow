CREATE TABLE bucket_totals (
    device_id UUID NOT NULL,
    bucket    TIMESTAMPTZ NOT NULL,
    kwh       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (device_id, bucket)
);

SELECT create_hypertable('bucket_totals', by_range('bucket'));

CREATE TABLE readings (
    device_id  UUID NOT NULL,
    reading_id UUID,
    dedup_key  TEXT NOT NULL,
    timestamp  TIMESTAMPTZ NOT NULL,
    kwh        DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (dedup_key, timestamp)
);

SELECT create_hypertable('readings', by_range('timestamp'));

-- Read-path comparison against the app-computed aggregate (ADR-0008), not a
-- correctness source. Interval must match aggregation.Config.BucketSize.
-- WITH NO DATA: Timescale can't populate a continuous aggregate inside a
-- transaction (each migration file runs in one). Real-time aggregation
-- (default: materialized_only=false) computes unmaterialized rows live at
-- query time, so no manual refresh is needed for correctness.
CREATE MATERIALIZED VIEW reading_bucket_totals
WITH (timescaledb.continuous) AS
SELECT device_id,
       time_bucket('15 minutes', timestamp) AS bucket,
       sum(kwh) AS kwh
FROM readings
GROUP BY device_id, bucket
WITH NO DATA;
