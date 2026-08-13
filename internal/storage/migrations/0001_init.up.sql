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
