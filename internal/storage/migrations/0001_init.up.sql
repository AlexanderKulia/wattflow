CREATE TABLE bucket_totals (
    device_id TEXT NOT NULL,
    bucket    TIMESTAMPTZ NOT NULL,
    kwh       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (device_id, bucket)
);

SELECT create_hypertable('bucket_totals', by_range('bucket'));
