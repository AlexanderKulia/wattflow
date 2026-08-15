package storage

import (
	"testing"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/AlexanderKulia/wattflow/internal/producer"
	"github.com/AlexanderKulia/wattflow/internal/testutil"
	"go.opentelemetry.io/otel"
)

func TestFlushBucketsIsIdempotentOnRetry(t *testing.T) {
	ctx, pool, _ := testutil.SetupTestDB(t, Migrate)

	bucket := aggregation.Bucket{
		DeviceID:    "11111111-1111-1111-1111-111111111111",
		BucketStart: time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC),
		KWh:         1.5,
	}
	batch := []observability.Envelope[aggregation.Bucket]{{Data: bucket, Ctx: ctx}}
	tracer := otel.Tracer("test")

	// same batch flushed twice, simulating a retried/replayed write
	flushBuckets(ctx, tracer, pool, batch)
	flushBuckets(ctx, tracer, pool, batch)

	var gotKWh float64
	row := pool.QueryRow(ctx,
		"SELECT kwh FROM bucket_totals WHERE device_id = $1 AND bucket = $2",
		bucket.DeviceID, bucket.BucketStart,
	)
	if err := row.Scan(&gotKWh); err != nil {
		t.Fatalf("failed to query persisted bucket: %v", err)
	}

	if gotKWh != bucket.KWh {
		t.Errorf("persisted kwh = %v after retried write, want %v (no double-counting)", gotKWh, bucket.KWh)
	}
}

func TestFlushReadingsIsIdempotentOnRetry(t *testing.T) {
	ctx, pool, _ := testutil.SetupTestDB(t, Migrate)

	reading := producer.Reading{
		DeviceID:  "11111111-1111-1111-1111-111111111111",
		Timestamp: time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC),
		ReadingID: "22222222-2222-2222-2222-222222222222",
		KWh:       1.5,
	}
	batch := []observability.Envelope[producer.Reading]{{Data: reading, Ctx: ctx}}
	tracer := otel.Tracer("test")

	// same batch flushed twice, simulating a retried/replayed write
	flushReadings(ctx, tracer, pool, batch)
	flushReadings(ctx, tracer, pool, batch)

	var count int
	row := pool.QueryRow(ctx,
		"SELECT count(*) FROM readings WHERE dedup_key = $1 AND timestamp = $2",
		reading.DedupKey(), reading.Timestamp,
	)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("failed to query persisted reading: %v", err)
	}

	if count != 1 {
		t.Errorf("persisted reading count = %d after retried write, want 1 (no duplicate row)", count)
	}
}

func TestContinuousAggregateMatchesAppBucket(t *testing.T) {
	ctx, pool, _ := testutil.SetupTestDB(t, Migrate)
	tracer := otel.Tracer("test")

	deviceID := "11111111-1111-1111-1111-111111111111"
	bucketStart := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)

	readings := []observability.Envelope[producer.Reading]{
		{Ctx: ctx, Data: producer.Reading{
			DeviceID: deviceID, ReadingID: "22222222-2222-2222-2222-222222222222",
			Timestamp: bucketStart, KWh: 0.5,
		}},
		{Ctx: ctx, Data: producer.Reading{
			DeviceID: deviceID, ReadingID: "33333333-3333-3333-3333-333333333333",
			Timestamp: bucketStart.Add(10 * time.Minute), KWh: 1.0,
		}},
	}
	flushReadings(ctx, tracer, pool, readings)

	appBucket := aggregation.Bucket{DeviceID: deviceID, BucketStart: bucketStart, KWh: 1.5}
	flushBuckets(ctx, tracer, pool, []observability.Envelope[aggregation.Bucket]{{Ctx: ctx, Data: appBucket}})

	// Continuous aggregate is WITH NO DATA (Timescale can't materialize inside
	// the migration transaction); refresh once so the spot-check sees the rows.
	if _, err := pool.Exec(ctx, "CALL refresh_continuous_aggregate('reading_bucket_totals', NULL, NULL)"); err != nil {
		t.Fatalf("failed to refresh continuous aggregate: %v", err)
	}

	var caKWh float64
	row := pool.QueryRow(ctx,
		"SELECT kwh FROM reading_bucket_totals WHERE device_id = $1 AND bucket = $2",
		deviceID, bucketStart,
	)
	if err := row.Scan(&caKWh); err != nil {
		t.Fatalf("failed to query continuous aggregate: %v", err)
	}

	if caKWh != appBucket.KWh {
		t.Errorf("continuous aggregate kwh = %v, want %v (app-computed bucket)", caKWh, appBucket.KWh)
	}
}
