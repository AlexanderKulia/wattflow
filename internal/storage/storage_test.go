package storage

import (
	"context"
	"testing"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/AlexanderKulia/wattflow/internal/producer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupTestDB starts a timescaledb container, runs migrations, and returns
// a connected pool. Container and pool are torn down via t.Cleanup.
func setupTestDB(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"timescale/timescaledb:latest-pg16",
		postgres.WithDatabase("test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start timescaledb container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Fatalf("failed to terminate container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	if err := Migrate(dsn); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	t.Cleanup(pool.Close)

	return ctx, pool
}

func TestFlushBucketsIsIdempotentOnRetry(t *testing.T) {
	ctx, pool := setupTestDB(t)

	bucket := aggregation.Bucket{
		DeviceID:    "11111111-1111-1111-1111-111111111111",
		BucketStart: time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC),
		KWh:         1.5,
	}
	batch := []aggregation.Bucket{bucket}

	// same batch flushed twice, simulating a retried/replayed write
	flushBuckets(ctx, pool, batch)
	flushBuckets(ctx, pool, batch)

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
	ctx, pool := setupTestDB(t)

	reading := producer.Reading{
		DeviceID:  "11111111-1111-1111-1111-111111111111",
		Timestamp: time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC),
		ReadingID: "22222222-2222-2222-2222-222222222222",
		KWh:       1.5,
	}
	batch := []producer.Reading{reading}

	// same batch flushed twice, simulating a retried/replayed write
	flushReadings(ctx, pool, batch)
	flushReadings(ctx, pool, batch)

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
