package storage

import (
	"context"
	"testing"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestFlushIsIdempotentOnRetry(t *testing.T) {
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

	bucket := aggregation.Bucket{
		DeviceID:    "device-1",
		BucketStart: time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC),
		KWh:         1.5,
	}
	batch := []aggregation.Bucket{bucket}

	// same batch flushed twice, simulating a retried/replayed write
	flush(ctx, pool, batch)
	flush(ctx, pool, batch)

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
