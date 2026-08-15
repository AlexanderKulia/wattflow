// Package testutil holds fixtures shared across the module's tests and
// benchmarks (both satisfy testing.TB).
package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// SetupTestDB starts a timescaledb container, runs migrate against it, and
// returns a connected pool. Container and pool are torn down via
// tb.Cleanup. migrate is injected so this package need not depend on
// internal/storage (avoids an import cycle for storage's own tests).
func SetupTestDB(tb testing.TB, migrate func(dsn string) error) (context.Context, *pgxpool.Pool, string) {
	tb.Helper()
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
		tb.Fatalf("failed to start timescaledb container: %v", err)
	}
	tb.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			tb.Fatalf("failed to terminate container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		tb.Fatalf("failed to get connection string: %v", err)
	}

	if err := migrate(dsn); err != nil {
		tb.Fatalf("failed to run migrations: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		tb.Fatalf("failed to connect to database: %v", err)
	}
	tb.Cleanup(pool.Close)

	return ctx, pool, dsn
}
