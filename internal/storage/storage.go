package storage

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/AlexanderKulia/wattflow/internal/producer"
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	DSN               string
	BatchSizeBytes    int
	BatchFlushTimeout time.Duration
}

func flushBuckets(ctx context.Context, pool *pgxpool.Pool, buf []aggregation.Bucket) {
	if len(buf) == 0 {
		return
	}

	query := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).
		Insert("bucket_totals").
		Columns("device_id", "bucket", "kwh").
		Suffix("ON CONFLICT (device_id, bucket) DO UPDATE SET kwh = EXCLUDED.kwh")

	for _, bucket := range buf {
		query = query.Values(bucket.DeviceID, bucket.BucketStart, bucket.KWh)
	}

	sqlStr, args, err := query.ToSql()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build query: %v\n", err)
		return
	}

	if _, err := pool.Exec(ctx, sqlStr, args...); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write batch: %v\n", err)
	}
}

func flushReadings(ctx context.Context, pool *pgxpool.Pool, buf []producer.Reading) {
	if len(buf) == 0 {
		return
	}

	query := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Insert("readings").Columns("device_id", "reading_id", "dedup_key", "timestamp", "kwh").Suffix("ON CONFLICT (dedup_key, timestamp) DO NOTHING")

	for _, reading := range buf {
		var readingID any
		if reading.ReadingID != "" {
			readingID = reading.ReadingID
		}
		query = query.Values(reading.DeviceID, readingID, reading.DedupKey(), reading.Timestamp, reading.KWh)
	}

	sqlStr, args, err := query.ToSql()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build query: %v\n", err)
		return
	}

	if _, err := pool.Exec(ctx, sqlStr, args...); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write batch: %v\n", err)
	}
}

func RunBuckets(cfg Config, in <-chan aggregation.Bucket) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	buf := make([]aggregation.Bucket, 0, cfg.BatchSizeBytes/32)
	var bufBytes int
	timer := time.NewTimer(cfg.BatchFlushTimeout)
	defer timer.Stop()

	flushAndReset := func() {
		flushBuckets(ctx, pool, buf)
		buf = buf[:0]
		bufBytes = 0
	}

	for {
		select {
		case bucket, ok := <-in:
			if !ok {
				// channel closed
				flushBuckets(ctx, pool, buf)
				return
			}
			buf = append(buf, bucket)
			bufBytes += bucket.PgSize()
			if bufBytes >= cfg.BatchSizeBytes {
				flushAndReset()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(cfg.BatchFlushTimeout)
			}
		case <-timer.C:
			if len(buf) > 0 {
				flushAndReset()
			}
			timer.Reset(cfg.BatchFlushTimeout)
		}
	}
}

func RunReadings(cfg Config, in <-chan producer.Reading) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	buf := make([]producer.Reading, 0, cfg.BatchSizeBytes/32)
	var bufBytes int
	timer := time.NewTimer(cfg.BatchFlushTimeout)
	defer timer.Stop()

	flushAndReset := func() {
		flushReadings(ctx, pool, buf)
		buf = buf[:0]
		bufBytes = 0
	}

	for {
		select {
		case reading, ok := <-in:
			if !ok {
				// channel closed
				flushReadings(ctx, pool, buf)
				return
			}
			buf = append(buf, reading)
			bufBytes += reading.PgSize()
			if bufBytes >= cfg.BatchSizeBytes {
				flushAndReset()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(cfg.BatchFlushTimeout)
			}
		case <-timer.C:
			if len(buf) > 0 {
				flushAndReset()
			}
			timer.Reset(cfg.BatchFlushTimeout)
		}
	}
}
