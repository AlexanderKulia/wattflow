package storage

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/AlexanderKulia/wattflow/internal/producer"
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	DSN               string
	BatchSizeBytes    int
	BatchFlushTimeout time.Duration
}

func linksFromEnvelopes[T any](buf []observability.Envelope[T]) []trace.Link {
	links := make([]trace.Link, 0, len(buf))
	for _, env := range buf {
		links = append(links, trace.LinkFromContext(env.Ctx))
	}
	return links
}

func flushBuckets(tracer trace.Tracer, pool *pgxpool.Pool, buf []observability.Envelope[aggregation.Bucket]) {
	if len(buf) == 0 {
		return
	}

	ctx, span := tracer.Start(context.Background(), "flush_buckets", trace.WithLinks(linksFromEnvelopes(buf)...))
	defer span.End()

	query := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).
		Insert("bucket_totals").
		Columns("device_id", "bucket", "kwh").
		Suffix("ON CONFLICT (device_id, bucket) DO UPDATE SET kwh = EXCLUDED.kwh")

	for _, env := range buf {
		bucket := env.Data
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

func flushReadings(tracer trace.Tracer, pool *pgxpool.Pool, buf []observability.Envelope[producer.Reading]) {
	if len(buf) == 0 {
		return
	}

	ctx, span := tracer.Start(context.Background(), "flush_readings", trace.WithLinks(linksFromEnvelopes(buf)...))
	defer span.End()

	query := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Insert("readings").Columns("device_id", "reading_id", "dedup_key", "timestamp", "kwh").Suffix("ON CONFLICT (dedup_key, timestamp) DO NOTHING")

	for _, env := range buf {
		reading := env.Data
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

func RunBuckets(cfg Config, in <-chan observability.Envelope[aggregation.Bucket]) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	tracer := otel.Tracer("wattflow/storage")
	buf := make([]observability.Envelope[aggregation.Bucket], 0, cfg.BatchSizeBytes/32)
	var bufBytes int
	timer := time.NewTimer(cfg.BatchFlushTimeout)
	defer timer.Stop()

	flushAndReset := func() {
		flushBuckets(tracer, pool, buf)
		buf = buf[:0]
		bufBytes = 0
	}

	for {
		select {
		case env, ok := <-in:
			if !ok {
				// channel closed
				flushBuckets(tracer, pool, buf)
				return
			}
			buf = append(buf, env)
			bufBytes += env.Data.PgSize()
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

func RunReadings(cfg Config, in <-chan observability.Envelope[producer.Reading]) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	tracer := otel.Tracer("wattflow/storage")
	buf := make([]observability.Envelope[producer.Reading], 0, cfg.BatchSizeBytes/32)
	var bufBytes int
	timer := time.NewTimer(cfg.BatchFlushTimeout)
	defer timer.Stop()

	flushAndReset := func() {
		flushReadings(tracer, pool, buf)
		buf = buf[:0]
		bufBytes = 0
	}

	for {
		select {
		case env, ok := <-in:
			if !ok {
				// channel closed
				flushReadings(tracer, pool, buf)
				return
			}
			buf = append(buf, env)
			bufBytes += env.Data.PgSize()
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
