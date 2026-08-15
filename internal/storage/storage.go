package storage

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/AlexanderKulia/wattflow/internal/producer"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	DSN               string
	BatchSizeBytes    int
	BatchFlushTimeout time.Duration
}

// shutdownFlushTimeout bounds the final best-effort flush issued when ctx is
// cancelled, so a stalled database can't hang process shutdown indefinitely.
const shutdownFlushTimeout = 5 * time.Second

func linksFromEnvelopes[T any](buf []observability.Envelope[T]) []trace.Link {
	links := make([]trace.Link, 0, len(buf))
	for _, env := range buf {
		links = append(links, trace.LinkFromContext(env.Ctx))
	}
	return links
}

func flushBuckets(ctx context.Context, tracer trace.Tracer, pool *pgxpool.Pool, buf []observability.Envelope[aggregation.Bucket]) {
	if len(buf) == 0 {
		return
	}

	ctx, span := tracer.Start(ctx, "flush_buckets", trace.WithLinks(linksFromEnvelopes(buf)...))
	defer span.End()

	var sqlStr strings.Builder
	sqlStr.WriteString("INSERT INTO bucket_totals (device_id, bucket, kwh) VALUES ")
	args := make([]any, 0, len(buf)*3)

	for i, env := range buf {
		if i > 0 {
			sqlStr.WriteByte(',')
		}
		n := i * 3
		fmt.Fprintf(&sqlStr, "($%d,$%d,$%d)", n+1, n+2, n+3)

		bucket := env.Data
		args = append(args,
			pgtype.UUID{Bytes: [16]byte(uuid.MustParse(bucket.DeviceID)), Valid: true},
			bucket.BucketStart,
			bucket.KWh,
		)
	}
	sqlStr.WriteString(" ON CONFLICT (device_id, bucket) DO UPDATE SET kwh = EXCLUDED.kwh")

	if _, err := pool.Exec(ctx, sqlStr.String(), args...); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write batch: %v\n", err)
	}
}

func flushReadings(ctx context.Context, tracer trace.Tracer, pool *pgxpool.Pool, buf []observability.Envelope[producer.Reading]) {
	if len(buf) == 0 {
		return
	}

	ctx, span := tracer.Start(ctx, "flush_readings", trace.WithLinks(linksFromEnvelopes(buf)...))
	defer span.End()

	var sqlStr strings.Builder
	sqlStr.WriteString("INSERT INTO readings (device_id, reading_id, dedup_key, timestamp, kwh) VALUES ")
	args := make([]any, 0, len(buf)*5)

	for i, env := range buf {
		if i > 0 {
			sqlStr.WriteByte(',')
		}
		n := i * 5
		fmt.Fprintf(&sqlStr, "($%d,$%d,$%d,$%d,$%d)", n+1, n+2, n+3, n+4, n+5)

		reading := env.Data
		var readingID pgtype.UUID
		if reading.ReadingID != "" {
			readingID = pgtype.UUID{Bytes: [16]byte(uuid.MustParse(reading.ReadingID)), Valid: true}
		}
		args = append(args,
			pgtype.UUID{Bytes: [16]byte(uuid.MustParse(reading.DeviceID)), Valid: true},
			readingID,
			reading.DedupKey(),
			reading.Timestamp,
			reading.KWh,
		)
	}
	sqlStr.WriteString(" ON CONFLICT (dedup_key, timestamp) DO NOTHING")

	if _, err := pool.Exec(ctx, sqlStr.String(), args...); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write batch: %v\n", err)
	}
}

func RunBuckets(ctx context.Context, cfg Config, in <-chan observability.Envelope[aggregation.Bucket]) {
	pool, err := pgxpool.New(context.Background(), cfg.DSN)
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
		flushBuckets(context.Background(), tracer, pool, buf)
		buf = buf[:0]
		bufBytes = 0
	}

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
			flushBuckets(shutdownCtx, tracer, pool, buf)
			cancel()
			return
		case env, ok := <-in:
			if !ok {
				// channel closed
				flushBuckets(context.Background(), tracer, pool, buf)
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

func RunReadings(ctx context.Context, cfg Config, in <-chan observability.Envelope[producer.Reading]) {
	pool, err := pgxpool.New(context.Background(), cfg.DSN)
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
		flushReadings(context.Background(), tracer, pool, buf)
		buf = buf[:0]
		bufBytes = 0
	}

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
			flushReadings(shutdownCtx, tracer, pool, buf)
			cancel()
			return
		case env, ok := <-in:
			if !ok {
				// channel closed
				flushReadings(context.Background(), tracer, pool, buf)
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
