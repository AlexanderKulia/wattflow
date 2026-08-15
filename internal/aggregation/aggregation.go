package aggregation

import (
	"context"
	"log"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/AlexanderKulia/wattflow/internal/producer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	LatenessWindow time.Duration
	BucketSize     time.Duration
}

type bucketAccumulator struct {
	// milliwatt-hours. integer addition is commutative and associative, float addition is not
	total int64
	links []trace.Link
}

type deviceState struct {
	watermark    time.Time
	bucketTotals map[time.Time]*bucketAccumulator
}

type Bucket struct {
	DeviceID    string
	BucketStart time.Time
	KWh         float64
}

func (bucket *Bucket) PgSize() int {
	return len(bucket.DeviceID) + 16 // timestampz = 8 bytes, float8 = 8 bytes
}

func Run(ctx context.Context, cfg Config, in <-chan observability.Envelope[producer.Reading], out chan<- observability.Envelope[Bucket]) {
	defer close(out)

	devices := make(map[string]*deviceState)
	tracer := otel.Tracer("wattflow/aggregation")

	dropCounter, err := otel.Meter("wattflow/aggregation").Int64Counter(
		"aggregation.drops",
		metric.WithDescription("Readings dropped during aggregation because their bucket was already flushed"),
	)
	if err != nil {
		log.Fatalf("failed to create drop counter: %v", err)
	}

	for {
		var env observability.Envelope[producer.Reading]
		var open bool
		select {
		case <-ctx.Done():
			return
		case env, open = <-in:
			if !open {
				// input drained normally: flush whatever's left in every bucket
				for deviceID, state := range devices {
					for bucketStart, acc := range state.bucketTotals {
						if !emitBucket(ctx, tracer, out, deviceID, bucketStart, acc) {
							return
						}
					}
				}
				return
			}
		}

		reading := env.Data
		spanCtx, span := tracer.Start(env.Ctx, "aggregate")

		state, exists := devices[reading.DeviceID]
		if !exists {
			state = &deviceState{
				watermark:    reading.Timestamp,
				bucketTotals: make(map[time.Time]*bucketAccumulator),
			}
			devices[reading.DeviceID] = state
		}

		bucket := reading.Timestamp.Truncate(cfg.BucketSize)
		acc, exists := state.bucketTotals[bucket]
		if !exists {
			cutoffTimestamp := state.watermark.Add(-cfg.LatenessWindow)
			if !bucket.Add(cfg.BucketSize).After(cutoffTimestamp) {
				log.Printf("Reading for %s for DeviceID %s dropped: bucket %s already flushed", reading.Timestamp, reading.DeviceID, bucket)
				dropCounter.Add(spanCtx, 1, metric.WithAttributes(
					attribute.String("device_id", reading.DeviceID),
				))
				span.End()
				continue
			}
			acc = &bucketAccumulator{}
			state.bucketTotals[bucket] = acc
		}
		acc.total += int64(reading.KWh * 1_000_000)
		acc.links = append(acc.links, trace.LinkFromContext(spanCtx))

		if state.watermark.Before(reading.Timestamp) {
			state.watermark = reading.Timestamp
		}

		span.End()

		cutoffTimestamp := state.watermark.Add(-cfg.LatenessWindow)
		for bucketStart, acc := range state.bucketTotals {
			bucketEnd := bucketStart.Add(cfg.BucketSize)
			if !bucketEnd.After(cutoffTimestamp) {
				if !emitBucket(ctx, tracer, out, reading.DeviceID, bucketStart, acc) {
					return
				}
				delete(state.bucketTotals, bucketStart)
			}
		}
	}
}

// emitBucket sends the finished bucket downstream, reporting false if ctx is
// cancelled before the send completes so the caller can stop without blocking
// on a consumer that may already have exited.
func emitBucket(ctx context.Context, tracer trace.Tracer, out chan<- observability.Envelope[Bucket], deviceID string, bucketStart time.Time, acc *bucketAccumulator) bool {
	spanCtx, span := tracer.Start(context.Background(), "bucket", trace.WithLinks(acc.links...))
	defer span.End()

	bucket := Bucket{
		DeviceID:    deviceID,
		BucketStart: bucketStart,
		KWh:         float64(acc.total) / 1_000_000,
	}
	select {
	case <-ctx.Done():
		return false
	case out <- observability.Envelope[Bucket]{Data: bucket, Ctx: spanCtx}:
		return true
	}
}
