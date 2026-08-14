package aggregation

import (
	"context"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/AlexanderKulia/wattflow/internal/producer"
	"go.opentelemetry.io/otel"
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

func Run(cfg Config, in <-chan observability.Envelope[producer.Reading], out chan<- observability.Envelope[Bucket]) {
	devices := make(map[string]*deviceState)
	tracer := otel.Tracer("wattflow/aggregation")

	for env := range in {
		reading := env.Data
		ctx, span := tracer.Start(env.Ctx, "aggregate")

		state, ok := devices[reading.DeviceID]
		if !ok {
			state = &deviceState{
				watermark:    reading.Timestamp,
				bucketTotals: make(map[time.Time]*bucketAccumulator),
			}
			devices[reading.DeviceID] = state
		}

		bucket := reading.Timestamp.Truncate(cfg.BucketSize)
		acc, ok := state.bucketTotals[bucket]
		if !ok {
			acc = &bucketAccumulator{}
			state.bucketTotals[bucket] = acc
		}
		acc.total += int64(reading.KWh * 1_000_000)
		acc.links = append(acc.links, trace.LinkFromContext(ctx))

		if state.watermark.Before(reading.Timestamp) {
			state.watermark = reading.Timestamp
		}

		span.End()

		cutoffTimestamp := state.watermark.Add(-cfg.LatenessWindow)
		for bucketStart, acc := range state.bucketTotals {
			bucketEnd := bucketStart.Add(cfg.BucketSize)
			if !bucketEnd.After(cutoffTimestamp) {
				emitBucket(tracer, out, reading.DeviceID, bucketStart, acc)
				delete(state.bucketTotals, bucketStart)
			}
		}
	}

	// flush after in channel closes
	for key, state := range devices {
		for bucketStart, acc := range state.bucketTotals {
			emitBucket(tracer, out, key, bucketStart, acc)
		}
	}
	close(out)
}

func emitBucket(tracer trace.Tracer, out chan<- observability.Envelope[Bucket], deviceID string, bucketStart time.Time, acc *bucketAccumulator) {
	ctx, span := tracer.Start(context.Background(), "bucket", trace.WithLinks(acc.links...))
	defer span.End()

	bucket := Bucket{
		DeviceID:    deviceID,
		BucketStart: bucketStart,
		KWh:         float64(acc.total) / 1_000_000,
	}
	out <- observability.Envelope[Bucket]{Data: bucket, Ctx: ctx}
}
