package producer

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
)

type Reading struct {
	DeviceID  string
	Timestamp time.Time
	ReadingID string
	KWh       float64
}

func (r Reading) PgSize() int {
	return len(r.DeviceID) + len(r.ReadingID) + 16 // timestampz = 8 bytes, float8 = 8 bytes
}

func (r Reading) DedupKey() string {
	if len(r.ReadingID) == 0 {
		return r.DeviceID + "|" + r.Timestamp.String() + "|" + strconv.FormatFloat(r.KWh, 'e', -1, 64)
	} else {
		return r.DeviceID + "|" + r.ReadingID
	}
}

type Config struct {
	DeviceCount                    int
	ReadingCountPerSecond          int
	OutOfOrderProbability          float32
	DuplicateProbability           float32
	UnreliableReadingIDProbability float32
	DelayProbability               float32
	MaxDelay                       time.Duration
	BurstSize                      int
	BurstInterval                  time.Duration
	Duration                       time.Duration
	Count                          int
}

func (cfg *Config) Validate() {
	if cfg.ReadingCountPerSecond == 0 {
		panic("Reading count per second cannot be zero")
	}

	if cfg.Duration != 0 && cfg.Count != 0 {
		panic("Set either duration of count, not both")
	}
}

func Run(ctx context.Context, cfg Config, out chan<- observability.Envelope[Reading]) {
	defer close(out)

	devices := make([]string, cfg.DeviceCount)
	for i := range devices {
		devices[i] = uuid.NewString()
	}

	count := cfg.Count
	start := time.Now()
	var prevReading *Reading
	var reading Reading
	sentInBurst := 0

	for {
		if cfg.Duration > 0 {
			if time.Since(start) >= cfg.Duration {
				return
			}
		} else if count <= 0 {
			return
		}

		if prevReading != nil && rand.Float32() <= cfg.DuplicateProbability {
			reading = *prevReading
		} else {
			var timestamp time.Time
			if rand.Float32() <= cfg.OutOfOrderProbability {
				timestamp = time.Now().Add(-time.Duration(rand.Intn(10)+1) * time.Second)
			} else {
				timestamp = time.Now()
			}

			var readingID string
			if rand.Float32() <= cfg.UnreliableReadingIDProbability {
				readingID = ""
			} else {
				readingID = uuid.NewString()
			}

			reading = Reading{
				DeviceID:  devices[rand.Intn(len(devices))],
				Timestamp: timestamp,
				ReadingID: readingID,
				KWh:       rand.Float64() * 100,
			}
		}

		if rand.Float32() <= cfg.DelayProbability {
			if !sleepOrDone(ctx, time.Duration(rand.Float32()*float32(cfg.MaxDelay))) {
				return
			}
		}
		spanCtx, span := otel.Tracer("wattflow/producer").Start(context.Background(), "produce")
		span.End()
		select {
		case <-ctx.Done():
			return
		case out <- observability.Envelope[Reading]{Data: reading, Ctx: spanCtx}:
		}
		prevReading = &reading
		count--

		if cfg.BurstSize > 0 {
			sentInBurst++

			if sentInBurst >= cfg.BurstSize {
				if !sleepOrDone(ctx, cfg.BurstInterval) {
					return
				}
				sentInBurst = 0
			}
		} else {
			if !sleepOrDone(ctx, time.Second/time.Duration(cfg.ReadingCountPerSecond)) {
				return
			}
		}
	}
}

// sleepOrDone waits for d, reporting false if ctx is cancelled first.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
