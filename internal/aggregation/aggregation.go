package aggregation

import (
	"time"

	"github.com/AlexanderKulia/wattflow/internal/producer"
)

type Config struct {
	LatenessWindow time.Duration
	BucketSize     time.Duration
}

type deviceState struct {
	watermark time.Time
	// milliwatt-hours. integer addition is commutative and associative, float addition is not
	bucketTotals map[time.Time]int64
}

type Bucket struct {
	DeviceID    string
	BucketStart time.Time
	KWh         float64
}

func Run(cfg Config, in <-chan producer.Reading, out chan<- Bucket) {
	devices := make(map[string]*deviceState)

	for reading := range in {
		state, ok := devices[reading.DeviceID]
		if !ok {
			state = &deviceState{
				watermark:    reading.Timestamp,
				bucketTotals: make(map[time.Time]int64),
			}
			devices[reading.DeviceID] = state
		}

		bucket := reading.Timestamp.Truncate(cfg.BucketSize)
		state.bucketTotals[bucket] += int64(reading.KWh * 1_000_000)

		if state.watermark.Before(reading.Timestamp) {
			state.watermark = reading.Timestamp
		}

		cutoffTimestamp := state.watermark.Add(-cfg.LatenessWindow)
		for bucketStart, total := range state.bucketTotals {
			bucketEnd := bucketStart.Add(cfg.BucketSize)
			if !bucketEnd.After(cutoffTimestamp) {
				bucket := Bucket{
					DeviceID:    reading.DeviceID,
					BucketStart: bucketStart,
					KWh:         float64(total) / 1_000_000,
				}
				out <- bucket
				delete(state.bucketTotals, bucketStart)
			}
		}
	}

	// flush after in channel closes
	for key, state := range devices {
		for bucketStart, total := range state.bucketTotals {
			bucket := Bucket{
				DeviceID:    key,
				BucketStart: bucketStart,
				KWh:         float64(total) / 1_000_000,
			}
			out <- bucket
		}
	}
	close(out)
}
