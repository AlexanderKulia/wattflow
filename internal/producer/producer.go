package producer

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type Reading struct {
	DeviceID  string
	Timestamp time.Time
	ReadingID string
	KWh       float64
}

type Config struct {
	DeviceCount           int
	ReadingCountPerSecond int
	OutOfOrderProbability float32
	DuplicateProbability  float32
	DelayProbability      float32
	MaxDelaySeconds       int
	BurstSize             int
	BurstIntervalSeconds  int
	Duration              time.Duration
	Count                 int
}

func (cfg *Config) Validate() {
	if cfg.ReadingCountPerSecond == 0 {
		panic("Reading count per second cannot be zero")
	}

	if cfg.Duration != 0 && cfg.Count != 0 {
		panic("Set either duration of count, not both")
	}
}

func Run(cfg Config, out chan<- Reading) {
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
				break
			}
		} else if count <= 0 {
			break
		}

		timestamp := time.Now()
		if rand.Float32() <= cfg.OutOfOrderProbability {
			timestamp = timestamp.Add(-time.Duration(rand.Intn(10)+1) * time.Second)
		}

		if prevReading != nil && rand.Float32() <= cfg.DuplicateProbability {
			reading = *prevReading
		} else {
			reading = Reading{
				DeviceID:  devices[rand.Intn(len(devices))],
				Timestamp: timestamp,
				ReadingID: uuid.NewString(),
				KWh:       rand.Float64(),
			}
		}

		if rand.Float32() <= cfg.DelayProbability {
			time.Sleep(time.Duration(rand.Float32() * float32(cfg.MaxDelaySeconds) * float32(time.Second)))
		}
		out <- reading
		prevReading = &reading
		count--
		fmt.Println(reading)

		if cfg.BurstSize > 0 {
			sentInBurst++

			if sentInBurst >= cfg.BurstSize {
				time.Sleep(time.Duration(cfg.BurstIntervalSeconds) * time.Second)
				sentInBurst = 0
			}
		} else {
			time.Sleep(time.Second / time.Duration(cfg.ReadingCountPerSecond))
		}
	}

	close(out)
}
