package producer

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
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
			time.Sleep(time.Duration(rand.Float32() * float32(cfg.MaxDelay)))
		}
		out <- reading
		prevReading = &reading
		count--
		fmt.Println(reading)

		if cfg.BurstSize > 0 {
			sentInBurst++

			if sentInBurst >= cfg.BurstSize {
				time.Sleep(cfg.BurstInterval)
				sentInBurst = 0
			}
		} else {
			time.Sleep(time.Second / time.Duration(cfg.ReadingCountPerSecond))
		}
	}

	close(out)
}
