package aggregation

import (
	"fmt"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/producer"
)

func Run(in <-chan producer.Reading) {
	totals := make(map[string]float64)

	for reading := range in {
		bucket := reading.Timestamp.Truncate(15 * time.Minute)
		key := reading.DeviceID + "|" + bucket.String()
		totals[key] += reading.KWh

	}

	for key, kwh := range totals {
		fmt.Println(key, kwh)
	}
}
