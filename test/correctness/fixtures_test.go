package correctness

import (
	"time"

	"github.com/AlexanderKulia/wattflow/internal/producer"
)

const (
	FixtureBucketSize     = time.Duration(15) * time.Minute
	FixtureLatenessWindow = time.Duration(30) * time.Minute
)

var fixtureBaseTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// Every reading's timestamp sits within FixtureLatenessWindow of its device's
// latest timestamp, so admit/drop outcomes can't flip under any delivery order.
// Late-drop behavior is deliberately out of scope here; it needs order held fixed.
func fixedReadings() []producer.Reading {
	return []producer.Reading{
		// device-a, bucket [00:00,00:15)
		{DeviceID: "device-a", Timestamp: fixtureBaseTime, ReadingID: "a-1", KWh: 1.5},
		{DeviceID: "device-a", Timestamp: fixtureBaseTime.Add(5 * time.Minute), ReadingID: "a-2", KWh: 2.25},
		{DeviceID: "device-a", Timestamp: fixtureBaseTime.Add(5 * time.Minute), ReadingID: "a-2", KWh: 2.25}, // exact duplicate of a-2
		{DeviceID: "device-a", Timestamp: fixtureBaseTime.Add(10 * time.Minute), ReadingID: "a-3", KWh: 3.75},

		// device-a, bucket [00:15,00:30)
		{DeviceID: "device-a", Timestamp: fixtureBaseTime.Add(16 * time.Minute), ReadingID: "a-4", KWh: 4.0},
		{DeviceID: "device-a", Timestamp: fixtureBaseTime.Add(20 * time.Minute), ReadingID: "a-5", KWh: 0.5},

		// device-b, bucket [00:00,00:15)
		{DeviceID: "device-b", Timestamp: fixtureBaseTime.Add(2 * time.Minute), ReadingID: "b-1", KWh: 10.0},
		{DeviceID: "device-b", Timestamp: fixtureBaseTime.Add(12 * time.Minute), ReadingID: "b-2", KWh: 5.25},

		// device-b, bucket [00:15,00:30)
		{DeviceID: "device-b", Timestamp: fixtureBaseTime.Add(18 * time.Minute), ReadingID: "b-3", KWh: 7.0},
		{DeviceID: "device-b", Timestamp: fixtureBaseTime.Add(18 * time.Minute), ReadingID: "b-3", KWh: 7.0}, // exact duplicate of b-3
	}
}
