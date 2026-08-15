package ingestion

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/AlexanderKulia/wattflow/internal/producer"
)

var testBaseTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// runIngestion feeds readings through Run, in the given order, and returns
// every reading forwarded to aggOut (storageOut receives the same readings).
func runIngestion(cfg Config, readings []producer.Reading) []producer.Reading {
	in := make(chan observability.Envelope[producer.Reading])
	aggOut := make(chan observability.Envelope[producer.Reading])
	storageOut := make(chan observability.Envelope[producer.Reading])

	go func() {
		for _, r := range readings {
			in <- observability.Envelope[producer.Reading]{Data: r, Ctx: context.Background()}
		}
		close(in)
	}()

	go func() {
		for range storageOut {
		}
	}()

	var forwarded []producer.Reading
	done := make(chan struct{})
	go func() {
		for env := range aggOut {
			forwarded = append(forwarded, env.Data)
		}
		close(done)
	}()

	Run(context.Background(), cfg, in, aggOut, storageOut)
	<-done

	return forwarded
}

func TestRunDropsReadingOlderThanLatenessWindow(t *testing.T) {
	cfg := Config{LatenessWindow: 10 * time.Minute}
	readings := []producer.Reading{
		{DeviceID: "d1", Timestamp: testBaseTime.Add(20 * time.Minute), ReadingID: "r1", KWh: 1.0},
		{DeviceID: "d1", Timestamp: testBaseTime.Add(5 * time.Minute), ReadingID: "r2", KWh: 2.0}, // before watermark(20m) - window(10m) = 10m cutoff
	}

	got := runIngestion(cfg, readings)
	want := readings[:1]

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestRunDropsDuplicateWithinLatenessWindow(t *testing.T) {
	cfg := Config{LatenessWindow: 10 * time.Minute}
	readings := []producer.Reading{
		{DeviceID: "d1", Timestamp: testBaseTime, ReadingID: "r1", KWh: 1.0},
		{DeviceID: "d1", Timestamp: testBaseTime.Add(1 * time.Minute), ReadingID: "r1", KWh: 1.0}, // same DedupKey, still within window
	}

	got := runIngestion(cfg, readings)
	want := readings[:1]

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestRunReadmitsKeyEvictedFromWindow(t *testing.T) {
	cfg := Config{LatenessWindow: 10 * time.Minute}
	r1 := producer.Reading{DeviceID: "d1", Timestamp: testBaseTime, ReadingID: "r1", KWh: 1.0}
	r2 := producer.Reading{DeviceID: "d1", Timestamp: testBaseTime.Add(20 * time.Minute), ReadingID: "r2", KWh: 2.0} // advances watermark to 20m, cutoff 10m, evicts r1
	r3 := producer.Reading{DeviceID: "d1", Timestamp: testBaseTime.Add(25 * time.Minute), ReadingID: "r1", KWh: 3.0} // same key as r1, but r1 was evicted -> not a duplicate

	got := runIngestion(cfg, []producer.Reading{r1, r2, r3})
	want := []producer.Reading{r1, r2, r3}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestRunDedupsFallbackKeyOnMissingReadingID(t *testing.T) {
	cfg := Config{LatenessWindow: 10 * time.Minute}
	r1 := producer.Reading{DeviceID: "d1", Timestamp: testBaseTime, ReadingID: "", KWh: 5.0}
	r2 := producer.Reading{DeviceID: "d1", Timestamp: testBaseTime, ReadingID: "", KWh: 5.0} // identical device+timestamp+kWh -> same fallback key, duplicate
	r3 := producer.Reading{DeviceID: "d1", Timestamp: testBaseTime, ReadingID: "", KWh: 6.0} // different kWh -> different fallback key, admitted

	got := runIngestion(cfg, []producer.Reading{r1, r2, r3})
	want := []producer.Reading{r1, r3}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
