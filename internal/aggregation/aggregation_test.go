package aggregation

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/AlexanderKulia/wattflow/internal/producer"
)

var testBaseTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// runAggregation feeds readings through Run, in the given order, and returns
// every Bucket emitted on out.
func runAggregation(cfg Config, readings []producer.Reading) []Bucket {
	in := make(chan observability.Envelope[producer.Reading])
	out := make(chan observability.Envelope[Bucket])

	go func() {
		for _, r := range readings {
			in <- observability.Envelope[producer.Reading]{Data: r, Ctx: context.Background()}
		}
		close(in)
	}()

	var got []Bucket
	done := make(chan struct{})
	go func() {
		for env := range out {
			got = append(got, env.Data)
		}
		close(done)
	}()

	Run(context.Background(), cfg, in, out)
	<-done

	return got
}

type bucketKey struct {
	deviceID    string
	bucketStart time.Time
}

func toBucketSet(buckets []Bucket) map[bucketKey]float64 {
	set := make(map[bucketKey]float64, len(buckets))
	for _, b := range buckets {
		set[bucketKey{b.DeviceID, b.BucketStart}] = b.KWh
	}
	return set
}

func TestRunSumsReadingsInSameBucket(t *testing.T) {
	cfg := Config{BucketSize: 15 * time.Minute, LatenessWindow: 15 * time.Minute}
	readings := []producer.Reading{
		{DeviceID: "d1", Timestamp: testBaseTime, ReadingID: "r1", KWh: 0.1},
		{DeviceID: "d1", Timestamp: testBaseTime.Add(time.Minute), ReadingID: "r2", KWh: 0.2},
	}

	// naive float64 addition gives 0.30000000000000004; summing via the
	// int64 milliwatt-hour accumulator avoids that.
	got := runAggregation(cfg, readings)
	want := []Bucket{{DeviceID: "d1", BucketStart: testBaseTime, KWh: 0.3}}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestRunFlushesBucketNotYetPastLatenessCutoffOnClose(t *testing.T) {
	cfg := Config{BucketSize: 15 * time.Minute, LatenessWindow: time.Hour}
	readings := []producer.Reading{
		{DeviceID: "d1", Timestamp: testBaseTime, ReadingID: "r1", KWh: 5.0},
	}

	got := runAggregation(cfg, readings)
	want := []Bucket{{DeviceID: "d1", BucketStart: testBaseTime, KWh: 5.0}}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestRunTracksWatermarksPerDeviceIndependently(t *testing.T) {
	cfg := Config{BucketSize: 15 * time.Minute, LatenessWindow: 5 * time.Minute}
	readings := []producer.Reading{
		{DeviceID: "d1", Timestamp: testBaseTime, ReadingID: "r1", KWh: 1.0},
		{DeviceID: "d2", Timestamp: testBaseTime, ReadingID: "r2", KWh: 2.0},
		// advances only d1's watermark to 30m, cutoff 25m, flushing d1's bucket0 mid-stream
		{DeviceID: "d1", Timestamp: testBaseTime.Add(30 * time.Minute), ReadingID: "r3", KWh: 3.0},
	}

	got := runAggregation(cfg, readings)
	want := map[bucketKey]float64{
		{"d1", testBaseTime}:                       1.0,
		{"d2", testBaseTime}:                       2.0,
		{"d1", testBaseTime.Add(30 * time.Minute)}: 3.0,
	}

	if len(got) != 3 {
		t.Fatalf("got %d buckets, want 3: %+v", len(got), got)
	}
	if gotSet := toBucketSet(got); !reflect.DeepEqual(gotSet, want) {
		t.Fatalf("got %+v, want %+v", gotSet, want)
	}
}

func TestRunTruncatesTimestampToBucketBoundary(t *testing.T) {
	cfg := Config{BucketSize: 15 * time.Minute, LatenessWindow: time.Hour}
	boundary := testBaseTime.Add(15 * time.Minute)
	readings := []producer.Reading{
		{DeviceID: "d1", Timestamp: boundary.Add(-time.Nanosecond), ReadingID: "r1", KWh: 1.0}, // last ns of bucket0
		{DeviceID: "d1", Timestamp: boundary, ReadingID: "r2", KWh: 2.0},                       // first ns of bucket15m
	}

	got := runAggregation(cfg, readings)
	want := map[bucketKey]float64{
		{"d1", testBaseTime}: 1.0,
		{"d1", boundary}:     2.0,
	}

	if len(got) != 2 {
		t.Fatalf("got %d buckets, want 2: %+v", len(got), got)
	}
	if gotSet := toBucketSet(got); !reflect.DeepEqual(gotSet, want) {
		t.Fatalf("got %+v, want %+v", gotSet, want)
	}
}

// TestRunDropsLateReadingForAlreadyFlushedBucket guards the
// bitwise-identical-regardless-of-order invariant: a reading arriving after
// its bucket was already flushed must be dropped, not given a fresh
// single-reading accumulator and re-emitted. A re-emission would collide
// with the first Bucket's (DeviceID, BucketStart) key downstream, and
// storage.flushBuckets writes with ON CONFLICT ... DO UPDATE SET kwh =
// EXCLUDED.kwh, so the second write would silently overwrite the first
// instead of adding to it.
func TestRunDropsLateReadingForAlreadyFlushedBucket(t *testing.T) {
	cfg := Config{BucketSize: 15 * time.Minute, LatenessWindow: 5 * time.Minute}
	readings := []producer.Reading{
		{DeviceID: "d1", Timestamp: testBaseTime, ReadingID: "r1", KWh: 1.0},
		// advances watermark to 30m, cutoff 25m, flushing bucket0 (total 1.0)
		{DeviceID: "d1", Timestamp: testBaseTime.Add(30 * time.Minute), ReadingID: "r2", KWh: 7.0},
		// late arrival for bucket0, already flushed: must be dropped, not re-emitted
		{DeviceID: "d1", Timestamp: testBaseTime.Add(time.Minute), ReadingID: "r3", KWh: 9.0},
	}

	got := runAggregation(cfg, readings)
	want := []Bucket{
		{DeviceID: "d1", BucketStart: testBaseTime, KWh: 1.0},
		{DeviceID: "d1", BucketStart: testBaseTime.Add(30 * time.Minute), KWh: 7.0},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
