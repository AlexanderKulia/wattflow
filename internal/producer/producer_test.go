package producer

import (
	"context"
	"testing"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/observability"
)

var testBaseTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// runProducer runs Run to completion and returns every Reading emitted on out.
func runProducer(ctx context.Context, cfg Config) []Reading {
	out := make(chan observability.Envelope[Reading])
	var got []Reading
	done := make(chan struct{})
	go func() {
		for env := range out {
			got = append(got, env.Data)
		}
		close(done)
	}()

	Run(ctx, cfg, out)
	<-done

	return got
}

func TestValidatePanicsOnZeroReadingCountPerSecond(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for zero ReadingCountPerSecond")
		}
	}()
	cfg := Config{ReadingCountPerSecond: 0}
	cfg.Validate()
}

func TestValidatePanicsOnDurationAndCountBothSet(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic when Duration and Count are both set")
		}
	}()
	cfg := Config{ReadingCountPerSecond: 1, Duration: time.Second, Count: 5}
	cfg.Validate()
}

func TestRunEmitsExactlyCountReadingsThenCloses(t *testing.T) {
	cfg := Config{DeviceCount: 2, ReadingCountPerSecond: 1_000_000, Count: 50}

	got := runProducer(context.Background(), cfg)

	if len(got) != 50 {
		t.Fatalf("got %d readings, want 50", len(got))
	}
}

func TestRunUsesOnlyConfiguredDeviceCount(t *testing.T) {
	cfg := Config{DeviceCount: 1, ReadingCountPerSecond: 1_000_000, Count: 20}

	got := runProducer(context.Background(), cfg)

	if len(got) != 20 {
		t.Fatalf("got %d readings, want 20", len(got))
	}
	want := got[0].DeviceID
	if want == "" {
		t.Fatal("expected a non-empty DeviceID")
	}
	for _, r := range got {
		if r.DeviceID != want {
			t.Fatalf("reading has DeviceID %q, want %q (only one device configured)", r.DeviceID, want)
		}
	}
}

func TestRunStopsPromptlyOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := Config{DeviceCount: 1, ReadingCountPerSecond: 1, Count: 1_000_000}

	out := make(chan observability.Envelope[Reading])
	go Run(ctx, cfg, out)

	<-out // first reading is sent without any prior delay
	cancel()

	done := make(chan struct{})
	go func() {
		for range out {
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop promptly after context cancellation")
	}
}

func TestDedupKeyUsesReadingIDWhenPresent(t *testing.T) {
	r := Reading{DeviceID: "d1", Timestamp: testBaseTime, ReadingID: "r1", KWh: 1.0}

	got := r.DedupKey()
	want := "d1|r1"

	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestDedupKeyFallsBackToContentHashWhenReadingIDMissing(t *testing.T) {
	r1 := Reading{DeviceID: "d1", Timestamp: testBaseTime, KWh: 5.0}
	r2 := Reading{DeviceID: "d1", Timestamp: testBaseTime, KWh: 5.0} // identical device+timestamp+kWh
	r3 := Reading{DeviceID: "d1", Timestamp: testBaseTime, KWh: 6.0} // different kWh

	if r1.DedupKey() != r2.DedupKey() {
		t.Fatalf("identical readings produced different fallback keys: %q vs %q", r1.DedupKey(), r2.DedupKey())
	}
	if r1.DedupKey() == r3.DedupKey() {
		t.Fatalf("readings with different kWh produced the same fallback key: %q", r1.DedupKey())
	}
}
