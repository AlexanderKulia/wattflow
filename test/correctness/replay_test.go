package correctness

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/AlexanderKulia/wattflow/internal/ingestion"
	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/AlexanderKulia/wattflow/internal/producer"
)

// runPipeline feeds readings through ingestion -> aggregation, in the given
// order, and collects every emitted Bucket.
func runPipeline(readings []producer.Reading) []aggregation.Bucket {
	ingestCfg := ingestion.Config{LatenessWindow: FixtureLatenessWindow}
	aggCfg := aggregation.Config{LatenessWindow: FixtureLatenessWindow, BucketSize: FixtureBucketSize}

	inCh := make(chan observability.Envelope[producer.Reading])
	aggCh := make(chan observability.Envelope[producer.Reading])
	outCh := make(chan observability.Envelope[aggregation.Bucket])
	storageCh := make(chan observability.Envelope[producer.Reading])

	go func() {
		for _, r := range readings {
			inCh <- observability.Envelope[producer.Reading]{Data: r, Ctx: context.Background()}
		}
		close(inCh)
	}()

	go ingestion.Run(context.Background(), ingestCfg, inCh, aggCh, storageCh)
	go func() {
		for range storageCh {
		}
	}()

	var buckets []aggregation.Bucket
	done := make(chan struct{})
	go func() {
		for env := range outCh {
			buckets = append(buckets, env.Data)
		}
		close(done)
	}()

	aggregation.Run(context.Background(), aggCfg, aggCh, outCh)
	<-done

	return buckets
}

func inOrder(readings []producer.Reading) []producer.Reading {
	sorted := append([]producer.Reading(nil), readings...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.Before(sorted[j].Timestamp)
	})
	return sorted
}

func reverse(readings []producer.Reading) []producer.Reading {
	reversed := make([]producer.Reading, len(readings))
	for i, r := range readings {
		reversed[len(readings)-1-i] = r
	}
	return reversed
}

func reordered(readings []producer.Reading) []producer.Reading {
	return reverse(inOrder(readings))
}

func duplicated(readings []producer.Reading) []producer.Reading {
	return append(append([]producer.Reading(nil), readings...), readings...)
}

func mixed(readings []producer.Reading) []producer.Reading {
	return reverse(duplicated(inOrder(readings)))
}

func sortedBuckets(buckets []aggregation.Bucket) []aggregation.Bucket {
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].DeviceID != buckets[j].DeviceID {
			return buckets[i].DeviceID < buckets[j].DeviceID
		}
		return buckets[i].BucketStart.Before(buckets[j].BucketStart)
	})
	return buckets
}

func TestReplayBitwiseIdentical(t *testing.T) {
	base := fixedReadings()
	want := sortedBuckets(runPipeline(inOrder(base)))

	patterns := []struct {
		name     string
		readings []producer.Reading
	}{
		{"in-order", inOrder(base)},
		{"reordered", reordered(base)},
		{"duplicated", duplicated(inOrder(base))},
		{"mixed", mixed(base)},
	}

	for _, p := range patterns {
		t.Run(p.name, func(t *testing.T) {
			got := sortedBuckets(runPipeline(p.readings))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("pattern %q produced different aggregate:\ngot:  %+v\nwant: %+v", p.name, got, want)
			}
		})
	}
}
