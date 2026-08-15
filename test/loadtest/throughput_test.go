package load_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/AlexanderKulia/wattflow/internal/ingestion"
	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/AlexanderKulia/wattflow/internal/producer"
	"github.com/AlexanderKulia/wattflow/internal/storage"
	"github.com/AlexanderKulia/wattflow/internal/testutil"
)

// paramCombo is one point in the matrix swept by BenchmarkPipelineThroughputMatrix.
// To add a dimension: add a field here, a value range in the combos loop
// below, and read the field in runPipelineOnce.
type paramCombo struct {
	channelBufferSize     int
	readingBatchSizeBytes int
	readingFlushTimeout   time.Duration
}

func (c paramCombo) name() string {
	return fmt.Sprintf("buf=%d/batch=%dKB/flush=%s",
		c.channelBufferSize, c.readingBatchSizeBytes/1024, c.readingFlushTimeout)
}

func BenchmarkPipelineThroughputMatrix(b *testing.B) {
	channelBufferSizes := []int{256, 1024}
	readingBatchSizes := []int{64 * 1024, 128 * 1024, 256 * 1024}
	readingFlushTimeouts := []time.Duration{5 * time.Second, 30 * time.Second}

	var combos []paramCombo
	for _, buf := range channelBufferSizes {
		for _, batch := range readingBatchSizes {
			for _, flush := range readingFlushTimeouts {
				combos = append(combos, paramCombo{
					channelBufferSize:     buf,
					readingBatchSizeBytes: batch,
					readingFlushTimeout:   flush,
				})
			}
		}
	}

	for _, c := range combos {
		b.Run(c.name(), func(b *testing.B) {
			runPipelineOnce(b, c)
		})
	}
}

// runPipelineOnce runs the full pipeline once against a fresh TimescaleDB
// container for combo c, and reports events/sec. A fresh container per combo
// (via testutil.SetupTestDB) avoids re-running migrations against an
// already-migrated schema and avoids relying on TRUNCATE to reset hypertable
// state between runs.
func runPipelineOnce(b *testing.B, c paramCombo) {
	_, _, dsn := testutil.SetupTestDB(b, storage.Migrate)

	bucketStorageCfg := storage.Config{
		DSN:               dsn,
		BatchSizeBytes:    2 * 1024 * 1024,
		BatchFlushTimeout: 30 * time.Second,
	}
	readingStorageCfg := storage.Config{
		DSN:               dsn,
		BatchSizeBytes:    c.readingBatchSizeBytes,
		BatchFlushTimeout: c.readingFlushTimeout,
	}

	producerCfg := producer.Config{
		DeviceCount:                    1,
		ReadingCountPerSecond:          1_000_000,
		Count:                          50_000,
		OutOfOrderProbability:          0.1,
		DuplicateProbability:           0.05,
		DelayProbability:               0.05,
		UnreliableReadingIDProbability: 0.01,
	}
	producerCfg.Validate()
	latenessWindow := 15 * time.Minute
	ingestConfig := ingestion.Config{
		LatenessWindow: latenessWindow,
	}
	aggregationCfg := aggregation.Config{
		LatenessWindow: latenessWindow,
		BucketSize:     15 * time.Minute,
	}

	ingestCh := make(chan observability.Envelope[producer.Reading], c.channelBufferSize)
	aggCh := make(chan observability.Envelope[producer.Reading], c.channelBufferSize)
	bucketStorageCh := make(chan observability.Envelope[aggregation.Bucket], producerCfg.DeviceCount*2)
	readingStorageCh := make(chan observability.Envelope[producer.Reading], c.channelBufferSize)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		storage.RunReadings(readingStorageCfg, readingStorageCh)
	}()
	go func() {
		defer wg.Done()
		storage.RunBuckets(bucketStorageCfg, bucketStorageCh)
	}()

	b.ResetTimer()
	go producer.Run(producerCfg, ingestCh)
	go ingestion.Run(ingestConfig, ingestCh, aggCh, readingStorageCh)
	go aggregation.Run(aggregationCfg, aggCh, bucketStorageCh)
	wg.Wait()
	b.StopTimer()

	eventsPerSec := float64(producerCfg.Count) / b.Elapsed().Seconds()
	b.ReportMetric(eventsPerSec, "events/sec")
}
