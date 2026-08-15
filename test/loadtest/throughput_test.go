package load_test

import (
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

func BenchmarkPipelineThroughput(b *testing.B) {
	_, _, dsn := testutil.SetupTestDB(b, storage.Migrate)

	bucketStorageCfg := storage.Config{
		DSN:               dsn,
		BatchSizeBytes:    2 * 1024 * 1024,
		BatchFlushTimeout: time.Duration(30) * time.Second,
	}
	readingStorageCfg := storage.Config{
		DSN:               dsn,
		BatchSizeBytes:    8 * 1024 * 1024,
		BatchFlushTimeout: time.Duration(5) * time.Second,
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
	latenessWindow := time.Duration(15) * time.Minute
	ingestConfig := ingestion.Config{
		LatenessWindow: latenessWindow,
	}
	aggregationCfg := aggregation.Config{
		LatenessWindow: latenessWindow,
		BucketSize:     time.Duration(15) * time.Minute,
	}

	const channelBufferSize = 256
	ingestCh := make(chan observability.Envelope[producer.Reading], channelBufferSize)
	aggCh := make(chan observability.Envelope[producer.Reading], channelBufferSize)
	bucketStorageCh := make(chan observability.Envelope[aggregation.Bucket], producerCfg.DeviceCount*2)
	readingStorageCh := make(chan observability.Envelope[producer.Reading], channelBufferSize)

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
