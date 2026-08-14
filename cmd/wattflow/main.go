package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/AlexanderKulia/wattflow/internal/ingestion"
	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/AlexanderKulia/wattflow/internal/producer"
	"github.com/AlexanderKulia/wattflow/internal/storage"
)

func main() {
	fmt.Println("wattflow starting")
	// Handle SIGINT (CTRL+C) gracefully.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	otelShutdown, err := setupOTelSDK(ctx)
	if err != nil {
		os.Exit(1)
	}
	defer otelShutdown(ctx)

	dsn := "postgres://test:test@localhost:5432/test?sslmode=disable"
	bucketStorageCfg := storage.Config{
		DSN:               dsn,
		BatchSizeBytes:    2 * 1024 * 1024,
		BatchFlushTimeout: time.Duration(30) * time.Second,
	}
	readingStorageCfg := storage.Config{
		DSN:               dsn,
		BatchSizeBytes:    2 * 1024 * 1024,
		BatchFlushTimeout: time.Duration(5) * time.Second,
	}
	err = storage.Migrate(dsn)
	if err != nil {
		os.Exit(1)
	}

	producerCfg := producer.Config{
		DeviceCount:                    1,
		ReadingCountPerSecond:          10,
		Count:                          100,
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

	ingestCh := make(chan observability.Envelope[producer.Reading], producerCfg.Count)
	aggCh := make(chan observability.Envelope[producer.Reading], producerCfg.Count)
	bucketStorageCh := make(chan observability.Envelope[aggregation.Bucket], producerCfg.DeviceCount*2)
	readingStorageCh := make(chan observability.Envelope[producer.Reading], producerCfg.Count)
	go producer.Run(producerCfg, ingestCh)
	go ingestion.Run(ingestConfig, ingestCh, aggCh, readingStorageCh)
	go aggregation.Run(aggregationCfg, aggCh, bucketStorageCh)
	go storage.RunReadings(readingStorageCfg, readingStorageCh)
	storage.RunBuckets(bucketStorageCfg, bucketStorageCh)
}
