package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
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
	defer func() {
		if err := otelShutdown(ctx); err != nil {
			fmt.Println(err)
		}
	}()

	dsn := "postgres://test:test@localhost:5432/test?sslmode=disable"
	bucketStorageCfg := storage.Config{
		DSN:               dsn,
		BatchSizeBytes:    2 * 1024 * 1024,
		BatchFlushTimeout: 30 * time.Second,
	}
	readingStorageCfg := storage.Config{
		DSN:               dsn,
		BatchSizeBytes:    2 * 1024 * 1024,
		BatchFlushTimeout: 5 * time.Second,
	}
	err = storage.Migrate(dsn)
	if err != nil {
		fmt.Println(err)
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
	latenessWindow := 15 * time.Minute
	ingestConfig := ingestion.Config{
		LatenessWindow: latenessWindow,
	}
	aggregationCfg := aggregation.Config{
		LatenessWindow: latenessWindow,
		BucketSize:     15 * time.Minute,
	}

	const channelBufferSize = 256
	ingestCh := make(chan observability.Envelope[producer.Reading], channelBufferSize)
	aggCh := make(chan observability.Envelope[producer.Reading], channelBufferSize)
	bucketStorageCh := make(chan observability.Envelope[aggregation.Bucket], producerCfg.DeviceCount*2)
	readingStorageCh := make(chan observability.Envelope[producer.Reading], channelBufferSize)
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		storage.RunReadings(ctx, readingStorageCfg, readingStorageCh)
	}()
	go func() {
		defer wg.Done()
		storage.RunBuckets(ctx, bucketStorageCfg, bucketStorageCh)
	}()
	go func() {
		defer wg.Done()
		producer.Run(ctx, producerCfg, ingestCh)
	}()
	go func() {
		defer wg.Done()
		ingestion.Run(ctx, ingestConfig, ingestCh, aggCh, readingStorageCh)
	}()
	go func() {
		defer wg.Done()
		aggregation.Run(ctx, aggregationCfg, aggCh, bucketStorageCh)
	}()
	wg.Wait()
}
