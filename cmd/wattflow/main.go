package main

import (
	"fmt"
	"os"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/AlexanderKulia/wattflow/internal/ingestion"
	"github.com/AlexanderKulia/wattflow/internal/producer"
	"github.com/AlexanderKulia/wattflow/internal/storage"
)

func main() {
	fmt.Println("wattflow starting")

	storageCfg := storage.Config{
		DSN:               "postgres://test:test@localhost:5432/test?sslmode=disable",
		BatchSizeBytes:    2 * 1024 * 1024,
		BatchFlushTimeout: time.Duration(30) * time.Second,
	}
	err := storage.Migrate(storageCfg.DSN)
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

	ingestCh := make(chan producer.Reading, producerCfg.Count)
	aggCh := make(chan producer.Reading, producerCfg.Count)
	storageCh := make(chan aggregation.Bucket, producerCfg.DeviceCount*2)
	go producer.Run(producerCfg, ingestCh)
	go ingestion.Run(ingestConfig, ingestCh, aggCh)
	go aggregation.Run(aggregationCfg, aggCh, storageCh)
	storage.Run(storageCfg, storageCh)
}
