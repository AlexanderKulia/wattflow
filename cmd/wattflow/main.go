package main

import (
	"fmt"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/AlexanderKulia/wattflow/internal/ingestion"
	"github.com/AlexanderKulia/wattflow/internal/producer"
)

func main() {
	fmt.Println("wattflow starting")

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
	aggOutCh := make(chan aggregation.Bucket, producerCfg.DeviceCount*2)
	go producer.Run(producerCfg, ingestCh)
	go ingestion.Run(ingestConfig, ingestCh, aggCh)

	go func(ch <-chan aggregation.Bucket) {
		for bucket := range ch {
			fmt.Println(bucket)
		}
	}(aggOutCh)
	aggregation.Run(aggregationCfg, aggCh, aggOutCh)
}
