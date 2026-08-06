package main

import (
	"fmt"

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

	ingestConfig := ingestion.Config{
		LatenessWindowSeconds: 15 * 60,
	}

	ingestCh := make(chan producer.Reading, producerCfg.Count)
	aggCh := make(chan producer.Reading, producerCfg.Count)
	go producer.Run(producerCfg, ingestCh)
	go ingestion.Run(ingestConfig, ingestCh, aggCh)
	aggregation.Run(aggCh)
}
