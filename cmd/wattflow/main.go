package main

import (
	"fmt"

	"github.com/AlexanderKulia/wattflow/internal/aggregation"
	"github.com/AlexanderKulia/wattflow/internal/ingestion"
	"github.com/AlexanderKulia/wattflow/internal/producer"
)

func main() {
	fmt.Println("wattflow starting")

	cfg := producer.Config{
		DeviceCount:           1,
		ReadingCountPerSecond: 10,
		Count:                 100,
	}
	cfg.Validate()

	ingestCh := make(chan producer.Reading, cfg.Count)
	aggCh := make(chan producer.Reading, cfg.Count)
	go producer.Run(cfg, ingestCh)
	go ingestion.Run(ingestCh, aggCh)
	aggregation.Run(aggCh)
}
