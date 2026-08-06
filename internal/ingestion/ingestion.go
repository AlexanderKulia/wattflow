package ingestion

import "github.com/AlexanderKulia/wattflow/internal/producer"

func Run(in <-chan producer.Reading, out chan<- producer.Reading) {
	for reading := range in {
		out <- reading
	}
	close(out)
}
