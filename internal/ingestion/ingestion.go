package ingestion

import (
	"container/heap"
	"log"
	"time"

	"github.com/AlexanderKulia/wattflow/internal/observability"
	"github.com/AlexanderKulia/wattflow/internal/producer"
	"go.opentelemetry.io/otel"
)

type expiryHeap []dedupEntry

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].timestamp.Before(h[j].timestamp) }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *expiryHeap) Push(x any)        { *h = append(*h, x.(dedupEntry)) }

func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type dedupEntry struct {
	key       string
	timestamp time.Time
}

type DropReason int

const (
	Late DropReason = iota
	Duplicate
)

var DropReasonName = map[DropReason]string{
	Late:      "late",
	Duplicate: "duplicate",
}

func (r DropReason) String() string {
	return DropReasonName[r]
}

type deviceState struct {
	seen        map[string]struct{} // existence check
	expiry      expiryHeap          // min-heap by timestamp, for eviction
	watermark   time.Time           // latest Reading.Timestamp seen for this device
	dropCounter map[DropReason]int
}

type Config struct {
	LatenessWindow time.Duration
}

func Run(cfg Config, in <-chan observability.Envelope[producer.Reading], aggOut chan<- observability.Envelope[producer.Reading], storageOut chan<- observability.Envelope[producer.Reading]) {
	devices := make(map[string]*deviceState)

	for env := range in {
		reading := env.Data
		ctx, span := otel.Tracer("wattflow/ingestion").Start(env.Ctx, "ingest")

		state, ok := devices[reading.DeviceID]
		if !ok {
			state = &deviceState{
				seen:        make(map[string]struct{}),
				dropCounter: make(map[DropReason]int),
			}
			devices[reading.DeviceID] = state
		}

		cutoffTimestamp := state.watermark.Add(-cfg.LatenessWindow)
		if reading.Timestamp.Before(cutoffTimestamp) {
			log.Printf("Reading for %s for DeviceID %s dropped because it was %s late", reading.Timestamp, reading.DeviceID, cutoffTimestamp.Sub(reading.Timestamp))
			state.dropCounter[Late]++
			span.End()
			continue
		}

		key := reading.DedupKey()
		_, seen := state.seen[key]
		if seen {
			log.Printf("Duplicate entry for key %s discarded", key)
			state.dropCounter[Duplicate]++
			span.End()
			continue
		} else {
			if reading.Timestamp.After(state.watermark) {
				state.watermark = reading.Timestamp
			}
			heap.Push(&state.expiry, dedupEntry{key: key, timestamp: reading.Timestamp})
			state.seen[key] = struct{}{}

			// recalculate because current reading might have advanced the watermark
			cutoffTimestamp := state.watermark.Add(-cfg.LatenessWindow)
			for len(state.expiry) > 0 && state.expiry[0].timestamp.Before(cutoffTimestamp) {
				evicted := heap.Pop(&state.expiry).(dedupEntry)
				delete(state.seen, evicted.key)
			}
		}

		span.End()
		aggOut <- observability.Envelope[producer.Reading]{Data: reading, Ctx: ctx}
		storageOut <- observability.Envelope[producer.Reading]{Data: reading, Ctx: ctx}
	}
	close(aggOut)
	close(storageOut)
}
