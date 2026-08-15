.PHONY: build run test test-correctness vet profile

build:
	go build ./...

run:
	go run ./cmd/wattflow

test:
	go test ./...

test-correctness:
	go test ./test/correctness/...

vet:
	go vet ./...

profile:
	go test ./test/loadtest/ -run ^$$ \
		-bench 'BenchmarkPipelineThroughputMatrix/buf=256/batch=64KB/flush=5s' \
		-benchtime 1x \
		-cpuprofile cpu.prof -memprofile mem.prof \
		-blockprofile block.prof -blockprofilerate 1 \
		> bench_run.txt
	go tool pprof -top -cum cpu.prof         > cpu_top_cum.txt
	go tool pprof -top -alloc_space mem.prof > mem_top_allocspace.txt
	go tool pprof -top -cum block.prof       > block_top_cum.txt
	go tool pprof -list=flushReadings cpu.prof   > cpu_list_flushReadings.txt
	go tool pprof -list=ToSql cpu.prof           > cpu_list_tosql.txt
	go tool pprof -list=flushReadings mem.prof   > mem_list_flushReadings.txt
	go tool pprof -list=flushReadings block.prof > block_list_flushReadings.txt
