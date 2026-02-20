# PeerRPC Performance Benchmarks

## Methodology

### Hardware

Benchmarks run on the GitHub Actions `ubuntu-latest` runner (2 vCPU,
7 GB RAM). Results are not absolute — they are relative baselines for
regression detection. Local runs on dedicated hardware will produce
higher absolute numbers.

### Network

All benchmarks run over the in-process signaling backend on localhost.
This isolates the PeerRPC stack's overhead (framing, marshaling,
multiplexing, transport) from network latency. Cross-network
benchmarks require real STUN/TURN and are out of scope for CI.

### Message Sizes

| Label | Size | Notes |
|-------|------|-------|
| tiny  | 1 KB  | Inline path (fits in Call.inline_data ≤ 16 KB) |
| small | 16 KB | Inline threshold boundary |
| medium| 64 KB | Single Data.message (≤ 256 KB) |
| large | 256 KB| Message-max boundary |
| huge  | 1 MB  | Chunked (multiple Data.chunk frames) |

### Concurrency

Each benchmark runs at concurrency 1, 10, 100 to measure how
multiplexing scales. Higher concurrency stresses the sequence→stream
map and the outbound writer goroutine.

### Warmup

Every benchmark includes a 100-iteration warmup phase (Go's built-in
`b.ResetTimer()` after warmup). Without this the first few iterations
include pion/webrtc connection setup and distort the median.

### Reporting

Go's `testing.B` reports `ns/op` and `B/op` + `allocs/op` with
`-benchmem`. The CI workflow converts these into a time-series via
`benchmark-action/github-action-benchmark` and alerts on > 20%
regression.

## Target Metrics (v1)

| Metric | Target | Verification |
|--------|--------|-------------|
| Unary latency (P50, localhost) | < 5 ms | BenchmarkUnaryRPC |
| Unary throughput (100 concurrent) | > 5,000 RPC/s | BenchmarkUnaryRPC_Concurrent |
| Server Streaming (100 chunks) | < 50 ms total | BenchmarkServerStreaming |
| Large payload (1 MB, chunked) | < 100 MB/s throughput | BenchmarkLargePayload |
| Connection setup | < 500 ms | BenchmarkConnectionSetup |
| Memory per RPC | < 10 KB | -benchmem |

These are localhost-only targets. Real-network latency is dominated
by ICE negotiation and transport, not the PeerRPC stack.

## Running Locally

```bash
cd peerrpc-go

# All benchmarks.
go test -bench=. -benchmem -count=5 ./bench/...

# Specific benchmark.
go test -bench=BenchmarkUnaryRPC -benchmem -count=10 ./bench/...

# With CPU profiling.
go test -bench=. -benchmem -cpuprofile=cpu.prof ./bench/...
go tool pprof cpu.prof
```

## CI Integration

The `.github/workflows/bench.yml` workflow runs on every pull request
against main. It:

1. Runs `go test -bench=. -benchmem` against the PR branch.
2. Stores results in the `benchmark-data` branch via
   `benchmark-action/github-action-benchmark`.
3. Alerts when any benchmark regresses by more than 20%.

Historical charts are viewable at the GitHub Pages site configured by
the benchmark action.
