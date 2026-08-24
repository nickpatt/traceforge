// Command loadgen generates synthetic OTLP traffic against the collector to
// load-test the ingestion pipeline.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/nickpatt/traceforge/internal/loadgen"
)

func main() {
	cfg := loadgen.Config{}
	flag.StringVar(&cfg.Endpoint, "endpoint", "localhost:4317", "collector OTLP gRPC endpoint")
	flag.IntVar(&cfg.RPS, "rps", 1000, "target root traces per second")
	flag.DurationVar(&cfg.Duration, "duration", 30*time.Second, "run duration")
	flag.IntVar(&cfg.Concurrency, "concurrency", 8, "parallel workers")
	flag.Float64Var(&cfg.ErrorRate, "error-rate", 0.05, "fraction of traces with an error")
	flag.Float64Var(&cfg.SlowRate, "slow-rate", 0.05, "fraction of slow traces")
	flag.IntVar(&cfg.Depth, "depth", 3, "topology call depth")
	flag.Parse()

	log.Printf("loadgen: endpoint=%s rps=%d duration=%s concurrency=%d",
		cfg.Endpoint, cfg.RPS, cfg.Duration, cfg.Concurrency)

	if err := loadgen.Run(context.Background(), cfg); err != nil {
		log.Fatalf("loadgen: %v", err)
	}
	log.Printf("loadgen: done")
}
