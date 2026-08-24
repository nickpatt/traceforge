// Package loadgen generates synthetic OTLP traffic using the real OpenTelemetry
// Go SDK, exercising the true ingestion path (collector -> Kafka -> ...).
//
// It models a small service topology so the dependency graph and latency
// heatmaps have realistic shape, and injects configurable error/slow rates.
package loadgen

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Config controls the generator.
type Config struct {
	Endpoint    string        // collector OTLP gRPC endpoint (host:4317)
	RPS         int           // target root traces per second
	Duration    time.Duration // total run time
	Concurrency int           // parallel workers
	ErrorRate   float64       // fraction of traces with an error span
	SlowRate    float64       // fraction of traces that are slow
	Depth       int           // call depth of the topology
}

// topology: frontend -> api -> {auth, db, cache}
var services = []string{"frontend", "api", "auth", "db", "cache"}

// Run drives the generator until Duration elapses.
func Run(ctx context.Context, cfg Config) error {
	tracers, shutdown, err := buildTracers(ctx, cfg.Endpoint)
	if err != nil {
		return err
	}
	defer shutdown(context.Background())

	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	interval := time.Second / time.Duration(max(cfg.RPS, 1))
	var wg sync.WaitGroup
	for i := 0; i < max(cfg.Concurrency, 1); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(interval * time.Duration(cfg.Concurrency))
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					emitTrace(ctx, tracers, cfg)
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

// emitTrace produces one nested trace across the topology.
func emitTrace(ctx context.Context, tracers map[string]trace.Tracer, cfg Config) {
	isError := rand.Float64() < cfg.ErrorRate
	isSlow := rand.Float64() < cfg.SlowRate

	ctx, root := tracers["frontend"].Start(ctx, "GET /checkout", trace.WithSpanKind(trace.SpanKindServer))
	defer root.End()

	work(root, isSlow)
	ctx, api := tracers["api"].Start(ctx, "api.handle", trace.WithSpanKind(trace.SpanKindServer))
	work(api, isSlow)

	for _, child := range []string{"auth", "db", "cache"} {
		_, span := tracers[child].Start(ctx, child+".call", trace.WithSpanKind(trace.SpanKindClient))
		work(span, isSlow)
		if isError && child == "db" {
			span.SetStatus(codes.Error, "db timeout")
			span.SetAttributes(attribute.Bool("error", true))
		}
		span.End()
	}
	api.End()
	if isError {
		root.SetStatus(codes.Error, "downstream error")
	}
}

func work(span trace.Span, slow bool) {
	d := time.Duration(5+rand.Intn(20)) * time.Millisecond
	if slow {
		d += time.Duration(200+rand.Intn(400)) * time.Millisecond
	}
	time.Sleep(d)
}

// buildTracers creates one TracerProvider per service, all exporting OTLP to the
// same collector endpoint.
func buildTracers(ctx context.Context, endpoint string) (map[string]trace.Tracer, func(context.Context) error, error) {
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, nil, err
	}
	tracers := make(map[string]trace.Tracer)
	var providers []*sdktrace.TracerProvider
	for _, svc := range services {
		res, _ := resource.New(ctx, resource.WithAttributes(
			semconv.ServiceName(svc),
		))
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithResource(res),
		)
		providers = append(providers, tp)
		tracers[svc] = tp.Tracer("loadgen")
	}
	otel.SetTracerProvider(providers[0])
	shutdown := func(ctx context.Context) error {
		for _, tp := range providers {
			_ = tp.Shutdown(ctx)
		}
		return nil
	}
	return tracers, shutdown, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Summary is a placeholder for reporting achieved throughput.
type Summary struct{ SpansSent int }

func (s Summary) String() string { return fmt.Sprintf("spans_sent=%d", s.SpansSent) }
