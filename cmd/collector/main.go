// Command collector is the OTLP ingestion service: it accepts spans over gRPC
// and HTTP and produces them to Kafka keyed by trace_id.
package main

import (
	"context"

	"github.com/nickpatt/traceforge/internal/config"
	"github.com/nickpatt/traceforge/internal/kafka"
	"github.com/nickpatt/traceforge/internal/model"
	"github.com/nickpatt/traceforge/internal/otlp"
	"github.com/nickpatt/traceforge/internal/telemetry"
)

func main() {
	log := telemetry.NewLogger("collector")

	producer, err := kafka.NewProducer(config.KafkaBrokers(), config.TopicSpans)
	if err != nil {
		log.Error("kafka producer", "err", err)
		return
	}
	defer producer.Close()

	sink := func(ctx context.Context, spans []*model.Span) error {
		for _, s := range spans {
			if err := producer.Produce(ctx, s, func(err error) {
				if err != nil {
					log.Error("produce", "err", err)
					return
				}
				telemetry.SpansProduced.Inc()
			}); err != nil {
				return err // backpressure -> OTLP client gets 429/RESOURCE_EXHAUSTED
			}
		}
		telemetry.SpansIngested.WithLabelValues("otlp").Add(float64(len(spans)))
		return nil
	}

	go telemetry.ServeAdmin(config.Env("ADMIN_ADDR", ":9101"), log)
	go func() {
		addr := config.Env("OTLP_HTTP_ADDR", ":4318")
		log.Info("otlp http listening", "addr", addr)
		if err := otlp.ServeHTTP(addr, sink); err != nil {
			log.Error("otlp http", "err", err)
		}
	}()

	addr := config.Env("OTLP_GRPC_ADDR", ":4317")
	log.Info("otlp grpc listening", "addr", addr)
	if err := otlp.ServeGRPC(addr, sink); err != nil {
		log.Error("otlp grpc", "err", err)
	}
}
