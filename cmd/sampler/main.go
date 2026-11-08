// Command sampler consumes raw spans, buffers them per trace, applies tail-based
// sampling policies, and forwards kept traces to the spans.kept topic.
package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/nickpatt/traceforge/internal/config"
	"github.com/nickpatt/traceforge/internal/kafka"
	"github.com/nickpatt/traceforge/internal/sampling"
	"github.com/nickpatt/traceforge/internal/telemetry"
)

func main() {
	log := telemetry.NewLogger("sampler")

	// Create the topics this service reads and writes; clients do not auto-create them.
	topics := []kafka.TopicSpec{
		{Name: config.TopicSpans, Partitions: int32(config.EnvInt("KAFKA_PARTITIONS", 12)), Replicas: 1},
		{Name: config.TopicSpansKept, Partitions: int32(config.EnvInt("KAFKA_PARTITIONS", 12)), Replicas: 1},
	}
	if err := kafka.EnsureTopics(context.Background(), config.KafkaBrokers(), topics...); err != nil {
		log.Error("kafka topics", "err", err)
		return
	}

	consumer, err := kafka.NewConsumer(config.KafkaBrokers(), "sampler", config.TopicSpans)
	if err != nil {
		log.Error("consumer", "err", err)
		return
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(config.KafkaBrokers(), config.TopicSpansKept)
	if err != nil {
		log.Error("producer", "err", err)
		return
	}
	defer producer.Close()

	policy := sampling.Policy{
		KeepAllErrors:    config.EnvBool("SAMPLE_KEEP_ERRORS", true),
		LatencyThreshold: config.EnvDuration("SAMPLE_LATENCY_THRESHOLD", 250*time.Millisecond),
		BaseSampleRate:   0.05,
	}
	s := sampling.New(sampling.DefaultConfig(), policy, rand.Float64)

	go telemetry.ServeAdmin(config.Env("ADMIN_ADDR", ":9102"), log)

	ctx := context.Background()
	sweep := time.NewTicker(500 * time.Millisecond)
	defer sweep.Stop()

	go func() {
		for range sweep.C {
			for _, d := range s.Sweep(time.Now()) {
				if !d.Keep {
					telemetry.TracesSampledOut.Inc()
					continue
				}
				telemetry.TracesSampledIn.Inc()
				for _, sp := range d.Spans {
					_ = producer.Produce(ctx, sp, nil)
				}
			}
			telemetry.BufferedTraces.Set(float64(s.Buffered()))
		}
	}()

	log.Info("sampler running", "policy", policy)
	for {
		spans, err := consumer.Poll(ctx)
		if err != nil {
			log.Error("poll", "err", err)
			continue
		}
		for _, sp := range spans {
			s.Add(sp)
		}
		_ = consumer.Commit(ctx)
	}
}
