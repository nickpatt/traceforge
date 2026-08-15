// Command writer consumes kept spans and performs batched, async bulk inserts
// into ClickHouse. Set NO_SAMPLING=true to consume the raw spans topic instead
// (used to measure sampling's volume reduction).
package main

import (
	"context"
	"time"

	"github.com/nickpatt/traceforge/internal/clickhouse"
	"github.com/nickpatt/traceforge/internal/config"
	"github.com/nickpatt/traceforge/internal/kafka"
	"github.com/nickpatt/traceforge/internal/telemetry"
)

func main() {
	log := telemetry.NewLogger("writer")

	topic := config.TopicSpansKept
	if config.EnvBool("NO_SAMPLING", false) {
		topic = config.TopicSpans
	}

	consumer, err := kafka.NewConsumer(config.KafkaBrokers(), "writer", topic)
	if err != nil {
		log.Error("consumer", "err", err)
		return
	}
	defer consumer.Close()

	conn, err := clickhouse.Connect(config.Env("CLICKHOUSE_ADDR", "localhost:9000"),
		config.Env("CLICKHOUSE_DB", "traceforge"))
	if err != nil {
		log.Error("clickhouse", "err", err)
		return
	}
	defer conn.Close()

	w := clickhouse.NewWriter(conn,
		config.EnvInt("BATCH_MAX_ROWS", 5000),
		config.EnvDuration("BATCH_MAX_WAIT", time.Second),
		config.EnvInt("MAX_INFLIGHT", 4),
	)

	go telemetry.ServeAdmin(config.Env("ADMIN_ADDR", ":9103"), log)

	ctx := context.Background()
	flush := time.NewTicker(200 * time.Millisecond)
	defer flush.Stop()
	go func() {
		for range flush.C {
			_ = w.MaybeFlush(ctx)
		}
	}()

	log.Info("writer running", "topic", topic)
	for {
		spans, err := consumer.Poll(ctx)
		if err != nil {
			log.Error("poll", "err", err)
			continue
		}
		for _, s := range spans {
			_ = w.Add(ctx, s)
		}
		telemetry.SpansWritten.Add(float64(len(spans)))
		_ = consumer.Commit(ctx)
	}
}
