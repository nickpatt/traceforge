// Package kafka wraps the franz-go client with TraceForge-specific producer and
// consumer helpers. All span messages are keyed by trace_id so every span of a
// trace lands on the same partition.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nickpatt/traceforge/internal/model"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes spans to a topic keyed by trace_id.
type Producer struct {
	cl    *kgo.Client
	topic string
}

// NewProducer creates a batching, async producer.
func NewProducer(brokers []string, topic string) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ProducerLinger(0),             // tune for throughput vs latency
		kgo.MaxBufferedRecords(100_000),   // bounded buffer => backpressure
		kgo.RequiredAcks(kgo.LeaderAck()), // acks=1
		kgo.DisableIdempotentWrite(),      // idempotence needs acks=all; we trade it for latency
		kgo.ProducerBatchMaxBytes(16<<20), // 16MiB batches
	)
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl, topic: topic}, nil
}

// Produce enqueues a span. The callback (may be nil) fires on delivery.
// Returns an error immediately if the internal buffer is full (backpressure).
func (p *Producer) Produce(ctx context.Context, s *model.Span, cb func(error)) error {
	val, err := json.Marshal(s)
	if err != nil {
		return err
	}
	rec := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(s.TraceID),
		Value: val,
	}
	// The record outlives the caller (e.g. an OTLP request that returns as soon as
	// spans are enqueued), so its delivery must not be cancelled with that context.
	p.cl.Produce(context.WithoutCancel(ctx), rec, func(_ *kgo.Record, err error) {
		if cb != nil {
			cb(err)
		}
	})
	return nil
}

// Flush blocks until buffered records are delivered.
func (p *Producer) Flush(ctx context.Context) error { return p.cl.Flush(ctx) }

// Close flushes and shuts down.
func (p *Producer) Close() { p.cl.Close() }

// Consumer reads spans from one or more topics in a consumer group.
type Consumer struct {
	cl *kgo.Client
}

// NewConsumer creates a group consumer.
func NewConsumer(brokers []string, group string, topics ...string) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(), // commit only after successful processing
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{cl: cl}, nil
}

// Poll fetches a batch and decodes spans. Caller commits after processing.
func (c *Consumer) Poll(ctx context.Context) ([]*model.Span, error) {
	fetches := c.cl.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		return nil, fmt.Errorf("kafka poll: %v", errs[0].Err)
	}
	var out []*model.Span
	fetches.EachRecord(func(r *kgo.Record) {
		var s model.Span
		if err := json.Unmarshal(r.Value, &s); err == nil {
			out = append(out, &s)
		}
	})
	return out, nil
}

// Commit commits the offsets of the last poll.
func (c *Consumer) Commit(ctx context.Context) error {
	return c.cl.CommitUncommittedOffsets(ctx)
}

// Close shuts down the consumer.
func (c *Consumer) Close() { c.cl.Close() }
