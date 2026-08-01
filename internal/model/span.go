// Package model defines the internal, storage-agnostic representation of a
// span that flows through TraceForge: OTLP -> Kafka -> sampler -> ClickHouse.
package model

import "time"

// StatusCode mirrors OTLP span status.
type StatusCode string

const (
	StatusUnset StatusCode = "UNSET"
	StatusOK    StatusCode = "OK"
	StatusError StatusCode = "ERROR"
)

// SpanKind mirrors OTLP span kind.
type SpanKind string

const (
	KindUnspecified SpanKind = "UNSPECIFIED"
	KindInternal    SpanKind = "INTERNAL"
	KindServer      SpanKind = "SERVER"
	KindClient      SpanKind = "CLIENT"
	KindProducer    SpanKind = "PRODUCER"
	KindConsumer    SpanKind = "CONSUMER"
)

// Span is the normalized, flattened span used across the pipeline.
//
// It is produced by the collector after decoding OTLP, keyed in Kafka by
// TraceID, buffered by the sampler, and written to ClickHouse by the writer.
type Span struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id"`
	ServiceName  string            `json:"service_name"`
	SpanName     string            `json:"span_name"`
	Kind         SpanKind          `json:"kind"`
	StartUnixNs  uint64            `json:"start_unix_ns"`
	EndUnixNs    uint64            `json:"end_unix_ns"`
	StatusCode   StatusCode        `json:"status_code"`
	StatusMsg    string            `json:"status_message,omitempty"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	ResourceAttr map[string]string `json:"resource_attrs,omitempty"`
	IngestedNs   uint64            `json:"ingested_ns"`
}

// DurationNs returns the span duration in nanoseconds.
func (s *Span) DurationNs() uint64 {
	if s.EndUnixNs < s.StartUnixNs {
		return 0
	}
	return s.EndUnixNs - s.StartUnixNs
}

// IsRoot reports whether the span is the root of its trace.
func (s *Span) IsRoot() bool { return s.ParentSpanID == "" }

// StartTime returns the span start as a time.Time.
func (s *Span) StartTime() time.Time {
	return time.Unix(0, int64(s.StartUnixNs))
}
