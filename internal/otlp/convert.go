// Package otlp decodes OTLP protobuf trace requests into internal spans and
// hosts the gRPC + HTTP ingestion servers.
package otlp

import (
	"encoding/hex"
	"time"

	"github.com/nickpatt/traceforge/internal/model"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// FlattenRequest converts an OTLP ExportTraceServiceRequest payload (the
// ResourceSpans list) into internal spans.
func FlattenRequest(resourceSpans []*tracepb.ResourceSpans) []*model.Span {
	var out []*model.Span
	now := uint64(time.Now().UnixNano())
	for _, rs := range resourceSpans {
		resAttrs := attrsToMap(rs.GetResource().GetAttributes())
		svc := resAttrs["service.name"]
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				m := &model.Span{
					TraceID:      hex.EncodeToString(sp.GetTraceId()),
					SpanID:       hex.EncodeToString(sp.GetSpanId()),
					ParentSpanID: hex.EncodeToString(sp.GetParentSpanId()),
					ServiceName:  svc,
					SpanName:     sp.GetName(),
					Kind:         kind(sp.GetKind()),
					StartUnixNs:  sp.GetStartTimeUnixNano(),
					EndUnixNs:    sp.GetEndTimeUnixNano(),
					StatusCode:   status(sp.GetStatus()),
					StatusMsg:    sp.GetStatus().GetMessage(),
					Attributes:   attrsToMap(sp.GetAttributes()),
					ResourceAttr: resAttrs,
					IngestedNs:   now,
				}
				out = append(out, m)
			}
		}
	}
	return out
}

func attrsToMap(kvs []*commonpb.KeyValue) map[string]string {
	if len(kvs) == 0 {
		return nil
	}
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.GetKey()] = anyValueString(kv.GetValue())
	}
	return m
}

func anyValueString(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		if x.BoolValue {
			return "true"
		}
		return "false"
	case *commonpb.AnyValue_IntValue:
		return itoa(x.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return ftoa(x.DoubleValue)
	default:
		return ""
	}
}

func kind(k tracepb.Span_SpanKind) model.SpanKind {
	switch k {
	case tracepb.Span_SPAN_KIND_SERVER:
		return model.KindServer
	case tracepb.Span_SPAN_KIND_CLIENT:
		return model.KindClient
	case tracepb.Span_SPAN_KIND_PRODUCER:
		return model.KindProducer
	case tracepb.Span_SPAN_KIND_CONSUMER:
		return model.KindConsumer
	case tracepb.Span_SPAN_KIND_INTERNAL:
		return model.KindInternal
	default:
		return model.KindUnspecified
	}
}

func status(s *tracepb.Status) model.StatusCode {
	switch s.GetCode() {
	case tracepb.Status_STATUS_CODE_OK:
		return model.StatusOK
	case tracepb.Status_STATUS_CODE_ERROR:
		return model.StatusError
	default:
		return model.StatusUnset
	}
}
