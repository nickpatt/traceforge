package otlp

import (
	"encoding/hex"
	"testing"

	"github.com/nickpatt/traceforge/internal/model"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_StringValue{StringValue: v},
	}}
}

func TestFlattenRequest(t *testing.T) {
	traceID, _ := hex.DecodeString("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := hex.DecodeString("1122334455667788")
	parentID, _ := hex.DecodeString("99aabbccddeeff00")

	rs := []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{strAttr("service.name", "checkout")},
		},
		ScopeSpans: []*tracepb.ScopeSpans{{
			Spans: []*tracepb.Span{{
				TraceId:           traceID,
				SpanId:            spanID,
				ParentSpanId:      parentID,
				Name:              "POST /pay",
				Kind:              tracepb.Span_SPAN_KIND_SERVER,
				StartTimeUnixNano: 1000,
				EndTimeUnixNano:   4000,
				Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "boom"},
				Attributes:        []*commonpb.KeyValue{strAttr("http.method", "POST")},
			}},
		}},
	}}

	spans := FlattenRequest(rs)
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	s := spans[0]
	if s.TraceID != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("trace id: got %q", s.TraceID)
	}
	if s.SpanID != "1122334455667788" {
		t.Errorf("span id: got %q", s.SpanID)
	}
	if s.ServiceName != "checkout" {
		t.Errorf("service: got %q", s.ServiceName)
	}
	if s.Kind != model.KindServer {
		t.Errorf("kind: got %q", s.Kind)
	}
	if s.StatusCode != model.StatusError || s.StatusMsg != "boom" {
		t.Errorf("status: got %q %q", s.StatusCode, s.StatusMsg)
	}
	if s.DurationNs() != 3000 {
		t.Errorf("duration: got %d", s.DurationNs())
	}
	if s.Attributes["http.method"] != "POST" {
		t.Errorf("attr: got %v", s.Attributes)
	}
	if s.ResourceAttr["service.name"] != "checkout" {
		t.Errorf("resource attr: got %v", s.ResourceAttr)
	}
}

func TestAnyValueString(t *testing.T) {
	cases := map[*commonpb.AnyValue]string{
		{Value: &commonpb.AnyValue_StringValue{StringValue: "x"}}: "x",
		{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}:    "true",
		{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}:        "42",
		{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 1.5}}: "1.5",
	}
	for in, want := range cases {
		if got := anyValueString(in); got != want {
			t.Errorf("anyValueString(%v) = %q, want %q", in, got, want)
		}
	}
}
