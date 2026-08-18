package sampling

import (
	"testing"
	"time"

	"github.com/nickpatt/traceforge/internal/model"
)

// neverSample forces the probabilistic policy off so we test error/latency paths.
func neverSample() float64 { return 1.0 }

func rootSpan(traceID string, durNs uint64, status model.StatusCode) *model.Span {
	return &model.Span{
		TraceID: traceID, SpanID: "root", ParentSpanID: "",
		StartUnixNs: 0, EndUnixNs: durNs, StatusCode: status,
	}
}

func TestKeepsErrorTraces(t *testing.T) {
	s := New(Config{QuietPeriod: 0, MaxWait: time.Hour, MaxTraces: 10},
		Policy{KeepAllErrors: true}, neverSample)
	s.Add(rootSpan("t1", 10, model.StatusError))

	decisions := s.Sweep(time.Now().Add(time.Second))
	if len(decisions) != 1 || !decisions[0].Keep || decisions[0].Reason != "error" {
		t.Fatalf("expected error trace kept, got %+v", decisions)
	}
}

func TestKeepsSlowTraces(t *testing.T) {
	s := New(Config{QuietPeriod: 0, MaxWait: time.Hour, MaxTraces: 10},
		Policy{LatencyThreshold: 100 * time.Millisecond}, neverSample)
	s.Add(rootSpan("t2", uint64(200*time.Millisecond), model.StatusOK))

	decisions := s.Sweep(time.Now().Add(time.Second))
	if len(decisions) != 1 || !decisions[0].Keep || decisions[0].Reason != "slow" {
		t.Fatalf("expected slow trace kept, got %+v", decisions)
	}
}

func TestDropsBaselineTraces(t *testing.T) {
	s := New(Config{QuietPeriod: 0, MaxWait: time.Hour, MaxTraces: 10},
		Policy{KeepAllErrors: true, BaseSampleRate: 0}, neverSample)
	s.Add(rootSpan("t3", 5, model.StatusOK))

	decisions := s.Sweep(time.Now().Add(time.Second))
	if len(decisions) != 1 || decisions[0].Keep {
		t.Fatalf("expected baseline trace dropped, got %+v", decisions)
	}
}

func TestReconstructsOutOfOrderSpans(t *testing.T) {
	s := New(Config{QuietPeriod: 0, MaxWait: time.Hour, MaxTraces: 10},
		Policy{BaseSampleRate: 0}, func() float64 { return 0 }) // keep-all
	// add out of order: child (start=50) before root (start=0)
	s.Add(&model.Span{TraceID: "t4", SpanID: "child", ParentSpanID: "root", StartUnixNs: 50})
	s.Add(&model.Span{TraceID: "t4", SpanID: "root", StartUnixNs: 0})

	decisions := s.Sweep(time.Now().Add(time.Second))
	if len(decisions) != 1 {
		t.Fatalf("expected one decision, got %d", len(decisions))
	}
	spans := decisions[0].Spans
	if spans[0].SpanID != "root" || spans[1].SpanID != "child" {
		t.Fatalf("spans not reconstructed in start-time order: %+v", spans)
	}
}
