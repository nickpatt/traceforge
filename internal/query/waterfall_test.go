package query

import (
	"testing"

	"github.com/nickpatt/traceforge/internal/model"
)

func TestBuildWaterfallNesting(t *testing.T) {
	// root (0-100ms) -> child (10-40ms)
	spans := []model.Span{
		{SpanID: "root", ParentSpanID: "", ServiceName: "frontend", SpanName: "GET /",
			StartUnixNs: 0, EndUnixNs: 100_000_000, StatusCode: model.StatusOK},
		{SpanID: "child", ParentSpanID: "root", ServiceName: "api", SpanName: "api.call",
			StartUnixNs: 10_000_000, EndUnixNs: 40_000_000, StatusCode: model.StatusOK},
	}

	wf := BuildWaterfall("t1", spans)
	if wf.SpanCount != 2 {
		t.Fatalf("span count: got %d", wf.SpanCount)
	}
	if len(wf.Roots) != 1 {
		t.Fatalf("roots: got %d", len(wf.Roots))
	}
	root := wf.Roots[0]
	if root.SpanID != "root" || root.DurationMs != 100 {
		t.Errorf("root: got %+v", root)
	}
	if len(root.Children) != 1 {
		t.Fatalf("children: got %d", len(root.Children))
	}
	child := root.Children[0]
	if child.OffsetMs != 10 || child.DurationMs != 30 {
		t.Errorf("child offset/duration: got %+v", child)
	}
	if wf.TotalMs != 100 {
		t.Errorf("total: got %v", wf.TotalMs)
	}
}

func TestBuildWaterfallOrphans(t *testing.T) {
	// child references a parent that is not present -> orphan, still rendered.
	spans := []model.Span{
		{SpanID: "child", ParentSpanID: "missing", ServiceName: "db", SpanName: "query",
			StartUnixNs: 5_000_000, EndUnixNs: 8_000_000},
	}
	wf := BuildWaterfall("t2", spans)
	if len(wf.Roots) != 0 {
		t.Errorf("expected no roots, got %d", len(wf.Roots))
	}
	if len(wf.Orphans) != 1 || wf.Orphans[0].SpanID != "child" {
		t.Errorf("expected 1 orphan, got %+v", wf.Orphans)
	}
}

func TestBuildWaterfallEmpty(t *testing.T) {
	wf := BuildWaterfall("t3", nil)
	if wf.SpanCount != 0 || len(wf.Roots) != 0 {
		t.Errorf("expected empty waterfall, got %+v", wf)
	}
}
