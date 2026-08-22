package query

import (
	"sort"

	"github.com/nickpatt/traceforge/internal/model"
)

// WaterfallNode is a span in the reconstructed trace tree, annotated with its
// offset from the trace start so a UI can render a waterfall directly.
type WaterfallNode struct {
	SpanID       string           `json:"span_id"`
	ParentSpanID string           `json:"parent_span_id,omitempty"`
	Service      string           `json:"service"`
	Name         string           `json:"name"`
	Kind         string           `json:"kind"`
	OffsetMs     float64          `json:"offset_ms"` // start relative to trace start
	DurationMs   float64          `json:"duration_ms"`
	Status       string           `json:"status"`
	Children     []*WaterfallNode `json:"children,omitempty"`
}

// Waterfall is the full response for GET /api/traces/:id.
type Waterfall struct {
	TraceID   string           `json:"trace_id"`
	StartTime int64            `json:"start_unix_ns"`
	TotalMs   float64          `json:"total_ms"`
	SpanCount int              `json:"span_count"`
	Roots     []*WaterfallNode `json:"roots"`
	Orphans   []*WaterfallNode `json:"orphans,omitempty"` // parent not present in trace
}

// BuildWaterfall converts a flat span list into a nested waterfall tree.
//
// It is tolerant of out-of-order and missing-parent spans: any span whose
// parent is absent from the set is surfaced under Orphans so the trace still
// renders (graceful degradation for partial traces).
func BuildWaterfall(traceID string, spans []model.Span) Waterfall {
	if len(spans) == 0 {
		return Waterfall{TraceID: traceID}
	}

	// Earliest start = trace origin.
	minStart := spans[0].StartUnixNs
	maxEnd := spans[0].StartUnixNs + spans[0].DurationNs()
	for _, s := range spans {
		if s.StartUnixNs < minStart {
			minStart = s.StartUnixNs
		}
		if end := s.StartUnixNs + s.DurationNs(); end > maxEnd {
			maxEnd = end
		}
	}

	nodes := make(map[string]*WaterfallNode, len(spans))
	for i := range spans {
		s := spans[i]
		nodes[s.SpanID] = &WaterfallNode{
			SpanID:       s.SpanID,
			ParentSpanID: s.ParentSpanID,
			Service:      s.ServiceName,
			Name:         s.SpanName,
			Kind:         string(s.Kind),
			OffsetMs:     float64(s.StartUnixNs-minStart) / 1e6,
			DurationMs:   float64(s.DurationNs()) / 1e6,
			Status:       string(s.StatusCode),
		}
	}

	var roots, orphans []*WaterfallNode
	for _, n := range nodes {
		switch {
		case n.ParentSpanID == "":
			roots = append(roots, n)
		default:
			if parent, ok := nodes[n.ParentSpanID]; ok {
				parent.Children = append(parent.Children, n)
			} else {
				orphans = append(orphans, n)
			}
		}
	}

	sortByOffset(roots)
	sortByOffset(orphans)
	for _, n := range nodes {
		sortByOffset(n.Children)
	}

	return Waterfall{
		TraceID:   traceID,
		StartTime: int64(minStart),
		TotalMs:   float64(maxEnd-minStart) / 1e6,
		SpanCount: len(spans),
		Roots:     roots,
		Orphans:   orphans,
	}
}

func sortByOffset(ns []*WaterfallNode) {
	sort.Slice(ns, func(i, j int) bool { return ns[i].OffsetMs < ns[j].OffsetMs })
}
