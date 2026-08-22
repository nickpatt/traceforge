package clickhouse

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nickpatt/traceforge/internal/model"
)

// Queries provides analytical reads for the query API.
type Queries struct{ conn driver.Conn }

// NewQueries wraps a connection.
func NewQueries(conn driver.Conn) *Queries { return &Queries{conn: conn} }

// Trace returns all spans of a trace ordered by start time (waterfall view).
func (q *Queries) Trace(ctx context.Context, traceID string) ([]model.Span, error) {
	rows, err := q.conn.Query(ctx, `
		SELECT trace_id, span_id, parent_span_id, service_name, span_name,
		       kind, start_time, duration_ns, status_code, status_message
		FROM spans WHERE trace_id = ? ORDER BY start_time`, traceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Span
	for rows.Next() {
		var s model.Span
		var st time.Time
		var kind, status string
		if err := rows.Scan(&s.TraceID, &s.SpanID, &s.ParentSpanID, &s.ServiceName,
			&s.SpanName, &kind, &st, new(uint64), &status, &s.StatusMsg); err != nil {
			return nil, err
		}
		s.Kind = model.SpanKind(kind)
		s.StatusCode = model.StatusCode(status)
		s.StartUnixNs = uint64(st.UnixNano())
		out = append(out, s)
	}
	return out, rows.Err()
}

// LatencyPoint is one time-bucket of latency percentiles for a service.
type LatencyPoint struct {
	Bucket   time.Time `json:"bucket"`
	Requests uint64    `json:"requests"`
	P50Ms    float64   `json:"p50_ms"`
	P95Ms    float64   `json:"p95_ms"`
	P99Ms    float64   `json:"p99_ms"`
}

// Latency reads p50/p95/p99 from the materialized view for a service.
func (q *Queries) Latency(ctx context.Context, service string, since time.Duration) ([]LatencyPoint, error) {
	rows, err := q.conn.Query(ctx, `
		SELECT bucket,
		       sum(request_count) AS reqs,
		       quantileMerge(0.50)(p50_state)/1e6 AS p50,
		       quantileMerge(0.95)(p95_state)/1e6 AS p95,
		       quantileMerge(0.99)(p99_state)/1e6 AS p99
		FROM mv_service_latency
		WHERE service_name = ? AND bucket >= now() - INTERVAL ? SECOND
		GROUP BY bucket ORDER BY bucket`, service, int(since.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LatencyPoint
	for rows.Next() {
		var p LatencyPoint
		if err := rows.Scan(&p.Bucket, &p.Requests, &p.P50Ms, &p.P95Ms, &p.P99Ms); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DependencyEdge is one edge of the service dependency graph.
type DependencyEdge struct {
	Parent string `json:"parent"`
	Child  string `json:"child"`
	Calls  uint64 `json:"calls"`
	Errors uint64 `json:"errors"`
}

// Dependencies returns aggregated service-to-service edges.
func (q *Queries) Dependencies(ctx context.Context, since time.Duration) ([]DependencyEdge, error) {
	rows, err := q.conn.Query(ctx, `
		SELECT parent_service, child_service,
		       sum(call_count) AS calls, sum(error_count) AS errors
		FROM mv_dependencies
		WHERE bucket >= now() - INTERVAL ? SECOND
		GROUP BY parent_service, child_service
		ORDER BY calls DESC`, int(since.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DependencyEdge
	for rows.Next() {
		var e DependencyEdge
		if err := rows.Scan(&e.Parent, &e.Child, &e.Calls, &e.Errors); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
