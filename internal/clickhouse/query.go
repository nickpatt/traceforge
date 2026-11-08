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
		var dur uint64
		var kind, status string
		if err := rows.Scan(&s.TraceID, &s.SpanID, &s.ParentSpanID, &s.ServiceName,
			&s.SpanName, &kind, &st, &dur, &status, &s.StatusMsg); err != nil {
			return nil, err
		}
		s.Kind = model.SpanKind(kind)
		s.StatusCode = model.StatusCode(status)
		s.StartUnixNs = uint64(st.UnixNano())
		// duration_ns is stored, end time is not: rebuild it so DurationNs() is correct.
		s.EndUnixNs = s.StartUnixNs + dur
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

// ServiceStats is one row of the per-service overview: span volume, errors and
// latency percentiles over the window, computed from every span of the service
// (not only roots, which is what mv_service_latency covers).
type ServiceStats struct {
	Service string  `json:"service"`
	Spans   uint64  `json:"spans"`
	Errors  uint64  `json:"errors"`
	P50Ms   float64 `json:"p50_ms"`
	P95Ms   float64 `json:"p95_ms"`
	P99Ms   float64 `json:"p99_ms"`
}

func (q *Queries) Services(ctx context.Context, since time.Duration) ([]ServiceStats, error) {
	rows, err := q.conn.Query(ctx, `
		SELECT service_name,
		       count() AS spans,
		       countIf(status_code = 'ERROR') AS errors,
		       quantilesTDigest(0.50, 0.95, 0.99)(duration_ns) AS qs
		FROM spans
		WHERE start_time >= now64(9) - INTERVAL ? SECOND
		GROUP BY service_name
		ORDER BY spans DESC`, int(since.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServiceStats
	for rows.Next() {
		var st ServiceStats
		var qs []float64
		if err := rows.Scan(&st.Service, &st.Spans, &st.Errors, &qs); err != nil {
			return nil, err
		}
		if len(qs) == 3 {
			st.P50Ms, st.P95Ms, st.P99Ms = qs[0]/1e6, qs[1]/1e6, qs[2]/1e6
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// TraceSummary describes a trace by its root span, for picking one to open.
type TraceSummary struct {
	TraceID    string    `json:"trace_id"`
	Service    string    `json:"service"`
	Name       string    `json:"name"`
	Start      time.Time `json:"start"`
	DurationMs float64   `json:"duration_ms"`
	Status     string    `json:"status"`
}

// RecentTraces lists root spans in the window, newest first, or slowest first
// when slowest is true. errorsOnly keeps traces whose root span errored.
func (q *Queries) RecentTraces(ctx context.Context, since time.Duration, slowest, errorsOnly bool, limit int) ([]TraceSummary, error) {
	order := "start_time DESC"
	if slowest {
		order = "duration_ns DESC"
	}
	where := ""
	if errorsOnly {
		where = " AND status_code = 'ERROR'"
	}
	rows, err := q.conn.Query(ctx, `
		SELECT trace_id, service_name, span_name, start_time, duration_ns, status_code
		FROM spans
		WHERE parent_span_id = '' AND start_time >= now64(9) - INTERVAL ? SECOND`+where+`
		ORDER BY `+order+`
		LIMIT ?`, int(since.Seconds()), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TraceSummary
	for rows.Next() {
		var t TraceSummary
		var dur uint64
		if err := rows.Scan(&t.TraceID, &t.Service, &t.Name, &t.Start, &dur, &t.Status); err != nil {
			return nil, err
		}
		t.DurationMs = float64(dur) / 1e6
		out = append(out, t)
	}
	return out, rows.Err()
}
