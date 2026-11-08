// Package query exposes the analytical HTTP API over ClickHouse.
package query

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/nickpatt/traceforge/internal/clickhouse"
	"github.com/nickpatt/traceforge/internal/telemetry"
	"github.com/nickpatt/traceforge/web"
)

// API bundles handlers.
type API struct{ q *clickhouse.Queries }

// New creates the API.
func New(q *clickhouse.Queries) *API { return &API{q: q} }

// Routes returns the chi router.
func (a *API) Routes() http.Handler {
	r := chi.NewRouter()
	r.Get("/api/traces", instrument("traces", a.listTraces))
	r.Get("/api/traces/{traceID}", instrument("trace", a.getTrace))
	r.Get("/api/latency", instrument("latency", a.getLatency))
	r.Get("/api/services", instrument("services", a.getServices))
	r.Get("/api/dependencies", instrument("dependencies", a.getDependencies))
	// The web UI (web/static) is embedded in the binary and served at /.
	r.Handle("/*", http.FileServer(http.FS(web.Static())))
	return r
}

// getTrace returns the trace as a waterfall: spans nested under their parents
// with offsets relative to the trace start.
func (a *API) getTrace(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "traceID")
	spans, err := a.q.Trace(r.Context(), id)
	if err == nil && len(spans) == 0 {
		http.Error(w, "trace not found", http.StatusNotFound)
		return
	}
	writeJSON(w, BuildWaterfall(id, spans), err)
}

// listTraces lists recent root spans. ?sort=slowest orders by duration,
// ?errors=1 keeps failed traces, ?limit= caps the list (default 25, max 200).
func (a *API) listTraces(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil || limit <= 0 {
		limit = 25
	}
	if limit > 200 {
		limit = 200
	}
	ts, err := a.q.RecentTraces(r.Context(), since(r, 15*time.Minute),
		q.Get("sort") == "slowest", q.Get("errors") == "1", limit)
	writeJSON(w, ts, err)
}

func (a *API) getServices(w http.ResponseWriter, r *http.Request) {
	st, err := a.q.Services(r.Context(), since(r, 15*time.Minute))
	writeJSON(w, st, err)
}

func (a *API) getLatency(w http.ResponseWriter, r *http.Request) {
	svc := r.URL.Query().Get("service")
	pts, err := a.q.Latency(r.Context(), svc, since(r, time.Hour))
	writeJSON(w, pts, err)
}

func (a *API) getDependencies(w http.ResponseWriter, r *http.Request) {
	edges, err := a.q.Dependencies(r.Context(), since(r, time.Hour))
	writeJSON(w, edges, err)
}

func since(r *http.Request, def time.Duration) time.Duration {
	if v := r.URL.Query().Get("since"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func writeJSON(w http.ResponseWriter, v any, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func instrument(name string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t := time.Now()
		h(w, r)
		telemetry.QueryLatency.WithLabelValues(name).Observe(time.Since(t).Seconds())
	}
}
