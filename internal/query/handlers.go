// Package query exposes the analytical HTTP API over ClickHouse.
package query

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/nickpatt/traceforge/internal/clickhouse"
	"github.com/nickpatt/traceforge/internal/telemetry"
)

// API bundles handlers.
type API struct{ q *clickhouse.Queries }

// New creates the API.
func New(q *clickhouse.Queries) *API { return &API{q: q} }

// Routes returns the chi router.
func (a *API) Routes() http.Handler {
	r := chi.NewRouter()
	r.Get("/api/traces/{traceID}", instrument("trace", a.getTrace))
	r.Get("/api/latency", instrument("latency", a.getLatency))
	r.Get("/api/dependencies", instrument("dependencies", a.getDependencies))
	return r
}

func (a *API) getTrace(w http.ResponseWriter, r *http.Request) {
	spans, err := a.q.Trace(r.Context(), chi.URLParam(r, "traceID"))
	writeJSON(w, spans, err)
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
