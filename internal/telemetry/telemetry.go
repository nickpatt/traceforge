// Package telemetry provides shared logging and Prometheus metrics helpers so
// every service exposes /metrics and /healthz consistently.
package telemetry

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewLogger returns a structured JSON logger tagged with the service name.
func NewLogger(service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h).With("service", service)
}

// ServeAdmin starts an HTTP server exposing /healthz and /metrics.
// It blocks; run it in a goroutine.
func ServeAdmin(addr string, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	log.Info("admin server listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("admin server stopped", "err", err)
	}
}

// Common metrics registered against the default registry. Services increment
// the ones relevant to them.
var (
	SpansIngested = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "traceforge_spans_ingested_total",
		Help: "Spans accepted by the collector.",
	}, []string{"transport"})

	SpansProduced = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "traceforge_spans_produced_total",
		Help: "Spans produced to Kafka.",
	})

	SpansWritten = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "traceforge_spans_written_total",
		Help: "Spans written to ClickHouse.",
	})

	TracesSampledIn = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "traceforge_traces_sampled_in_total",
		Help: "Traces kept by the tail sampler.",
	})

	TracesSampledOut = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "traceforge_traces_sampled_out_total",
		Help: "Traces dropped by the tail sampler.",
	})

	BufferedTraces = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "traceforge_sampler_buffered_traces",
		Help: "Traces currently buffered awaiting a sampling decision.",
	})

	QueryLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "traceforge_query_duration_seconds",
		Help:    "Query API handler latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"endpoint"})
)

func init() {
	prometheus.MustRegister(
		SpansIngested, SpansProduced, SpansWritten,
		TracesSampledIn, TracesSampledOut, BufferedTraces, QueryLatency,
	)
}
