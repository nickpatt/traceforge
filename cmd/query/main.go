// Command query serves the analytical HTTP API (waterfall, latency heatmap,
// service dependency graph) backed by ClickHouse.
package main

import (
	"net/http"

	"github.com/nickpatt/traceforge/internal/clickhouse"
	"github.com/nickpatt/traceforge/internal/config"
	"github.com/nickpatt/traceforge/internal/query"
	"github.com/nickpatt/traceforge/internal/telemetry"
)

func main() {
	log := telemetry.NewLogger("query")

	conn, err := clickhouse.Connect(config.Env("CLICKHOUSE_ADDR", "localhost:9000"),
		config.Env("CLICKHOUSE_DB", "traceforge"))
	if err != nil {
		log.Error("clickhouse", "err", err)
		return
	}
	defer conn.Close()

	api := query.New(clickhouse.NewQueries(conn))

	go telemetry.ServeAdmin(config.Env("ADMIN_ADDR", ":9104"), log)

	addr := config.Env("QUERY_ADDR", ":8080")
	log.Info("query api listening", "addr", addr)
	if err := http.ListenAndServe(addr, api.Routes()); err != nil {
		log.Error("query api", "err", err)
	}
}
