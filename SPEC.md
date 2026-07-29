# TraceForge — Technical Specification

> A distributed tracing platform: OTLP-compatible ingestion, Kafka-partitioned
> event routing, tail-based sampling, out-of-order trace reconstruction, and
> columnar analytics on ClickHouse.

**Stack:** Go · OpenTelemetry (OTLP) · gRPC/HTTP · Kafka · ClickHouse · Docker

---

## 1. Goals & Non-Goals

### Goals
- Accept spans over the **OTLP** protocol (gRPC and HTTP/protobuf) so any
  OpenTelemetry SDK or Collector can point at TraceForge with zero code changes.
- Decouple ingestion from storage using **Kafka**, partitioning by `trace_id`
  so all spans of a trace land on the same partition (ordering + locality).
- Perform **tail-based sampling**: buffer whole traces, then keep/drop based on
  latency, errors, and rate policies — decided *after* the trace is complete.
- Reconstruct **out-of-order** and late-arriving spans into coherent traces.
- Persist spans in **ClickHouse** and expose **materialized views** for
  per-service RPS and p50/p95/p99 latency.
- Serve a **query API** powering waterfall views, latency heatmaps, and an
  auto-generated **service dependency graph**.
- Ship a **load generator** to benchmark sustained spans/sec under bounded
  memory via batching, backpressure, and async writes.

### Non-Goals (v1)
- Metrics and logs pipelines (traces only).
- A polished web UI (query API + JSON is enough; optional lightweight UI later).
- Multi-tenant auth / RBAC.
- Horizontal auto-scaling / k8s operator (single-node compose is the target).

---

## 2. High-Level Architecture

```
                 OTLP gRPC :4317
  SDKs / OTel    OTLP HTTP :4318          Kafka topic: spans
  Collectors  ───────────────────►  ┌──────────────┐   (partitioned by trace_id)
                                     │  collector   │──────────────┐
                                     │ (ingest svc) │              │
                                     └──────────────┘              ▼
                                                          ┌───────────────────┐
                                                          │      Kafka        │
                                                          │  topic: spans     │
                                                          │  key = trace_id   │
                                                          └───────────────────┘
                                                             │            │
                              ┌──────────────────────────────┘            │
                              ▼                                           ▼
                     ┌─────────────────┐                        ┌──────────────────┐
                     │    sampler      │  keep decisions        │     writer       │
                     │ tail-based +    │───────────────────────►│  batch inserts   │
                     │ reconstruction  │  topic: spans.kept     │  to ClickHouse   │
                     └─────────────────┘                        └──────────────────┘
                                                                          │
                                                                          ▼
                                                                 ┌──────────────────┐
                                                                 │   ClickHouse     │
                                                                 │  spans (MergeTree)│
                                                                 │  + materialized  │
                                                                 │    views (MVs)   │
                                                                 └──────────────────┘
                                                                          ▲
                                                                          │ SQL
                                                                 ┌──────────────────┐
                                                                 │   query API      │
                                                                 │ waterfall /      │
                                                                 │ heatmap / deps / │
                                                                 │ RPS / latency    │
                                                                 └──────────────────┘
```

### Data flow
1. **collector** receives OTLP `ExportTraceServiceRequest` over gRPC/HTTP,
   flattens resource+scope+span into a normalized `Span` record, and produces
   to Kafka topic `spans` **keyed by `trace_id`**.
2. **sampler** consumes `spans`, groups by `trace_id` in an in-memory buffer,
   and closes a trace when it is "complete" (root seen + quiet period) or a
   `max_wait` timeout fires. It applies tail policies and forwards kept traces
   to `spans.kept`.
3. **writer** consumes `spans.kept`, batches rows, and does async bulk inserts
   into ClickHouse.
4. **ClickHouse** stores spans and continuously updates materialized views for
   RPS and latency percentiles.
5. **query API** answers analytical queries for the UI/consumers.

> **Simplification switch:** `sampler` and `writer` can run as one process or
> two. v1 default runs them separately so backpressure is observable. A
> `NO_SAMPLING` mode lets `writer` consume `spans` directly for A/B comparison
> of stored span volume (this is how the "reduced stored span volume by X%"
> number is measured).

---

## 3. Components

### 3.1 `collector` (ingest service)
- Implements OTLP:
  - gRPC: `opentelemetry.proto.collector.trace.v1.TraceService/Export` on `:4317`.
  - HTTP: `POST /v1/traces` (protobuf + JSON) on `:4318`.
- Normalizes each span to the internal `Span` struct (see §4).
- Kafka producer with:
  - `key = trace_id` (16-byte hex) → co-partitions all spans of a trace.
  - batching (`linger.ms`, `batch.size`) for throughput.
  - `acks=1` (tunable) balancing durability vs latency.
- Backpressure: bounded producer queue; when full, gRPC returns
  `RESOURCE_EXHAUSTED` / HTTP `429` so clients retry (OTLP-compliant).
- Health/metrics: `/healthz`, `/metrics` (Prometheus).

### 3.2 `sampler` (tail-based sampling + reconstruction)
- Consumes `spans`, keyed by partition so a trace stays local.
- **Trace buffer:** `map[trace_id] -> traceBuffer{spans, firstSeen, lastSeen,
  hasRoot}`. Bounded by `max_traces` and total span count (memory guard).
- **Completion detection** (out-of-order safe):
  - A trace is *decided* when either:
    - root span seen **and** no new spans for `quiet_period` (e.g. 2s), or
    - `max_wait` elapsed since `firstSeen` (e.g. 10s) — late spans after a
      decision are handled by a `late_span` policy (drop or best-effort append).
- **Tail policies** (evaluated in order, first match wins → keep):
  1. `error`: any span `status = ERROR` → keep.
  2. `slow`: root duration ≥ `latency_threshold` (or dynamic pXX) → keep.
  3. `probabilistic`: keep with probability `p` (e.g. 0.05) for baseline traffic.
  - Everything else → drop.
- Emits kept traces to `spans.kept`. Records `sampled_in`/`sampled_out`
  counters and estimated volume reduction.
- **Reconstruction:** on decision, spans are sorted by `start_time`, parent
  links validated, and orphan spans (missing parent) flagged
  `reconstructed=false` but still stored so the waterfall degrades gracefully.

### 3.3 `writer` (ClickHouse sink)
- Consumes `spans.kept` (or `spans` in `NO_SAMPLING` mode).
- **Batching:** accumulate up to `batch_max_rows` or `batch_max_wait`, then
  async bulk `INSERT`. Multiple in-flight batches bounded by `max_inflight`
  (backpressure to the consumer / Kafka lag).
- Idempotency: `ReplacingMergeTree(ingested_at)` de-dups on
  `(trace_id, span_id)` so retries don't double-count.
- Commits Kafka offsets only after a successful insert (at-least-once).

### 3.4 `query` (analytics API)
HTTP/JSON API backed by ClickHouse SQL. Endpoints:
- `GET /api/traces/:trace_id` — full span list for the **waterfall** view
  (nested by parent_span_id, ordered by start_time).
- `GET /api/traces?service=&min_duration=&status=&limit=` — trace search.
- `GET /api/services` — list of services + current RPS.
- `GET /api/latency?service=&quantiles=50,95,99&step=1m` — time-series of
  p50/p95/p99 for the **latency heatmap**.
- `GET /api/rps?service=&step=1m` — requests/sec per service over time.
- `GET /api/dependencies?since=1h` — service **dependency graph** (edges from
  parent-service → child-service with call counts + error rates).

### 3.5 `loadgen` (OTLP traffic generator)
- Generates realistic synthetic traces using the real OpenTelemetry Go SDK
  exporting OTLP to `collector` — so it exercises the true ingestion path.
- Configurable: `--services`, `--rps`, `--duration`, `--depth` (span tree
  depth), `--error-rate`, `--slow-rate`, `--concurrency`.
- Models a service topology (e.g. `frontend → api → {auth, db, cache}`) so the
  dependency graph and heatmaps have meaningful shape.
- Reports achieved spans/sec, p50/p95/p99 client-side latency, and drop/429
  counts to validate backpressure.

---

## 4. Data Model

### Internal `Span` (Go / Kafka payload — protobuf or JSON)
| field              | type              | notes                                  |
|--------------------|-------------------|----------------------------------------|
| `trace_id`         | string (32 hex)   | Kafka partition key                    |
| `span_id`          | string (16 hex)   |                                        |
| `parent_span_id`   | string (16 hex)   | empty ⇒ root                           |
| `service_name`     | string            | from resource attr `service.name`      |
| `span_name`        | string            |                                        |
| `kind`             | enum              | SERVER/CLIENT/INTERNAL/PRODUCER/CONSUMER|
| `start_time_unix_nano` | uint64        |                                        |
| `end_time_unix_nano`   | uint64        |                                        |
| `duration_ns`      | uint64            | derived                                |
| `status_code`      | enum              | UNSET/OK/ERROR                         |
| `status_message`   | string            |                                        |
| `attributes`       | map<string,string>| flattened                              |
| `resource_attrs`   | map<string,string>| flattened                              |
| `ingested_at`      | uint64            | writer stamp (for ReplacingMergeTree)  |

### ClickHouse `spans` table (MergeTree family)
```sql
CREATE TABLE spans (
  trace_id       String,
  span_id        String,
  parent_span_id String,
  service_name   LowCardinality(String),
  span_name      LowCardinality(String),
  kind           LowCardinality(String),
  start_time     DateTime64(9),
  duration_ns    UInt64,
  status_code    LowCardinality(String),
  status_message String,
  attributes     Map(String, String),
  ingested_at    DateTime64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toDate(start_time)
ORDER BY (service_name, start_time, trace_id, span_id)
TTL toDateTime(start_time) + INTERVAL 7 DAY;
```

### Materialized views
```sql
-- Per-service, per-minute latency percentiles (state-based for correct merge).
CREATE MATERIALIZED VIEW mv_service_latency
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket) ORDER BY (service_name, bucket) AS
SELECT
  service_name,
  toStartOfMinute(start_time)                       AS bucket,
  count()                                            AS request_count,
  quantileState(0.50)(duration_ns)                   AS p50_state,
  quantileState(0.95)(duration_ns)                   AS p95_state,
  quantileState(0.99)(duration_ns)                   AS p99_state
FROM spans
WHERE parent_span_id = ''            -- root spans = one request
GROUP BY service_name, bucket;

-- Service dependency edges (parent service -> child service).
CREATE MATERIALIZED VIEW mv_dependencies
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket) ORDER BY (parent_service, child_service, bucket) AS
SELECT
  p.service_name AS parent_service,
  c.service_name AS child_service,
  toStartOfMinute(c.start_time) AS bucket,
  count() AS call_count,
  countIf(c.status_code = 'ERROR') AS error_count
FROM spans AS c
INNER JOIN spans AS p ON c.parent_span_id = p.span_id AND c.trace_id = p.trace_id
GROUP BY parent_service, child_service, bucket;
```
> RPS is derived from `request_count / 60` off `mv_service_latency`.
> Percentiles are read with `quantileMerge(0.95)(p95_state)`.

---

## 5. Kafka Topics
| topic         | key        | partitions | purpose                          |
|---------------|------------|------------|----------------------------------|
| `spans`       | `trace_id` | 12         | raw normalized spans from ingest |
| `spans.kept`  | `trace_id` | 12         | tail-sampled traces to persist   |
| `spans.dlq`   | `trace_id` | 3          | poison messages / write failures |

Keying by `trace_id` guarantees all spans of a trace share a partition, which
makes the sampler's per-trace buffering correct without cross-partition shuffling.

---

## 6. Backpressure & Memory Bounds (Bullet 4)
- **collector:** bounded Kafka producer buffer → returns 429/RESOURCE_EXHAUSTED
  when saturated instead of growing unbounded.
- **sampler:** hard caps on `max_traces` and total buffered spans; when
  exceeded, forces early decisions (evict oldest) — memory stays bounded.
- **writer:** bounded in-flight batches; slows Kafka consumption (natural
  backpressure via consumer lag) rather than buffering in RAM.
- **async writes:** insert batches on a worker pool; producers/consumers never
  block on ClickHouse round-trips beyond the in-flight limit.

These are what let us claim "sustained X spans/sec with bounded memory."

---

## 7. Metrics to Fill Resume Blanks
Instrument everything with Prometheus so the `[X]` numbers are **measured**, not
made up:

| Bullet blank                    | Where measured                                   |
|---------------------------------|--------------------------------------------------|
| stored span volume reduced X%   | `sampler`: `1 - sampled_in/total` (run with vs without sampling) |
| query p95 X ms                  | `query`: per-endpoint histogram; report `/api/latency` p95 |
| X+ spans/sec                    | `loadgen` achieved rate + `collector` ingest counter |
| X concurrent services           | `loadgen --services` (topology size)             |

A `make bench` target runs loadgen at increasing rates and prints a table.

---

## 8. Milestones
1. **M1 – Skeleton & compose:** repo layout, `docker-compose` (Kafka+ClickHouse),
   go.mod, health endpoints. *(this commit)*
2. **M2 – Ingest path:** OTLP gRPC/HTTP → Kafka; loadgen v0 → verify messages.
3. **M3 – Writer + schema:** ClickHouse tables + batched async writer; end-to-end
   span visible via SQL.
4. **M4 – Query API:** waterfall + trace search endpoints.
5. **M5 – Materialized views:** RPS + p50/p95/p99 + dependency graph endpoints.
6. **M6 – Tail sampling:** buffering, completion detection, policies, volume-
   reduction measurement.
7. **M7 – Load test & tuning:** backpressure, batching knobs, fill in the `[X]`s.
8. **M8 – Polish:** README diagrams, dashboards, optional minimal UI.

---

## 9. Repository Layout
```
traceforge/
├── cmd/                    # main() entrypoints (thin)
│   ├── collector/
│   ├── sampler/
│   ├── writer/
│   ├── query/
│   └── loadgen/
├── internal/
│   ├── model/              # Span struct + conversions
│   ├── otlp/               # OTLP gRPC/HTTP servers + decoding
│   ├── kafka/              # producer/consumer wrappers
│   ├── sampling/           # buffer, completion detection, policies
│   ├── clickhouse/         # schema, batched writer, queries
│   ├── query/              # HTTP handlers
│   ├── loadgen/            # topology + OTLP SDK traffic
│   └── telemetry/          # prometheus metrics, logging
├── deploy/
│   ├── docker-compose.yml
│   └── clickhouse/init/    # *.sql schema + MVs (auto-applied)
├── config/                 # yaml/env defaults
├── Makefile
├── go.mod
├── SPEC.md
└── README.md
```

---

## 10. Key Libraries
- `go.opentelemetry.io/proto/otlp` — OTLP protobuf types.
- `google.golang.org/grpc` — gRPC server.
- `github.com/twmb/franz-go` — modern Kafka client (producer/consumer, good perf).
- `github.com/ClickHouse/clickhouse-go/v2` — ClickHouse native client (batch insert).
- `go.opentelemetry.io/otel` + `otlptracegrpc` — used by `loadgen` to emit real OTLP.
- `github.com/prometheus/client_golang` — metrics.
- `github.com/go-chi/chi/v5` — HTTP routing for query API.
