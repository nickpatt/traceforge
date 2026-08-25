# Architecture

This document walks through TraceForge end-to-end: the lifecycle of a span, the
role of each service, and the design decisions behind them. For the full
specification (data model, schema, milestones) see [../SPEC.md](../SPEC.md).

## The lifecycle of a span

```
 ┌────────────┐   OTLP    ┌───────────┐   Kafka: spans     ┌──────────┐
 │ Instrumented│ ────────► │ collector │ ─────(key=trace_id)─► │  sampler │
 │  service /  │  gRPC/HTTP└───────────┘                    └────┬─────┘
 │  OTel SDK   │                                                 │ keep?
 └────────────┘                                                  ▼
                                               Kafka: spans.kept │
                                                                 ▼
                                                           ┌──────────┐
                                                           │  writer  │
                                                           └────┬─────┘
                                                     batched    │ INSERT
                                                     async      ▼
                                                          ┌────────────┐
                                                          │ ClickHouse │◄── query API (SQL)
                                                          │  spans +   │
                                                          │  mat views │
                                                          └────────────┘
```

### 1. Ingestion — `collector`
An OpenTelemetry SDK or Collector exports spans to TraceForge using **OTLP**,
the vendor-neutral wire format. The collector speaks both transports:

- **gRPC** on `:4317` — `TraceService/Export` (see `internal/otlp/server.go`).
- **HTTP** on `:4318` — `POST /v1/traces` with a protobuf body.

Each incoming `ExportTraceServiceRequest` carries resource/scope/span nesting.
`internal/otlp/convert.go` **flattens** it into the internal `model.Span`: one
flat row per span with `service.name` lifted out of the resource attributes,
attributes stringified, and status/kind normalized.

Flattened spans are produced to the Kafka topic `spans` **keyed by `trace_id`**.
The key matters: Kafka routes identical keys to the same partition, so **every
span of a trace lands on the same partition**. That gives the sampler locality
(it can buffer a whole trace without cross-partition coordination) and ordering
within a trace.

**Backpressure:** the Kafka producer has a bounded buffer. When it fills, the
sink returns an error, which the OTLP server translates to gRPC
`RESOURCE_EXHAUSTED` / HTTP `429` — the OTLP-compliant "slow down" signal.
Memory stays bounded instead of ballooning under load.

### 2. Tail-based sampling — `sampler`
Head sampling (decide at trace *start*) can't know if a trace will error or be
slow. **Tail sampling decides after seeing the whole trace.** The tradeoff is
you must buffer spans until the trace is "done."

`internal/sampling` buffers spans per `trace_id`:

- **Completion detection** (out-of-order safe): a trace is decided when the root
  span has arrived **and** no new spans have shown up for a `QuietPeriod`, or a
  hard `MaxWait` timeout fires (covers dropped/never-arriving roots).
- **Reconstruction:** on decision, buffered spans are sorted by start time and
  emitted in order, so late/out-of-order arrivals still render as a clean
  waterfall.
- **Policies** (first match keeps the whole trace):
  1. any `ERROR` span → keep,
  2. root duration ≥ latency threshold → keep,
  3. otherwise keep with a small baseline probability.

Kept traces are forwarded to `spans.kept`. This is where the "reduced stored
span volume by X%" number comes from — compare kept vs. total, or run the writer
in `NO_SAMPLING` mode for an A/B baseline.

**Memory bound:** the buffer caps total traces; beyond the cap the oldest are
force-decided so RAM stays bounded under sustained load.

### 3. Persistence — `writer` + ClickHouse
`writer` consumes `spans.kept` and performs **batched, asynchronous bulk
inserts**. Rather than one insert per span (slow) it accumulates up to
`BATCH_MAX_ROWS` or `BATCH_MAX_WAIT`, then fires an async insert. A bounded
number of in-flight batches (`MAX_INFLIGHT`) provides backpressure: when
ClickHouse is slow, the writer stops consuming and Kafka lag absorbs the burst.

ClickHouse stores spans in a `ReplacingMergeTree` keyed by
`(service_name, start_time, trace_id, span_id)`. It's columnar, so analytical
scans (percentiles, group-bys) are fast, and `LowCardinality`/`Map` types keep
storage tight. At-least-once delivery + `ReplacingMergeTree` de-dup means
retries don't double-count.

**Materialized views** pre-aggregate at write time:
- `mv_service_latency` — per-service, per-minute request count and
  `quantileState` for p50/p95/p99 (RPS is `request_count / 60`).
- `mv_dependencies` — parent-service → child-service call/error counts, built by
  self-joining spans on `parent_span_id`.

### 4. Analytics — `query`
`internal/query` serves JSON over ClickHouse:
- `/api/traces/:trace_id` — ordered spans for the **waterfall** view.
- `/api/latency?service=&since=` — p50/p95/p99 time series (**heatmap**), read
  via `quantileMerge` over the materialized view.
- `/api/dependencies?since=` — edges for the **service dependency graph**.

Because the heavy aggregation lives in materialized views, these endpoints read
small pre-aggregated rows and stay low-latency — the source of the "p95 X ms
query latency" number.

### 5. Load testing — `loadgen`
`internal/loadgen` uses the **real OpenTelemetry Go SDK** to emit OTLP, so it
exercises the exact production path. It models a topology
(`frontend → api → {auth, db, cache}`) with configurable RPS, concurrency, and
error/slow rates, giving the dependency graph and heatmaps realistic shape and
producing the "X+ spans/sec across X concurrent services" numbers.

## Why these technologies

| Choice        | Reason                                                                 |
|---------------|------------------------------------------------------------------------|
| **OTLP**      | Any OTel SDK/Collector works with zero app changes.                    |
| **Kafka**     | Decouples ingest from storage; `trace_id` keying co-locates a trace.   |
| **ClickHouse**| Columnar store built for high-throughput analytical queries.           |
| **Go**        | Cheap goroutines for high-concurrency ingest; single static binaries.  |
| **Tail sampling** | Keeps anomalous (error/slow) traces while dropping boring volume. |

## Observability of the platform itself
Every service exposes Prometheus metrics on `/metrics` (ports `9101–9104`):
spans ingested/produced/written, traces sampled in/out, buffered traces, and
per-endpoint query latency — the same metrics used to fill in the resume `[X]`s.
