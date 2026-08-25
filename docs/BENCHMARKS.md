# Benchmarks

Measured on a single developer machine (Apple Silicon, Docker Desktop) running
the full stack via `docker compose` — Kafka (KRaft, 12 partitions), ClickHouse,
and the collector / sampler / writer / query services — with load driven by the
built-in OpenTelemetry generator (`cmd/loadgen`).

These are local numbers meant to demonstrate the pipeline's behavior and scaling
shape, not a tuned production benchmark.

## Ingestion throughput

`loadgen` emits nested 5-span traces across a `frontend → api → {auth, db,
cache}` topology over OTLP/gRPC. Throughput scales cleanly with the target rate:

| Target trace rate | Achieved traces/sec | Achieved spans/sec |
|-------------------|---------------------|--------------------|
| 5,000             | 5,029               | 25,146             |
| 10,000            | 10,082              | 50,408             |
| 20,000            | 20,155              | 100,774            |
| 50,000            | 52,224              | **261,122**        |

The collector reported ingested == produced with **zero dropped spans** across
runs; memory stays bounded via the batching producer, bounded buffer (which
returns backpressure when full), and the writer's bounded in-flight batches.

## Tail-sampling volume reduction

With the default policy (keep all errors, keep root latency ≥ 250 ms, plus a 5%
probabilistic baseline) over ~6.5M observed spans:

```
spans_seen = 6,563,987
spans_kept =   924,181   (error + slow + probabilistic)
reduction  = 1 - kept/seen = 85.9%
```

So roughly **86% of span volume is dropped** while retaining every error trace
and every slow trace — the anomalous requests you actually want to inspect.

## Query latency (p95)

Analytical endpoints are served from ClickHouse materialized views. Over a
dataset of ~826k stored spans, measured from the query service's own Prometheus
histogram (`traceforge_query_duration_seconds`):

| Endpoint            | p95 latency |
|---------------------|-------------|
| `/api/services`     | < 10 ms     |
| `/api/latency`      | < 10 ms     |
| `/api/dependencies` | < 5 ms      |

Single-digit-millisecond p95 because the heavy aggregation (percentiles, RPS,
dependency edges) is pre-computed at write time by the materialized views;
endpoints read small pre-aggregated rows.

## Reproducing

```bash
make up                                    # start the full stack
make bench                                 # throughput sweep (this table)
# volume reduction:
curl -s localhost:9102/metrics | grep sampler_spans
# query p95:
curl -s localhost:9104/metrics | grep query_duration_seconds_bucket
```
