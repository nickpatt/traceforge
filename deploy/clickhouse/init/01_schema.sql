-- TraceForge ClickHouse schema. Auto-applied on container init.

CREATE DATABASE IF NOT EXISTS traceforge;

CREATE TABLE IF NOT EXISTS traceforge.spans
(
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

-- Per-service, per-minute latency percentiles (root spans = one request).
CREATE MATERIALIZED VIEW IF NOT EXISTS traceforge.mv_service_latency
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (service_name, bucket)
AS
SELECT
    service_name,
    toStartOfMinute(start_time)      AS bucket,
    count()                          AS request_count,
    quantileState(0.50)(duration_ns) AS p50_state,
    quantileState(0.95)(duration_ns) AS p95_state,
    quantileState(0.99)(duration_ns) AS p99_state
FROM traceforge.spans
WHERE parent_span_id = ''
GROUP BY service_name, bucket;

-- Service dependency edges (parent service -> child service).
CREATE MATERIALIZED VIEW IF NOT EXISTS traceforge.mv_dependencies
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (parent_service, child_service, bucket)
AS
SELECT
    p.service_name                     AS parent_service,
    c.service_name                     AS child_service,
    toStartOfMinute(c.start_time)      AS bucket,
    count()                            AS call_count,
    countIf(c.status_code = 'ERROR')   AS error_count
FROM traceforge.spans AS c
INNER JOIN traceforge.spans AS p
    ON c.parent_span_id = p.span_id AND c.trace_id = p.trace_id
GROUP BY parent_service, child_service, bucket;
