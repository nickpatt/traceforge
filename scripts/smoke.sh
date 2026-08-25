#!/usr/bin/env bash
#
# End-to-end smoke test: brings up the stack, drives a short load, and asserts
# that spans flow OTLP -> collector -> Kafka -> sampler -> writer -> ClickHouse
# and are queryable via the API.
#
# Usage: ./scripts/smoke.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="docker compose -f ${ROOT}/deploy/docker-compose.yml"

echo "==> Starting stack"
${COMPOSE} up -d --build

echo "==> Waiting for collector to accept OTLP"
for i in $(seq 1 30); do
  if curl -sf localhost:9101/healthz >/dev/null 2>&1; then break; fi
  sleep 2
done

echo "==> Generating load (2000 rps for 10s)"
go run "${ROOT}/cmd/loadgen" --endpoint localhost:4317 --rps 2000 --duration 10s --concurrency 12

echo "==> Waiting for pipeline to drain to ClickHouse"
sleep 12

ROWS=$(docker exec traceforge-clickhouse-1 clickhouse-client --query "SELECT count() FROM traceforge.spans")
echo "==> ClickHouse rows: ${ROWS}"
if [ "${ROWS}" -lt 1000 ]; then
  echo "FAIL: expected spans in ClickHouse, got ${ROWS}"; exit 1
fi

echo "==> Checking query API"
SERVICES=$(curl -sf "localhost:8080/api/services?since=1h")
echo "    services: ${SERVICES}"
DEPS=$(curl -sf "localhost:8080/api/dependencies?since=1h")
echo "    dependencies: ${DEPS}"

if ! echo "${DEPS}" | grep -q "frontend"; then
  echo "FAIL: dependency graph missing expected edges"; exit 1
fi

echo "==> Sampling reduction"
curl -sf localhost:9102/metrics | grep -E "traceforge_sampler_spans_(seen|kept)_total" | grep -v '#'

echo "PASS: end-to-end smoke test succeeded"
