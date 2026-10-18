#!/bin/bash
set -euo pipefail

NAMESPACE="dev"
POD="elasticsearch-0"

ELASTIC_USERNAME="elastic"
ELASTIC_PASSWORD="elasticsearch"

echo "===== Elasticsearch Indices ====="

kubectl exec -n "$NAMESPACE" "$POD" -- \
  curl -fsS -u "${ELASTIC_USERNAME}:${ELASTIC_PASSWORD}" \
  "http://localhost:9200/_cat/indices?v"

echo
echo "===== Logs Count ====="

kubectl exec -n "$NAMESPACE" "$POD" -- \
  curl -fsS -u "${ELASTIC_USERNAME}:${ELASTIC_PASSWORD}" \
  "http://localhost:9200/logs/_count?pretty"

echo
echo "===== Logs Data (First 10 Documents) ====="

kubectl exec -n "$NAMESPACE" "$POD" -- \
  curl -fsS -u "${ELASTIC_USERNAME}:${ELASTIC_PASSWORD}" \
  "http://localhost:9200/logs/_search?size=10&pretty"

echo