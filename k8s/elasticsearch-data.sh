#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${1:-dev}"
POD="elasticsearch-0"

ELASTIC_USERNAME="elastic"
ELASTIC_PASSWORD="elasticsearch"
ES_URL="http://localhost:9200"

echo "===== Elasticsearch Indices ====="
kubectl exec -n "$NAMESPACE" "$POD" -c elasticsearch -- \
  curl -fsS -u "${ELASTIC_USERNAME}:${ELASTIC_PASSWORD}" \
  "${ES_URL}/_cat/indices?v"

echo
echo "===== Logs Count ====="
kubectl exec -n "$NAMESPACE" "$POD" -c elasticsearch -- \
  curl -fsS -u "${ELASTIC_USERNAME}:${ELASTIC_PASSWORD}" \
  "${ES_URL}/logs/_count?pretty"

echo
echo "===== Logs Data (First 10 Documents) ====="
kubectl exec -n "$NAMESPACE" "$POD" -c elasticsearch -- \
  curl -fsS -u "${ELASTIC_USERNAME}:${ELASTIC_PASSWORD}" \
  "${ES_URL}/logs/_search?size=10&pretty"

echo
