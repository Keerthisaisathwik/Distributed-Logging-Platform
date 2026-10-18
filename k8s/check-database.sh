#!/bin/bash

set -e

# Usage: ./k8s/start.sh dev
NAMESPACE="$1"

if [ -z "$NAMESPACE" ]; then
    echo "Usage: ./k8s/start.sh <namespace>"
    exit 1
fi

echo "===== Kafka ====="
kubectl exec -it -n dev kafka-broker-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
echo "===== Cassandra ====="
kubectl exec -it -n dev cassandra-0 -- cqlsh cassandra 9042 -e "DESCRIBE KEYSPACES;"
echo "===== Elasticsearch ====="
kubectl exec -it -n dev elasticsearch-0 -- curl -k -u elastic:YrjdqoiKESDUXOF4g4kXjCBe "http://localhost:9200/_cat/indices?v"
echo "===== MinIO ====="
kubectl exec -it -n dev minio-0 -- sh -c 'mc alias set local http://localhost:9000 admin minioadmin >/dev/null && mc ls local'