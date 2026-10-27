#!/bin/bash

set -e

NAMESPACE="${1:-dev}"

# Cassandra credentials
CASSANDRA_USERNAME="cassandraAdmin"
CASSANDRA_PASSWORD="$(openssl rand -hex 24)"

# Reuse the existing Secret instead of accidentally rotating credentials.
if kubectl get secret cassandra-credentials -n "$NAMESPACE" >/dev/null 2>&1; then
    echo "✅ Existing Cassandra credentials Secret found — reusing it"
    exit 0
fi

echo "Kubernetes namespace: $NAMESPACE"
echo "Cassandra username: $CASSANDRA_USERNAME"
echo "Cassandra password: $CASSANDRA_PASSWORD"

kubectl create secret generic cassandra-credentials \
  -n "$NAMESPACE" \
  --from-literal=username="$CASSANDRA_USERNAME" \
  --from-literal=password="$CASSANDRA_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

unset CASSANDRA_USERNAME
unset CASSANDRA_PASSWORD

echo "✅ Cassandra credentials Secret created in namespace: $NAMESPACE"
