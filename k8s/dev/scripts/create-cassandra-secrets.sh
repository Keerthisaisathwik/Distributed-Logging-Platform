#!/bin/bash
set -euo pipefail

NAMESPACE="dev"
POD="cassandra-0"

CASSANDRA_USERNAME="cassandra_admin"
CASSANDRA_PASSWORD="cassandra_admin_password"

cql() {
  local query="$1"

  kubectl exec -n "$NAMESPACE" "$POD" -- \
    cqlsh -u "$CASSANDRA_USERNAME" \
           -p "$CASSANDRA_PASSWORD" \
           -e "$query"
}

echo "===== Logging Tables ====="
cql "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'logging';"

echo
echo "===== Logs by ID ====="
cql "SELECT * FROM logging.logs_by_id LIMIT 10;"

echo
echo "===== Logs by Level ====="
cql "SELECT * FROM logging.logs_by_level LIMIT 10;"

echo
echo "===== Logs by Service ====="
cql "SELECT * FROM logging.logs_by_service LIMIT 10;"

echo
echo "===== Logs by Trace ID ====="
cql "SELECT * FROM logging.logs_by_traceid LIMIT 10;"

echo