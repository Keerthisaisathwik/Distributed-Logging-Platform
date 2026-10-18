cql() {
  kubectl exec -n dev cassandra-0 -- \
    bash -c 'cqlsh -u "$CASSANDRA_USER" -p "$CASSANDRA_PASSWORD" -e "$1"' \
    _ "$1"
}


echo "===== Logging Tables ====="
cql "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'logging';"

echo "===== Logs by ID ====="
cql "SELECT * FROM logging.logs_by_id LIMIT 10;"

echo "===== Logs by Level ====="
cql "SELECT * FROM logging.logs_by_level LIMIT 10;"

echo "===== Logs by Service ====="
cql "SELECT * FROM logging.logs_by_service LIMIT 10;"

echo "===== Logs by Trace ID ====="
cql "SELECT * FROM logging.logs_by_traceid LIMIT 10;"
