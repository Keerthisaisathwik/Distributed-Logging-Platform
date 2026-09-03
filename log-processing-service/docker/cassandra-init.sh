#!/bin/sh
set -e

echo "Waiting for Cassandra..."

until cqlsh cassandra 9042 -e "DESCRIBE KEYSPACES" >/dev/null 2>&1
do
    echo "Cassandra not ready..."
    sleep 3
done

echo "Cassandra ready."

echo "Creating Cassandra schema..."
cqlsh cassandra 9042 -f /init-schema.cql

echo "Cassandra schema initialization complete."