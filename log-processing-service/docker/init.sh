#!/bin/sh
# This configuration file is no longer required for the current setup, but I'll keep it for future reference.

set -ex

echo "Init script started"

# Wait for Kafka
until kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
  sleep 2
done

# Create topic if it doesn't exist
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic logs.raw \
  --partitions 4 \
  --replication-factor 1

# Wait for Elasticsearch
until curl -s http://elasticsearch:9200 >/dev/null; do
  sleep 2
done

# Create index if it doesn't exist
curl -X PUT "http://elasticsearch:9200/logs" \
     -H "Content-Type: application/json" \
     -d '{}' || true