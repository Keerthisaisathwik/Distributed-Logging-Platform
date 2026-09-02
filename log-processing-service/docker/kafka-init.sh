#!/bin/sh
set -ex

echo "Waiting for Kafka cluster..."

until /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --list >/dev/null 2>&1
do
    echo "Kafka cluster not ready..."
    sleep 2
done

echo "Kafka cluster ready."

echo "Creating logs.raw topic..."

/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic logs.raw \
    --partitions 4 \
    --replication-factor 3

echo "Kafka initialization complete."