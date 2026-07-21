#!/bin/sh
set -ex

echo "Waiting for Kafka..."

until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1
do
    echo "Kafka not ready..."
    sleep 2
done

echo "Creating topic..."

/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic logs.raw \
    --partitions 4 \
    --replication-factor 1

echo "Done."