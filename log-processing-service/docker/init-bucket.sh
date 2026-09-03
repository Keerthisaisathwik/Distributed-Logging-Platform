#!/bin/sh
set -e

echo "Waiting for MinIO..."

until mc alias set local http://minio:9000 admin minioadmin >/dev/null 2>&1
do
    echo "MinIO not ready..."
    sleep 3
done

echo "MinIO ready."

echo "Creating logging bucket..."
mc mb --ignore-existing local/logging

echo "MinIO bucket initialization complete."