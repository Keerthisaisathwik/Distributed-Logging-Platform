#!/bin/bash

echo "===== Starting Port Forwards ====="

kubectl port-forward -n dev svc/file-ingestion 8080:8080 &
kubectl port-forward -n dev svc/minio 9001:9001 &
kubectl port-forward -n dev svc/flink-jobmanager 8081:8081 &

echo
echo "File Ingestion: http://localhost:8080"
echo "MinIO Console:  http://localhost:9001"
echo
echo "Port forwards are running."
echo "Press Ctrl+C to stop them."

wait