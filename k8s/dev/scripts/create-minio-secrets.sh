#!/bin/bash

set -e

NAMESPACE="${1:-dev}"

MINIO_ROOT_USER="admin"
MINIO_ROOT_PASSWORD="minioadmin"

echo "Kubernetes namespace: $NAMESPACE"
echo "MinIO username: $MINIO_ROOT_USER"
echo "MinIO password: $MINIO_ROOT_PASSWORD"

kubectl create secret generic minio-credentials \
  -n "$NAMESPACE" \
  --from-literal=root-user="$MINIO_ROOT_USER" \
  --from-literal=root-password="$MINIO_ROOT_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

unset MINIO_ROOT_USER
unset MINIO_ROOT_PASSWORD

echo "✅ MinIO credentials Secret created in namespace: $NAMESPACE"