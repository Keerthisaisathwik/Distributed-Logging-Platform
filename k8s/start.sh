#!/bin/bash

set -e

# Usage: ./k8s/start.sh dev
NAMESPACE="$1"

if [ -z "$NAMESPACE" ]; then
    echo "Usage: ./k8s/start.sh <namespace>"
    exit 1
fi

cd "/mnt/c/Users/yoga 6/github-desktop/Distributed-Logging-Platform"

echo "===== Deleting existing cluster ====="
kind delete cluster --name logging

echo "===== Creating Kubernetes cluster ====="
kind create cluster --name logging

echo "===== Creating namespace: $NAMESPACE ====="
kubectl create namespace "$NAMESPACE"

echo "===== Loading Docker images ====="
./k8s/"$NAMESPACE"/scripts/load-images.sh logging

echo "===== Creating Elasticsearch secrets ====="
./k8s/"$NAMESPACE"/scripts/create-elasticsearch-secrets.sh "$NAMESPACE"

echo "===== Creating MinIO secrets ====="
./k8s/"$NAMESPACE"/scripts/create-minio-secrets.sh "$NAMESPACE"

echo "===== Creating Cassandra secrets ====="
./k8s/"$NAMESPACE"/scripts/create-cassandra-secrets.sh "$NAMESPACE"

echo "===== Applying Kubernetes manifests ====="
kubectl apply -n "$NAMESPACE" -k ./k8s/"$NAMESPACE"/

echo "===== Watching pods in $NAMESPACE ====="
kubectl get pods -n "$NAMESPACE" -o wide -w

# cd "/mnt/c/Users/yoga 6/github-desktop/Distributed-Logging-Platform" && \
# kind delete cluster --name logging && \
# kind create cluster --name logging && \
# kubectl create namespace dev && \
# ./k8s/dev/scripts/load-images.sh && \
# ./k8s/dev/scripts/create-elasticsearch-secrets.sh && \
# ./k8s/dev/scripts/create-minio-secrets.sh && \
# kubectl apply -n dev -k ./k8s/dev/ && \
# kubectl get pods -n dev -o wide -w