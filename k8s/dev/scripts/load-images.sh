#!/bin/bash

set -e

CLUSTER="${1:-logging}"

IMAGES=(
  "docker.elastic.co/elasticsearch/elasticsearch:9.1.3"
  "apache/kafka:4.3.1"
  "busybox:1.36"
  "busybox:1.37"
  "cassandra:5.0"
  "coollabsio/minio:RELEASE.2025-10-15T17-29-55Z"
  "curlimages/curl:8.18.0"
  "fluent/fluent-bit:3.2"
  "keerthisaisathwik/file-ingestion-service:latest"
  "keerthisaisathwik/log-processing-flink:latest"
)

echo "Using kind cluster: $CLUSTER"

for IMAGE in "${IMAGES[@]}"; do
  echo "Checking $IMAGE..."

  if ! docker image inspect "$IMAGE" > /dev/null 2>&1; then
    echo "ERROR: $IMAGE is not available locally."
    echo "Pull/build this image before loading it into kind."
    exit 1
  fi

  echo "Loading $IMAGE into kind..."
  kind load docker-image "$IMAGE" --name "$CLUSTER"
done

echo "All local images loaded into kind cluster: $CLUSTER"

# Command I use to run the whole setup:
# cd "/mnt/c/Users/yoga 6/github-desktop/Distributed-Logging-Platform" && \
# kind delete cluster --name logging && \
# kind create cluster --name logging && \
# kubectl create namespace dev && \
# ./k8s/dev/scripts/load-images.sh logging && \
# ./k8s/dev/scripts/create-elasticsearch-secrets.sh dev && \
# ./k8s/dev/scripts/create-minio-secrets.sh dev && \
# kubectl apply -n dev -k ./k8s/dev/ && \
# kubectl get pods -n dev -o wide -w