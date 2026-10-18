#!/bin/bash

set -Eeuo pipefail

CLUSTER="${1:-logging}"
PLATFORM="linux/amd64"

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

# Verify that the Kind cluster exists.
if ! kind get clusters | grep -Fxq "$CLUSTER"; then
  echo "ERROR: Kind cluster '$CLUSTER' does not exist."
  echo "Create it first with: kind create cluster --name $CLUSTER"
  exit 1
fi

echo "Using kind cluster: $CLUSTER"
echo "Target platform: $PLATFORM"

for IMAGE in "${IMAGES[@]}"; do
  echo
  echo "=========================================="
  echo "Processing image: $IMAGE"
  echo "=========================================="

  # Pull the image if it is missing locally.
  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "Image not found locally. Pulling $IMAGE..."
    docker pull --platform "$PLATFORM" "$IMAGE"
  fi

  # Create a temporary archive and remove it even if a command fails.
  ARCHIVE="$(mktemp)"

  cleanup() {
    rm -f "$ARCHIVE"
  }
  trap cleanup EXIT

  echo "Saving $IMAGE for $PLATFORM..."

  docker image save \
    --platform "$PLATFORM" \
    --output "$ARCHIVE" \
    "$IMAGE"

  echo "Loading archive into Kind cluster '$CLUSTER'..."

  kind load image-archive "$ARCHIVE" --name "$CLUSTER"

  cleanup
  trap - EXIT

  echo "Successfully loaded: $IMAGE"
done

echo
echo "All images loaded successfully into Kind cluster: $CLUSTER"

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