#!/usr/bin/env bash

set -Eeuo pipefail

# Usage:

#   ./k8s/start.sh              # uses dev

#   ./k8s/start.sh test         # uses test

#   RECREATE_CLUSTER=false ./k8s/start.sh test  # reuse the existing Kind cluster

NAMESPACE="${1:-dev}"

CLUSTER_NAME="${KIND_CLUSTER_NAME:-logging}"

PLATFORM="${TARGET_PLATFORM:-linux/amd64}"

RECREATE_CLUSTER="${RECREATE_CLUSTER:-true}"

# Resolve paths from this script's location, not from the shell's current directory.

# Expected location: <project-root>/k8s/start.sh

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

K8S_DIR="$PROJECT_ROOT/k8s"

ENV_DIR="$K8S_DIR/$NAMESPACE"

CERT_DIR="$ENV_DIR/certs"

# Keep image versions in one place so the certificate-generation image and the

# Kind image-loading list use the same Elasticsearch version.

ELASTICSEARCH_IMAGE="docker.elastic.co/elasticsearch/elasticsearch:9.1.3"

KAFKA_IMAGE="apache/kafka:4.3.1"

BUSYBOX_136_IMAGE="busybox:1.36"

BUSYBOX_137_IMAGE="busybox:1.37"

CASSANDRA_IMAGE="cassandra:5.0"

MINIO_IMAGE="coollabsio/minio:RELEASE.2025-10-15T17-29-55Z"

CURL_IMAGE="curlimages/curl:8.18.0"

FLUENT_BIT_IMAGE="fluent/fluent-bit:3.2"

FILE_INGESTION_IMAGE="keerthisaisathwik/file-ingestion-service:latest"

FLINK_PROCESSING_IMAGE="keerthisaisathwik/log-processing-flink:latest"

IMAGES=(

  "$ELASTICSEARCH_IMAGE"

  "$KAFKA_IMAGE"

  "$BUSYBOX_136_IMAGE"

  "$BUSYBOX_137_IMAGE"

  "$CASSANDRA_IMAGE"

  "$MINIO_IMAGE"

  "$CURL_IMAGE"

  "$FLUENT_BIT_IMAGE"

  "$FILE_INGESTION_IMAGE"

  "$FLINK_PROCESSING_IMAGE"

)

# Remove a temporary image archive if the script exits unexpectedly.

TEMP_ARCHIVE=""

trap 'if [[ -n "${TEMP_ARCHIVE:-}" ]]; then rm -f -- "$TEMP_ARCHIVE"; fi' EXIT

fail() {

  echo "ERROR: $*" >&2

  exit 1

}

# Validate inputs and tools before deleting or creating any cluster.

if [[ ! "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then

  fail "Invalid Kubernetes namespace '$NAMESPACE'. Use lowercase letters, numbers, and hyphens."

fi

if [[ "$RECREATE_CLUSTER" != "true" && "$RECREATE_CLUSTER" != "false" ]]; then

  fail "RECREATE_CLUSTER must be 'true' or 'false'."

fi

if [[ ! -d "$ENV_DIR" ]]; then

  fail "Environment directory not found: $ENV_DIR"

fi

if [[ ! -f "$ENV_DIR/kustomization.yaml" && \
      ! -f "$ENV_DIR/kustomization.yml" && \
      ! -f "$ENV_DIR/Kustomization" ]]; then

  fail "No kustomization.yaml, kustomization.yml, or Kustomization found in: $ENV_DIR"

fi

for REQUIRED_COMMAND in docker kind kubectl openssl; do

  command -v "$REQUIRED_COMMAND" >/dev/null 2>&1 || fail "Required command not found: $REQUIRED_COMMAND"

done

docker info >/dev/null 2>&1 || fail "Docker is not running or is not accessible from this shell."

mkdir -p "$CERT_DIR"

cd "$PROJECT_ROOT"

echo "=================================================="

echo "Kubernetes namespace : $NAMESPACE"

echo "Kind cluster         : $CLUSTER_NAME"

echo "Platform             : $PLATFORM"

echo "Project root         : $PROJECT_ROOT"

echo "Kubernetes manifests : $ENV_DIR"

echo "Certificate directory: $CERT_DIR"

echo "Recreate cluster     : $RECREATE_CLUSTER"

echo "=================================================="

# Recreating the Kind cluster removes ALL namespaces and data stored in it.

if [[ "$RECREATE_CLUSTER" == "true" ]]; then

  if kind get clusters | grep -Fxq "$CLUSTER_NAME"; then

    echo "===== Deleting existing Kind cluster: $CLUSTER_NAME ====="

    kind delete cluster --name "$CLUSTER_NAME"

  fi

  echo "===== Creating Kind cluster: $CLUSTER_NAME ====="

  kind create cluster --name "$CLUSTER_NAME" --wait 60s

else

  if kind get clusters | grep -Fxq "$CLUSTER_NAME"; then

    echo "===== Reusing existing Kind cluster: $CLUSTER_NAME ====="

  else

    echo "===== Creating Kind cluster: $CLUSTER_NAME ====="

    kind create cluster --name "$CLUSTER_NAME" --wait 60s

  fi

fi

# Explicitly select this cluster in case another Kubernetes context is active.

kubectl config use-context "kind-$CLUSTER_NAME"

# Create the namespace if missing; this is safe when reusing the cluster.

echo "===== Ensuring namespace exists: $NAMESPACE ====="

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Pull the requested architecture when the local copy is missing or has a

# different architecture, then load an archive into Kind.

echo "===== Loading Docker images into Kind ====="

for IMAGE in "${IMAGES[@]}"; do

  echo

  echo "----- Processing image: $IMAGE -----"

  LOCAL_PLATFORM="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$IMAGE" 2>/dev/null || true)"

  if [[ "$LOCAL_PLATFORM" != "$PLATFORM" ]]; then

    echo "Pulling $IMAGE for $PLATFORM..."

    docker pull --platform "$PLATFORM" "$IMAGE"

  else

    echo "Local image already matches $PLATFORM."

  fi

  TEMP_ARCHIVE="$(mktemp)"

  docker image save --platform "$PLATFORM" --output "$TEMP_ARCHIVE" "$IMAGE"

  kind load image-archive "$TEMP_ARCHIVE" --name "$CLUSTER_NAME"

  rm -f -- "$TEMP_ARCHIVE"

  TEMP_ARCHIVE=""

  echo "Loaded: $IMAGE"

done

echo

echo "===== Creating Elasticsearch certificates ====="

if [[ ! -s "$CERT_DIR/elastic-stack-ca.p12" || ! -s "$CERT_DIR/elastic-certificates.p12" ]]; then

  rm -f -- "$CERT_DIR/elastic-stack-ca.p12" "$CERT_DIR/elastic-certificates.p12"

  ELASTIC_DNS="elasticsearch,elasticsearch.${NAMESPACE}.svc.cluster.local,elasticsearch-headless,elasticsearch-headless.${NAMESPACE}.svc.cluster.local,*.elasticsearch-headless.${NAMESPACE}.svc.cluster.local,localhost"

  echo "Generating certificates for namespace '$NAMESPACE'..."

  docker run --rm \
    --volume "$CERT_DIR:/certs" \
    --env "ELASTIC_DNS=$ELASTIC_DNS" \
    "$ELASTICSEARCH_IMAGE" \
    bash -ec '

      bin/elasticsearch-certutil ca \
        --silent \
        --out /certs/elastic-stack-ca.p12 \
        --pass "" \
        --days 3650

      bin/elasticsearch-certutil cert \
        --silent \
        --ca /certs/elastic-stack-ca.p12 \
        --ca-pass "" \
        --name elasticsearch \
        --dns "$ELASTIC_DNS" \
        --ip "127.0.0.1" \
        --out /certs/elastic-certificates.p12 \
        --pass "" \
        --days 1095

    '

  echo "Elasticsearch certificates generated."

else

  echo "Existing certificates found; reusing: $CERT_DIR"

fi

echo "===== Creating Elasticsearch credential Secret ====="
ELASTIC_USERNAME="elastic"
ELASTIC_PASSWORD="elasticsearch"

kubectl create secret generic elasticsearch-credentials \
  -n "$NAMESPACE" \
  --from-literal=username="$ELASTIC_USERNAME" \
  --from-literal=password="$ELASTIC_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

unset ELASTIC_USERNAME ELASTIC_PASSWORD
echo "Elasticsearch credentials Secret created or updated."

echo "===== Creating Elasticsearch certificate Secret ====="

kubectl create secret generic elasticsearch-certs \
  -n "$NAMESPACE" \
  --from-file=elastic-certificates.p12="$CERT_DIR/elastic-certificates.p12" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Elasticsearch certificate Secret created or updated."

echo "===== Creating MinIO credential Secret ====="

MINIO_ROOT_USER="${MINIO_ROOT_USER:-admin}"

MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"

kubectl create secret generic minio-credentials \
  -n "$NAMESPACE" \
  --from-literal=root-user="$MINIO_ROOT_USER" \
  --from-literal=root-password="$MINIO_ROOT_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

unset MINIO_ROOT_USER MINIO_ROOT_PASSWORD

echo "MinIO credentials Secret created or updated."

echo "===== Creating Cassandra credential Secret ====="
CASSANDRA_USERNAME="cassandra_admin"
CASSANDRA_PASSWORD="cassandra_admin_password"

kubectl create secret generic cassandra-credentials \
  -n "$NAMESPACE" \
  --from-literal=username="$CASSANDRA_USERNAME" \
  --from-literal=password="$CASSANDRA_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

unset CASSANDRA_USERNAME CASSANDRA_PASSWORD
echo "Cassandra credentials Secret created or updated."

echo "===== Applying Kubernetes manifests for $NAMESPACE ====="

kubectl apply -n "$NAMESPACE" -k "$ENV_DIR"

echo "===== Current pods in $NAMESPACE ====="

kubectl get pods -n "$NAMESPACE" -o wide

echo "===== Watching pods in $NAMESPACE (press Ctrl+C to stop watching) ====="

kubectl get pods -n "$NAMESPACE" -o wide --watch