#!/bin/bash

set -e

NAMESPACE="${1:-dev}"
CERT_DIR="./k8s/dev/certs"

mkdir -p "$CERT_DIR"

# Generate certificates only when they do not already exist.
if [ ! -f "$CERT_DIR/elastic-stack-ca.p12" ] || \
   [ ! -f "$CERT_DIR/elastic-certificates.p12" ]; then

    echo "Generating Elasticsearch certificates for namespace: $NAMESPACE..."

    docker run --rm \
      -v "$(pwd)/$CERT_DIR:/certs" \
      docker.elastic.co/elasticsearch/elasticsearch:9.1.3 \
      bash -c "
        bin/elasticsearch-certutil ca \
          --silent \
          --out /certs/elastic-stack-ca.p12 \
          --pass '' \
          --days 3650

        bin/elasticsearch-certutil cert \
          --silent \
          --ca /certs/elastic-stack-ca.p12 \
          --ca-pass '' \
          --name elasticsearch \
          --dns \"elasticsearch,elasticsearch.${NAMESPACE}.svc.cluster.local,elasticsearch-headless,elasticsearch-headless.${NAMESPACE}.svc.cluster.local,*.elasticsearch-headless.${NAMESPACE}.svc.cluster.local,localhost\" \
          --ip \"127.0.0.1\" \
          --out /certs/elastic-certificates.p12 \
          --pass '' \
          --days 1095
      "

    echo "✅ Certificates generated"
else
    echo "✅ Existing Elasticsearch certificates found — reusing them"
fi

# Create a new password for the newly created cluster.
ELASTIC_PASSWORD="$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 24)"

echo "Kubernetes namespace: $NAMESPACE"
echo "Elasticsearch username: elastic"
echo "Elasticsearch password: $ELASTIC_PASSWORD"

kubectl create secret generic elasticsearch-credentials \
  -n "$NAMESPACE" \
  --from-literal=password="$ELASTIC_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic elasticsearch-certs \
  -n "$NAMESPACE" \
  --from-file=elastic-certificates.p12="$CERT_DIR/elastic-certificates.p12" \
  --dry-run=client -o yaml | kubectl apply -f -

unset ELASTIC_PASSWORD

echo "✅ Elasticsearch password Secret created in namespace: $NAMESPACE"
echo "✅ Elasticsearch certificate Secret created in namespace: $NAMESPACE"