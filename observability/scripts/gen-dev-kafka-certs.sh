#!/usr/bin/env bash
# Generate self-signed CA + client cert for local Kafka TLS experiments.
# Production: use your org PKI or managed Kafka-provided trust material.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/secrets/kafka}"
DAYS="${2:-825}"

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

if [[ -f ca.pem ]]; then
  echo "CA already exists in $OUT_DIR (delete to regenerate)"
  exit 0
fi

echo "Generating dev Kafka TLS material in $(pwd) ..."

openssl req -x509 -newkey rsa:4096 -sha256 -days "$DAYS" -nodes \
  -keyout ca.key -out ca.pem \
  -subj "/CN=Wildfire Dev Kafka CA"

openssl req -newkey rsa:4096 -nodes -keyout client.key -out client.csr \
  -subj "/CN=wildfire-client"

openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out client.crt -days "$DAYS" -sha256

chmod 0400 ca.key client.key
rm -f client.csr ca.srl

cat <<EOF

Created:
  $(pwd)/ca.pem       -> KAFKA_SSL_CA_LOCATION
  $(pwd)/client.crt   -> KAFKA_SSL_CERTIFICATE_LOCATION (optional mTLS)
  $(pwd)/client.key   -> KAFKA_SSL_KEY_LOCATION

Mount secrets/kafka into containers at /etc/kafka/secrets
and set KAFKA_SECURITY_PROTOCOL=SSL or SASL_SSL per docs/production/KAFKA_TLS_SASL.md

Broker listener reconfiguration is still required for TLS in the dev Compose cluster.
EOF
