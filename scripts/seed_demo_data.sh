#!/usr/bin/env bash
# Load sample wildfire rows into the demo Postgres container.
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.demo.yml}"
POSTGRES_USER="${POSTGRES_USER:-airflow}"

docker compose -f "$COMPOSE_FILE" exec -T postgres \
  psql -U "$POSTGRES_USER" -d wildfire_db -f /seed/demo_sample.sql

echo "Demo seed data loaded."
