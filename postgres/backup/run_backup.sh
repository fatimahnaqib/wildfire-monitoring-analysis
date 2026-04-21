#!/usr/bin/env bash
# Logical backups of PostgreSQL databases used by this stack (wildfire + Airflow).
# Intended to run inside the postgres-backup sidecar or manually with matching env.
set -euo pipefail

POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-airflow}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set}"
BACKUP_DIR="${BACKUP_DIR:-/backups}"
POSTGRES_BACKUP_DATABASES="${POSTGRES_BACKUP_DATABASES:-wildfire_db airflowdb}"
BACKUP_RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-14}"

export PGPASSWORD="${POSTGRES_PASSWORD}"
# libpq TLS for pg_dump: set POSTGRES_SSLMODE (e.g. require) and POSTGRES_SSL_ROOT_CERT when the server uses SSL.
if [[ -n "${POSTGRES_SSLMODE:-}" ]]; then
  export PGSSLMODE="${POSTGRES_SSLMODE}"
fi
if [[ -n "${POSTGRES_SSL_ROOT_CERT:-}" ]]; then
  export PGSSLROOTCERT="${POSTGRES_SSL_ROOT_CERT}"
fi
mkdir -p "${BACKUP_DIR}"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
failed=0

for db in ${POSTGRES_BACKUP_DATABASES}; do
  out="${BACKUP_DIR}/${db}_${ts}.sql.gz"
  echo "Backing up database '${db}' to ${out}"
  if ! pg_dump -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" \
      --no-owner --no-acl --clean --if-exists \
      "${db}" | gzip -c > "${out}.tmp"; then
    echo "ERROR: pg_dump failed for ${db}" >&2
    rm -f "${out}.tmp"
    failed=1
    continue
  fi
  mv "${out}.tmp" "${out}"
done

echo "Pruning backups older than ${BACKUP_RETENTION_DAYS} days in ${BACKUP_DIR}"
find "${BACKUP_DIR}" -maxdepth 1 -type f -name '*.sql.gz' -mtime "+${BACKUP_RETENTION_DAYS}" -print -delete || true

if [[ "${failed}" -ne 0 ]]; then
  echo "ERROR: one or more databases failed to backup" >&2
  exit 1
fi

echo "Backup run completed successfully."
