#!/usr/bin/env bash
# Run run_backup.sh immediately, then every BACKUP_INTERVAL_SEC seconds.
set -euo pipefail

BACKUP_INTERVAL_SEC="${BACKUP_INTERVAL_SEC:-86400}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "postgres-backup: interval=${BACKUP_INTERVAL_SEC}s retention_days=${BACKUP_RETENTION_DAYS:-14}"

while true; do
  if bash "${SCRIPT_DIR}/run_backup.sh"; then
    echo "postgres-backup: next run in ${BACKUP_INTERVAL_SEC}s"
  else
    echo "postgres-backup: backup failed; retrying in ${BACKUP_INTERVAL_SEC}s" >&2
  fi
  sleep "${BACKUP_INTERVAL_SEC}"
done
