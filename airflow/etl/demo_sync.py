"""
Sync NASA FIRMS CSV directly into PostgreSQL (demo / cron path, no Kafka).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from etl.download import download_firms_api_csv
from etl.kafka_producer import read_csv_records
from etl.postgres_write import insert_wildfire_batch
from etl.validation import is_valid_record

logger = logging.getLogger(__name__)


def sync_firms_to_postgres(
    *,
    url: str | None = None,
    output_file: str | None = None,
    batch_size: int = 500,
) -> Dict[str, Any]:
    """
    Download FIRMS data, validate records, and insert into wildfire_events.

    Returns summary counts for operators and demo refresh endpoints.
    """
    csv_path = download_firms_api_csv(url=url, output_file=output_file)
    records = read_csv_records(csv_path)

    valid: List[Dict[str, Any]] = []
    invalid = 0
    for record in records:
        try:
            if is_valid_record(record):
                valid.append(record)
            else:
                invalid += 1
        except Exception:
            invalid += 1

    inserted_attempts = 0
    for i in range(0, len(valid), batch_size):
        chunk = valid[i : i + batch_size]
        inserted_attempts += insert_wildfire_batch(chunk)

    summary = {
        "csv_path": csv_path,
        "records_read": len(records),
        "records_valid": len(valid),
        "records_invalid": invalid,
        "rows_insert_attempted": inserted_attempts,
    }
    logger.info("FIRMS sync complete: %s", summary)
    return summary
