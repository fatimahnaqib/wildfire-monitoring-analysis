"""
Direct PostgreSQL writes for demo / offline ingest (no Kafka).

Mirrors the consumer bulk-insert path used in production.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from psycopg2 import DatabaseError, IntegrityError
from psycopg2.extras import execute_values

from etl.config import config
from etl.generate_map import pooled_connection
from etl.transport_config import postgres_connect_kwargs

logger = logging.getLogger(__name__)

_INSERT_SQL = """
    INSERT INTO wildfire_events (
        latitude, longitude, bright_ti4, bright_ti5, scan, track,
        acq_date, acq_time, satellite, confidence, version, frp, daynight
    ) VALUES %s
    ON CONFLICT (latitude, longitude, acq_date, acq_time, satellite)
    DO NOTHING
"""


def normalize_acq_time(acq_time_raw: Any) -> str:
    """Convert FIRMS acquisition time to HH:MM:SS."""
    try:
        acq_time_str = str(acq_time_raw).zfill(4)
        hour = acq_time_str[:2]
        minute = acq_time_str[2:]
        if not (0 <= int(hour) <= 23 and 0 <= int(minute) <= 59):
            logger.warning("Invalid time components: %s:%s", hour, minute)
            return "00:00:00"
        return f"{hour}:{minute}:00"
    except (ValueError, TypeError) as e:
        logger.warning("Failed to normalize acq_time %s: %s", acq_time_raw, e)
        return "00:00:00"


def row_to_tuple(data: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        data["latitude"],
        data["longitude"],
        data["bright_ti4"],
        data["bright_ti5"],
        data["scan"],
        data["track"],
        data["acq_date"],
        normalize_acq_time(data.get("acq_time")),
        data["satellite"],
        data["confidence"],
        data["version"],
        data["frp"],
        data["daynight"],
    )


def insert_wildfire_batch(
    rows: List[Dict[str, Any]],
    *,
    connection: Optional[Any] = None,
) -> int:
    """
    Insert validated wildfire records. Returns number of rows attempted.

    Uses ON CONFLICT DO NOTHING (dedup matches production consumer).
    """
    if not rows:
        return 0

    tuples = [row_to_tuple(r) for r in rows]
    page_size = min(500, len(tuples))

    def _run_insert(conn: Any) -> int:
        try:
            with conn.cursor() as cursor:
                execute_values(
                    cursor,
                    _INSERT_SQL,
                    tuples,
                    page_size=page_size,
                )
            conn.commit()
            logger.info(
                "Inserted batch of %s wildfire rows (conflicts ignored)", len(tuples)
            )
            return len(tuples)
        except IntegrityError as e:
            logger.warning("Integrity constraint violation in batch: %s", e)
            conn.rollback()
            return 0
        except DatabaseError:
            conn.rollback()
            raise

    if connection is not None:
        return _run_insert(connection)

    with pooled_connection() as conn:
        return _run_insert(conn)


def postgres_connection_kwargs() -> Dict[str, Any]:
    """Connection kwargs for standalone scripts (outside the map pool)."""
    return {
        "dbname": config.postgres_db,
        "user": config.postgres_user,
        "password": config.postgres_password,
        "host": config.postgres_host,
        "port": config.postgres_port,
        **postgres_connect_kwargs(),
    }
