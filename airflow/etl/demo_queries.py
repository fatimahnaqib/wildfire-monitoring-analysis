"""
Read-only PostgreSQL queries for the public demo API.

Used by the map service demo endpoints; no Kafka dependency.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

from etl.generate_map import MapGenerationError, pooled_connection

logger = logging.getLogger(__name__)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def _rows_to_dicts(columns: List[str], rows: List[tuple]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        record = {
            col: _serialize_value(val) for col, val in zip(columns, row, strict=True)
        }
        out.append(record)
    return out


def fetch_wildfire_events(
    *,
    lookback_days: Optional[int] = 30,
    limit: int = 500,
    min_date: Optional[date] = None,
    max_date: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Return wildfire events for the demo dashboard and JSON API.

    Args:
        lookback_days: Include rows with acq_date >= today - N days. <=0 disables.
        limit: Maximum rows (1–10000).
        min_date: Optional lower bound on acq_date (inclusive).
        max_date: Optional upper bound on acq_date (inclusive).
    """
    limit = max(1, min(int(limit), 10000))
    lookback_days = None if lookback_days is None else int(lookback_days)

    conditions = ["1=1"]
    params: List[Any] = []

    if lookback_days is not None and lookback_days > 0:
        conditions.append("acq_date >= (CURRENT_DATE - %s)")
        params.append(lookback_days)

    if min_date is not None:
        conditions.append("acq_date >= %s")
        params.append(min_date)

    if max_date is not None:
        conditions.append("acq_date <= %s")
        params.append(max_date)

    where_clause = " AND ".join(conditions)
    params.append(limit)

    query = f"""
        SELECT
            id,
            latitude,
            longitude,
            bright_ti4,
            bright_ti5,
            frp,
            acq_date,
            acq_time,
            satellite,
            confidence,
            daynight,
            created_at
        FROM wildfire_events
        WHERE {where_clause}
        ORDER BY acq_date DESC, acq_time DESC NULLS LAST, id DESC
        LIMIT %s
    """

    try:
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
        events = _rows_to_dicts(columns, rows)
        return {
            "count": len(events),
            "limit": limit,
            "lookback_days": lookback_days,
            "min_date": min_date.isoformat() if min_date else None,
            "max_date": max_date.isoformat() if max_date else None,
            "events": events,
        }
    except Exception as e:
        logger.exception("Demo wildfire query failed")
        raise MapGenerationError(str(e)) from e


def fetch_demo_stats() -> Dict[str, Any]:
    """Aggregate stats for the demo dashboard header."""
    queries = {
        "total_events": "SELECT COUNT(*) FROM wildfire_events",
        "latest_acq_date": "SELECT MAX(acq_date) FROM wildfire_events",
        "earliest_acq_date": "SELECT MIN(acq_date) FROM wildfire_events",
        "events_last_7_days": """
            SELECT COUNT(*) FROM wildfire_events
            WHERE acq_date >= (CURRENT_DATE - 7)
        """,
        "events_last_30_days": """
            SELECT COUNT(*) FROM wildfire_events
            WHERE acq_date >= (CURRENT_DATE - 30)
        """,
    }
    stats: Dict[str, Any] = {}
    try:
        with pooled_connection() as conn:
            with conn.cursor() as cur:
                for key, sql in queries.items():
                    cur.execute(sql)
                    val = cur.fetchone()[0]
                    stats[key] = _serialize_value(val)
        return stats
    except Exception as e:
        logger.exception("Demo stats query failed")
        raise MapGenerationError(str(e)) from e
