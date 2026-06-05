#!/usr/bin/env python3
"""
Download NASA FIRMS CSV and insert into PostgreSQL (no Kafka).

Usage (from repo root, with Postgres reachable):
  PYTHONPATH=airflow POSTGRES_PASSWORD=... FIRMS_MAP_KEY=... \\
    python scripts/sync_firms_to_postgres.py
"""

from __future__ import annotations

import json
import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, os.path.join(_REPO_ROOT, "airflow"))

from etl.demo_sync import sync_firms_to_postgres  # noqa: E402


def main() -> int:
    summary = sync_firms_to_postgres()
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
