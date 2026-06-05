#!/usr/bin/env python3
"""Create required Kafka topics if missing (used by docker-compose kafka-topics-init)."""

import logging
import os
import sys

# Airflow image layout: repo mounted at /opt/airflow
sys.path.insert(0, "/opt/airflow")

from etl.kafka_topics import ensure_topics_exist  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ensure_kafka_topics")

if __name__ == "__main__":
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info("Ensuring topics against %s", bootstrap)
    results = ensure_topics_exist(bootstrap)
    failures = [name for name, ok in results.items() if not ok]
    if failures:
        logger.error("Topic ensure failed for: %s", failures)
        sys.exit(1)
    logger.info("All topics OK: %s", list(results.keys()))
