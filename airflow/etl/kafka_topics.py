"""
Kafka topic management for event-driven architecture.

This module defines and manages Kafka topics for the wildfire monitoring system.
Topics follow a clear naming convention:
- wildfire.raw.* - Raw events from external sources
- wildfire.processed.* - Processed/validated events
- wildfire.commands.* - Commands to trigger actions
- wildfire.map.* - Map-related events

Replication factor and partition counts are driven by environment variables so the
same code works against a single-broker dev stack (RF=1) or a multi-broker cluster
(RF=3, more partitions for consumer scaling).
"""

import logging
import os
from typing import Any, Dict, List

from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r, using default %s", name, raw, default)
        return default


def get_topic_definitions() -> Dict[str, Dict[str, Any]]:
    """
    Build topic definitions from environment.

    KAFKA_TOPIC_REPLICATION_FACTOR:
        Must be <= broker count. Use 1 for a single broker; 3 for a 3-broker cluster.
    KAFKA_EVENT_TOPIC_PARTITIONS:
        Partitions for high-throughput event topics (raw/processed). More partitions
        allow more consumers in the same group to process in parallel.
    KAFKA_COMMAND_TOPIC_PARTITIONS:
        Partitions for command topics (multiple consumers can share load; ordering
        is only per-partition).
    """
    # Default RF=1 for single-broker dev; docker-compose sets 3 for the 3-broker profile.
    replication_factor = _int_env("KAFKA_TOPIC_REPLICATION_FACTOR", 1)
    event_partitions = _int_env("KAFKA_EVENT_TOPIC_PARTITIONS", 6)
    command_partitions = _int_env("KAFKA_COMMAND_TOPIC_PARTITIONS", 3)
    legacy_partitions = _int_env("KAFKA_LEGACY_TOPIC_PARTITIONS", 3)

    return {
        "wildfire.raw.events": {
            "num_partitions": event_partitions,
            "replication_factor": replication_factor,
            "config": {
                "retention.ms": 604800000,
                "compression.type": "gzip",
            },
            "description": "Raw wildfire detection events from NASA FIRMS API",
        },
        "wildfire.processed.events": {
            "num_partitions": event_partitions,
            "replication_factor": replication_factor,
            "config": {
                "retention.ms": 604800000,
                "compression.type": "gzip",
            },
            "description": "Validated wildfire events ready for database storage",
        },
        "wildfire.commands.ingest": {
            "num_partitions": command_partitions,
            "replication_factor": replication_factor,
            "config": {
                "retention.ms": 86400000,
                "compression.type": "gzip",
            },
            "description": "Commands to trigger data ingestion from NASA FIRMS",
        },
        "wildfire.commands.map.regenerate": {
            "num_partitions": command_partitions,
            "replication_factor": replication_factor,
            "config": {
                "retention.ms": 86400000,
                "compression.type": "gzip",
            },
            "description": "Commands to trigger map regeneration",
        },
        "wildfire_data": {
            "num_partitions": legacy_partitions,
            "replication_factor": replication_factor,
            "config": {
                "retention.ms": 604800000,
                "compression.type": "gzip",
            },
            "description": "Legacy topic - will be deprecated",
        },
    }


def create_kafka_admin_client(bootstrap_servers: str) -> AdminClient:
    """
    Create a Kafka admin client for topic management.

    Args:
        bootstrap_servers: Kafka bootstrap servers (e.g., "kafka:9092")

    Returns:
        AdminClient: Configured admin client
    """
    admin_config = {"bootstrap.servers": bootstrap_servers}
    return AdminClient(admin_config)


def ensure_topics_exist(
    bootstrap_servers: str,
    topics: Dict[str, Dict[str, Any]] = None,
) -> Dict[str, bool]:
    """
    Ensure that all required Kafka topics exist, creating them if needed.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        topics: Dictionary of topic definitions (defaults to get_topic_definitions())

    Returns:
        Dict mapping topic names to creation success status
    """
    if topics is None:
        topics = get_topic_definitions()

    admin_client = create_kafka_admin_client(bootstrap_servers)
    existing_topics = admin_client.list_topics(timeout=10).topics

    results = {}
    topics_to_create = []

    for topic_name, topic_config in topics.items():
        if topic_name in existing_topics:
            logger.info(f"Topic '{topic_name}' already exists")
            results[topic_name] = True
        else:
            logger.info(f"Creating topic '{topic_name}'...")
            new_topic = NewTopic(
                topic=topic_name,
                num_partitions=topic_config["num_partitions"],
                replication_factor=topic_config["replication_factor"],
                config=topic_config.get("config", {}),
            )
            topics_to_create.append((topic_name, new_topic))

    # Create all topics at once
    if topics_to_create:
        create_futures = admin_client.create_topics(
            [topic for _, topic in topics_to_create]
        )

        for topic_name, future in create_futures.items():
            try:
                future.result(timeout=10)
                logger.info(f"Successfully created topic '{topic_name}'")
                results[topic_name] = True
            except Exception as e:
                logger.error(f"Failed to create topic '{topic_name}': {e}")
                results[topic_name] = False

    return results


def list_topics(bootstrap_servers: str) -> List[str]:
    """
    List all existing Kafka topics.

    Args:
        bootstrap_servers: Kafka bootstrap servers

    Returns:
        List of topic names
    """
    admin_client = create_kafka_admin_client(bootstrap_servers)
    metadata = admin_client.list_topics(timeout=10)
    return list(metadata.topics.keys())


if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logging.basicConfig(level=logging.INFO)

    logger.info("Ensuring all required topics exist...")
    results = ensure_topics_exist(bootstrap_servers)

    for topic, success in results.items():
        status = "✓" if success else "✗"
        logger.info(f"{status} {topic}")
