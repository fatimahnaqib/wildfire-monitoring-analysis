"""
Kafka topic management for event-driven architecture.

This module defines and manages Kafka topics for the wildfire monitoring system.
Topics follow a clear naming convention:
- wildfire.raw.* - Raw events from external sources
- wildfire.processed.* - Processed/validated events
- wildfire.commands.* - Commands to trigger actions
- wildfire.map.* - Map-related events
"""

from confluent_kafka.admin import AdminClient, NewTopic
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


# Topic definitions with configuration
TOPIC_DEFINITIONS = {
    # Raw wildfire events from ingestion
    "wildfire.raw.events": {
        "num_partitions": 3,  # Allow parallel processing
        "replication_factor": 1,  # Single broker for now
        "config": {
            "retention.ms": 604800000,  # 7 days retention
            "compression.type": "gzip",
        },
        "description": "Raw wildfire detection events from NASA FIRMS API",
    },
    # Processed/validated events ready for storage
    "wildfire.processed.events": {
        "num_partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": 604800000,  # 7 days retention
            "compression.type": "gzip",
        },
        "description": "Validated wildfire events ready for database storage",
    },
    # Commands to trigger ingestion
    "wildfire.commands.ingest": {
        "num_partitions": 1,
        "replication_factor": 1,
        "config": {
            "retention.ms": 86400000,  # 1 day retention for commands
            "compression.type": "gzip",
        },
        "description": "Commands to trigger data ingestion from NASA FIRMS",
    },
    # Commands to regenerate maps
    "wildfire.commands.map.regenerate": {
        "num_partitions": 1,
        "replication_factor": 1,
        "config": {
            "retention.ms": 86400000,  # 1 day retention
            "compression.type": "gzip",
        },
        "description": "Commands to trigger map regeneration",
    },
    # Legacy topic (kept for backward compatibility during migration)
    "wildfire_data": {
        "num_partitions": 1,
        "replication_factor": 1,
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
    bootstrap_servers: str, topics: Dict[str, Dict[str, Any]] = None
) -> Dict[str, bool]:
    """
    Ensure that all required Kafka topics exist, creating them if needed.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        topics: Dictionary of topic definitions (defaults to TOPIC_DEFINITIONS)

    Returns:
        Dict mapping topic names to creation success status
    """
    if topics is None:
        topics = TOPIC_DEFINITIONS

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
    import os

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    logging.basicConfig(level=logging.INFO)

    logger.info("Ensuring all required topics exist...")
    results = ensure_topics_exist(bootstrap_servers)

    for topic, success in results.items():
        status = "✓" if success else "✗"
        logger.info(f"{status} {topic}")

