"""
Command producer for event-driven architecture.

This module provides utilities to send commands to Kafka topics,
allowing services to trigger actions asynchronously via events.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from confluent_kafka import KafkaError, Producer

logger = logging.getLogger(__name__)


class CommandProducerError(Exception):
    """Custom exception for command producer errors."""


def create_command_producer(bootstrap_servers: str) -> Producer:
    """
    Create and configure a Kafka producer for sending commands.

    Args:
        bootstrap_servers: Kafka bootstrap servers (e.g., "kafka:9092")

    Returns:
        Producer: Configured Kafka producer instance

    Raises:
        CommandProducerError: If producer creation fails
    """
    try:
        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "wildfire-command-producer",
            "acks": "all",  # Wait for all replicas to acknowledge
            "retries": 3,
            "retry.backoff.ms": 1000,
            "compression.type": "gzip",
        }

        producer = Producer(producer_config)
        logger.info(f"Command producer created with config: {producer_config}")
        return producer

    except Exception as e:
        error_msg = f"Failed to create command producer: {e}"
        logger.error(error_msg)
        raise CommandProducerError(error_msg) from e


def delivery_callback(error: KafkaError, message: Any) -> None:
    """Handle Kafka message delivery callback."""
    if error:
        logger.warning(f"Kafka delivery failed: {error}")
    else:
        logger.debug(
            f"Delivered command to {message.topic()} [{message.partition()}] @ offset {message.offset()}"
        )


def send_ingestion_command(
    producer: Producer,
    topic: str,
    area: Optional[str] = None,
    day_range: Optional[str] = None,
    source: Optional[str] = None,
    map_key: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Send an ingestion command to Kafka.

    Args:
        producer: Kafka producer instance
        topic: Kafka topic for ingestion commands (e.g., "wildfire.commands.ingest")
        area: FIRMS area bbox (lonW,latS,lonE,latN)
        day_range: Past N days to fetch
        source: FIRMS source (e.g., "VIIRS_SNPP_NRT")
        map_key: FIRMS API map key
        metadata: Additional metadata to include in command

    Returns:
        bool: True if command was sent successfully
    """
    try:
        command = {
            "command_type": "ingest",
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Add optional parameters
        if area:
            command["area"] = area
        if day_range:
            command["day_range"] = day_range
        if source:
            command["source"] = source
        if map_key:
            command["map_key"] = map_key
        if metadata:
            command["metadata"] = metadata

        # Serialize command to JSON
        message_value = json.dumps(command)

        # Produce message to Kafka
        producer.produce(
            topic=topic, value=message_value, callback=delivery_callback
        )

        # Poll for delivery reports
        producer.poll(0)

        logger.info(f"Ingestion command sent to {topic}: {command}")
        return True

    except json.JSONEncodeError as e:
        logger.error(f"JSON encoding error for command: {e}")
        return False

    except Exception as e:
        logger.error(f"Error sending ingestion command: {e}")
        return False


def send_map_regeneration_command(
    producer: Producer,
    topic: str,
    center_lat: Optional[float] = None,
    center_lon: Optional[float] = None,
    zoom: Optional[int] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Send a map regeneration command to Kafka.

    Args:
        producer: Kafka producer instance
        topic: Kafka topic for map commands (e.g., "wildfire.commands.map.regenerate")
        center_lat: Center latitude for the map
        center_lon: Center longitude for the map
        zoom: Zoom level
        metadata: Additional metadata to include in command

    Returns:
        bool: True if command was sent successfully
    """
    try:
        command = {
            "command_type": "regenerate_map",
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Add optional parameters
        if center_lat is not None:
            command["center_lat"] = center_lat
        if center_lon is not None:
            command["center_lon"] = center_lon
        if zoom is not None:
            command["zoom"] = zoom
        if metadata:
            command["metadata"] = metadata

        # Serialize command to JSON
        message_value = json.dumps(command)

        # Produce message to Kafka
        producer.produce(
            topic=topic, value=message_value, callback=delivery_callback
        )

        # Poll for delivery reports
        producer.poll(0)

        logger.info(f"Map regeneration command sent to {topic}: {command}")
        return True

    except json.JSONEncodeError as e:
        logger.error(f"JSON encoding error for command: {e}")
        return False

    except Exception as e:
        logger.error(f"Error sending map regeneration command: {e}")
        return False


def send_command_and_flush(
    producer: Producer,
    command_func,
    *args,
    timeout: int = 30,
    **kwargs,
) -> bool:
    """
    Send a command and flush the producer to ensure delivery.

    Args:
        producer: Kafka producer instance
        command_func: Function to call to send the command
        *args: Positional arguments for command_func
        timeout: Flush timeout in seconds
        **kwargs: Keyword arguments for command_func

    Returns:
        bool: True if command was sent and flushed successfully
    """
    try:
        success = command_func(producer, *args, **kwargs)
        if success:
            producer.flush(timeout=timeout)
        return success
    except Exception as e:
        logger.error(f"Error sending command and flushing: {e}")
        return False

