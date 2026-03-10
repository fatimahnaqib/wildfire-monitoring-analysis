"""
Map generation consumer for event-driven architecture.

This consumer listens to Kafka events and automatically regenerates maps
when new wildfire data is processed. It decouples map generation from
the ingestion pipeline, allowing maps to be updated asynchronously.
"""

import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Any, Optional

from confluent_kafka import Consumer, KafkaError
from etl.generate_map import generate_wildfire_map

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MapConsumerError(Exception):
    """Custom exception for map consumer errors."""


class MapGenerationConsumer:
    """
    Kafka consumer that triggers map regeneration based on events.

    This consumer listens to:
    - wildfire.processed.events: Regenerates map when new events are processed
    - wildfire.commands.map.regenerate: Regenerates map on explicit command
    """

    def __init__(self):
        """Initialize the map consumer with configuration from environment variables."""
        # Kafka configuration
        self.kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
        self.processed_events_topic = os.getenv(
            "KAFKA_PROCESSED_EVENTS_TOPIC", "wildfire.processed.events"
        )
        self.map_command_topic = os.getenv(
            "KAFKA_MAP_COMMAND_TOPIC", "wildfire.commands.map.regenerate"
        )
        self.group_id = os.getenv("KAFKA_MAP_GROUP_ID", "wildfire_map_generator")

        # Map generation configuration
        self.default_center_lat = float(os.getenv("MAP_CENTER_LAT", "37.0"))
        self.default_center_lon = float(os.getenv("MAP_CENTER_LON", "-120.0"))
        self.default_zoom = int(os.getenv("MAP_ZOOM", "5"))

        # Consumer configuration
        self.consumer_config = {
            "bootstrap.servers": self.kafka_broker,
            "group.id": self.group_id,
            "auto.offset.reset": "latest",  # Only process new events
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,  # Commit every 5 seconds
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }

        self.consumer = None
        self.running = False
        self.maps_generated = 0
        self.last_map_time = None

        # Setup signal handlers for graceful shutdown (only valid in main thread)
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        try:
            if threading.current_thread() is not threading.main_thread():
                return
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except (ValueError, RuntimeError) as e:
            logger.debug("Skipping signal handler registration: %s", e)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False

    def generate_map(
        self,
        center_lat: float = None,
        center_lon: float = None,
        zoom: int = None,
    ) -> bool:
        """
        Generate a wildfire map with the specified parameters.

        Args:
            center_lat: Center latitude (defaults to configured value)
            center_lon: Center longitude (defaults to configured value)
            zoom: Zoom level (defaults to configured value)

        Returns:
            bool: True if map generation was successful
        """
        try:
            center_lat = center_lat or self.default_center_lat
            center_lon = center_lon or self.default_center_lon
            zoom = zoom or self.default_zoom

            logger.info(
                f"Generating map: center=({center_lat}, {center_lon}), zoom={zoom}"
            )

            output_path = generate_wildfire_map(
                center_lat=center_lat, center_lon=center_lon, zoom=zoom
            )

            self.maps_generated += 1
            self.last_map_time = time.time()

            logger.info(f"Map generated successfully: {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to generate map: {e}")
            return False

    def process_processed_event(self, message: Any) -> bool:
        """
        Process a wildfire.processed.events message.

        When a new event is processed, we may want to regenerate the map.
        For efficiency, we could batch these or use a debounce mechanism,
        but for now we'll regenerate on each event (can be optimized later).

        Args:
            message: Kafka message object

        Returns:
            bool: True if processing was successful
        """
        try:
            # Decode message value
            message_value = message.value().decode("utf-8")
            data = json.loads(message_value)

            logger.debug(
                "Received processed event: %s, %s",
                data.get("latitude", "N/A"),
                data.get("longitude", "N/A"),
            )

            # Regenerate map with default parameters
            # In a production system, you might want to:
            # 1. Batch multiple events and regenerate once per batch
            # 2. Use a debounce mechanism to avoid regenerating too frequently
            # 3. Only regenerate if significant changes occurred
            return self.generate_map()

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
            return False

        except Exception as e:
            logger.error(f"Error processing processed event: {e}")
            return False

    def process_map_command(self, message: Any) -> bool:
        """
        Process a wildfire.commands.map.regenerate command message.

        Commands can specify custom map parameters.

        Args:
            message: Kafka message object

        Returns:
            bool: True if processing was successful
        """
        try:
            # Decode message value
            message_value = message.value().decode("utf-8")
            command = json.loads(message_value)

            # Extract map parameters from command (with defaults)
            center_lat = command.get("center_lat", self.default_center_lat)
            center_lon = command.get("center_lon", self.default_center_lon)
            zoom = command.get("zoom", self.default_zoom)

            logger.info(f"Received map regeneration command: {command}")

            return self.generate_map(
                center_lat=center_lat,
                center_lon=center_lon,
                zoom=zoom,
            )

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON command: {e}")
            return False

        except Exception as e:
            logger.error(f"Error processing map command: {e}")
            return False

    def create_consumer(self) -> Optional[Consumer]:
        """
        Create and configure Kafka consumer.

        Returns:
            Consumer: Configured Kafka consumer or None if failed
        """
        try:
            consumer = Consumer(self.consumer_config)
            logger.info(f"Kafka consumer created with config: {self.consumer_config}")
            return consumer

        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            return None

    def consume_messages(self) -> None:
        """
        Main consumer loop to process messages from Kafka.

        Subscribes to both processed events and map commands topics.

        Raises:
            MapConsumerError: If consumer setup fails
        """
        logger.info("Starting map generation consumer...")

        # Create consumer
        self.consumer = self.create_consumer()
        if not self.consumer:
            raise MapConsumerError("Failed to create Kafka consumer")

        try:
            # Subscribe to both topics
            topics = [self.processed_events_topic, self.map_command_topic]
            self.consumer.subscribe(topics)
            logger.info(f"Subscribed to topics: {topics}")

            self.running = True
            processed_count = 0
            error_count = 0

            while self.running:
                try:
                    # Poll for messages
                    message = self.consumer.poll(timeout=1.0)

                    if message is None:
                        continue

                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            logger.debug(
                                f"Reached end of partition {message.partition()}"
                            )
                            continue
                        else:
                            logger.error(f"Kafka error: {message.error()}")
                            error_count += 1
                            continue

                    # Route message based on topic
                    topic = message.topic()
                    if topic == self.processed_events_topic:
                        success = self.process_processed_event(message)
                    elif topic == self.map_command_topic:
                        success = self.process_map_command(message)
                    else:
                        logger.warning(f"Received message from unknown topic: {topic}")
                        continue

                    if success:
                        processed_count += 1
                    else:
                        error_count += 1

                    # Log progress periodically
                    if (processed_count + error_count) % 10 == 0:
                        logger.info(
                            f"Processed {processed_count} messages, {error_count} errors, "
                            f"{self.maps_generated} maps generated"
                        )

                except KeyboardInterrupt:
                    logger.info("Consumer interrupted by user")
                    break

                except Exception as e:
                    logger.error(f"Error in consumer loop: {e}")
                    error_count += 1
                    continue

            logger.info(
                f"Consumer shutdown. Processed: {processed_count}, Errors: {error_count}, "
                f"Maps generated: {self.maps_generated}"
            )

        except Exception as e:
            error_msg = f"Consumer error: {e}"
            logger.error(error_msg)
            raise MapConsumerError(error_msg) from e

        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")


def main():
    """Main entry point for the map consumer application."""
    try:
        consumer = MapGenerationConsumer()
        consumer.consume_messages()

    except MapConsumerError as e:
        logger.error(f"Map consumer error: {e}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

