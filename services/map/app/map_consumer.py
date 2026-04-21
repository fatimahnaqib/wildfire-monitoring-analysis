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

import psycopg2
from confluent_kafka import Consumer, KafkaError, Producer
from psycopg2 import OperationalError
from etl.generate_map import generate_wildfire_map
from etl.kafka_dlq import build_dlq_envelope, publish_single_dlq
from etl.transport_config import (
    kafka_common_client_config,
    kafka_config_for_log,
    postgres_connect_kwargs,
)

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
        self.kafka_broker = os.getenv(
            "KAFKA_BROKER", "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        )
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
        self.default_lookback_days = int(os.getenv("MAP_LOOKBACK_DAYS", "7"))
        self.default_max_records = int(os.getenv("MAP_MAX_RECORDS", "5000"))

        # Consumer configuration
        self.consumer_config = {
            "bootstrap.servers": self.kafka_broker,
            "group.id": self.group_id,
            "auto.offset.reset": "latest",  # Only process new events
            # Manual commit to avoid offset advancement if map generation fails.
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
            **kafka_common_client_config(),
        }

        self.consumer = None
        self.running = False
        self.maps_generated = 0
        self.last_map_time = None
        self._failure_streak = 0
        self._backoff_base_sec = float(
            os.getenv("KAFKA_CONSUMER_BACKOFF_BASE_SEC", "0.5")
        )
        self._backoff_max_sec = float(os.getenv("KAFKA_CONSUMER_BACKOFF_MAX_SEC", "30"))

        # Postgres advisory lock for map generation (prevents multiple replicas
        # from writing the same output file concurrently).
        self._pg_config = {
            "dbname": os.getenv("POSTGRES_DB", "wildfire_db"),
            "user": os.getenv("POSTGRES_USER", "airflow"),
            "password": os.environ["POSTGRES_PASSWORD"],
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            **postgres_connect_kwargs(),
        }
        # Any stable 64-bit integer works; keep it constant across deployments.
        self._map_lock_key = int(os.getenv("MAP_GENERATION_LOCK_KEY", "94531021"))

        self.dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "wildfire.dlq.events")
        self._dlq_flush_timeout = float(os.getenv("KAFKA_DLQ_FLUSH_TIMEOUT_SEC", "15"))
        self._dlq_producer: Optional[Producer] = None

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

    def _create_dlq_producer(self) -> Producer:
        return Producer(
            {
                "bootstrap.servers": self.kafka_broker,
                "client.id": f"{self.group_id}-map-dlq",
                "acks": "all",
                "retries": 3,
                "retry.backoff.ms": 1000,
                "compression.type": "gzip",
                **kafka_common_client_config(),
            }
        )

    def generate_map(
        self,
        center_lat: float = None,
        center_lon: float = None,
        zoom: int = None,
        lookback_days: int = None,
        max_records: int = None,
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
            lookback_days = (
                self.default_lookback_days if lookback_days is None else lookback_days
            )
            max_records = (
                self.default_max_records if max_records is None else max_records
            )

            logger.info(
                "Generating map: center=(%s, %s), zoom=%s, lookback_days=%s, max_records=%s",
                center_lat,
                center_lon,
                zoom,
                lookback_days,
                max_records,
            )

            lock_conn = None
            have_lock = False
            try:
                lock_conn = psycopg2.connect(**self._pg_config)
                lock_conn.autocommit = True
                with lock_conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_try_advisory_lock(%s)", (self._map_lock_key,)
                    )
                    have_lock = bool(cur.fetchone()[0])
                if not have_lock:
                    logger.info(
                        "Skipping map generation: another replica holds advisory lock key=%s",
                        self._map_lock_key,
                    )
                    return True
            except OperationalError as e:
                # Best-effort: if DB is unavailable we still attempt generation so the API
                # remains functional (it may fail later if DB is required for data).
                logger.warning(
                    "Could not acquire map-generation lock (db unavailable): %s; proceeding without lock",
                    e,
                )
            except Exception as e:
                logger.warning(
                    "Could not acquire map-generation lock: %s; proceeding without lock",
                    e,
                )

            try:
                output_path = generate_wildfire_map(
                    center_lat=center_lat,
                    center_lon=center_lon,
                    zoom=zoom,
                    lookback_days=lookback_days,
                    max_records=max_records,
                )
            finally:
                if have_lock and lock_conn is not None:
                    try:
                        with lock_conn.cursor() as cur:
                            cur.execute(
                                "SELECT pg_advisory_unlock(%s)", (self._map_lock_key,)
                            )
                    except Exception as e:
                        logger.warning("Failed to release advisory lock: %s", e)
                if lock_conn is not None:
                    try:
                        lock_conn.close()
                    except Exception:
                        pass

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

        except UnicodeDecodeError as e:
            logger.error("Processed event message is not valid UTF-8: %s", e)
            env = build_dlq_envelope(
                source="map_generation_consumer",
                failure_reason="processed_event_utf8_decode_error",
                message=message,
                detail=str(e),
            )
            publish_single_dlq(
                self._dlq_producer,
                self.dlq_topic,
                env,
                flush_timeout=self._dlq_flush_timeout,
            )
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
            env = build_dlq_envelope(
                source="map_generation_consumer",
                failure_reason="processed_event_json_decode_error",
                message=message,
                detail=str(e),
            )
            publish_single_dlq(
                self._dlq_producer,
                self.dlq_topic,
                env,
                flush_timeout=self._dlq_flush_timeout,
            )
            return True

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
            lookback_days = command.get("lookback_days", self.default_lookback_days)
            max_records = command.get("max_records", self.default_max_records)

            logger.info(f"Received map regeneration command: {command}")

            return self.generate_map(
                center_lat=center_lat,
                center_lon=center_lon,
                zoom=zoom,
                lookback_days=lookback_days,
                max_records=max_records,
            )

        except UnicodeDecodeError as e:
            logger.error("Map command message is not valid UTF-8: %s", e)
            env = build_dlq_envelope(
                source="map_generation_consumer",
                failure_reason="map_command_utf8_decode_error",
                message=message,
                detail=str(e),
            )
            publish_single_dlq(
                self._dlq_producer,
                self.dlq_topic,
                env,
                flush_timeout=self._dlq_flush_timeout,
            )
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON command: {e}")
            env = build_dlq_envelope(
                source="map_generation_consumer",
                failure_reason="map_command_json_decode_error",
                message=message,
                detail=str(e),
            )
            publish_single_dlq(
                self._dlq_producer,
                self.dlq_topic,
                env,
                flush_timeout=self._dlq_flush_timeout,
            )
            return True

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
            logger.info(
                "Kafka consumer created with config: %s",
                kafka_config_for_log(self.consumer_config),
            )
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

            self._dlq_producer = self._create_dlq_producer()

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
                            self._failure_streak += 1
                            self._sleep_backoff()
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
                        try:
                            self.consumer.commit(message=message, asynchronous=False)
                        except Exception as e:
                            logger.error("Offset commit failed: %s", e)
                            error_count += 1
                            self._failure_streak += 1
                            self._sleep_backoff()
                            continue
                        processed_count += 1
                        self._failure_streak = 0
                    else:
                        error_count += 1
                        self._failure_streak += 1
                        self._sleep_backoff()

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
                    self._failure_streak += 1
                    self._sleep_backoff()
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
            if self._dlq_producer is not None:
                try:
                    self._dlq_producer.flush(timeout=5)
                except Exception as e:
                    logger.warning("DLQ producer flush on shutdown: %s", e)
                self._dlq_producer = None
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")

    def _sleep_backoff(self) -> None:
        """Exponential backoff with jitter to avoid hot-looping on failures."""
        if self._failure_streak <= 0:
            return
        cap = self._backoff_max_sec
        base = self._backoff_base_sec
        delay = min(cap, base * (2 ** max(0, self._failure_streak - 1)))
        jitter = delay * 0.1
        time.sleep(max(0.0, delay - jitter))


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
