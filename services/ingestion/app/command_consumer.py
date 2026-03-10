"""
Command consumer for ingestion service.

This consumer listens to Kafka commands to trigger data ingestion,
making the ingestion service fully event-driven. It can be triggered
by Airflow (for scheduled ingestion) or other services.
"""

import json
import logging
import os
import signal
import threading
from typing import Any, Dict, Optional

import requests
from requests.exceptions import ConnectionError, RequestException, Timeout

from confluent_kafka import Consumer, KafkaError

from etl.config import config as airflow_config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class IngestionCommandConsumer:
    """
    Kafka consumer that listens for ingestion commands and triggers data fetch.

    This consumer:
    1. Listens to wildfire.commands.ingest topic
    2. Processes commands to fetch data from NASA FIRMS API
    3. Produces raw events to wildfire.raw.events topic
    """

    def __init__(self, kafka_producer):
        """
        Initialize the command consumer.

        Args:
            kafka_producer: Kafka producer instance for publishing events
        """
        # Kafka configuration
        self.kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
        self.command_topic = os.getenv(
            "KAFKA_INGESTION_COMMAND_TOPIC", "wildfire.commands.ingest"
        )
        self.raw_events_topic = os.getenv(
            "KAFKA_RAW_EVENTS_TOPIC", "wildfire.raw.events"
        )
        self.group_id = os.getenv("KAFKA_INGESTION_GROUP_ID", "wildfire_ingestion")

        self.kafka_producer = kafka_producer

        # Consumer configuration
        self.consumer_config = {
            "bootstrap.servers": self.kafka_broker,
            "group.id": self.group_id,
            "auto.offset.reset": "latest",  # Only process new commands
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }

        self.consumer = None
        self.running = False

        # Setup signal handlers (only valid in main thread)
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        try:
            if threading.current_thread() is not threading.main_thread():
                return
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except (ValueError, RuntimeError) as e:
            # Some runtimes disallow signal handlers outside main interpreter thread.
            logger.debug("Skipping signal handler registration: %s", e)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False

    def download_csv_in_memory(self, url: str, timeout: int = 30) -> str:
        """
        Download CSV data from NASA FIRMS API and return as string.

        Args:
            url: FIRMS API URL
            timeout: Request timeout in seconds

        Returns:
            str: CSV content as string

        Raises:
            Exception: If download fails
        """
        logger.info(f"Requesting wildfire CSV from NASA FIRMS API: {url}")

        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()

            csv_content = response.text

            if not csv_content or len(csv_content.strip()) == 0:
                raise Exception("Downloaded CSV content is empty")

            logger.info(f"Successfully downloaded CSV ({len(csv_content)} bytes)")
            return csv_content

        except (Timeout, ConnectionError, RequestException) as e:
            error_msg = f"Download failed: {e}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error during download: {e}"
            logger.error(error_msg)
            raise Exception(error_msg) from e

    def parse_csv_from_string(self, csv_content: str) -> list:
        """
        Parse CSV content from string and return list of records.

        Args:
            csv_content: CSV content as string

        Returns:
            List of record dictionaries
        """
        import csv
        import io

        try:
            records = []
            csv_io = io.StringIO(csv_content)
            reader = csv.DictReader(csv_io)
            records = list(reader)

            logger.info(f"Parsed {len(records)} records from CSV")
            return records

        except Exception as e:
            error_msg = f"Error parsing CSV: {e}"
            logger.error(error_msg)
            raise Exception(error_msg) from e

    def produce_raw_events(self, records: list) -> Dict[str, int]:
        """
        Produce raw events to Kafka.

        Args:
            records: List of record dictionaries

        Returns:
            Dict containing production statistics
        """
        valid_count = 0
        invalid_count = 0

        for record in records:
            try:
                # Produce all records to raw events topic (validation happens later)
                message_value = json.dumps(record)

                # Produce message to Kafka
                self.kafka_producer.produce(
                    topic=self.raw_events_topic,
                    value=message_value,
                    callback=self._delivery_callback,
                )

                # Poll for delivery reports
                self.kafka_producer.poll(0)
                valid_count += 1

            except json.JSONEncodeError as e:
                logger.warning(f"JSON encoding error for record: {e}")
                invalid_count += 1

            except Exception as e:
                logger.warning(f"Error producing record: {e}")
                invalid_count += 1

        # Flush producer to ensure all messages are sent
        self.kafka_producer.flush(timeout=30)

        return {
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "total_records": len(records),
        }

    def _delivery_callback(self, error: KafkaError, message: Any) -> None:
        """Handle Kafka message delivery callback."""
        if error:
            logger.warning(f"Kafka delivery failed: {error}")
        else:
            logger.debug(
                f"Delivered to {message.topic()} [{message.partition()}] @ offset {message.offset()}"
            )

    def process_ingestion_command(self, message: Any) -> bool:
        """
        Process an ingestion command message.

        Args:
            message: Kafka message object

        Returns:
            bool: True if processing was successful
        """
        try:
            # Decode message value
            message_value = message.value().decode("utf-8")
            command = json.loads(message_value)

            logger.info(f"Received ingestion command: {command}")

            # Extract command parameters (with defaults from config)
            area = command.get("area", airflow_config.area)
            day_range = command.get("day_range", airflow_config.day_range)
            source = command.get("source", airflow_config.source)
            map_key = command.get("map_key", airflow_config.map_key)

            # Build FIRMS API URL
            firms_url = (
                f"https://firms.modaps.eosdis.nasa.gov/usfs/api/area/csv/"
                f"{map_key}/{source}/{area}/{day_range}"
            )

            # Step 1: Download CSV in memory
            csv_content = self.download_csv_in_memory(url=firms_url)

            # Step 2: Parse CSV in memory
            records = self.parse_csv_from_string(csv_content)

            if not records:
                logger.warning("No records found in CSV")
                return True  # Not an error, just no data

            # Step 3: Produce raw events to Kafka
            production_stats = self.produce_raw_events(records)

            logger.info(f"Ingestion completed: {production_stats}")
            return True

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON command: {e}")
            return False

        except Exception as e:
            logger.error(f"Error processing ingestion command: {e}")
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
        Main consumer loop to process commands from Kafka.

        Raises:
            Exception: If consumer setup fails
        """
        logger.info("Starting ingestion command consumer...")

        # Create consumer
        self.consumer = self.create_consumer()
        if not self.consumer:
            raise Exception("Failed to create Kafka consumer")

        try:
            # Subscribe to command topic
            self.consumer.subscribe([self.command_topic])
            logger.info(f"Subscribed to topic: {self.command_topic}")

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

                    # Process command
                    success = self.process_ingestion_command(message)

                    if success:
                        processed_count += 1
                    else:
                        error_count += 1

                    # Log progress periodically
                    if (processed_count + error_count) % 10 == 0:
                        logger.info(
                            f"Processed {processed_count} commands, {error_count} errors"
                        )

                except KeyboardInterrupt:
                    logger.info("Consumer interrupted by user")
                    break

                except Exception as e:
                    logger.error(f"Error in consumer loop: {e}")
                    error_count += 1
                    continue

            logger.info(
                f"Consumer shutdown. Processed: {processed_count}, Errors: {error_count}"
            )

        except Exception as e:
            error_msg = f"Consumer error: {e}"
            logger.error(error_msg)
            raise

        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")
