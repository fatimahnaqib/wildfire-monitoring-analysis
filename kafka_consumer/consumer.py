"""
Kafka consumer module for wildfire data processing.

This module consumes wildfire data from Kafka topics and inserts
valid records into PostgreSQL database with deduplication.
"""

import json
import logging
import os
import signal
import sys
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2 import OperationalError, DatabaseError, IntegrityError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class WildfireConsumerError(Exception):
    """Custom exception for wildfire consumer errors."""


class WildfireConsumer:
    """Kafka consumer for wildfire data processing."""

    def __init__(self):
        """Initialize the consumer with configuration from environment variables."""
        # Kafka configuration
        self.kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
        self.topic_name = os.getenv("KAFKA_TOPIC", "wildfire_data")
        self.group_id = os.getenv("KAFKA_GROUP_ID", "wildfire_group")

        # PostgreSQL configuration
        self.pg_config = {
            "dbname": os.getenv("POSTGRES_DB", "wildfire_db"),
            "user": os.getenv("POSTGRES_USER", "airflow"),
            "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
        }

        # Consumer configuration
        self.consumer_config = {
            "bootstrap.servers": self.kafka_broker,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 1000,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }

        self.consumer = None
        self.running = False

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False

    def connect_to_postgres(self) -> Optional[psycopg2.extensions.connection]:
        """
        Establish connection to PostgreSQL database.

        Returns:
            psycopg2.connection: Database connection object or None if failed
        """
        try:
            connection = psycopg2.connect(**self.pg_config)
            logger.info("Successfully connected to PostgreSQL database")
            return connection

        except OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            return None

        except Exception as e:
            logger.error(f"Unexpected database connection error: {e}")
            return None

    def normalize_acq_time(self, acq_time_raw: Any) -> str:
        """
        Convert raw acquisition time to HH:MM:SS format.

        Args:
            acq_time_raw: Raw acquisition time (e.g., '939', 939)

        Returns:
            str: Normalized time string (e.g., '09:39:00')
        """
        try:
            # Convert to string and pad to 4 digits
            acq_time_str = str(acq_time_raw).zfill(4)

            # Extract hour and minute
            hour = acq_time_str[:2]
            minute = acq_time_str[2:]

            # Validate time components
            if not (0 <= int(hour) <= 23 and 0 <= int(minute) <= 59):
                logger.warning(f"Invalid time components: {hour}:{minute}")
                return "00:00:00"

            return f"{hour}:{minute}:00"

        except (ValueError, TypeError) as e:
            logger.warning(f"Error normalizing time '{acq_time_raw}': {e}")
            return "00:00:00"

    def insert_into_db(self, data: Dict[str, Any]) -> bool:
        """
        Insert wildfire record into PostgreSQL database.

        Args:
            data: Dictionary containing wildfire record data

        Returns:
            bool: True if insert was successful, False otherwise
        """
        connection = self.connect_to_postgres()
        if not connection:
            return False

        cursor = None
        try:
            cursor = connection.cursor()

            # Normalize acquisition time
            data["acq_time"] = self.normalize_acq_time(data["acq_time"])

            # Prepare insert query with deduplication
            insert_query = """
            INSERT INTO wildfire_events (
                latitude, longitude, bright_ti4, bright_ti5, scan, track,
                acq_date, acq_time, satellite, confidence, version, frp, daynight
            ) VALUES (
                %(latitude)s, %(longitude)s, %(bright_ti4)s, %(bright_ti5)s, 
                %(scan)s, %(track)s, %(acq_date)s, %(acq_time)s, %(satellite)s, 
                %(confidence)s, %(version)s, %(frp)s, %(daynight)s
            )
            ON CONFLICT (latitude, longitude, acq_date, acq_time, satellite) 
            DO NOTHING
            """

            cursor.execute(insert_query, data)
            connection.commit()

            if cursor.rowcount == 0:
                logger.debug("Duplicate record skipped (already exists)")
                return True
            else:
                logger.info("Successfully inserted record into PostgreSQL")
                return True

        except IntegrityError as e:
            logger.warning(f"Integrity constraint violation: {e}")
            connection.rollback()
            return False

        except DatabaseError as e:
            logger.error(f"Database error during insert: {e}")
            connection.rollback()
            return False

        except Exception as e:
            logger.error(f"Unexpected error during insert: {e}")
            connection.rollback()
            return False

        finally:
            if cursor:
                cursor.close()
            connection.close()

    def process_message(self, message: Any) -> bool:
        """
        Process a single Kafka message.

        Args:
            message: Kafka message object

        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            # Decode message value
            message_value = message.value().decode("utf-8")
            data = json.loads(message_value)

            # Insert into database
            success = self.insert_into_db(data)

            if success:
                logger.debug(
                    f"Successfully processed message from partition {message.partition()}"
                )
            else:
                logger.warning(
                    f"Failed to process message from partition {message.partition()}"
                )

            return success

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
            return False

        except UnicodeDecodeError as e:
            logger.error(f"Failed to decode message value: {e}")
            return False

        except Exception as e:
            logger.error(f"Error processing message: {e}")
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

        Raises:
            WildfireConsumerError: If consumer setup fails
        """
        logger.info("Starting Kafka consumer...")

        # Create consumer
        self.consumer = self.create_consumer()
        if not self.consumer:
            raise WildfireConsumerError("Failed to create Kafka consumer")

        try:
            # Subscribe to topic
            self.consumer.subscribe([self.topic_name])
            logger.info(f"Subscribed to topic: {self.topic_name}")

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

                    # Process message
                    success = self.process_message(message)
                    if success:
                        processed_count += 1
                    else:
                        error_count += 1

                    # Log progress periodically
                    if (processed_count + error_count) % 100 == 0:
                        logger.info(
                            f"Processed {processed_count} messages, {error_count} errors"
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
            raise WildfireConsumerError(error_msg) from e

        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")


def main():
    """Main entry point for the consumer application."""
    try:
        consumer = WildfireConsumer()
        consumer.consume_messages()

    except WildfireConsumerError as e:
        logger.error(f"Consumer error: {e}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
