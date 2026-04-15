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
import threading
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, TopicPartition
from psycopg2 import OperationalError, DatabaseError, IntegrityError
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class WildfireConsumerError(Exception):
    """Custom exception for wildfire consumer errors."""


class WildfireConsumer:
    """Kafka consumer for wildfire data processing."""

    _INSERT_SQL = """
        INSERT INTO wildfire_events (
            latitude, longitude, bright_ti4, bright_ti5, scan, track,
            acq_date, acq_time, satellite, confidence, version, frp, daynight
        ) VALUES %s
        ON CONFLICT (latitude, longitude, acq_date, acq_time, satellite)
        DO NOTHING
    """

    def __init__(self):
        """Initialize the consumer with configuration from environment variables."""
        # Kafka configuration
        self.kafka_broker = os.getenv(
            "KAFKA_BROKER", "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        )
        # Default to processed events topic for event-driven architecture
        self.topic_name = os.getenv(
            "KAFKA_TOPIC",
            os.getenv("KAFKA_PROCESSED_EVENTS_TOPIC", "wildfire.processed.events"),
        )
        # Fallback to legacy topic if explicitly set
        if os.getenv("KAFKA_TOPIC") and os.getenv("KAFKA_TOPIC") == "wildfire_data":
            self.topic_name = "wildfire_data"
        self.group_id = os.getenv("KAFKA_GROUP_ID", "wildfire_group")

        # PostgreSQL configuration
        self.pg_config = {
            "dbname": os.getenv("POSTGRES_DB", "wildfire_db"),
            "user": os.getenv("POSTGRES_USER", "airflow"),
            "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
        }

        self.batch_size = max(1, int(os.getenv("CONSUMER_BATCH_SIZE", "100")))
        self.batch_poll_timeout = float(os.getenv("CONSUMER_POLL_TIMEOUT_SEC", "1.0"))
        self.pg_pool_min = max(1, int(os.getenv("PG_POOL_MIN_CONN", "1")))
        self.pg_pool_max = max(
            self.pg_pool_min, int(os.getenv("PG_POOL_MAX_CONN", "4"))
        )
        self._pool: Optional[SimpleConnectionPool] = None

        # Consumer configuration
        self.consumer_config = {
            "bootstrap.servers": self.kafka_broker,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }

        self.consumer = None
        self.running = False

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

    def _create_pool(self) -> SimpleConnectionPool:
        """Create a PostgreSQL connection pool."""
        try:
            pool = SimpleConnectionPool(
                self.pg_pool_min,
                self.pg_pool_max,
                **self.pg_config,
            )
            logger.info(
                "PostgreSQL connection pool ready (min=%s max=%s)",
                self.pg_pool_min,
                self.pg_pool_max,
            )
            return pool
        except OperationalError as e:
            logger.error("Database pool creation failed: %s", e)
            raise WildfireConsumerError("Failed to create PostgreSQL pool") from e

    def _close_pool(self) -> None:
        if self._pool is not None:
            try:
                self._pool.closeall()
            except Exception as e:
                logger.warning("Error while closing connection pool: %s", e)
            finally:
                self._pool = None

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

    def _row_to_tuple(self, data: Dict[str, Any]) -> Tuple[Any, ...]:
        """Build an insert tuple from a record dict (does not mutate the original)."""
        acq_time = self.normalize_acq_time(data.get("acq_time"))
        return (
            data["latitude"],
            data["longitude"],
            data["bright_ti4"],
            data["bright_ti5"],
            data["scan"],
            data["track"],
            data["acq_date"],
            acq_time,
            data["satellite"],
            data["confidence"],
            data["version"],
            data["frp"],
            data["daynight"],
        )

    def insert_batch(self, rows: List[Dict[str, Any]]) -> bool:
        """
        Insert multiple wildfire records in one transaction using bulk INSERT.

        Args:
            rows: Parsed record dicts (caller ensures non-empty when calling).

        Returns:
            bool: True if the batch was committed successfully.
        """
        if not rows:
            return True
        if self._pool is None:
            logger.error("Connection pool is not initialized")
            return False

        conn = None
        try:
            conn = self._pool.getconn()
            tuples = [self._row_to_tuple(r) for r in rows]
            page_size = min(500, len(tuples))
            with conn.cursor() as cursor:
                execute_values(
                    cursor,
                    self._INSERT_SQL,
                    tuples,
                    page_size=page_size,
                )
            conn.commit()
            logger.debug("Bulk inserted batch of %s rows", len(tuples))
            return True

        except IntegrityError as e:
            logger.warning("Integrity constraint violation in batch: %s", e)
            if conn is not None:
                conn.rollback()
            return False

        except DatabaseError as e:
            logger.error("Database error during batch insert: %s", e)
            if conn is not None:
                conn.rollback()
            return False

        except Exception as e:
            logger.error("Unexpected error during batch insert: %s", e)
            if conn is not None:
                conn.rollback()
            return False

        finally:
            if conn is not None:
                try:
                    self._pool.putconn(conn)
                except Exception as e:
                    logger.warning("Returning connection to pool failed: %s", e)

    def _commit_messages(self, messages: List[Any]) -> None:
        """Commit Kafka offsets for the highest contiguous processed offset per partition."""
        if not messages or self.consumer is None:
            return
        by_tp: Dict[Tuple[str, int], int] = {}
        for msg in messages:
            key = (msg.topic(), msg.partition())
            off = msg.offset()
            prev = by_tp.get(key)
            if prev is None or off > prev:
                by_tp[key] = off
        tps = [
            TopicPartition(topic, partition, offset + 1)
            for (topic, partition), offset in by_tp.items()
        ]
        self.consumer.commit(offsets=tps, asynchronous=False)

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

        self._pool = self._create_pool()

        try:
            # Subscribe to topic
            self.consumer.subscribe([self.topic_name])
            logger.info(f"Subscribed to topic: {self.topic_name}")

            self.running = True
            processed_count = 0
            error_count = 0

            while self.running:
                try:
                    messages = self.consumer.consume(
                        num_messages=self.batch_size,
                        timeout=self.batch_poll_timeout,
                    )

                    if not messages:
                        continue

                    kafka_errors = 0
                    parse_fail_messages: List[Any] = []
                    good_pairs: List[Tuple[Any, Dict[str, Any]]] = []

                    for message in messages:
                        if message.error():
                            if message.error().code() == KafkaError._PARTITION_EOF:
                                logger.debug(
                                    "Reached end of partition %s",
                                    message.partition(),
                                )
                                continue
                            logger.error("Kafka error: %s", message.error())
                            kafka_errors += 1
                            continue

                        try:
                            raw = message.value().decode("utf-8")
                            record = json.loads(raw)
                            self._row_to_tuple(record)
                            good_pairs.append((message, record))
                        except KeyError as e:
                            logger.error("Record missing required field, skipping: %s", e)
                            parse_fail_messages.append(message)
                        except (json.JSONDecodeError, UnicodeDecodeError) as e:
                            logger.error("Unparseable message skipped: %s", e)
                            parse_fail_messages.append(message)
                        except Exception as e:
                            logger.error("Error decoding message: %s", e)
                            parse_fail_messages.append(message)

                    if parse_fail_messages:
                        try:
                            self._commit_messages(parse_fail_messages)
                        except Exception as e:
                            logger.error("Failed to commit parse-error offsets: %s", e)

                    if kafka_errors:
                        error_count += kafka_errors

                    if not good_pairs:
                        continue

                    batch_data = [data for _, data in good_pairs]
                    batch_messages = [msg for msg, _ in good_pairs]

                    if self.insert_batch(batch_data):
                        try:
                            self._commit_messages(batch_messages)
                        except Exception as e:
                            logger.error(
                                "Failed to commit offsets after successful insert: %s",
                                e,
                            )
                            error_count += len(batch_messages)
                            continue

                        processed_count += len(batch_messages)
                    else:
                        error_count += len(batch_messages)
                        # Do not commit: messages remain available for retry

                    if (processed_count + error_count) % 100 == 0 and (
                        processed_count + error_count
                    ) > 0:
                        logger.info(
                            "Processed %s messages, %s errors",
                            processed_count,
                            error_count,
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
            self._close_pool()
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
