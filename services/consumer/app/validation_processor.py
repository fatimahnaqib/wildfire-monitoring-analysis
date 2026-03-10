"""
Validation processor for event-driven architecture.

This processor consumes raw events from wildfire.raw.events,
validates them, and produces validated events to wildfire.processed.events.
This separates validation from ingestion, allowing for better scalability.
"""

import json
import logging
import os
import signal
import sys
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, Producer, KafkaError
from etl.validation import is_valid_record

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ValidationProcessorError(Exception):
    """Custom exception for validation processor errors."""


class ValidationProcessor:
    """
    Kafka consumer/producer that validates raw events and produces processed events.

    This processor:
    1. Consumes from wildfire.raw.events
    2. Validates each record
    3. Produces valid records to wildfire.processed.events
    4. Optionally sends invalid records to a dead letter queue (future enhancement)
    """

    def __init__(self):
        """Initialize the validation processor with configuration from environment variables."""
        # Kafka configuration
        self.kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
        self.raw_events_topic = os.getenv(
            "KAFKA_RAW_EVENTS_TOPIC", "wildfire.raw.events"
        )
        self.processed_events_topic = os.getenv(
            "KAFKA_PROCESSED_EVENTS_TOPIC", "wildfire.processed.events"
        )
        self.group_id = os.getenv("KAFKA_VALIDATION_GROUP_ID", "wildfire_validation")

        # Consumer configuration
        self.consumer_config = {
            "bootstrap.servers": self.kafka_broker,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",  # Process all events
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }

        # Producer configuration
        self.producer_config = {
            "bootstrap.servers": self.kafka_broker,
            "client.id": "wildfire-validation-processor",
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 1000,
            "compression.type": "gzip",
        }

        self.consumer = None
        self.producer = None
        self.running = False

        # Statistics
        self.valid_count = 0
        self.invalid_count = 0

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False

    def _delivery_callback(self, error: KafkaError, message: Any) -> None:
        """Handle Kafka message delivery callback."""
        if error:
            logger.warning(f"Kafka delivery failed: {error}")
        else:
            logger.debug(
                f"Delivered to {message.topic()} [{message.partition()}] @ offset {message.offset()}"
            )

    def process_raw_event(self, message: Any) -> bool:
        """
        Process a raw event message: validate and produce to processed events topic.

        Args:
            message: Kafka message object

        Returns:
            bool: True if processing was successful
        """
        try:
            # Decode message value
            message_value = message.value().decode("utf-8")
            record = json.loads(message_value)

            # Validate record
            if is_valid_record(record):
                # Produce to processed events topic
                processed_message = json.dumps(record)

                self.producer.produce(
                    topic=self.processed_events_topic,
                    value=processed_message,
                    callback=self._delivery_callback,
                )

                # Poll for delivery reports
                self.producer.poll(0)
                self.valid_count += 1

                logger.debug(f"Validated and produced event: {record.get('latitude', 'N/A')}, {record.get('longitude', 'N/A')}")
                return True
            else:
                # Invalid record - log but don't produce
                # TODO: Send to dead letter queue
                self.invalid_count += 1
                logger.debug("Skipped invalid record")
                return True  # Not an error, just invalid data

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
            self.invalid_count += 1
            return False

        except Exception as e:
            logger.error(f"Error processing raw event: {e}")
            self.invalid_count += 1
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

    def create_producer(self) -> Optional[Producer]:
        """
        Create and configure Kafka producer.

        Returns:
            Producer: Configured Kafka producer or None if failed
        """
        try:
            producer = Producer(self.producer_config)
            logger.info(f"Kafka producer created with config: {self.producer_config}")
            return producer

        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            return None

    def consume_messages(self) -> None:
        """
        Main consumer loop to process messages from Kafka.

        Raises:
            ValidationProcessorError: If consumer setup fails
        """
        logger.info("Starting validation processor...")

        # Create consumer and producer
        self.consumer = self.create_consumer()
        if not self.consumer:
            raise ValidationProcessorError("Failed to create Kafka consumer")

        self.producer = self.create_producer()
        if not self.producer:
            raise ValidationProcessorError("Failed to create Kafka producer")

        try:
            # Subscribe to raw events topic
            self.consumer.subscribe([self.raw_events_topic])
            logger.info(f"Subscribed to topic: {self.raw_events_topic}")

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
                    success = self.process_raw_event(message)

                    if success:
                        processed_count += 1
                    else:
                        error_count += 1

                    # Log progress periodically
                    if (processed_count + error_count) % 100 == 0:
                        logger.info(
                            f"Processed {processed_count} messages, {error_count} errors, "
                            f"Valid: {self.valid_count}, Invalid: {self.invalid_count}"
                        )

                except KeyboardInterrupt:
                    logger.info("Consumer interrupted by user")
                    break

                except Exception as e:
                    logger.error(f"Error in consumer loop: {e}")
                    error_count += 1
                    continue

            # Flush producer before shutdown
            self.producer.flush(timeout=30)

            logger.info(
                f"Consumer shutdown. Processed: {processed_count}, Errors: {error_count}, "
                f"Valid: {self.valid_count}, Invalid: {self.invalid_count}"
            )

        except Exception as e:
            error_msg = f"Consumer error: {e}"
            logger.error(error_msg)
            raise ValidationProcessorError(error_msg) from e

        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            if self.producer:
                self.producer.flush(timeout=10)
                logger.info("Kafka producer closed")


def main():
    """Main entry point for the validation processor application."""
    try:
        processor = ValidationProcessor()
        processor.consume_messages()

    except ValidationProcessorError as e:
        logger.error(f"Validation processor error: {e}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

