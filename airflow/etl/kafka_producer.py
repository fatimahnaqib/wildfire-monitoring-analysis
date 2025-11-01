"""
Kafka producer module for wildfire data.

This module handles reading CSV data and producing valid records
to a Kafka topic for downstream processing.
"""

import csv
import json
import logging
from typing import Dict, Any, Optional, Callable
from confluent_kafka import Producer, KafkaError

from etl.config import config
from etl.validation import is_valid_record, get_validation_summary

# Configure logging
logger = logging.getLogger(__name__)


class KafkaProducerError(Exception):
    """Custom exception for Kafka producer errors."""


def create_delivery_callback() -> Callable[[KafkaError, Any], None]:
    """
    Create a delivery callback function for Kafka message delivery reports.
    
    Returns:
        Callable: Function to handle delivery reports
    """
    def delivery_callback(error: KafkaError, message: Any) -> None:
        """Handle Kafka message delivery callback."""
        if error:
            logger.warning(f"Kafka delivery failed: {error}")
        else:
            logger.debug(f"Delivered to {message.topic()} [{message.partition()}] @ offset {message.offset()}")
    
    return delivery_callback


def create_kafka_producer() -> Producer:
    """
    Create and configure a Kafka producer.
    
    Returns:
        Producer: Configured Kafka producer instance
        
    Raises:
        KafkaProducerError: If producer creation fails
    """
    try:
        producer_config = {
            'bootstrap.servers': config.kafka_bootstrap_servers,
            'client.id': 'wildfire-producer',
            'acks': 'all',  # Wait for all replicas to acknowledge
            'retries': 3,
            'retry.backoff.ms': 1000,
            'compression.type': 'gzip'
        }
        
        producer = Producer(producer_config)
        logger.info(f"Kafka producer created with config: {producer_config}")
        return producer
        
    except Exception as e:
        error_msg = f"Failed to create Kafka producer: {e}"
        logger.error(error_msg)
        raise KafkaProducerError(error_msg) from e


def read_csv_records(file_path: str) -> list[Dict[str, Any]]:
    """
    Read CSV file and return list of records.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        List of record dictionaries
        
    Raises:
        KafkaProducerError: If file reading fails
    """
    try:
        records = []
        with open(file_path, 'r', encoding='utf-8') as file_handle:
            reader = csv.DictReader(file_handle)
            records = list(reader)
        
        logger.info(f"Read {len(records)} records from {file_path}")
        return records
        
    except FileNotFoundError as e:
        error_msg = f"CSV file not found: {file_path}"
        logger.error(error_msg)
        raise KafkaProducerError(error_msg) from e
        
    except Exception as e:
        error_msg = f"Error reading CSV file: {e}"
        logger.error(error_msg)
        raise KafkaProducerError(error_msg) from e


def produce_records_to_kafka(
    producer: Producer,
    records: list[Dict[str, Any]],
    topic: str,
    delivery_callback: Callable[[KafkaError, Any], None]
) -> Dict[str, int]:
    """
    Produce valid records to Kafka topic.
    
    Args:
        producer: Kafka producer instance
        records: List of record dictionaries
        topic: Kafka topic name
        delivery_callback: Callback function for delivery reports
        
    Returns:
        Dict containing production statistics
    """
    valid_count = 0
    invalid_count = 0
    
    for record in records:
        try:
            if is_valid_record(record):
                # Serialize record to JSON
                message_value = json.dumps(record)
                
                # Produce message to Kafka
                producer.produce(
                    topic=topic,
                    value=message_value,
                    callback=delivery_callback
                )
                
                # Poll for delivery reports
                producer.poll(0)
                valid_count += 1
                
            else:
                invalid_count += 1
                logger.debug("Skipped invalid record")
                
        except json.JSONEncodeError as e:
            logger.warning(f"JSON encoding error for record: {e}")
            invalid_count += 1
            
        except Exception as e:
            logger.warning(f"Error processing record: {e}")
            invalid_count += 1
    
    return {
        'valid_records': valid_count,
        'invalid_records': invalid_count,
        'total_records': len(records)
    }


def produce_to_kafka(
    input_file: Optional[str] = None,
    topic: Optional[str] = None
) -> Dict[str, int]:
    """
    Main function to produce wildfire data to Kafka.
    
    Args:
        input_file: Path to input CSV file (defaults to config.output_file)
        topic: Kafka topic name (defaults to config.kafka_topic)
        
    Returns:
        Dict containing production statistics
        
    Raises:
        KafkaProducerError: If production fails
    """
    # Use defaults from config if not provided
    csv_file = input_file or config.output_file
    kafka_topic = topic or config.kafka_topic
    
    logger.info(f"Starting Kafka producer for file: {csv_file}")
    
    try:
        # Create producer and callback
        producer = create_kafka_producer()
        delivery_callback = create_delivery_callback()
        
        # Read CSV records
        records = read_csv_records(csv_file)
        
        if not records:
            logger.warning("No records found in CSV file")
            return {'valid_records': 0, 'invalid_records': 0, 'total_records': 0}
        
        # Get validation summary
        validation_summary = get_validation_summary(records)
        logger.info(f"Validation summary: {validation_summary}")
        
        # Produce records to Kafka
        production_stats = produce_records_to_kafka(
            producer, records, kafka_topic, delivery_callback
        )
        
        # Flush producer to ensure all messages are sent
        producer.flush(timeout=30)
        
        logger.info(f"Kafka production completed: {production_stats}")
        return production_stats
        
    except Exception as e:
        error_msg = f"Kafka production error: {e}"
        logger.error(error_msg)
        raise KafkaProducerError(error_msg) from e
