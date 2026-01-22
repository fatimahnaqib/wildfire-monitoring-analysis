import os
import csv
import json
import logging
import io
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from confluent_kafka import Producer, KafkaError
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

from etl.config import config as airflow_config
from etl.validation import is_valid_record


logger = logging.getLogger("ingestion_service")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(title="Wildfire Ingestion Service", version="1.0.0")

# Metrics
ingest_requests_total = Counter(
    "ingest_requests_total", "Total number of ingestion requests", ["outcome"]
)
ingest_records_total = Counter(
    "ingest_records_total", "Total number of records processed", ["status"]
)


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    return PlainTextResponse(
        generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST
    )


def download_csv_in_memory(url: str, timeout: int = 30) -> str:
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


def parse_csv_from_string(csv_content: str) -> List[Dict[str, Any]]:
    """
    Parse CSV content from string and return list of records.
    
    Args:
        csv_content: CSV content as string
        
    Returns:
        List of record dictionaries
        
    Raises:
        Exception: If parsing fails
    """
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


def create_kafka_producer() -> Producer:
    """
    Create and configure a Kafka producer.
    
    Returns:
        Producer: Configured Kafka producer instance
    """
    producer_config = {
        "bootstrap.servers": airflow_config.kafka_bootstrap_servers,
        "client.id": "wildfire-ingestion-producer",
        "acks": "all",  # Wait for all replicas to acknowledge
        "retries": 3,
        "retry.backoff.ms": 1000,
        "compression.type": "gzip",
    }
    
    producer = Producer(producer_config)
    logger.info(f"Kafka producer created with config: {producer_config}")
    return producer


def delivery_callback(error: KafkaError, message: Any) -> None:
    """Handle Kafka message delivery callback."""
    if error:
        logger.warning(f"Kafka delivery failed: {error}")
    else:
        logger.debug(
            f"Delivered to {message.topic()} [{message.partition()}] @ offset {message.offset()}"
        )


def produce_records_to_kafka(
    producer: Producer,
    records: List[Dict[str, Any]],
    topic: str,
) -> Dict[str, int]:
    """
    Produce valid records to Kafka topic.
    
    Args:
        producer: Kafka producer instance
        records: List of record dictionaries
        topic: Kafka topic name
        
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
                    topic=topic, value=message_value, callback=delivery_callback
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
        "valid_records": valid_count,
        "invalid_records": invalid_count,
        "total_records": len(records),
    }


@app.post("/ingest")
def ingest(
    area: Optional[str] = Query(
        default=None, description="FIRMS area bbox: lonW,latS,lonE,latN"
    ),
    day_range: Optional[str] = Query(default=None, description="Past N days to fetch"),
    source: Optional[str] = Query(
        default=None, description="FIRMS source e.g. VIIRS_SNPP_NRT"
    ),
) -> JSONResponse:
    try:
        # Build a custom URL if query params provided, otherwise rely on config
        if any([area, day_range, source]):
            map_key = airflow_config.map_key
            source_val = source or airflow_config.source
            area_val = area or airflow_config.area
            day_val = day_range or airflow_config.day_range
            firms_url = (
                f"https://firms.modaps.eosdis.nasa.gov/usfs/api/area/csv/"
                f"{map_key}/{source_val}/{area_val}/{day_val}"
            )
        else:
            firms_url = airflow_config.firms_url

        # Step 1: Download CSV in memory
        csv_content = download_csv_in_memory(url=firms_url)
        
        # Step 2: Parse CSV in memory
        records = parse_csv_from_string(csv_content)
        
        if not records:
            logger.warning("No records found in CSV")
            stats = {
                "total_records": 0,
                "valid_records": 0,
                "invalid_records": 0,
            }
            ingest_requests_total.labels(outcome="success").inc()
            return JSONResponse(
                {
                    "status": "success",
                    "url": firms_url,
                    "stats": stats,
                }
            )
        
        # Step 3 & 4: Validate and produce valid records to Kafka
        producer = create_kafka_producer()
        production_stats = produce_records_to_kafka(
            producer, records, airflow_config.kafka_topic
        )
        
        # Flush producer to ensure all messages are sent
        producer.flush(timeout=30)
        
        # Update metrics
        ingest_requests_total.labels(outcome="success").inc()
        ingest_records_total.labels(status="valid").inc(production_stats["valid_records"])
        ingest_records_total.labels(status="invalid").inc(production_stats["invalid_records"])
        
        logger.info(f"Ingestion completed: {production_stats}")
        
        # Step 5: Return statistics
        return JSONResponse(
            {
                "status": "success",
                "url": firms_url,
                "stats": {
                    "total_records": production_stats["total_records"],
                    "valid_records": production_stats["valid_records"],
                    "invalid_records": production_stats["invalid_records"],
                },
            }
        )
    except Exception as e:
        logger.exception("Ingestion failed")
        ingest_requests_total.labels(outcome="error").inc()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
