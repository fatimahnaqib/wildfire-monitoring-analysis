"""
Wildfire ETL Pipeline DAG - Event-Driven Architecture.

This DAG orchestrates the wildfire data processing pipeline using an event-driven architecture:
1. Send ingestion command to Kafka (triggers ingestion service asynchronously)
2. Optionally send map regeneration command (map service listens to events automatically)

The pipeline runs daily and processes wildfire data for the past 3 days.
Key changes for event-driven architecture:
- Airflow sends commands to Kafka topics instead of making synchronous HTTP calls
- Services process events asynchronously without blocking Airflow
- Map generation is triggered automatically by processed events (via map consumer)
- Airflow is no longer in the hot path - it only triggers periodic ingestion
"""

from datetime import timedelta
from typing import Dict, Any
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from etl.command_producer import (
    create_command_producer,
    send_ingestion_command,
    send_map_regeneration_command,
    send_command_and_flush,
)
from etl.config import config

# Default arguments for the DAG
default_args: Dict[str, Any] = {
    "owner": "wildfire-team",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "max_active_runs": 1,
}

# DAG definition
dag = DAG(
    dag_id="wildfire_etl_pipeline",
    default_args=default_args,
    description="Wildfire data ETL pipeline with NASA FIRMS API, Kafka, and visualization",
    schedule_interval="@daily",
    catchup=False,
    tags=["wildfire", "etl", "kafka", "visualization", "nasa"],
    doc_md=__doc__,
    params={
        "firms_area": "-125.0,32.0,-113.0,42.0",  # California region
        "day_range": "3",  # Past 3 days
        "map_center_lat": 37.0,
        "map_center_lon": -120.0,
        "map_zoom": 5,
    },
)


def send_ingestion_command_to_kafka(**context) -> None:
    """
    Send an ingestion command to Kafka for event-driven processing.

    This function:
    - Creates a Kafka producer
    - Sends a command to wildfire.commands.ingest topic
    - Does NOT wait for ingestion to complete (fully asynchronous)
    - The ingestion service listens to this topic and processes commands

    This is the key change: Airflow no longer waits for ingestion to complete.
    The ingestion service processes commands asynchronously via Kafka.
    """
    params = context.get("params", {})
    
    # Get Kafka configuration
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", config.kafka_bootstrap_servers)
    ingestion_command_topic = os.getenv(
        "KAFKA_INGESTION_COMMAND_TOPIC", "wildfire.commands.ingest"
    )
    
    try:
        # Create command producer
        producer = create_command_producer(kafka_bootstrap)
        
        # Send ingestion command with parameters
        success = send_command_and_flush(
            producer,
            send_ingestion_command,
            topic=ingestion_command_topic,
            area=params.get("firms_area"),
            day_range=params.get("day_range"),
            source=config.source,
            map_key=config.map_key,
            metadata={
                "triggered_by": "airflow",
                "dag_run_id": context.get("dag_run").run_id if context.get("dag_run") else None,
            },
        )
        
        if success:
            print(
                f"Ingestion command sent to Kafka topic '{ingestion_command_topic}'. "
                f"Processing will happen asynchronously via event-driven architecture."
            )
        else:
            raise RuntimeError("Failed to send ingestion command to Kafka")
            
    except Exception as e:
        raise RuntimeError(f"Failed to send ingestion command: {e}")


def send_map_regeneration_command_to_kafka(**context) -> None:
    """
    Send a map regeneration command to Kafka (optional).

    Note: In the event-driven architecture, maps are automatically regenerated
    when processed events are consumed by the map consumer. This command is
    only needed if you want to explicitly trigger a map regeneration with
    custom parameters.

    This function:
    - Creates a Kafka producer
    - Sends a command to wildfire.commands.map.regenerate topic
    - Does NOT wait for map generation to complete
    """
    params = context.get("params", {})
    
    # Get Kafka configuration
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", config.kafka_bootstrap_servers)
    map_command_topic = os.getenv(
        "KAFKA_MAP_COMMAND_TOPIC", "wildfire.commands.map.regenerate"
    )
    
    try:
        # Create command producer
        producer = create_command_producer(kafka_bootstrap)
        
        # Send map regeneration command
        success = send_command_and_flush(
            producer,
            send_map_regeneration_command,
            topic=map_command_topic,
            center_lat=params.get("map_center_lat", 37.0),
            center_lon=params.get("map_center_lon", -120.0),
            zoom=params.get("map_zoom", 5),
            metadata={
                "triggered_by": "airflow",
                "dag_run_id": context.get("dag_run").run_id if context.get("dag_run") else None,
            },
        )
        
        if success:
            print(
                f"Map regeneration command sent to Kafka topic '{map_command_topic}'. "
                f"Map will be regenerated asynchronously."
            )
        else:
            print("Warning: Failed to send map regeneration command (maps will still be auto-generated from events)")
            
    except Exception as e:
        print(f"Warning: Failed to send map regeneration command: {e} (maps will still be auto-generated from events)")


# Task definitions
start_task = DummyOperator(
    task_id="start_pipeline", dag=dag, doc_md="Start of the wildfire ETL pipeline"
)

# Event-driven ingestion task
ingestion_command_task = PythonOperator(
    task_id="send_ingestion_command",
    python_callable=send_ingestion_command_to_kafka,
    dag=dag,
    doc_md="""
    Send ingestion command to Kafka for event-driven processing.
    
    This task:
    - Sends a command to wildfire.commands.ingest Kafka topic
    - Does NOT wait for ingestion to complete (fully asynchronous)
    - The ingestion service listens to this topic and processes commands
    - Raw events are produced to wildfire.raw.events
    - Validation processor consumes raw events and produces to wildfire.processed.events
    - Database consumer consumes processed events and writes to PostgreSQL
    - Map consumer listens to processed events and regenerates maps automatically
    
    This is the key architectural change: Airflow is no longer in the hot path.
    """,
    retries=3,
    retry_delay=timedelta(minutes=2),
)

# Optional map regeneration command (maps are auto-generated from events)
map_command_task = PythonOperator(
    task_id="send_map_regeneration_command",
    python_callable=send_map_regeneration_command_to_kafka,
    dag=dag,
    doc_md="""
    Optionally send map regeneration command to Kafka.
    
    Note: Maps are automatically regenerated when processed events are consumed
    by the map consumer. This task is only needed for explicit regeneration with
    custom parameters.
    
    This task:
    - Sends a command to wildfire.commands.map.regenerate Kafka topic
    - Does NOT wait for map generation to complete
    - The map consumer listens to this topic and processes commands
    """,
    retries=1,
    retry_delay=timedelta(minutes=1),
)

end_task = DummyOperator(
    task_id="end_pipeline", dag=dag, doc_md="End of the wildfire ETL pipeline"
)

# Task dependencies for event-driven architecture
# Airflow only sends commands - it doesn't wait for processing to complete
start_task >> ingestion_command_task >> map_command_task >> end_task

# Note: The actual processing happens asynchronously:
# 1. Ingestion command → Ingestion service → Raw events → Validation processor → Processed events
# 2. Processed events → Database consumer (writes to PostgreSQL)
# 3. Processed events → Map consumer (regenerates maps automatically)
# 4. Map command (optional) → Map consumer (explicit regeneration)
