"""
Wildfire ETL Pipeline DAG.

This DAG orchestrates the complete wildfire data processing pipeline:
1. Download wildfire data from NASA FIRMS API
2. Validate and produce data to Kafka
3. Generate interactive map visualization

The pipeline runs daily and processes wildfire data for the past 3 days.
"""

from datetime import timedelta
from typing import Dict, Any
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

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


def call_ingestion_service(**context) -> None:
    """Call the ingestion microservice to download the latest CSV."""
    params = context.get("params", {})
    url = "http://ingestion:8000/ingest"
    try:
        response = requests.post(
            url,
            params={
                "area": params.get("firms_area"),
                "day_range": params.get("day_range"),
            },
            timeout=60,
        )
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Ingestion service call failed: {e}")


def call_producer_service(**context) -> None:
    """Call the producer microservice to produce records to Kafka.
    Uses the CSV file created by the ingestion service.
    """
    url = "http://producer:8001/produce"
    try:
        response = requests.post(url, timeout=60)
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Producer service call failed: {e}")


def call_map_service(**context) -> None:
    """Call the map microservice to generate the wildfire map."""
    params = context.get("params", {})
    url = "http://map:8003/generate"
    try:
        response = requests.post(
            url,
            params={
                "center_lat": params.get("map_center_lat", 37.0),
                "center_lon": params.get("map_center_lon", -120.0),
                "zoom": params.get("map_zoom", 5),
            },
            timeout=120,
        )
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Map service call failed: {e}")


# Task definitions
start_task = DummyOperator(
    task_id="start_pipeline", dag=dag, doc_md="Start of the wildfire ETL pipeline"
)

download_task = PythonOperator(
    task_id="download_wildfire_api_csv",
    python_callable=call_ingestion_service,
    dag=dag,
    doc_md="""
    Download wildfire data from NASA FIRMS API.
    
    This task:
    - Fetches CSV data from NASA FIRMS API for the past 3 days
    - Saves data to local filesystem for processing
    - Handles API errors and timeouts gracefully
    """,
    retries=3,
    retry_delay=timedelta(minutes=2),
)

kafka_task = PythonOperator(
    task_id="produce_to_kafka",
    python_callable=call_producer_service,
    dag=dag,
    doc_md="""
    Produce validated wildfire data to Kafka topic.
    
    This task:
    - Calls the producer microservice
    - Reads downloaded CSV file
    - Validates each record for data quality
    - Produces valid records to Kafka topic 'wildfire_data'
    - Handles validation errors and Kafka connection issues
    """,
    retries=2,
    retry_delay=timedelta(minutes=3),
)

map_task = PythonOperator(
    task_id="generate_wildfire_map",
    python_callable=call_map_service,
    dag=dag,
    doc_md="""
    Generate interactive wildfire map from database data.
    
    This task:
    - Calls the map microservice
    - Connects to PostgreSQL database
    - Fetches wildfire events data
    - Creates interactive Folium map with markers
    - Saves HTML map to dashboard directory
    """,
    retries=2,
    retry_delay=timedelta(minutes=2),
)

end_task = DummyOperator(
    task_id="end_pipeline", dag=dag, doc_md="End of the wildfire ETL pipeline"
)

# Task dependencies
start_task >> download_task >> kafka_task >> map_task >> end_task
