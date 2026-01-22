"""
Wildfire ETL Pipeline DAG.

This DAG orchestrates the complete wildfire data processing pipeline:
1. Download wildfire data from NASA FIRMS API, validate, and produce to Kafka (in-memory)
2. Generate interactive map visualization from database

The pipeline runs daily and processes wildfire data for the past 3 days.
Note: Ingestion service now handles both download and Kafka production in-memory (no CSV files).
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
    """Call the ingestion microservice to download, validate, and produce to Kafka.
    
    The ingestion service now:
    - Downloads CSV from NASA FIRMS API in memory
    - Validates records
    - Produces valid records directly to Kafka
    - Returns statistics (no CSV file is written)
    """
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
        result = response.json()
        # Log statistics for monitoring
        if "stats" in result:
            stats = result["stats"]
            print(f"Ingestion stats: {stats['total_records']} total, "
                  f"{stats['valid_records']} valid, {stats['invalid_records']} invalid")
    except Exception as e:
        raise RuntimeError(f"Ingestion service call failed: {e}")


# Producer service is no longer needed - ingestion service now produces directly to Kafka
# This function is kept for backward compatibility but should not be used
def call_producer_service(**context) -> None:
    """DEPRECATED: Producer service is no longer needed.
    
    The ingestion service now handles both downloading and producing to Kafka.
    This task is kept for backward compatibility but should be skipped.
    """
    print("WARNING: Producer service step is deprecated. "
          "Ingestion service now produces directly to Kafka.")
    # No-op: ingestion already produced to Kafka
    pass


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
    Download, validate, and produce wildfire data to Kafka.
    
    This task:
    - Downloads CSV data from NASA FIRMS API in memory (no file I/O)
    - Validates each record using existing validation rules
    - Produces valid records directly to Kafka topic 'wildfire_data'
    - Returns statistics (total_records, valid_records, invalid_records)
    - Handles API errors and timeouts gracefully
    """,
    retries=3,
    retry_delay=timedelta(minutes=2),
)

# Kafka production is now handled by ingestion service
# This task is deprecated but kept for backward compatibility
kafka_task = PythonOperator(
    task_id="produce_to_kafka",
    python_callable=call_producer_service,
    dag=dag,
    doc_md="""
    DEPRECATED: Kafka production is now handled by ingestion service.
    
    This task is a no-op. The ingestion service now:
    - Downloads CSV in memory
    - Validates records
    - Produces directly to Kafka
    
    This task is kept for workflow compatibility but does nothing.
    """,
    retries=0,  # No retries needed for no-op
    retry_delay=timedelta(seconds=1),
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
# Note: kafka_task is now a no-op (ingestion handles Kafka production)
# It's kept in the workflow for backward compatibility
start_task >> download_task >> kafka_task >> map_task >> end_task

# Alternative workflow without producer step (for future cleanup):
# start_task >> download_task >> map_task >> end_task
