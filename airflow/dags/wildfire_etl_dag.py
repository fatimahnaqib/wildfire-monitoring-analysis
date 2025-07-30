from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.download import download_firms_api_csv
from etl.kafka_producer import produce_to_kafka
from etl.generate_map import generate_wildfire_map

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id="wildfire_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["wildfire", "etl", "kafka"]
) as dag:

    download_task = PythonOperator(
        task_id="download_wildfire_api_csv",
        python_callable=download_firms_api_csv,
    )

    kafka_task = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=produce_to_kafka,
    )

    map_task = PythonOperator(
        task_id="generate_wildfire_map",
        python_callable=generate_wildfire_map,
    )
    

    download_task >> kafka_task >> map_task
