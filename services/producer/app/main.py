import os
import logging
from typing import Optional

from fastapi import FastAPI, Query
from fastapi import Depends
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

from etl.command_producer import (
    create_command_producer,
    send_command_and_flush,
    send_ingestion_command,
)
from etl.config import config as airflow_config
from app.security import require_api_key


logger = logging.getLogger("producer_service")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(
    title="Wildfire Producer Service",
    version="1.0.0",
    description=(
        "Publishes ingestion commands to Kafka (event-driven). "
        "CSV file paths are not used; ingestion service fetches FIRMS data and emits raw events."
    ),
)

# Metrics
producer_requests_total = Counter(
    "producer_requests_total", "Total number of producer requests", ["outcome"]
)


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    return PlainTextResponse(
        generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST
    )


@app.post("/produce")
def produce(
    area: Optional[str] = Query(
        default=None,
        description="FIRMS area bbox (lonW,latS,lonE,latN); defaults from service config",
    ),
    day_range: Optional[str] = Query(
        default=None, description="Past N days to fetch; defaults from service config"
    ),
    source: Optional[str] = Query(
        default=None,
        description="FIRMS source e.g. VIIRS_SNPP_NRT; defaults from service config",
    ),
    _: None = Depends(require_api_key),
) -> JSONResponse:
    """
    Trigger ingestion asynchronously by publishing a command to Kafka.

    Same pattern as the Airflow DAG: does not wait for ingestion or downstream processing.
    """
    kafka_bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", airflow_config.kafka_bootstrap_servers
    )
    ingestion_command_topic = os.getenv(
        "KAFKA_INGESTION_COMMAND_TOPIC", "wildfire.commands.ingest"
    )

    try:
        producer = create_command_producer(kafka_bootstrap)
        success = send_command_and_flush(
            producer,
            send_ingestion_command,
            topic=ingestion_command_topic,
            area=area or airflow_config.area,
            day_range=day_range or airflow_config.day_range,
            source=source or airflow_config.source,
            map_key=airflow_config.map_key,
            metadata={"triggered_by": "producer_service_http"},
        )

        if success:
            producer_requests_total.labels(outcome="success").inc()
            return JSONResponse(
                {
                    "status": "success",
                    "mode": "kafka_command",
                    "topic": ingestion_command_topic,
                    "message": "Ingestion command published; processing is asynchronous.",
                }
            )

        producer_requests_total.labels(outcome="error").inc()
        return JSONResponse(
            {"status": "error", "message": "Failed to publish ingestion command"},
            status_code=500,
        )

    except Exception as e:
        logger.exception("Failed to publish ingestion command")
        producer_requests_total.labels(outcome="error").inc()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
