import os
import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

from etl.kafka_producer import produce_to_kafka
from etl.config import config as airflow_config


logger = logging.getLogger("producer_service")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(title="Wildfire Producer Service", version="1.0.0")

# Metrics
producer_requests_total = Counter(
    "producer_requests_total", "Total number of producer requests", ["outcome"]
)

producer_records_total = Counter(
    "producer_records_total", "Total number of records processed", ["status"]
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
def produce() -> JSONResponse:
    try:
        logger.info(f"Starting Kafka producer for file: {airflow_config.output_file}")
        production_stats = produce_to_kafka()

        # Update metrics
        producer_requests_total.labels(outcome="success").inc()
        producer_records_total.labels(status="valid").inc(
            production_stats.get("valid_records", 0)
        )
        producer_records_total.labels(status="invalid").inc(
            production_stats.get("invalid_records", 0)
        )

        return JSONResponse({"status": "success", "stats": production_stats})

    except Exception as e:
        logger.exception("Producer failed")
        producer_requests_total.labels(outcome="error").inc()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
