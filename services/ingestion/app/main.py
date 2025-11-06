import os
import logging
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

from etl.download import download_firms_api_csv
from etl.config import config as airflow_config


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


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    return PlainTextResponse(
        generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST
    )


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
        # Resolve output path to a container-local data dir so Airflow can read it via bind mount
        output_dir = os.getenv("AIRFLOW_DATA_DIR", "/data")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "wildfire_api_data.csv")

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

        saved_path = download_firms_api_csv(url=firms_url, output_file=output_path)

        ingest_requests_total.labels(outcome="success").inc()
        return JSONResponse(
            {"status": "success", "file_path": saved_path, "url": firms_url}
        )
    except Exception as e:
        logger.exception("Ingestion failed")
        ingest_requests_total.labels(outcome="error").inc()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
