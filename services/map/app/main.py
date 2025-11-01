import os
import logging
import time
from typing import Union

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

from etl.generate_map import generate_wildfire_map


logger = logging.getLogger("map_service")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s %(levelname)s %(name)s %(message)s'
)

app = FastAPI(title="Wildfire Map Service", version="1.0.0")

# Metrics
map_requests_total = Counter(
    "map_requests_total",
    "Total number of map generation requests",
    ["outcome"]
)

map_generation_time = Counter(
    "map_generation_seconds_total",
    "Total time spent generating maps"
)


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    return PlainTextResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)


@app.get("/map", response_model=None)
def get_map(
    center_lat: float = Query(default=37.0, description="Center latitude for the map"),
    center_lon: float = Query(default=-120.0, description="Center longitude for the map"),
    zoom: int = Query(default=5, description="Initial zoom level"),
    format: str = Query(default="html", description="Response format: html or json")
) -> Union[HTMLResponse, JSONResponse]:
    """
    Generate and return the wildfire map.
    
    Returns HTML map by default, or JSON with file path if format=json.
    """
    try:
        start_time = time.time()
        
        # Generate the map using existing logic
        output_path = generate_wildfire_map(
            center_lat=center_lat,
            center_lon=center_lon,
            zoom=zoom
        )
        
        generation_time = time.time() - start_time
        map_generation_time.inc(generation_time)
        map_requests_total.labels(outcome="success").inc()
        
        if format == "json":
            return JSONResponse({
                "status": "success",
                "file_path": output_path,
                "generation_time_seconds": generation_time,
                "center_lat": center_lat,
                "center_lon": center_lon,
                "zoom": zoom
            })
        else:
            # Return the HTML content
            with open(output_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            return HTMLResponse(content=html_content)
            
    except Exception as e:
        logger.exception("Map generation failed")
        map_requests_total.labels(outcome="error").inc()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/generate")
def generate_map(
    center_lat: float = Query(default=37.0, description="Center latitude for the map"),
    center_lon: float = Query(default=-120.0, description="Center longitude for the map"),
    zoom: int = Query(default=5, description="Initial zoom level")
) -> JSONResponse:
    """
    Generate the wildfire map and return metadata.
    """
    try:
        start_time = time.time()
        
        # Generate the map using existing logic
        output_path = generate_wildfire_map(
            center_lat=center_lat,
            center_lon=center_lon,
            zoom=zoom
        )
        
        generation_time = time.time() - start_time
        map_generation_time.inc(generation_time)
        map_requests_total.labels(outcome="success").inc()
        
        return JSONResponse({
            "status": "success",
            "file_path": output_path,
            "generation_time_seconds": generation_time,
            "center_lat": center_lat,
            "center_lon": center_lon,
            "zoom": zoom
        })
        
    except Exception as e:
        logger.exception("Map generation failed")
        map_requests_total.labels(outcome="error").inc()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
