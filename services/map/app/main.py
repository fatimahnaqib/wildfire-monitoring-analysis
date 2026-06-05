import logging
import os
import threading
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path
from typing import Dict, Optional, Union

from fastapi import FastAPI, Query, Request, Depends
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)
from fastapi.staticfiles import StaticFiles
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

from etl.config import config as wf_config
from etl.demo_queries import fetch_demo_stats, fetch_wildfire_events
from etl.demo_sync import sync_firms_to_postgres
from etl.generate_map import MapGenerationError, generate_wildfire_map
from app.security import require_api_key


logger = logging.getLogger("map_service")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

_demo_mode = os.getenv("DEMO_MODE", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
_app_title = "Wildfire Monitoring Public Demo" if _demo_mode else "Wildfire Map Service"
app = FastAPI(
    title=_app_title,
    version="1.1.0",
    description=(
        "Interactive wildfire map and public demo API. "
        "Production ingest uses Kafka; the demo path reads PostgreSQL only."
    ),
)

_STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
if _STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

_HTML_REFERRER_HEADERS: Dict[str, str] = {
    "Referrer-Policy": "strict-origin-when-cross-origin",
}

# Metrics
map_requests_total = Counter(
    "map_requests_total", "Total number of map generation requests", ["outcome"]
)

map_generation_time = Counter(
    "map_generation_seconds_total", "Total time spent generating maps"
)

# Global map consumer instance
map_consumer_thread = None
map_consumer_instance = None


def start_map_consumer_background():
    """Start the map consumer in a background thread for event-driven map generation."""
    global map_consumer_thread, map_consumer_instance

    if map_consumer_thread and map_consumer_thread.is_alive():
        logger.info("Map consumer already running")
        return

    def run_consumer():
        global map_consumer_instance
        try:
            from app.map_consumer import MapGenerationConsumer

            map_consumer_instance = MapGenerationConsumer()
            map_consumer_instance.consume_messages()
        except Exception as e:
            logger.error(f"Map consumer error: {e}")

    map_consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    map_consumer_thread.start()
    logger.info("Map consumer started in background thread")


@app.get("/", include_in_schema=False, response_model=None)
def demo_dashboard() -> Union[FileResponse, RedirectResponse]:
    """Public demo dashboard (static SPA)."""
    index = _STATIC_DIR / "index.html"
    if index.is_file():
        return FileResponse(index)
    return RedirectResponse(url="/map")


@app.get("/demo/wildfires")
def demo_wildfires(
    lookback_days: int = Query(
        default=30,
        ge=0,
        le=365,
        description="Include records from the last N days (0 = no lookback filter)",
    ),
    limit: int = Query(default=500, ge=1, le=10000),
    min_date: Optional[date] = Query(default=None),
    max_date: Optional[date] = Query(default=None),
) -> JSONResponse:
    """JSON list of wildfire events for the public dashboard."""
    try:
        payload = fetch_wildfire_events(
            lookback_days=lookback_days if lookback_days > 0 else None,
            limit=limit,
            min_date=min_date,
            max_date=max_date,
        )
        return JSONResponse(payload)
    except MapGenerationError as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.get("/demo/stats")
def demo_stats() -> JSONResponse:
    """Aggregate database stats for the demo dashboard."""
    try:
        return JSONResponse({"stats": fetch_demo_stats()})
    except MapGenerationError as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/demo/sync")
def demo_sync_firms(_: None = Depends(require_api_key)) -> JSONResponse:
    """
    Refresh demo data: download NASA FIRMS CSV and insert into PostgreSQL.

    Protected by API key. Not used by the public read path.
    """
    if not os.getenv("FIRMS_MAP_KEY"):
        return JSONResponse(
            {"status": "error", "message": "FIRMS_MAP_KEY is not configured"},
            status_code=500,
        )
    try:
        summary = sync_firms_to_postgres()
        return JSONResponse({"status": "success", **summary})
    except Exception as e:
        logger.exception("Demo FIRMS sync failed")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    return PlainTextResponse(
        generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST
    )


@app.get("/osm-tiles/{z:int}/{x:int}/{y:int}.png", response_class=Response)
def proxy_osm_raster_tile(request: Request, z: int, x: int, y: int) -> Response:
    """
    Proxy OSM raster tiles with policy-compliant Referer and User-Agent.

    Browser tile requests cannot set a custom User-Agent; this endpoint fetches
    tile.openstreetmap.org server-side with headers required by OSM operations.
    """
    if not (0 <= z <= 19):
        return Response(status_code=400)
    url = f"https://tile.openstreetmap.org/{z}/{x}/{y}.png"
    upstream_referer = (
        request.headers.get("referer")
        or request.headers.get("origin")
        or wf_config.osm_upstream_tile_referer
    )
    # Some user agents send Origin: null (e.g. file:// pages). OSM expects a real Referer.
    if not upstream_referer or upstream_referer.strip().lower() == "null":
        upstream_referer = wf_config.osm_upstream_tile_referer
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": wf_config.osm_http_user_agent,
            "Referer": upstream_referer,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            out_headers: Dict[str, str] = {}
            cache_control = resp.headers.get("Cache-Control")
            if cache_control:
                out_headers["Cache-Control"] = cache_control
            expires = resp.headers.get("Expires")
            if expires:
                out_headers["Expires"] = expires
            return Response(content=body, media_type="image/png", headers=out_headers)
    except urllib.error.HTTPError as e:
        body = e.read() if e.fp else b""
        return Response(content=body, status_code=e.code, media_type="image/png")
    except Exception as e:
        logger.warning("OSM tile proxy failed for %s: %s", url, e)
        return Response(status_code=502)


@app.get("/map", response_model=None)
def get_map(
    center_lat: float = Query(default=37.0, description="Center latitude for the map"),
    center_lon: float = Query(
        default=-120.0, description="Center longitude for the map"
    ),
    zoom: int = Query(default=5, description="Initial zoom level"),
    format: str = Query(default="html", description="Response format: html or json"),
    lookback_days: int = Query(
        default=int(os.getenv("MAP_LOOKBACK_DAYS", "30")),
        description="Only include records from the last N days (<=0 disables lookback filter)",
    ),
    max_records: int = Query(
        default=int(os.getenv("MAP_MAX_RECORDS", "5000")),
        description="Maximum number of records to plot",
    ),
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
            zoom=zoom,
            lookback_days=lookback_days,
            max_records=max_records,
        )

        generation_time = time.time() - start_time
        map_generation_time.inc(generation_time)
        map_requests_total.labels(outcome="success").inc()

        if format == "json":
            return JSONResponse(
                {
                    "status": "success",
                    "file_path": output_path,
                    "generation_time_seconds": generation_time,
                    "center_lat": center_lat,
                    "center_lon": center_lon,
                    "zoom": zoom,
                    "lookback_days": lookback_days,
                    "max_records": max_records,
                }
            )
        else:
            # Return the HTML content
            with open(output_path, "r", encoding="utf-8") as f:
                html_content = f.read()
            return HTMLResponse(
                content=html_content, headers=dict(_HTML_REFERRER_HEADERS)
            )

    except Exception as e:
        logger.exception("Map generation failed")
        map_requests_total.labels(outcome="error").inc()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/generate")
def generate_map(
    center_lat: float = Query(default=37.0, description="Center latitude for the map"),
    center_lon: float = Query(
        default=-120.0, description="Center longitude for the map"
    ),
    zoom: int = Query(default=5, description="Initial zoom level"),
    lookback_days: int = Query(
        default=int(os.getenv("MAP_LOOKBACK_DAYS", "30")),
        description="Only include records from the last N days (<=0 disables lookback filter)",
    ),
    max_records: int = Query(
        default=int(os.getenv("MAP_MAX_RECORDS", "5000")),
        description="Maximum number of records to plot",
    ),
    _: None = Depends(require_api_key),
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
            zoom=zoom,
            lookback_days=lookback_days,
            max_records=max_records,
        )

        generation_time = time.time() - start_time
        map_generation_time.inc(generation_time)
        map_requests_total.labels(outcome="success").inc()

        return JSONResponse(
            {
                "status": "success",
                "file_path": output_path,
                "generation_time_seconds": generation_time,
                "center_lat": center_lat,
                "center_lon": center_lon,
                "zoom": zoom,
                "lookback_days": lookback_days,
                "max_records": max_records,
            }
        )

    except Exception as e:
        logger.exception("Map generation failed")
        map_requests_total.labels(outcome="error").inc()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.on_event("startup")
def startup_event():
    """Optionally start Kafka map consumer and pre-generate Folium HTML."""
    if _demo_mode:
        logger.info("Demo mode enabled — Kafka map consumer disabled by default")
    # Generate map once on startup so /map works (empty DB is OK)
    try:
        generate_wildfire_map(center_lat=37.0, center_lon=-120.0, zoom=5)
        logger.info("Initial map generated at startup")
    except Exception as e:
        logger.warning(
            "Initial map generation at startup failed (will retry on /map): %s",
            e,
        )
    if _demo_mode:
        logger.info("Public demo dashboard at / — API at /demo/wildfires")
        return
    enabled = os.getenv("MAP_CONSUMER_ENABLED", "true").strip().lower()
    if enabled in {"1", "true", "yes", "y", "on"}:
        start_map_consumer_background()
    else:
        logger.info("Map consumer disabled (MAP_CONSUMER_ENABLED=%s)", enabled)


@app.on_event("shutdown")
def shutdown_event():
    """Stop the map consumer when the service shuts down."""
    logger.info("Stopping map service...")
    if map_consumer_instance:
        map_consumer_instance.running = False
