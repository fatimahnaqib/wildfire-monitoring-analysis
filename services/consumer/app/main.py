import os
import logging
import threading
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from app.consumer import WildfireConsumer

logger = logging.getLogger("consumer_service")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(title="Wildfire Consumer Service", version="1.0.0")

# Global consumer instance and status
consumer_instance = None
consumer_thread = None
consumer_running = False
consumer_stats = {
    "messages_processed": 0,
    "messages_failed": 0,
    "last_message_time": None,
    "start_time": None,
}


def start_consumer_background():
    """Start the consumer in a background thread."""
    global consumer_instance, consumer_thread, consumer_running, consumer_stats

    if consumer_running:
        return

    consumer_stats["start_time"] = time.time()
    consumer_running = True

    def run_consumer():
        global consumer_instance, consumer_running
        try:
            consumer_instance = WildfireConsumer()
            consumer_instance.consume_messages()
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            consumer_running = False

    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    consumer_thread.start()
    logger.info("Consumer started in background thread")


def stop_consumer():
    """Stop the consumer gracefully."""
    global consumer_running
    if consumer_instance and consumer_running:
        consumer_running = False
        logger.info("Consumer stop requested")


@app.get("/health")
def health() -> JSONResponse:
    """Health check endpoint."""
    status = "healthy" if consumer_running else "stopped"
    return JSONResponse(
        {
            "status": status,
            "consumer_running": consumer_running,
            "uptime_seconds": time.time() - consumer_stats.get("start_time", 0)
            if consumer_stats.get("start_time")
            else 0,
        }
    )


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    """Prometheus metrics endpoint."""
    return PlainTextResponse(
        generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST
    )


@app.post("/start")
def start_consumer() -> JSONResponse:
    """Start the consumer service."""
    try:
        start_consumer_background()
        return JSONResponse({"status": "success", "message": "Consumer started"})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/stop")
def stop_consumer_endpoint() -> JSONResponse:
    """Stop the consumer service."""
    try:
        stop_consumer()
        return JSONResponse({"status": "success", "message": "Consumer stop requested"})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.get("/stats")
def get_stats() -> JSONResponse:
    """Get consumer statistics."""
    return JSONResponse(
        {
            "consumer_running": consumer_running,
            "stats": consumer_stats,
            "uptime_seconds": time.time() - consumer_stats.get("start_time", 0)
            if consumer_stats.get("start_time")
            else 0,
        }
    )


# Start consumer automatically when the service starts
@app.on_event("startup")
def startup_event():
    """Start the consumer when the service starts."""
    logger.info("Starting consumer service...")
    start_consumer_background()


@app.on_event("shutdown")
def shutdown_event():
    """Stop the consumer when the service shuts down."""
    logger.info("Stopping consumer service...")
    stop_consumer()
