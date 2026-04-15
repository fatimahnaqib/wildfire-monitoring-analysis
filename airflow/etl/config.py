"""
Configuration module for wildfire monitoring pipeline.

This module handles all configuration settings including environment variables,
API endpoints, and file paths for the wildfire ETL pipeline.
"""

import os
import logging

# Configure logging
logger = logging.getLogger(__name__)


class WildfireConfig:
    """Configuration class for wildfire monitoring pipeline."""

    def __init__(self):
        """Initialize configuration with environment variables or defaults."""
        self.map_key = os.getenv("FIRMS_MAP_KEY", "6384efb79923adced5331fd947f43da1")
        self.source = os.getenv("FIRMS_SOURCE", "VIIRS_SNPP_NRT")
        self.area = os.getenv("FIRMS_AREA", "-125.0,32.0,-113.0,42.0")
        self.day_range = os.getenv("FIRMS_DAY_RANGE", "3")

        # Data paths
        self.data_dir = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")
        self.dashboard_dir = os.getenv(
            "AIRFLOW_DASHBOARD_DIR", "/opt/airflow/dashboard"
        )

        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka-1:9092,kafka-2:9092,kafka-3:9092",
        )
        self.kafka_topic = os.getenv("KAFKA_TOPIC", "wildfire_data")

        # PostgreSQL configuration
        self.postgres_host = os.getenv("POSTGRES_HOST", "postgres")
        self.postgres_port = os.getenv("POSTGRES_PORT", "5432")
        self.postgres_db = os.getenv("POSTGRES_DB", "wildfire_db")
        self.postgres_user = os.getenv("POSTGRES_USER", "airflow")
        self.postgres_password = os.getenv("POSTGRES_PASSWORD", "airflow")

        # OpenStreetMap tile policy (browser tiles cannot set User-Agent; optional HTTP proxy
        # on the map service adds Referer + app User-Agent on server-to-OSM requests).
        self.osm_tile_use_proxy = os.getenv("OSM_TILE_USE_PROXY", "true").lower() in (
            "1",
            "true",
            "yes",
        )
        self.map_public_base_url = os.getenv("MAP_PUBLIC_BASE_URL", "").rstrip("/")
        self.osm_http_user_agent = os.getenv(
            "OSM_HTTP_USER_AGENT",
            "WildfireMonitoring/1.0 (+https://operations.osmfoundation.org/policies/tiles/)",
        )
        # Fallback when the tile proxy cannot use the browser's Referer/Origin header.
        self.osm_upstream_tile_referer = os.getenv(
            "OSM_UPSTREAM_REFERER",
            self.map_public_base_url or "https://www.openstreetmap.org/",
        )
        self.osm_referrer_meta_policy = os.getenv(
            "MAP_REFERRER_META_POLICY", "strict-origin-when-cross-origin"
        )
        self.osm_leaflet_tile_referrer_policy = os.getenv(
            "OSM_LEAFLET_REFERRER_POLICY", "strict-origin-when-cross-origin"
        )

        # Create directories
        self._create_directories()

    def _create_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            os.makedirs(self.dashboard_dir, exist_ok=True)
            logger.info(f"Created directories: {self.data_dir}, {self.dashboard_dir}")
        except OSError as e:
            logger.error(f"Failed to create directories: {e}")
            raise

    @property
    def firms_url(self) -> str:
        """Get the FIRMS API URL."""
        return (
            f"https://firms.modaps.eosdis.nasa.gov/usfs/api/area/csv/"
            f"{self.map_key}/{self.source}/{self.area}/{self.day_range}"
        )

    @property
    def output_file(self) -> str:
        """Get the output CSV file path."""
        return os.path.join(self.data_dir, "wildfire_api_data.csv")

    @property
    def map_output_file(self) -> str:
        """Get the map output HTML file path."""
        return os.path.join(self.dashboard_dir, "wildfire_map.html")

    @property
    def osm_raster_tile_url_template(self) -> str:
        """
        Leaflet tile URL for the OSM basemap.

        When OSM_TILE_USE_PROXY is true, uses a root-relative path so the browser
        requests tiles from this app; the map service proxies to tile.openstreetmap.org
        with policy-compliant Referer and User-Agent headers.
        """
        if self.osm_tile_use_proxy:
            # Root-relative URLs work for pages served by the map app, but break when
            # the HTML is opened as a local file (file://...). If MAP_PUBLIC_BASE_URL is
            # set (recommended in docker-compose), bake an absolute tile URL so the saved
            # HTML still loads tiles as long as the map service is reachable.
            if self.map_public_base_url:
                return f"{self.map_public_base_url}/osm-tiles/{{z}}/{{x}}/{{y}}.png"
            return "/osm-tiles/{z}/{x}/{y}.png"
        return "https://tile.openstreetmap.org/{z}/{x}/{y}.png"

    @property
    def postgres_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


# Global configuration instance
config = WildfireConfig()
