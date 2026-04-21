"""
Map generation module for wildfire data visualization.

This module handles creating interactive Folium maps from wildfire data
stored in PostgreSQL and saving them as HTML files.
"""

import html
import os
import logging
from typing import Optional, Tuple
import threading
import tempfile
from contextlib import contextmanager
import pandas as pd
import folium
import psycopg2
from psycopg2 import OperationalError, DatabaseError
from psycopg2.pool import ThreadedConnectionPool

from etl.config import config
from etl.transport_config import postgres_connect_kwargs

# Configure logging
logger = logging.getLogger(__name__)

_POOL_LOCK = threading.Lock()
_POOL: Optional[ThreadedConnectionPool] = None


class MapGenerationError(Exception):
    """Custom exception for map generation errors."""


def _pool_min_conn() -> int:
    return max(1, int(os.getenv("PG_POOL_MIN_CONN", "1")))


def _pool_max_conn(min_conn: int) -> int:
    return max(min_conn, int(os.getenv("PG_POOL_MAX_CONN", "4")))


def _get_pool() -> ThreadedConnectionPool:
    """
    Lazily create (and reuse) a thread-safe PostgreSQL connection pool.

    This module is used by both Airflow tasks and the FastAPI map service.
    In the map service, requests may be handled concurrently, so we must use
    a pool that is safe across threads.
    """
    global _POOL
    if _POOL is not None:
        return _POOL
    with _POOL_LOCK:
        if _POOL is not None:
            return _POOL
        min_conn = _pool_min_conn()
        max_conn = _pool_max_conn(min_conn)
        try:
            _POOL = ThreadedConnectionPool(
                min_conn,
                max_conn,
                dbname=config.postgres_db,
                user=config.postgres_user,
                password=config.postgres_password,
                host=config.postgres_host,
                port=config.postgres_port,
                **postgres_connect_kwargs(),
            )
            logger.info(
                "PostgreSQL connection pool ready for map generation (min=%s max=%s)",
                min_conn,
                max_conn,
            )
            return _POOL
        except OperationalError as e:
            error_msg = f"Database pool creation failed: {e}"
            logger.error(error_msg)
            raise MapGenerationError(error_msg) from e


@contextmanager
def pooled_connection() -> psycopg2.extensions.connection:
    """
    Borrow a connection from the pool and always return it.

    This avoids creating/destroying TCP connections for every map generation,
    and protects Postgres from connection spikes under load.
    """
    pool = _get_pool()
    conn = None
    try:
        conn = pool.getconn()
        yield conn
    finally:
        if conn is not None:
            try:
                pool.putconn(conn)
            except Exception as e:
                logger.warning("Failed returning connection to pool: %s", e)


def connect_to_database() -> psycopg2.extensions.connection:
    """
    Establish connection to PostgreSQL database.

    Note: Prefer `pooled_connection()` for production services. This function
    remains for compatibility with existing code paths that expect a raw
    connection object.

    Returns:
        psycopg2.connection: Database connection object

    Raises:
        MapGenerationError: If database connection fails
    """
    try:
        pool = _get_pool()
        connection = pool.getconn()
        logger.info("Borrowed PostgreSQL connection from pool for map generation")
        return connection

    except OperationalError as e:
        error_msg = f"Database connection failed: {e}"
        logger.error(error_msg)
        raise MapGenerationError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected database connection error: {e}"
        logger.error(error_msg)
        raise MapGenerationError(error_msg) from e


def _default_map_query_limits() -> Tuple[int, int]:
    """
    Return default query constraints for map generation.

    These protect the map generator (and API) from loading an ever-growing table
    into memory.
    """
    lookback_days = int(os.getenv("MAP_LOOKBACK_DAYS", "7"))
    max_records = int(os.getenv("MAP_MAX_RECORDS", "5000"))
    # Guardrails for silly values
    lookback_days = max(1, lookback_days)
    max_records = max(1, max_records)
    return lookback_days, max_records


def fetch_wildfire_data(
    connection: psycopg2.extensions.connection,
    *,
    lookback_days: Optional[int] = None,
    max_records: Optional[int] = None,
) -> pd.DataFrame:
    """
    Fetch wildfire data from PostgreSQL database.

    Args:
        connection: Database connection object

    Returns:
        pd.DataFrame: Wildfire data with required columns

    Raises:
        MapGenerationError: If data fetching fails
    """
    try:
        default_lookback_days, default_max_records = _default_map_query_limits()
        lookback_days = (
            default_lookback_days if lookback_days is None else int(lookback_days)
        )
        max_records = default_max_records if max_records is None else int(max_records)

        # Safety: always apply a LIMIT; allow disabling lookback only if explicitly <= 0.
        max_records = max(1, max_records)

        if lookback_days is not None and int(lookback_days) > 0:
            query = """
            SELECT latitude, longitude, bright_ti4, acq_date, acq_time, satellite
            FROM wildfire_events
            WHERE acq_date >= (CURRENT_DATE - %s)
            ORDER BY acq_date DESC, acq_time DESC
            LIMIT %s
            """
            params = (int(lookback_days), max_records)
            df = pd.read_sql_query(query, connection, params=params)
            logger.info(
                "Fetched %s wildfire records from database (lookback_days=%s, limit=%s)",
                len(df),
                lookback_days,
                max_records,
            )
        else:
            query = """
            SELECT latitude, longitude, bright_ti4, acq_date, acq_time, satellite
            FROM wildfire_events
            ORDER BY acq_date DESC, acq_time DESC
            LIMIT %s
            """
            df = pd.read_sql_query(query, connection, params=(max_records,))
            logger.info(
                "Fetched %s wildfire records from database (lookback_days=disabled, limit=%s)",
                len(df),
                max_records,
            )

        if df.empty:
            logger.warning("No wildfire data found in database")
            return df

        # Validate required columns
        required_columns = ["latitude", "longitude", "bright_ti4", "acq_date"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            logger.error(error_msg)
            raise MapGenerationError(error_msg)

        return df

    except DatabaseError as e:
        error_msg = f"Database query failed: {e}"
        logger.error(error_msg)
        raise MapGenerationError(error_msg) from e
    except Exception as e:
        error_msg = f"Error fetching wildfire data: {e}"
        logger.error(error_msg)
        raise MapGenerationError(error_msg) from e


def create_base_map(
    center_lat: float = 37.0, center_lon: float = -120.0, zoom: int = 5
) -> folium.Map:
    """
    Create a base Folium map with standard configuration.

    Args:
        center_lat: Center latitude for the map
        center_lon: Center longitude for the map
        zoom: Initial zoom level

    Returns:
        folium.Map: Configured base map
    """
    try:
        osm_attr = (
            '&copy; <a href="https://www.openstreetmap.org/copyright">'
            "OpenStreetMap</a> contributors"
        )
        tile_url = config.osm_raster_tile_url_template

        # OSM tile policy: encourage a real Referer on cross-origin tile <img> requests
        # (Leaflet 1.9+). When using the local /osm-tiles proxy, tiles are same-origin.
        tile_kwargs = dict(
            tiles=tile_url,
            attr=osm_attr,
            name="OpenStreetMap",
            min_zoom=0,
            max_zoom=19,
            max_native_zoom=19,
            subdomains="",
            control=True,
            show=True,
        )
        if not config.osm_tile_use_proxy:
            tile_kwargs["referrer_policy"] = config.osm_leaflet_tile_referrer_policy

        base_tiles = folium.TileLayer(**tile_kwargs)

        map_obj = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=zoom,
            tiles=base_tiles,
            width="100%",
            height="100%",
        )

        # Document-level referrer policy (avoids empty Referer when a parent page used
        # Referrer-Policy: no-referrer).
        meta_policy = html.escape(config.osm_referrer_meta_policy, quote=True)
        policy_head = f'<meta name="referrer" content="{meta_policy}">\n'
        map_obj.get_root().header.add_child(folium.Element(policy_head))

        # Ensure the map fills the viewport when opened as a standalone HTML file.
        # Folium uses a div with class "folium-map" for the Leaflet container.
        map_css = """
        <style>
          html, body { height: 100%; margin: 0; padding: 0; }
          .folium-map { width: 100%; height: 100vh; }
        </style>
        """
        map_obj.get_root().header.add_child(folium.Element(map_css))

        # Add additional tile layers
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            attr="Esri",
            name="Satellite",
            overlay=False,
            control=True,
            show=False,
        ).add_to(map_obj)

        logger.info(f"Created base map centered at ({center_lat}, {center_lon})")
        return map_obj

    except Exception as e:
        error_msg = f"Error creating base map: {e}"
        logger.error(error_msg)
        raise MapGenerationError(error_msg) from e


def add_wildfire_markers(map_obj: folium.Map, df: pd.DataFrame) -> folium.Map:
    """
    Add wildfire markers to the map based on data.

    Args:
        map_obj: Folium map object
        df: DataFrame containing wildfire data

    Returns:
        folium.Map: Map with wildfire markers added
    """
    try:
        if df.empty:
            logger.warning("No data to add to map")
            return map_obj

        # Create feature groups for different intensity levels
        high_intensity = folium.FeatureGroup(name="High Intensity (>330K)")
        medium_intensity = folium.FeatureGroup(name="Medium Intensity (≤330K)")

        for _, row in df.iterrows():
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                brightness = float(row["bright_ti4"])
                date = str(row["acq_date"])

                # Determine marker color and size based on brightness
                if brightness > 330:
                    color = "red"
                    radius = 5
                    feature_group = high_intensity
                else:
                    color = "orange"
                    radius = 3
                    feature_group = medium_intensity

                # Create popup content
                popup_content = f"""
                <b>Wildfire Detection</b><br>
                Date: {date}<br>
                Brightness: {brightness:.1f}K<br>
                Location: ({lat:.4f}, {lon:.4f})
                """

                # Add marker to appropriate feature group
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=radius,
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.7,
                    popup=folium.Popup(popup_content, max_width=200),
                    tooltip=f"Brightness: {brightness:.1f}K",
                ).add_to(feature_group)

            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping invalid row: {e}")
                continue

        # Add feature groups to map
        high_intensity.add_to(map_obj)
        medium_intensity.add_to(map_obj)

        logger.info(f"Added {len(df)} wildfire markers to map")
        return map_obj

    except Exception as e:
        error_msg = f"Error adding wildfire markers: {e}"
        logger.error(error_msg)
        raise MapGenerationError(error_msg) from e


def add_map_legend(map_obj: folium.Map) -> folium.Map:
    try:
        legend_html = """
        <div style="
            position: fixed;
            bottom: 24px;
            left: 24px;
            z-index: 9999;
            background: rgba(255, 255, 255, 0.92);
            padding: 10px 12px;
            border-radius: 10px;
            box-shadow: 0 6px 18px rgba(0,0,0,0.18);
            font-size: 13px;
            line-height: 1.25;
            max-width: 260px;
        ">
            <div style="font-weight: 700; margin-bottom: 8px;">Wildfire intensity</div>
            <div style="display:flex; align-items:center; gap:8px; margin-bottom:6px;">
                <span style="width:10px; height:10px; border-radius:50%; background:#ff0000; display:inline-block;"></span>
                <span>High (&gt; 330K)</span>
            </div>
            <div style="display:flex; align-items:center; gap:8px;">
                <span style="width:10px; height:10px; border-radius:50%; background:#ffa500; display:inline-block;"></span>
                <span>Medium (≤ 330K)</span>
            </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))
        return map_obj
    except Exception as e:
        logger.warning("Failed to add legend: %s", e)
        return map_obj


def save_map_to_file(map_obj: folium.Map, output_path: str) -> str:
    """
    Save the map to an HTML file.

    Args:
        map_obj: Folium map object
        output_path: Path where to save the HTML file

    Returns:
        str: Path to the saved file

    Raises:
        MapGenerationError: If file saving fails
    """
    try:
        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        # Save atomically to avoid concurrent writers leaving a truncated file.
        # Write to a temp file in the same directory, then replace.
        fd, tmp_path = tempfile.mkstemp(
            prefix=".wildfire_map_",
            suffix=".html",
            dir=output_dir,
        )
        try:
            os.close(fd)
            map_obj.save(tmp_path)
            os.replace(tmp_path, output_path)
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                # Best-effort cleanup; not fatal.
                pass

        # Verify file was created
        if not os.path.exists(output_path):
            raise MapGenerationError(f"Map file was not created: {output_path}")

        file_size = os.path.getsize(output_path)
        logger.info(f"Map saved to {output_path} ({file_size} bytes)")
        return output_path

    except OSError as e:
        error_msg = f"File system error saving map: {e}"
        logger.error(error_msg)
        raise MapGenerationError(error_msg) from e

    except Exception as e:
        error_msg = f"Error saving map to file: {e}"
        logger.error(error_msg)
        raise MapGenerationError(error_msg) from e


def generate_wildfire_map(
    output_path: Optional[str] = None,
    center_lat: float = 37.0,
    center_lon: float = -120.0,
    zoom: int = 5,
    lookback_days: Optional[int] = None,
    max_records: Optional[int] = None,
) -> str:
    """
    Generate an interactive wildfire map from database data.

    Args:
        output_path: Path for output HTML file (defaults to config.map_output_file)
        center_lat: Center latitude for the map
        center_lon: Center longitude for the map
        zoom: Initial zoom level

    Returns:
        str: Path to the generated HTML file

    Raises:
        MapGenerationError: If map generation fails
    """
    # Use default output path if not provided
    output_file = output_path or config.map_output_file

    logger.info("Starting wildfire map generation")

    try:
        with pooled_connection() as connection:
            # Fetch wildfire data
            df = fetch_wildfire_data(
                connection,
                lookback_days=lookback_days,
                max_records=max_records,
            )

        # Create base map
        map_obj = create_base_map(center_lat, center_lon, zoom)

        # Add wildfire markers
        map_obj = add_wildfire_markers(map_obj, df)

        # Add legend for quick interpretation
        map_obj = add_map_legend(map_obj)

        # Add layer control after overlays are created, so the generated JS
        # references existing layer variables (avoids ReferenceError in browser).
        folium.LayerControl(collapsed=False).add_to(map_obj)

        # Save map to file
        saved_path = save_map_to_file(map_obj, output_file)

        logger.info(f"Wildfire map generation completed: {saved_path}")
        return saved_path

    except Exception as e:
        error_msg = f"Failed to generate wildfire map: {e}"
        logger.error(error_msg, exc_info=True)
        raise MapGenerationError(error_msg) from e

    finally:
        # No explicit close needed: pooled_connection() returns it to the pool.
        pass
