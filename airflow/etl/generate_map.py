"""
Map generation module for wildfire data visualization.

This module handles creating interactive Folium maps from wildfire data
stored in PostgreSQL and saving them as HTML files.
"""

import os
import logging
from typing import Optional
import pandas as pd
import folium
import psycopg2
from psycopg2 import OperationalError, DatabaseError

from etl.config import config

# Configure logging
logger = logging.getLogger(__name__)


class MapGenerationError(Exception):
    """Custom exception for map generation errors."""


def connect_to_database() -> psycopg2.extensions.connection:
    """
    Establish connection to PostgreSQL database.
    
    Returns:
        psycopg2.connection: Database connection object
        
    Raises:
        MapGenerationError: If database connection fails
    """
    try:
        connection = psycopg2.connect(
            dbname=config.postgres_db,
            user=config.postgres_user,
            password=config.postgres_password,
            host=config.postgres_host,
            port=config.postgres_port
        )
        logger.info("Successfully connected to PostgreSQL database")
        return connection
        
    except OperationalError as e:
        error_msg = f"Database connection failed: {e}"
        logger.error(error_msg)
        raise MapGenerationError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected database connection error: {e}"
        logger.error(error_msg)
        raise MapGenerationError(error_msg) from e


def fetch_wildfire_data(connection: psycopg2.extensions.connection) -> pd.DataFrame:
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
        query = """
        SELECT latitude, longitude, bright_ti4, acq_date, acq_time, satellite
        FROM wildfire_events
        ORDER BY acq_date DESC, acq_time DESC
        """
        
        df = pd.read_sql_query(query, connection)
        logger.info(f"Fetched {len(df)} wildfire records from database")
        
        if df.empty:
            logger.warning("No wildfire data found in database")
            return df
        
        # Validate required columns
        required_columns = ['latitude', 'longitude', 'bright_ti4', 'acq_date']
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


def create_base_map(center_lat: float = 37.0, center_lon: float = -120.0, zoom: int = 5) -> folium.Map:
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
        # Create map with custom tiles
        map_obj = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=zoom,
            tiles='OpenStreetMap'
        )
        
        # Add additional tile layers
        folium.TileLayer(
            tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
            attr='Esri',
            name='Satellite',
            overlay=False,
            control=True
        ).add_to(map_obj)
        
        # Add layer control
        folium.LayerControl().add_to(map_obj)
        
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
                lat = float(row['latitude'])
                lon = float(row['longitude'])
                brightness = float(row['bright_ti4'])
                date = str(row['acq_date'])
                
                # Determine marker color and size based on brightness
                if brightness > 330:
                    color = 'red'
                    radius = 5
                    feature_group = high_intensity
                else:
                    color = 'orange'
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
                    fill_opacity=0.7,
                    popup=folium.Popup(popup_content, max_width=200),
                    tooltip=f"Brightness: {brightness:.1f}K"
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
        
        # Save map to file
        map_obj.save(output_path)
        
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
    zoom: int = 5
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
    
    connection = None
    try:
        # Connect to database
        connection = connect_to_database()
        
        # Fetch wildfire data
        df = fetch_wildfire_data(connection)
        
        # Create base map
        map_obj = create_base_map(center_lat, center_lon, zoom)
        
        # Add wildfire markers
        map_obj = add_wildfire_markers(map_obj, df)
        
        # Save map to file
        saved_path = save_map_to_file(map_obj, output_file)
        
        logger.info(f"Wildfire map generation completed: {saved_path}")
        return saved_path
        
    except Exception as e:
        error_msg = f"Failed to generate wildfire map: {e}"
        logger.error(error_msg, exc_info=True)
        raise MapGenerationError(error_msg) from e
        
    finally:
        # Ensure database connection is closed
        if connection:
            try:
                connection.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}")
