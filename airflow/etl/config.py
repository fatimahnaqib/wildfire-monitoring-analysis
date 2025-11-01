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
        self.map_key = os.getenv('FIRMS_MAP_KEY', '6384efb79923adced5331fd947f43da1')
        self.source = os.getenv('FIRMS_SOURCE', 'VIIRS_SNPP_NRT')
        self.area = os.getenv('FIRMS_AREA', '-125.0,32.0,-113.0,42.0')
        self.day_range = os.getenv('FIRMS_DAY_RANGE', '3')
        
        # Data paths
        self.data_dir = os.getenv('AIRFLOW_DATA_DIR', '/opt/airflow/data')
        self.dashboard_dir = os.getenv('AIRFLOW_DASHBOARD_DIR', '/opt/airflow/dashboard')
        
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'wildfire_data')
        
        # PostgreSQL configuration
        self.postgres_host = os.getenv('POSTGRES_HOST', 'postgres')
        self.postgres_port = os.getenv('POSTGRES_PORT', '5432')
        self.postgres_db = os.getenv('POSTGRES_DB', 'wildfire_db')
        self.postgres_user = os.getenv('POSTGRES_USER', 'airflow')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', 'airflow')
        
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
        return (f"https://firms.modaps.eosdis.nasa.gov/usfs/api/area/csv/"
                f"{self.map_key}/{self.source}/{self.area}/{self.day_range}")
    
    @property
    def output_file(self) -> str:
        """Get the output CSV file path."""
        return os.path.join(self.data_dir, "wildfire_api_data.csv")
    
    @property
    def map_output_file(self) -> str:
        """Get the map output HTML file path."""
        return os.path.join(self.dashboard_dir, "wildfire_map.html")
    
    @property
    def postgres_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return (f"postgresql://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}")


# Global configuration instance
config = WildfireConfig()
