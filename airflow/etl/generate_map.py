import pandas as pd
import folium
import psycopg2
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_wildfire_map():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname='wildfire_db',
            user='airflow',
            password='airflow',
            host='postgres',
            port='5432'
        )
        logger.info("Connected to PostgreSQL database.")

        # Fetch data
        query = "SELECT latitude, longitude, bright_ti4, acq_date FROM wildfire_events"
        df = pd.read_sql_query(query, conn)
        logger.info(f"Fetched {len(df)} records from wildfire_firms_data.")

        # Create folium map centered on California
        m = folium.Map(location=[37.0, -120.0], zoom_start=5)

        for _, row in df.iterrows():
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=3,
                color='red' if row['bright_ti4'] > 330 else 'orange',
                fill=True,
                fill_opacity=0.7,
                popup=f"Date: {row['acq_date']}, Brightness: {row['bright_ti4']}"
            ).add_to(m)

        # Save to dashboard folder
        output_dir = "/opt/airflow/dashboard"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "wildfire_map.html")
        m.save(output_path)

        logger.info(f"Map successfully saved to: {output_path}")

    except Exception as e:
        logger.error("Failed to generate wildfire map", exc_info=True)
        raise
