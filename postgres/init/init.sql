-- Create wildfire data warehouse
CREATE DATABASE wildfire_db;

-- Create Airflow metadata DB
CREATE DATABASE airflowdb;

-- Switch to wildfire_db and create the wildfire_events table
\connect wildfire_db;

CREATE TABLE IF NOT EXISTS wildfire_events (
    id SERIAL PRIMARY KEY,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    bright_ti4 DOUBLE PRECISION,
    bright_ti5 DOUBLE PRECISION,
    scan DOUBLE PRECISION,
    track DOUBLE PRECISION,
    acq_date DATE,
    acq_time TIME,
    satellite VARCHAR(10),
    confidence VARCHAR(10),
    version VARCHAR(10),
    frp DOUBLE PRECISION,
    daynight VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (latitude, longitude, acq_date, acq_time, satellite)
);
