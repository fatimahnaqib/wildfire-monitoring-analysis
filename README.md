# Wildfire Monitoring Pipeline

Real-time wildfire detection pipeline using NASA FIRMS satellite data. Processes fire detection events through Kafka, stores them in PostgreSQL, and generates interactive map visualizations.

## What This Does

Downloads near real-time wildfire data from NASA's FIRMS API (VIIRS satellites), validates it, streams it through Kafka for real-time processing, stores it in PostgreSQL, and generates interactive Folium maps showing fire locations with intensity markers.

The pipeline runs as an Airflow DAG that orchestrates microservices for ingestion, processing, and visualization. Each component is containerized and exposed as a FastAPI service with health checks and Prometheus metrics.

## Architecture

```
NASA FIRMS API
    ↓
Ingestion Service (downloads CSV)
    ↓
Producer Service (validates → Kafka)
    ↓
Consumer Service (Kafka → PostgreSQL)
    ↓
Map Service (PostgreSQL → Folium HTML)
```

**Components:**
- **Airflow**: Orchestrates the daily pipeline via DAG
- **FastAPI Services**: Four microservices handling ingestion, production, consumption, and visualization
- **Kafka**: Streams validated records (KRaft mode, no Zookeeper)
- **PostgreSQL**: Stores historical fire events with deduplication
- **Standalone Consumer**: Alternative Kafka consumer that auto-creates topics

The pipeline is event-driven. Airflow triggers ingestion, which writes CSV to shared volume. Producer reads CSV, validates records, publishes to Kafka. Consumer processes messages and inserts into PostgreSQL. Map service queries the database and generates HTML maps.

## Setup

**Requirements:**
- Docker 20.10+ and Docker Compose 2.0+
- The following ports must be available: 8080, 5432, 9092, 8000–8003

**Installation:**

```bash
git clone <repo-url>
cd wildfire-monitoring-analysis
docker-compose up --build
```

Services start in dependency order (PostgreSQL → Kafka → Microservices → Airflow).

**Initial Setup:**

The `airflow-init` service creates the admin user. If login fails:

```bash
docker-compose run --rm airflow-init
docker-compose restart airflow-webserver
```

**Access:**
- Airflow UI: http://localhost:8080 (`admin`/`admin`)
- Service APIs: http://localhost:8000-8003
- API Docs: http://localhost:8000-8003/docs (each service)
- Generated Maps: `airflow/dashboard/wildfire_map.html`

## Map Example

The pipeline generates interactive Folium maps showing wildfire locations with color-coded markers based on fire intensity. Red markers indicate high-intensity fires (>330K brightness), orange markers show medium-intensity fires.

This is an example of the map generated from recent wildfire detection data:

![Wildfire Map Example](airflow/dashboard/wildfire_map.png)

The map is interactive—you can zoom, pan, switch between OpenStreetMap and satellite tile layers, and click markers for fire details (date, brightness, coordinates). Maps are regenerated each time the DAG runs, so the visualization updates with the latest data from the database.

## Running the Pipeline

**Automatic:** DAG `wildfire_etl_pipeline` runs daily at midnight UTC.

**Manual:** Trigger via Airflow UI or:

```bash
docker-compose exec airflow-webserver airflow dags trigger wildfire_etl_pipeline
```

**DAG Tasks:**
1. `download_wildfire_api_csv` - Calls ingestion service, downloads NASA data
2. `produce_to_kafka` - Validates CSV, publishes to Kafka topic `wildfire_data`
3. `generate_wildfire_map` - Queries PostgreSQL, generates Folium map

## Project Structure

```
wildfire-monitoring-analysis/
├── airflow/
│   ├── dags/wildfire_etl_dag.py    # Main DAG definition
│   ├── etl/                        # Shared ETL modules
│   │   ├── config.py               # Environment/config management
│   │   ├── download.py             # NASA API client
│   │   ├── validation.py           # Record validation logic
│   │   ├── kafka_producer.py       # Kafka producer
│   │   └── generate_map.py        # Folium map generation
│   ├── data/                       # Downloaded CSVs
│   └── dashboard/                  # Generated HTML maps
│
├── services/                        # FastAPI microservices
│   ├── ingestion/app/main.py       # NASA download endpoint
│   ├── producer/app/main.py        # Kafka producer endpoint
│   ├── consumer/app/main.py         # Kafka consumer + FastAPI
│   └── map/app/main.py             # Map generation endpoint
│
├── kafka_consumer/                  # Standalone consumer
│   ├── consumer.py                 # Kafka consumer impl
│   └── create_topic.py             # Topic creation utility
│
├── postgres/init/init.sql           # Database schema
└── docker-compose.yml              # Service definitions
```

**Design Notes:**
- ETL modules in `airflow/etl/` are copied into service containers for code reuse
- Services share volumes for CSV data (`./airflow/data`) and maps (`./airflow/dashboard`)
- Consumer service runs consumer in background thread alongside FastAPI
- All services expose `/health` and `/metrics` endpoints

## API Usage

**Test Services Directly:**

```bash
# Download data
curl -X POST "http://localhost:8000/ingest?area=-125.0,32.0,-113.0,42.0&day_range=3"

# Produce to Kafka
curl -X POST "http://localhost:8001/produce"

# Generate map
curl -X POST "http://localhost:8003/generate?center_lat=37.0&center_lon=-120.0&zoom=5"

# Check consumer status
curl http://localhost:8002/stats
```

**Health Checks:**

All services respond to `/health`. Consumer service includes running status and uptime.

**Metrics:**

Prometheus metrics at `/metrics` on each service. Tracks request counts, processing times, and Kafka/DB connection status.

## Configuration

**Environment Variables** (set in `docker-compose.yml`):

- `FIRMS_AREA`: Bounding box `lonW,latS,lonE,latN` (default: California)
- `FIRMS_DAY_RANGE`: Historical days to fetch (default: `3`)
- `KAFKA_TOPIC`: Topic name (default: `wildfire_data`)
- `POSTGRES_*`: Database credentials

**DAG Parameters** (editable in Airflow UI):

- `map_center_lat`, `map_center_lon`: Map center coordinates
- `map_zoom`: Initial zoom level
- `firms_area`: Override default geographic region

**Changing Map Center:**

Edit `airflow/dags/wildfire_etl_dag.py` params or pass via Airflow UI when triggering.

## Data Model

**PostgreSQL Schema** (`wildfire_events` table):

- `latitude`, `longitude`: Fire location (DOUBLE PRECISION)
- `bright_ti4`, `bright_ti5`: Brightness temperatures (Kelvin)
- `acq_date`, `acq_time`: Acquisition timestamp
- `satellite`: Satellite identifier (N, N20, N21)
- `confidence`: Detection confidence (n/h/l)
- `frp`: Fire radiative power (MW)

**Deduplication:**

Unique constraint on `(latitude, longitude, acq_date, acq_time, satellite)`. Duplicate inserts are silently skipped using `ON CONFLICT DO NOTHING`.

**Validation Rules:**

Records must have all required fields, numeric fields must parse to float, enum fields must match valid values. Invalid records are logged and skipped during Kafka production.

## Development

**Rebuilding Services:**

After changing ETL code, rebuild affected services:

```bash
docker-compose build ingestion producer consumer map
docker-compose up -d
```

**Viewing Logs:**

```bash
docker-compose logs -f <service-name>
docker-compose logs -f airflow-webserver
```

**Database Access:**

```bash
docker-compose exec postgres psql -U airflow -d wildfire_db
```

**Kafka Topics:**

```bash
# List topics
docker-compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Consume messages
docker-compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic wildfire_data \
  --from-beginning
```

**Local Development:**

Services can run locally, but you'll need:
- PostgreSQL running (update `POSTGRES_HOST` in config)
- Kafka running (update `KAFKA_BOOTSTRAP_SERVERS`)
- Shared volumes mounted or adjust file paths in config

## Troubleshooting

**Airflow login fails:**
```bash
docker-compose run --rm airflow-init
docker-compose restart airflow-webserver
```

**Services won't start:**
Check dependencies are healthy:
```bash
docker-compose ps
docker-compose logs postgres
docker-compose logs kafka
```

**No data in database:**
- NASA API may have no recent fires in the region
- Check ingestion service logs for API errors
- Verify CSV file exists: `ls airflow/data/wildfire_api_data.csv`
- Query Kafka to see if messages are being produced

**Map generation fails:**
- Verify database has records: `SELECT COUNT(*) FROM wildfire_events;`
- Check map service can connect to PostgreSQL
- View map service logs for Folium errors

**Kafka connection issues:**
- Kafka uses KRaft mode (no Zookeeper)
- Check health: `docker-compose ps kafka`
- Verify topic exists and has partitions
- Check consumer group lag: consumer service `/stats` endpoint

## Stack

- **Python 3.11**
- **Apache Airflow 2.8.1** - Workflow orchestration
- **FastAPI 0.115.0** - REST APIs
- **Apache Kafka** (Bitnami, KRaft) - Message streaming
- **PostgreSQL 14** - Data storage
- **Folium 0.16.0** - Map visualization
- **Confluent Kafka** - Python Kafka client
- **Prometheus client** - Metrics exposure

## Notes

- **NASA API delay**: FIRMS data has 15-30 minute latency
- **Default region**: California bounding box; adjust via DAG params
- **Data retention**: No automatic cleanup; all records persist
- **KRaft mode**: Kafka runs without Zookeeper for simplicity
- **Validation**: Multi-layer validation (producer validates before Kafka, consumer validates before DB)
- **Error handling**: Services retry with exponential backoff; failed records are logged but don't stop pipeline

## Contributing

Open issues for bugs or feature requests. PRs welcome. Make sure:
- Code follows existing patterns
- Tests pass (when added)
- Docker builds succeed
- Services start without errors
