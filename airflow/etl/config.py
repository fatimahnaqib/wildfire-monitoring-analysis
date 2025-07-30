import os

MAP_KEY = "6384efb79923adced5331fd947f43da1"
SOURCE = "VIIRS_SNPP_NRT"
AREA = "-125.0,32.0,-113.0,42.0"
DAY_RANGE = "3"

FIRMS_URL = f"https://firms.modaps.eosdis.nasa.gov/usfs/api/area/csv/{MAP_KEY}/{SOURCE}/{AREA}/{DAY_RANGE}"

DATA_DIR = "/opt/airflow/data"
os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_FILE = os.path.join(DATA_DIR, "wildfire_api_data.csv")

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "wildfire_data"
