import requests
import logging
from .config import FIRMS_URL, OUTPUT_FILE

def download_firms_api_csv():
    try:
        logging.info("Requesting wildfire CSV from NASA FIRMS API...")
        response = requests.get(FIRMS_URL)
        response.raise_for_status()

        with open(OUTPUT_FILE, "wb") as f:
            f.write(response.content)

        logging.info(f"Wildfire data saved to {OUTPUT_FILE}")
    except requests.RequestException as e:
        logging.error(f"Error downloading FIRMS data: {str(e)}")
        raise
