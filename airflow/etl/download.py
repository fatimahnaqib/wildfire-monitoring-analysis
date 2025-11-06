"""
Data download module for NASA FIRMS API.

This module handles downloading wildfire data from the NASA FIRMS API
and saving it to the local filesystem for further processing.
"""

import os
import requests
import logging
from typing import Optional
from requests.exceptions import RequestException, Timeout, ConnectionError

from etl.config import config

# Configure logging
logger = logging.getLogger(__name__)


class FIRMSDownloadError(Exception):
    """Custom exception for FIRMS API download errors."""


def download_firms_api_csv(
    url: Optional[str] = None, output_file: Optional[str] = None, timeout: int = 30
) -> str:
    """
    Download wildfire data from NASA FIRMS API.

    Args:
        url: FIRMS API URL (defaults to config.firms_url)
        output_file: Output file path (defaults to config.output_file)
        timeout: Request timeout in seconds

    Returns:
        str: Path to the downloaded CSV file

    Raises:
        FIRMSDownloadError: If download fails
        FileNotFoundError: If output directory doesn't exist
    """
    # Use defaults from config if not provided
    api_url = url or config.firms_url
    output_path = output_file or config.output_file

    logger.info(f"Requesting wildfire CSV from NASA FIRMS API: {api_url}")

    try:
        # Make HTTP request with timeout
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()

        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        # Write response content to file
        with open(output_path, "wb") as file_handle:
            file_handle.write(response.content)

        # Verify file was created and has content
        if not os.path.exists(output_path):
            raise FIRMSDownloadError(f"Output file was not created: {output_path}")

        file_size = os.path.getsize(output_path)
        if file_size == 0:
            raise FIRMSDownloadError("Downloaded file is empty")

        logger.info(
            f"Wildfire data successfully saved to {output_path} ({file_size} bytes)"
        )
        return output_path

    except (Timeout, ConnectionError, RequestException, OSError) as e:
        error_msg = f"Download failed: {e}"
        logger.error(error_msg)
        raise FIRMSDownloadError(error_msg) from e
    except Exception as e:
        error_msg = f"Unexpected error during download: {e}"
        logger.error(error_msg)
        raise FIRMSDownloadError(error_msg) from e
