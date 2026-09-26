import logging
import time

import requests
from commodity_api.constants import DATA_SOURCE_URL
from commodity_api.constants import DEFAULT_SERIES_ID

logger = logging.getLogger(__name__)


def fetch_raw_data(
    api_key: str,
    series_id: str = DEFAULT_SERIES_ID,
    observation_start: str | None = None,
    observation_end: str | None = None,
    connect_timeout: int = 10,
    read_timeout: int = 30,
    max_retries: int = 3,
    retry_base_delay: int = 10,
    exponential_backoff: bool = True,
) -> dict:
    """Fetch raw FRED series-observations JSON.

    Unlike spain_fuel_api.fetch (which shells out to curl because the gov server
    TLS-fingerprint-blocks Python's OpenSSL 3.x stack), FRED has no such blocking, so
    a plain `requests` call is used here.

    Retries on requests.exceptions.RequestException. With exponential_backoff the delay
    is retry_base_delay * 2**(attempt-1); otherwise it is a fixed retry_base_delay.
    Raises requests.exceptions.RequestException after exhaustion.
    """
    params = {"series_id": series_id, "api_key": api_key, "file_type": "json"}
    if observation_start:
        params["observation_start"] = observation_start
    if observation_end:
        params["observation_end"] = observation_end

    logger.info("Fetching commodity raw data for series %s from %s", series_id, DATA_SOURCE_URL)
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(DATA_SOURCE_URL, params=params, timeout=(connect_timeout, read_timeout))
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            if attempt < max_retries:
                delay = retry_base_delay * (2 ** (attempt - 1)) if exponential_backoff else retry_base_delay
                logger.warning("Fetch attempt %d/%d failed: %s. Retrying in %ds...", attempt, max_retries, exc, delay)
                time.sleep(delay)
            else:
                logger.error("All %d fetch attempts failed: %s", max_retries, exc)
                raise
    raise RuntimeError("All fetch attempts exhausted")
