import logging

import pandas as pd
from spain_fuel_api.fetch import fetch_raw_data
from spain_fuel_api.transform import transform_to_dataframe
from spain_fuel_api.validate import validate_api_response

logger = logging.getLogger(__name__)


def fetch_fuel_stations(
    curl_timeout: int = 120,
    connect_timeout: int = 10,
    max_retries: int = 3,
    retry_base_delay: int = 10,
    exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Fetch, validate and transform Spain fuel-price API data into a DataFrame.

    Convenience wrapper composing fetch_raw_data -> validate_api_response ->
    transform_to_dataframe. Raises on fetch failure or invalid API response.
    """
    raw_data = fetch_raw_data(
        curl_timeout=curl_timeout,
        connect_timeout=connect_timeout,
        max_retries=max_retries,
        retry_base_delay=retry_base_delay,
        exponential_backoff=exponential_backoff,
    )
    validate_api_response(raw_data)
    logger.info("Response status: %s", raw_data.get("ResultadoConsulta"))
    return transform_to_dataframe(raw_data)
