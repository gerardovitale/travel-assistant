import logging

import pandas as pd
from commodity_api.constants import DEFAULT_SERIES_ID
from commodity_api.fetch import fetch_raw_data
from commodity_api.transform import transform_to_dataframe
from commodity_api.validate import validate_api_response

logger = logging.getLogger(__name__)


def fetch_commodity_prices(
    api_key: str,
    series_id: str = DEFAULT_SERIES_ID,
    observation_start: str | None = None,
    observation_end: str | None = None,
    connect_timeout: int = 10,
    read_timeout: int = 30,
    max_retries: int = 3,
    retry_base_delay: int = 10,
    exponential_backoff: bool = True,
) -> pd.DataFrame:
    """Fetch, validate and transform FRED commodity data into a DataFrame.

    Convenience wrapper composing fetch_raw_data -> validate_api_response ->
    transform_to_dataframe. Raises on fetch failure or invalid API response.
    """
    raw_data = fetch_raw_data(
        api_key=api_key,
        series_id=series_id,
        observation_start=observation_start,
        observation_end=observation_end,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        max_retries=max_retries,
        retry_base_delay=retry_base_delay,
        exponential_backoff=exponential_backoff,
    )
    validate_api_response(raw_data)
    logger.info("Fetched %d raw observation(s) for series %s", len(raw_data.get("observations", [])), series_id)
    return transform_to_dataframe(raw_data, series_id)
