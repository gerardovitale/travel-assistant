# External commodity-price API client (FRED — Federal Reserve Economic Data).
#
# Single source of truth for fetching, validating and transforming commodity time
# series (starting with Brent crude, DCOILBRENTEU) into a normalized tidy DataFrame.
# Shared by fuel-ingestor and fuel-dashboard. See SCHEMA.md for the raw API + output
# DataFrame data contract.
from commodity_api.client import fetch_commodity_prices
from commodity_api.constants import DATA_SOURCE_URL
from commodity_api.constants import DEFAULT_SERIES_ID
from commodity_api.constants import SERIES_METADATA
from commodity_api.fetch import fetch_raw_data
from commodity_api.schema import get_expected_columns
from commodity_api.schema import get_series_metadata
from commodity_api.transform import transform_to_dataframe
from commodity_api.validate import validate_api_response

__all__ = [
    "DATA_SOURCE_URL",
    "DEFAULT_SERIES_ID",
    "SERIES_METADATA",
    "fetch_commodity_prices",
    "fetch_raw_data",
    "get_expected_columns",
    "get_series_metadata",
    "transform_to_dataframe",
    "validate_api_response",
]
