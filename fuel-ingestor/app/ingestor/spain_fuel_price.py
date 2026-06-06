import logging
import time
from datetime import datetime

import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage
from spain_fuel_api import get_expected_columns
from spain_fuel_api import get_float_columns

DATA_DESTINATION_BUCKET = "travel-assistant-spain-fuel-prices"

logger = logging.getLogger(__name__)


def write_spain_fuel_prices_data_as_parquet(spain_fuel_prices_df: pd.DataFrame) -> None:
    logger.info(f"Writing Spain Fuel Price Data to: {DATA_DESTINATION_BUCKET}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(DATA_DESTINATION_BUCKET)
    timestamp = datetime.now().isoformat(timespec="seconds")
    blob = bucket.blob(f"spain_fuel_prices_{timestamp}.parquet")
    parquet_data = spain_fuel_prices_df.to_parquet(index=False, compression="snappy")

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            blob.upload_from_string(parquet_data, "application/octet-stream")
            logger.info(f"Successfully uploaded {blob.name} ({len(spain_fuel_prices_df)} rows)")
            return
        except (GoogleAPIError, ConnectionError, TimeoutError) as exc:
            if attempt < max_attempts:
                delay = 2**attempt
                logger.warning(f"GCS upload attempt {attempt}/{max_attempts} failed: {exc}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"GCS upload failed after {max_attempts} attempts: {exc}")
                raise


MIN_EXPECTED_STATIONS = 5000
PRICE_MIN = 0.5
PRICE_MAX = 3.0
LATITUDE_RANGE = (27.0, 44.0)
LONGITUDE_RANGE = (-19.0, 5.0)


def validate_dataframe(df: pd.DataFrame) -> None:
    row_count = len(df)
    if row_count == 0:
        raise ValueError("DataFrame is empty — no fuel station data to upload")
    if row_count < MIN_EXPECTED_STATIONS:
        logger.warning(f"Low station count: {row_count} (expected >= {MIN_EXPECTED_STATIONS})")

    expected_columns = set(get_expected_columns())
    missing = expected_columns - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    float_columns = get_float_columns()
    price_columns = [c for c in float_columns if c.endswith("_price")]
    for col in price_columns:
        non_null = df[col].dropna()
        if non_null.empty:
            continue
        out_of_range = non_null[(non_null < PRICE_MIN) | (non_null > PRICE_MAX)]
        if len(out_of_range) > 0:
            logger.warning(f"{col}: {len(out_of_range)} values outside [{PRICE_MIN}, {PRICE_MAX}] range")
        null_pct = df[col].isna().mean() * 100
        if null_pct > 0:
            logger.info(f"{col}: {null_pct:.1f}% null values")

    if "latitude" in df.columns and "longitude" in df.columns:
        lat = df["latitude"].dropna()
        lon = df["longitude"].dropna()
        lat_out = lat[(lat < LATITUDE_RANGE[0]) | (lat > LATITUDE_RANGE[1])]
        lon_out = lon[(lon < LONGITUDE_RANGE[0]) | (lon > LONGITUDE_RANGE[1])]
        if len(lat_out) > 0:
            logger.warning(f"latitude: {len(lat_out)} values outside {LATITUDE_RANGE}")
        if len(lon_out) > 0:
            logger.warning(f"longitude: {len(lon_out)} values outside {LONGITUDE_RANGE}")
