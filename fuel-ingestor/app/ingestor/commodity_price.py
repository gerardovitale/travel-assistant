import logging
import time

import pandas as pd
from aggregator.pipeline.gcs import IncrementalGCSParquetSink
from commodity_api import get_expected_columns
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage

DATA_DESTINATION_BUCKET = "travel-assistant-spain-fuel-prices"
COMMODITY_PRICES_BLOB = "aggregates/commodity_prices.parquet"
COMMODITY_RETENTION_DAYS = 730

logger = logging.getLogger(__name__)

VALUE_MIN = 0.0
VALUE_MAX = 300.0


def validate_commodity_dataframe(df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError("DataFrame is empty — no commodity price data to upload")

    expected_columns = set(get_expected_columns())
    missing = expected_columns - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    out_of_range = df[(df["value"] < VALUE_MIN) | (df["value"] > VALUE_MAX)]
    if len(out_of_range) > 0:
        logger.warning(f"value: {len(out_of_range)} values outside [{VALUE_MIN}, {VALUE_MAX}] range")

    duplicate_keys = df.duplicated(subset=["date", "series_id"])
    if duplicate_keys.any():
        logger.warning(f"{duplicate_keys.sum()} duplicate (date, series_id) rows found")


def write_commodity_prices_data_as_parquet(df: pd.DataFrame) -> None:
    logger.info(f"Writing commodity price data to: {DATA_DESTINATION_BUCKET}/{COMMODITY_PRICES_BLOB}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(DATA_DESTINATION_BUCKET)
    sink = IncrementalGCSParquetSink(
        bucket, COMMODITY_PRICES_BLOB, extra_key_cols=("series_id",), retention_days=COMMODITY_RETENTION_DAYS
    )

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            sink.write(df)
            logger.info(f"Successfully wrote {COMMODITY_PRICES_BLOB} ({len(df)} new rows)")
            return
        except (GoogleAPIError, ConnectionError, TimeoutError) as exc:
            if attempt < max_attempts:
                delay = 2**attempt
                logger.warning(f"GCS write attempt {attempt}/{max_attempts} failed: {exc}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"GCS write failed after {max_attempts} attempts: {exc}")
                raise
