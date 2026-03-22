# One-time backfill script to build aggregate parquet files from all historical raw data.
# Usage: cd fuel-ingestor && python app/backfill.py
# Requires GCS credentials (GOOGLE_APPLICATION_CREDENTIALS env var or default credentials).
import io
import logging
import re

import pandas as pd
from aggregator import _get_bucket
from aggregator import _upload_parquet_to_gcs
from aggregator import build_day_of_week_stats_from_province_daily_stats
from aggregator import compute_province_daily_stats
from aggregator import DAY_OF_WEEK_STATS_BLOB
from aggregator import FUEL_PRICE_COLUMNS
from aggregator import PROVINCE_DAILY_STATS_BLOB

PARQUET_PATTERN = re.compile(r"spain_fuel_prices_(\d{4}-\d{2}-\d{2})T")

logging.basicConfig(
    format="%(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def _latest_raw_file_per_day(parquet_files):
    latest_by_day = {}
    for file_name in sorted(parquet_files):
        match = PARQUET_PATTERN.search(file_name)
        day_key = match.group(1) if match else file_name
        latest_by_day[day_key] = file_name
    return [latest_by_day[day_key] for day_key in sorted(latest_by_day)]


def backfill():
    bucket = _get_bucket()

    # List all raw parquet files
    logger.info("Listing all raw parquet files in GCS...")
    blobs = list(bucket.list_blobs(prefix="spain_fuel_prices_"))
    parquet_files = sorted([b.name for b in blobs if b.name.endswith(".parquet")])
    logger.info(f"Found {len(parquet_files)} raw files")

    if not parquet_files:
        logger.warning("No raw parquet files found. Nothing to backfill.")
        return

    parquet_files = _latest_raw_file_per_day(parquet_files)
    logger.info(f"Collapsed to {len(parquet_files)} raw files after deduplicating calendar days")

    # Only read the columns we need
    needed_columns = ["timestamp", "province"] + FUEL_PRICE_COLUMNS

    all_province_stats = []
    for i, file_name in enumerate(parquet_files):
        logger.info(f"Processing {i + 1}/{len(parquet_files)}: {file_name}")
        blob = bucket.blob(file_name)
        data = blob.download_as_bytes()
        raw_df = pd.read_parquet(io.BytesIO(data), columns=needed_columns)

        # Province daily stats
        province_stats = compute_province_daily_stats(raw_df)
        all_province_stats.append(province_stats)

    # Combine and upload province daily stats
    province_daily_df = pd.concat(all_province_stats, ignore_index=True)
    logger.info(f"Province daily stats: {len(province_daily_df)} rows")
    _upload_parquet_to_gcs(bucket, PROVINCE_DAILY_STATS_BLOB, province_daily_df)

    dow_stats_df = build_day_of_week_stats_from_province_daily_stats(province_daily_df)

    # Upload day-of-week stats
    logger.info(f"Day-of-week stats: {len(dow_stats_df)} rows")
    _upload_parquet_to_gcs(bucket, DAY_OF_WEEK_STATS_BLOB, dow_stats_df)

    logger.info("Backfill complete!")


if __name__ == "__main__":
    backfill()
