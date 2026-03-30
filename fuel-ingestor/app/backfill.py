# One-time backfill script to build aggregate parquet files from all historical raw data.
# Usage: cd fuel-ingestor && python app/backfill.py
# Requires GCS credentials (GOOGLE_APPLICATION_CREDENTIALS env var or default credentials).
import logging

from aggregator import _build_aggregate_dataframes_from_raw_files
from aggregator import _get_bucket
from aggregator import _latest_raw_file_per_day
from aggregator import _list_raw_parquet_files
from aggregator import _upload_parquet_to_gcs
from aggregator import BRAND_DAILY_STATS_BLOB
from aggregator import DAILY_INGESTION_STATS_BLOB
from aggregator import DAY_OF_WEEK_STATS_BLOB
from aggregator import PROVINCE_DAILY_STATS_BLOB

logging.basicConfig(
    format="%(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def backfill():
    bucket = _get_bucket()

    # List all raw parquet files
    logger.info("Listing all raw parquet files in GCS...")
    parquet_files = _list_raw_parquet_files(bucket)
    logger.info(f"Found {len(parquet_files)} raw files")

    if not parquet_files:
        logger.warning("No raw parquet files found. Nothing to backfill.")
        return

    parquet_files = _latest_raw_file_per_day(parquet_files)
    logger.info(f"Collapsed to {len(parquet_files)} raw files after deduplicating calendar days")

    province_daily_df, dow_stats_df, ingestion_stats_df, brand_daily_df = _build_aggregate_dataframes_from_raw_files(
        bucket, parquet_files
    )

    logger.info(f"Province daily stats: {len(province_daily_df)} rows")
    _upload_parquet_to_gcs(bucket, PROVINCE_DAILY_STATS_BLOB, province_daily_df)

    # Upload day-of-week stats
    logger.info(f"Day-of-week stats: {len(dow_stats_df)} rows")
    _upload_parquet_to_gcs(bucket, DAY_OF_WEEK_STATS_BLOB, dow_stats_df)

    logger.info(f"Daily ingestion stats: {len(ingestion_stats_df)} rows")
    _upload_parquet_to_gcs(bucket, DAILY_INGESTION_STATS_BLOB, ingestion_stats_df)

    logger.info(f"Brand daily stats: {len(brand_daily_df)} rows")
    _upload_parquet_to_gcs(bucket, BRAND_DAILY_STATS_BLOB, brand_daily_df)

    logger.info("Backfill complete!")


if __name__ == "__main__":
    backfill()
