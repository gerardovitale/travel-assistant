import io
import logging
import re
from datetime import datetime
from datetime import timezone

import pandas as pd
from aggregator.pipeline.runner import TaskRunner
from aggregator.pipeline.runner import write_step_summary
from aggregator.pipelines import brand_stats
from aggregator.pipelines import day_of_week_stats
from aggregator.pipelines import ingestion_stats
from aggregator.pipelines import province_stats
from aggregator.pipelines import zip_code_stats
from aggregator.pipelines.brand_stats import BRAND_DAILY_STATS_BLOB
from aggregator.pipelines.brand_stats import BRAND_DAILY_STATS_COLUMNS
from aggregator.pipelines.brand_stats import compute_brand_daily_stats
from aggregator.pipelines.day_of_week_stats import build_day_of_week_stats_from_province_daily_stats
from aggregator.pipelines.day_of_week_stats import compute_day_of_week_stats  # noqa: F401
from aggregator.pipelines.day_of_week_stats import DAY_OF_WEEK_STATS_BLOB
from aggregator.pipelines.ingestion_stats import compute_daily_ingestion_stats
from aggregator.pipelines.ingestion_stats import DAILY_INGESTION_STATS_BLOB
from aggregator.pipelines.ingestion_stats import DAILY_INGESTION_STATS_COLUMNS
from aggregator.pipelines.province_stats import compute_province_daily_stats
from aggregator.pipelines.province_stats import PROVINCE_DAILY_STATS_BLOB
from aggregator.pipelines.province_stats import PROVINCE_DAILY_STATS_COLUMNS
from aggregator.pipelines.zip_code_stats import compute_zip_code_daily_stats
from aggregator.pipelines.zip_code_stats import ZIP_CODE_DAILY_STATS_BLOB
from aggregator.pipelines.zip_code_stats import ZIP_CODE_DAILY_STATS_COLUMNS
from aggregator.pipelines.zip_code_stats import ZIP_CODE_DAILY_STATS_RETENTION_DAYS
from aggregator.shared import _log_event
from aggregator.shared import _snapshot_date
from aggregator.shared import FUEL_PRICE_COLUMNS
from google.cloud import storage

logger = logging.getLogger(__name__)

DATA_DESTINATION_BUCKET = "travel-assistant-spain-fuel-prices"
RAW_PARQUET_PATTERN = re.compile(r"spain_fuel_prices_(\d{4}-\d{2}-\d{2})T")
REQUIRED_AGGREGATE_BLOBS = [
    PROVINCE_DAILY_STATS_BLOB,
    DAY_OF_WEEK_STATS_BLOB,
    DAILY_INGESTION_STATS_BLOB,
    BRAND_DAILY_STATS_BLOB,
    ZIP_CODE_DAILY_STATS_BLOB,
]


def _zip_code_daily_stats_log_fields(df):
    return {
        "rows": len(df),
        "unique_dates": df["date"].nunique() if not df.empty else 0,
        "unique_zip_codes": df["zip_code"].nunique() if not df.empty else 0,
        "unique_provinces": df["province"].nunique() if not df.empty else 0,
        "unique_fuel_types": df["fuel_type"].nunique() if not df.empty else 0,
    }


def _get_bucket():
    client = storage.Client()
    return client.bucket(DATA_DESTINATION_BUCKET)


def _blob_exists(bucket, blob_name):
    return bucket.blob(blob_name).exists()


def _download_parquet_from_gcs(bucket, blob_name, columns=None):
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    data = blob.download_as_bytes()
    return pd.read_parquet(io.BytesIO(data), columns=columns)


def _upload_parquet_to_gcs(bucket, blob_name, df):
    blob = bucket.blob(blob_name)
    blob.upload_from_string(df.to_parquet(index=False, compression="snappy"), "application/octet-stream")
    _log_event(logger.info, "upload_complete", blob=blob_name, rows=len(df))


def _list_raw_parquet_files(bucket):
    blobs = list(bucket.list_blobs(prefix="spain_fuel_prices_"))
    parquet_files = sorted([b.name for b in blobs if b.name.endswith(".parquet")])
    _log_event(logger.info, "raw_files_listed", count=len(parquet_files))
    return parquet_files


def _latest_raw_file_per_day(parquet_files):
    latest_by_day = {}
    for file_name in sorted(parquet_files):
        match = RAW_PARQUET_PATTERN.search(file_name)
        if not match:
            continue
        day_key = match.group(1)
        latest_by_day[day_key] = file_name
    deduplicated_files = [latest_by_day[day_key] for day_key in sorted(latest_by_day)]
    _log_event(
        logger.info,
        "raw_files_deduplicated_by_day",
        input_files=len(parquet_files),
        unique_days=len(deduplicated_files),
    )
    return deduplicated_files


def _most_recent_raw_files(parquet_files, max_days):
    """Return the last *max_days* entries from *parquet_files* (assumes chronologically sorted input)."""
    if max_days is None or len(parquet_files) <= max_days:
        return parquet_files
    recent_files = parquet_files[-max_days:]
    _log_event(
        logger.info,
        "raw_files_limited_to_recent_days",
        input_files=len(parquet_files),
        selected_files=len(recent_files),
        max_days=max_days,
    )
    return recent_files


def _get_latest_raw_file(bucket):
    """Find the latest raw parquet file in the bucket."""
    now = datetime.now(timezone.utc)
    for days_ago in range(3):
        date_str = (now - pd.Timedelta(days=days_ago)).strftime("%Y-%m-%d")
        prefix = f"spain_fuel_prices_{date_str}"
        blobs = list(bucket.list_blobs(prefix=prefix))
        parquets = sorted([b.name for b in blobs if b.name.endswith(".parquet")])
        _log_event(
            logger.info,
            "latest_raw_file_check",
            prefix=prefix,
            days_ago=days_ago,
            matching_files=len(parquets),
        )
        if parquets:
            latest_file = parquets[-1]
            _log_event(logger.info, "latest_raw_file_selected", file=latest_file)
            return latest_file
    _log_event(logger.warning, "latest_raw_file_missing", checked_days=3)
    return None


def _build_aggregate_dataframes_from_raw_files(bucket, parquet_files):
    _log_event(logger.info, "historical_aggregate_build_start", files=len(parquet_files))
    needed_columns = [
        "timestamp",
        "eess_id",
        "municipality_id",
        "province_id",
        "label",
        "province",
        "municipality",
        "locality",
        *FUEL_PRICE_COLUMNS,
    ]

    all_province_stats = []
    all_ingestion_stats = []
    all_brand_stats = []

    for index, file_name in enumerate(parquet_files, start=1):
        _log_event(
            logger.info,
            "historical_raw_file_processing",
            file=file_name,
            file_index=index,
            total_files=len(parquet_files),
        )
        raw_df = _download_parquet_from_gcs(bucket, file_name, columns=needed_columns)
        if raw_df is None:
            _log_event(logger.warning, "historical_raw_file_missing", file=file_name)
            continue

        _log_event(
            logger.info,
            "historical_raw_file_loaded",
            file=file_name,
            rows=len(raw_df),
            cols=len(raw_df.columns),
            snapshot_date=_snapshot_date(raw_df),
        )

        all_province_stats.append(compute_province_daily_stats(raw_df))
        all_ingestion_stats.append(compute_daily_ingestion_stats(raw_df))
        all_brand_stats.append(compute_brand_daily_stats(raw_df))

    if all_province_stats:
        province_daily_df = pd.concat(all_province_stats, ignore_index=True)
    else:
        province_daily_df = pd.DataFrame(columns=PROVINCE_DAILY_STATS_COLUMNS)

    if all_ingestion_stats:
        ingestion_stats_df = pd.concat(all_ingestion_stats, ignore_index=True)
    else:
        ingestion_stats_df = pd.DataFrame(columns=DAILY_INGESTION_STATS_COLUMNS)

    if all_brand_stats:
        brand_daily_df = pd.concat(all_brand_stats, ignore_index=True)
    else:
        brand_daily_df = pd.DataFrame(columns=BRAND_DAILY_STATS_COLUMNS)

    dow_stats_df = build_day_of_week_stats_from_province_daily_stats(province_daily_df)
    _log_event(
        logger.info,
        "historical_aggregate_build_complete",
        province_daily_rows=len(province_daily_df),
        day_of_week_rows=len(dow_stats_df),
        ingestion_rows=len(ingestion_stats_df),
        brand_daily_rows=len(brand_daily_df),
    )
    return province_daily_df, dow_stats_df, ingestion_stats_df, brand_daily_df


def _build_zip_code_daily_stats_from_raw_files(bucket, parquet_files):
    _log_event(logger.info, "zip_code_daily_stats_build_start", files=len(parquet_files))
    needed_columns = [
        "timestamp",
        "zip_code",
        *FUEL_PRICE_COLUMNS,
    ]
    all_zip_code_stats = []

    for index, file_name in enumerate(parquet_files, start=1):
        _log_event(
            logger.info,
            "zip_code_daily_raw_file_processing",
            file=file_name,
            file_index=index,
            total_files=len(parquet_files),
        )
        raw_df = _download_parquet_from_gcs(bucket, file_name, columns=needed_columns)
        if raw_df is None:
            _log_event(logger.warning, "zip_code_daily_raw_file_missing", file=file_name)
            continue

        zip_code_stats = compute_zip_code_daily_stats(raw_df)
        if not zip_code_stats.empty:
            all_zip_code_stats.append(zip_code_stats)

    if all_zip_code_stats:
        zip_code_daily_df = pd.concat(all_zip_code_stats, ignore_index=True)
    else:
        zip_code_daily_df = pd.DataFrame(columns=ZIP_CODE_DAILY_STATS_COLUMNS)

    _log_event(
        logger.info,
        "zip_code_daily_stats_build_complete",
        **_zip_code_daily_stats_log_fields(zip_code_daily_df),
    )
    return zip_code_daily_df


def _bootstrap_aggregates(bucket):
    _log_event(logger.info, "bootstrap_aggregation_start")
    parquet_files = _list_raw_parquet_files(bucket)
    _log_event(logger.info, "bootstrap_raw_files_found", count=len(parquet_files))

    if not parquet_files:
        _log_event(logger.warning, "bootstrap_skipped_no_raw_files")
        return

    parquet_files = _latest_raw_file_per_day(parquet_files)
    _log_event(logger.info, "bootstrap_raw_files_selected", count=len(parquet_files))
    trend_parquet_files = _most_recent_raw_files(parquet_files, ZIP_CODE_DAILY_STATS_RETENTION_DAYS)

    province_daily_df, dow_stats_df, ingestion_stats_df, brand_daily_df = _build_aggregate_dataframes_from_raw_files(
        bucket, parquet_files
    )
    zip_code_daily_df = _build_zip_code_daily_stats_from_raw_files(bucket, trend_parquet_files)
    _log_event(
        logger.info,
        "bootstrap_aggregate_frames_ready",
        province_daily_rows=len(province_daily_df),
        day_of_week_rows=len(dow_stats_df),
        ingestion_rows=len(ingestion_stats_df),
        brand_daily_rows=len(brand_daily_df),
        zip_code_daily_rows=len(zip_code_daily_df),
    )

    _upload_parquet_to_gcs(bucket, PROVINCE_DAILY_STATS_BLOB, province_daily_df)
    _upload_parquet_to_gcs(bucket, DAILY_INGESTION_STATS_BLOB, ingestion_stats_df)
    _upload_parquet_to_gcs(bucket, DAY_OF_WEEK_STATS_BLOB, dow_stats_df)
    _upload_parquet_to_gcs(bucket, BRAND_DAILY_STATS_BLOB, brand_daily_df)
    _upload_parquet_to_gcs(bucket, ZIP_CODE_DAILY_STATS_BLOB, zip_code_daily_df)


def _backfill_brand_daily_stats(bucket):
    _log_event(logger.info, "brand_backfill_start")
    parquet_files = _list_raw_parquet_files(bucket)
    _log_event(logger.info, "brand_backfill_raw_files_found", count=len(parquet_files))

    if not parquet_files:
        _log_event(logger.warning, "brand_backfill_skipped_no_raw_files")
        return

    parquet_files = _latest_raw_file_per_day(parquet_files)
    _log_event(logger.info, "brand_backfill_raw_files_selected", count=len(parquet_files))

    _, _, _, brand_daily_df = _build_aggregate_dataframes_from_raw_files(bucket, parquet_files)
    _log_event(logger.info, "brand_backfill_frame_ready", brand_daily_rows=len(brand_daily_df))
    _upload_parquet_to_gcs(bucket, BRAND_DAILY_STATS_BLOB, brand_daily_df)


def _backfill_zip_code_daily_stats(bucket):
    _log_event(logger.info, "zip_code_daily_backfill_start")
    parquet_files = _list_raw_parquet_files(bucket)
    _log_event(logger.info, "zip_code_daily_backfill_raw_files_found", count=len(parquet_files))

    if not parquet_files:
        _log_event(logger.warning, "zip_code_daily_backfill_skipped_no_raw_files")
        return

    parquet_files = _latest_raw_file_per_day(parquet_files)
    parquet_files = _most_recent_raw_files(parquet_files, ZIP_CODE_DAILY_STATS_RETENTION_DAYS)
    _log_event(logger.info, "zip_code_daily_backfill_raw_files_selected", count=len(parquet_files))

    zip_code_daily_df = _build_zip_code_daily_stats_from_raw_files(bucket, parquet_files)
    _log_event(
        logger.info, "zip_code_daily_backfill_frame_ready", **_zip_code_daily_stats_log_fields(zip_code_daily_df)
    )
    _upload_parquet_to_gcs(bucket, ZIP_CODE_DAILY_STATS_BLOB, zip_code_daily_df)


def run_aggregation(bucket=None):
    """Run incremental aggregation for today's data."""
    _log_event(logger.info, "aggregation_start", bucket_provided=bucket is not None)
    if bucket is None:
        bucket = _get_bucket()

    missing_aggregate_blobs = [
        blob_name for blob_name in REQUIRED_AGGREGATE_BLOBS if not _blob_exists(bucket, blob_name)
    ]
    if missing_aggregate_blobs:
        if set(missing_aggregate_blobs) == {BRAND_DAILY_STATS_BLOB}:
            _log_event(
                logger.info,
                "aggregation_mode_selected",
                run_type="brand_backfill",
                missing_aggregates=missing_aggregate_blobs,
            )
            _backfill_brand_daily_stats(bucket)
            return
        if set(missing_aggregate_blobs) == {ZIP_CODE_DAILY_STATS_BLOB}:
            _log_event(
                logger.info,
                "aggregation_mode_selected",
                run_type="zip_code_daily_backfill",
                missing_aggregates=missing_aggregate_blobs,
            )
            _backfill_zip_code_daily_stats(bucket)
            return

        _log_event(
            logger.info,
            "aggregation_mode_selected",
            run_type="bootstrap",
            missing_aggregates=missing_aggregate_blobs,
        )
        _bootstrap_aggregates(bucket)
        return

    latest_file = _get_latest_raw_file(bucket)
    if latest_file is None:
        _log_event(logger.warning, "aggregation_skipped_no_raw_file")
        return

    _log_event(logger.info, "aggregation_mode_selected", run_type="incremental", file=latest_file)
    raw_df = _download_parquet_from_gcs(bucket, latest_file)
    if raw_df is None:
        _log_event(logger.warning, "raw_snapshot_missing_after_selection", file=latest_file)
        return

    snapshot_date = _snapshot_date(raw_df)
    _log_event(
        logger.info,
        "raw_snapshot_loaded",
        file=latest_file,
        rows=len(raw_df),
        cols=len(raw_df.columns),
        snapshot_date=snapshot_date,
    )

    daily_tasks = [
        province_stats.build_task(bucket, raw_df),
        ingestion_stats.build_task(bucket, raw_df),
        brand_stats.build_task(bucket, raw_df),
        zip_code_stats.build_task(bucket, raw_df),
        # day_of_week reads the uploaded province_stats from GCS — must stay last.
        day_of_week_stats.build_task(bucket),
    ]
    daily_results = TaskRunner().run(daily_tasks)

    from aggregator.brand_competitiveness import run_brand_analytics  # local import to avoid circular dependency

    brand_results = run_brand_analytics(bucket)

    write_step_summary(daily_results + brand_results, title=f"Aggregation Results — {str(snapshot_date)}")
    _log_event(logger.info, "aggregation_complete", file=latest_file, snapshot_date=str(snapshot_date))


if __name__ == "__main__":
    logging.basicConfig(
        format="%(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]",
        level=logging.INFO,
    )
    run_aggregation()
