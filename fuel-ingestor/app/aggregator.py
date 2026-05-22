import io
import logging
import re
import time
from datetime import datetime
from datetime import timezone

import pandas as pd
from google.cloud import storage
from pipeline.base import PipelineResult
from pipeline.runner import write_step_summary
from pipelines.brand_stats import BRAND_DAILY_STATS_COLUMNS
from pipelines.brand_stats import compute_brand_daily_stats
from pipelines.day_of_week_stats import build_day_of_week_stats_from_province_daily_stats
from pipelines.day_of_week_stats import compute_day_of_week_stats  # noqa: F401
from pipelines.ingestion_stats import compute_daily_ingestion_stats
from pipelines.ingestion_stats import DAILY_INGESTION_STATS_COLUMNS
from pipelines.province_stats import compute_province_daily_stats
from pipelines.province_stats import PROVINCE_DAILY_STATS_COLUMNS
from pipelines.zip_code_stats import compute_zip_code_daily_stats
from pipelines.zip_code_stats import ZIP_CODE_DAILY_STATS_COLUMNS
from shared import _log_event
from shared import _snapshot_date
from shared import FUEL_PRICE_COLUMNS

logger = logging.getLogger(__name__)

DATA_DESTINATION_BUCKET = "travel-assistant-spain-fuel-prices"
AGGREGATES_PREFIX = "aggregates/"
PROVINCE_DAILY_STATS_BLOB = f"{AGGREGATES_PREFIX}province_daily_stats.parquet"
DAY_OF_WEEK_STATS_BLOB = f"{AGGREGATES_PREFIX}day_of_week_stats.parquet"
DAILY_INGESTION_STATS_BLOB = f"{AGGREGATES_PREFIX}daily_ingestion_stats.parquet"
BRAND_DAILY_STATS_BLOB = f"{AGGREGATES_PREFIX}brand_daily_stats.parquet"
ZIP_CODE_DAILY_STATS_BLOB = f"{AGGREGATES_PREFIX}zip_code_daily_stats.parquet"
RAW_PARQUET_PATTERN = re.compile(r"spain_fuel_prices_(\d{4}-\d{2}-\d{2})T")
ZIP_CODE_DAILY_STATS_RETENTION_DAYS = 365
REQUIRED_AGGREGATE_BLOBS = [
    PROVINCE_DAILY_STATS_BLOB,
    DAY_OF_WEEK_STATS_BLOB,
    DAILY_INGESTION_STATS_BLOB,
    BRAND_DAILY_STATS_BLOB,
    ZIP_CODE_DAILY_STATS_BLOB,
]


def _province_daily_stats_log_fields(df):
    return {
        "rows": len(df),
        "unique_dates": df["date"].nunique() if not df.empty else 0,
        "unique_provinces": df["province"].nunique() if not df.empty else 0,
        "unique_fuel_types": df["fuel_type"].nunique() if not df.empty else 0,
    }


def _day_of_week_stats_log_fields(df):
    return {
        "rows": len(df),
        "unique_weekdays": df["day_of_week"].nunique() if not df.empty else 0,
        "unique_provinces": df["province"].nunique() if not df.empty else 0,
        "unique_fuel_types": df["fuel_type"].nunique() if not df.empty else 0,
    }


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

    pipeline_results = []
    raw_row_count = len(raw_df)

    # Province daily stats: append today's rows
    step_start = time.monotonic()
    today_province_stats = compute_province_daily_stats(raw_df)
    _log_event(
        logger.info,
        "province_daily_stats_computed",
        date=snapshot_date,
        **_province_daily_stats_log_fields(today_province_stats),
    )
    existing_province_stats = _download_parquet_from_gcs(bucket, PROVINCE_DAILY_STATS_BLOB)

    if existing_province_stats is not None:
        # Deduplicate: remove existing rows for today's date if re-running
        date_val = pd.Timestamp(today_province_stats["date"].iloc[0])
        existing_rows = len(existing_province_stats)
        existing_province_stats = existing_province_stats[pd.to_datetime(existing_province_stats["date"]) != date_val]
        removed_rows = existing_rows - len(existing_province_stats)
        province_stats = pd.concat([existing_province_stats, today_province_stats], ignore_index=True)
        _log_event(
            logger.info,
            "province_daily_stats_updated",
            date=str(date_val.date()),
            existing_rows=existing_rows,
            removed_rows=removed_rows,
            added_rows=len(today_province_stats),
            final_rows=len(province_stats),
        )
    else:
        province_stats = today_province_stats
        _log_event(
            logger.info,
            "province_daily_stats_initialized",
            date=str(snapshot_date),
            final_rows=len(province_stats),
        )

    _upload_parquet_to_gcs(bucket, PROVINCE_DAILY_STATS_BLOB, province_stats)
    pipeline_results.append(
        PipelineResult(
            name="province_daily_stats",
            description="Province × fuel type daily aggregation",
            output_blob=PROVINCE_DAILY_STATS_BLOB,
            input_rows=raw_row_count,
            output_rows=len(province_stats),
            duration_seconds=round(time.monotonic() - step_start, 2),
            status="ok",
        )
    )

    # Daily ingestion stats: append today's row
    step_start = time.monotonic()
    today_ingestion_stats = compute_daily_ingestion_stats(raw_df)
    _log_event(
        logger.info,
        "daily_ingestion_stats_computed",
        date=str(snapshot_date),
        rows=len(today_ingestion_stats),
        record_count=int(today_ingestion_stats["record_count"].iloc[0]),
        unique_stations=int(today_ingestion_stats["unique_stations"].iloc[0]),
        unique_provinces=int(today_ingestion_stats["unique_provinces"].iloc[0]),
        unique_municipalities=int(today_ingestion_stats["unique_municipalities"].iloc[0]),
        unique_localities=int(today_ingestion_stats["unique_localities"].iloc[0]),
    )
    existing_ingestion_stats = _download_parquet_from_gcs(bucket, DAILY_INGESTION_STATS_BLOB)

    if existing_ingestion_stats is not None:
        date_val = pd.Timestamp(today_ingestion_stats["date"].iloc[0])
        existing_rows = len(existing_ingestion_stats)
        existing_ingestion_stats = existing_ingestion_stats[
            pd.to_datetime(existing_ingestion_stats["date"]) != date_val
        ]
        removed_rows = existing_rows - len(existing_ingestion_stats)
        ingestion_stats = pd.concat([existing_ingestion_stats, today_ingestion_stats], ignore_index=True)
        _log_event(
            logger.info,
            "daily_ingestion_stats_updated",
            date=str(date_val.date()),
            existing_rows=existing_rows,
            removed_rows=removed_rows,
            added_rows=len(today_ingestion_stats),
            final_rows=len(ingestion_stats),
        )
    else:
        ingestion_stats = today_ingestion_stats
        _log_event(
            logger.info,
            "daily_ingestion_stats_initialized",
            date=str(snapshot_date),
            final_rows=len(ingestion_stats),
        )

    _upload_parquet_to_gcs(bucket, DAILY_INGESTION_STATS_BLOB, ingestion_stats)
    pipeline_results.append(
        PipelineResult(
            name="daily_ingestion_stats",
            description="Daily ingestion summary — station and entity counts",
            output_blob=DAILY_INGESTION_STATS_BLOB,
            input_rows=raw_row_count,
            output_rows=len(ingestion_stats),
            duration_seconds=round(time.monotonic() - step_start, 2),
            status="ok",
        )
    )

    # Day-of-week stats: rebuild from deduplicated daily province stats
    step_start = time.monotonic()
    dow_stats = build_day_of_week_stats_from_province_daily_stats(province_stats)
    _log_event(
        logger.info,
        "day_of_week_stats_rebuilt",
        date=str(snapshot_date),
        **_day_of_week_stats_log_fields(dow_stats),
    )
    _upload_parquet_to_gcs(bucket, DAY_OF_WEEK_STATS_BLOB, dow_stats)
    pipeline_results.append(
        PipelineResult(
            name="day_of_week_stats",
            description="Day-of-week price patterns (province + national)",
            output_blob=DAY_OF_WEEK_STATS_BLOB,
            input_rows=len(province_stats),
            output_rows=len(dow_stats),
            duration_seconds=round(time.monotonic() - step_start, 2),
            status="ok",
        )
    )

    # Brand daily stats: append today's rows
    step_start = time.monotonic()
    today_brand_stats = compute_brand_daily_stats(raw_df)
    _log_event(
        logger.info,
        "brand_daily_stats_computed",
        date=str(snapshot_date),
        rows=len(today_brand_stats),
    )
    existing_brand_stats = _download_parquet_from_gcs(bucket, BRAND_DAILY_STATS_BLOB)

    if existing_brand_stats is not None:
        date_val = pd.Timestamp(today_brand_stats["date"].iloc[0]) if not today_brand_stats.empty else None
        if date_val is not None:
            existing_rows = len(existing_brand_stats)
            existing_brand_stats = existing_brand_stats[pd.to_datetime(existing_brand_stats["date"]) != date_val]
            removed_rows = existing_rows - len(existing_brand_stats)
            brand_stats = pd.concat([existing_brand_stats, today_brand_stats], ignore_index=True)
            _log_event(
                logger.info,
                "brand_daily_stats_updated",
                date=str(date_val.date()),
                existing_rows=existing_rows,
                removed_rows=removed_rows,
                added_rows=len(today_brand_stats),
                final_rows=len(brand_stats),
            )
        else:
            brand_stats = existing_brand_stats
    else:
        brand_stats = today_brand_stats
        _log_event(
            logger.info,
            "brand_daily_stats_initialized",
            date=str(snapshot_date),
            final_rows=len(brand_stats),
        )

    _upload_parquet_to_gcs(bucket, BRAND_DAILY_STATS_BLOB, brand_stats)
    pipeline_results.append(
        PipelineResult(
            name="brand_daily_stats",
            description="Brand × fuel type daily aggregation",
            output_blob=BRAND_DAILY_STATS_BLOB,
            input_rows=raw_row_count,
            output_rows=len(brand_stats),
            duration_seconds=round(time.monotonic() - step_start, 2),
            status="ok",
        )
    )

    # Zip-code daily trend stats: append today's rows, replace same-day rows, and retain only the latest rolling year.
    step_start = time.monotonic()
    today_zip_code_stats = compute_zip_code_daily_stats(raw_df)
    _log_event(
        logger.info,
        "zip_code_daily_stats_computed",
        date=str(snapshot_date),
        **_zip_code_daily_stats_log_fields(today_zip_code_stats),
    )
    existing_zip_code_stats = _download_parquet_from_gcs(bucket, ZIP_CODE_DAILY_STATS_BLOB)

    if existing_zip_code_stats is not None and not today_zip_code_stats.empty:
        date_val = pd.Timestamp(today_zip_code_stats["date"].iloc[0])
        existing_rows = len(existing_zip_code_stats)
        existing_zip_code_stats = existing_zip_code_stats[pd.to_datetime(existing_zip_code_stats["date"]) != date_val]
        retention_cutoff = date_val - pd.Timedelta(days=ZIP_CODE_DAILY_STATS_RETENTION_DAYS - 1)
        within_retention = pd.to_datetime(existing_zip_code_stats["date"]) >= retention_cutoff
        pruned_rows = int((~within_retention).sum())
        existing_zip_code_stats = existing_zip_code_stats[within_retention]
        removed_rows = existing_rows - len(existing_zip_code_stats) - pruned_rows
        zip_code_stats = pd.concat([existing_zip_code_stats, today_zip_code_stats], ignore_index=True)
        _log_event(
            logger.info,
            "zip_code_daily_stats_updated",
            date=str(date_val.date()),
            existing_rows=existing_rows,
            removed_rows=removed_rows,
            pruned_rows=pruned_rows,
            added_rows=len(today_zip_code_stats),
            final_rows=len(zip_code_stats),
        )
    elif existing_zip_code_stats is not None:
        date_val = pd.Timestamp(snapshot_date)
        retention_cutoff = date_val - pd.Timedelta(days=ZIP_CODE_DAILY_STATS_RETENTION_DAYS - 1)
        within_retention = pd.to_datetime(existing_zip_code_stats["date"]) >= retention_cutoff
        pruned_rows = int((~within_retention).sum())
        zip_code_stats = existing_zip_code_stats[within_retention].copy()
        _log_event(
            logger.info,
            "zip_code_daily_stats_retained_existing",
            date=str(date_val.date()),
            pruned_rows=pruned_rows,
            final_rows=len(zip_code_stats),
        )
    else:
        zip_code_stats = today_zip_code_stats
        _log_event(
            logger.info,
            "zip_code_daily_stats_initialized",
            date=str(snapshot_date),
            final_rows=len(zip_code_stats),
        )

    _upload_parquet_to_gcs(bucket, ZIP_CODE_DAILY_STATS_BLOB, zip_code_stats)
    pipeline_results.append(
        PipelineResult(
            name="zip_code_daily_stats",
            description="Zip-code × fuel type daily aggregation (365-day rolling)",
            output_blob=ZIP_CODE_DAILY_STATS_BLOB,
            input_rows=raw_row_count,
            output_rows=len(zip_code_stats),
            duration_seconds=round(time.monotonic() - step_start, 2),
            status="ok",
        )
    )

    write_step_summary(pipeline_results, title=f"Aggregation Results — {str(snapshot_date)}")
    _log_event(logger.info, "aggregation_complete", file=latest_file, snapshot_date=str(snapshot_date))


if __name__ == "__main__":
    logging.basicConfig(
        format="%(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]",
        level=logging.INFO,
    )
    run_aggregation()
