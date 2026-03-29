import io
import logging
import re
from datetime import datetime
from datetime import timezone

import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)

DATA_DESTINATION_BUCKET = "travel-assistant-spain-fuel-prices"
AGGREGATES_PREFIX = "aggregates/"
PROVINCE_DAILY_STATS_BLOB = f"{AGGREGATES_PREFIX}province_daily_stats.parquet"
DAY_OF_WEEK_STATS_BLOB = f"{AGGREGATES_PREFIX}day_of_week_stats.parquet"
DAILY_INGESTION_STATS_BLOB = f"{AGGREGATES_PREFIX}daily_ingestion_stats.parquet"
RAW_PARQUET_PATTERN = re.compile(r"spain_fuel_prices_(\d{4}-\d{2}-\d{2})T")
PROVINCE_DAILY_STATS_COLUMNS = ["date", "province", "fuel_type", "avg_price", "min_price", "max_price", "station_count"]
DAILY_INGESTION_STATS_COLUMNS = [
    "date",
    "record_count",
    "unique_stations",
    "unique_station_labels",
    "unique_provinces",
    "unique_municipalities",
    "unique_municipality_names",
    "unique_localities",
    "unique_locality_names",
]
REQUIRED_AGGREGATE_BLOBS = [
    PROVINCE_DAILY_STATS_BLOB,
    DAY_OF_WEEK_STATS_BLOB,
    DAILY_INGESTION_STATS_BLOB,
]

FUEL_PRICE_COLUMNS = [
    "biodiesel_price",
    "bioethanol_price",
    "compressed_natural_gas_price",
    "liquefied_natural_gas_price",
    "liquefied_petroleum_gases_price",
    "diesel_a_price",
    "diesel_b_price",
    "diesel_premium_price",
    "gasoline_95_e10_price",
    "gasoline_95_e5_price",
    "gasoline_95_e5_premium_price",
    "gasoline_98_e10_price",
    "gasoline_98_e5_price",
    "hydrogen_price",
]


def _log_event(log_method, event, **fields):
    if fields:
        details = " ".join(f"{key}={value!r}" for key, value in fields.items())
        log_method(f"{event} {details}")
        return
    log_method(event)


def _snapshot_date(raw_df):
    return pd.to_datetime(raw_df["timestamp"].iloc[0]).date()


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


def compute_daily_ingestion_stats(raw_df):
    """Compute per-day ingestion stats from a raw snapshot."""
    date_val = _snapshot_date(raw_df)
    locality_keys = raw_df[raw_df["locality"].notna()][["province_id", "municipality_id", "locality"]].drop_duplicates()
    return pd.DataFrame(
        [
            {
                "date": date_val,
                "record_count": len(raw_df),
                "unique_stations": raw_df["eess_id"].nunique(),
                "unique_station_labels": raw_df["label"].nunique(),
                "unique_provinces": raw_df["province"].nunique(),
                "unique_municipalities": raw_df["municipality_id"].nunique(),
                "unique_municipality_names": raw_df["municipality"].nunique(),
                "unique_localities": len(locality_keys),
                "unique_locality_names": raw_df["locality"].nunique(),
            }
        ],
        columns=DAILY_INGESTION_STATS_COLUMNS,
    )


def compute_province_daily_stats(raw_df):
    """Compute per-province, per-fuel-type daily stats from a raw snapshot."""
    date_val = _snapshot_date(raw_df)
    rows = []
    for fuel_col in FUEL_PRICE_COLUMNS:
        if fuel_col not in raw_df.columns:
            continue
        valid = raw_df[raw_df[fuel_col].notna() & (raw_df[fuel_col] > 0)]
        if valid.empty:
            continue
        grouped = valid.groupby("province")[fuel_col].agg(["mean", "min", "max", "count"]).reset_index()
        for _, row in grouped.iterrows():
            rows.append(
                {
                    "date": date_val,
                    "province": row["province"],
                    "fuel_type": fuel_col,
                    "avg_price": round(row["mean"], 4),
                    "min_price": round(row["min"], 4),
                    "max_price": round(row["max"], 4),
                    "station_count": int(row["count"]),
                }
            )
    return pd.DataFrame(rows, columns=PROVINCE_DAILY_STATS_COLUMNS)


def compute_day_of_week_stats(raw_df, existing_dow_df=None):
    """Compute or update day-of-week running stats from a raw snapshot."""
    date_val = _snapshot_date(raw_df)
    dow = date_val.weekday()  # 0=Monday, 6=Sunday
    today_rows = []
    for fuel_col in FUEL_PRICE_COLUMNS:
        if fuel_col not in raw_df.columns:
            continue
        valid = raw_df[raw_df[fuel_col].notna() & (raw_df[fuel_col] > 0)]
        if valid.empty:
            continue
        national_avg = valid[fuel_col].mean()

        # Per-province stats
        province_groups = valid.groupby("province")[fuel_col].mean()
        for province, prov_avg in province_groups.items():
            today_rows.append(
                {
                    "day_of_week": dow,
                    "fuel_type": fuel_col,
                    "province": province,
                    "sum_price": round(prov_avg, 6),
                    "count_days": 1,
                    "min_daily_avg": round(prov_avg, 4),
                    "max_daily_avg": round(prov_avg, 4),
                }
            )

        # National stats (province=None)
        today_rows.append(
            {
                "day_of_week": dow,
                "fuel_type": fuel_col,
                "province": "__national__",
                "sum_price": round(national_avg, 6),
                "count_days": 1,
                "min_daily_avg": round(national_avg, 4),
                "max_daily_avg": round(national_avg, 4),
            }
        )

    today_df = pd.DataFrame(today_rows)

    if existing_dow_df is None or existing_dow_df.empty:
        return today_df

    # Merge with existing running stats
    merge_keys = ["day_of_week", "fuel_type", "province"]
    merged = existing_dow_df.merge(today_df, on=merge_keys, how="outer", suffixes=("_old", "_new"))

    result_rows = []
    for _, row in merged.iterrows():
        old_sum = 0 if pd.isna(row.get("sum_price_old")) else row["sum_price_old"]
        old_count = 0 if pd.isna(row.get("count_days_old")) else row["count_days_old"]
        old_min = row.get("min_daily_avg_old")
        old_max = row.get("max_daily_avg_old")
        new_sum = 0 if pd.isna(row.get("sum_price_new")) else row["sum_price_new"]
        new_count = 0 if pd.isna(row.get("count_days_new")) else row["count_days_new"]
        new_min = row.get("min_daily_avg_new")
        new_max = row.get("max_daily_avg_new")

        total_sum = old_sum + new_sum
        total_count = int(old_count + new_count)

        combined_min = min(v for v in [old_min, new_min] if pd.notna(v))
        combined_max = max(v for v in [old_max, new_max] if pd.notna(v))

        result_rows.append(
            {
                "day_of_week": int(row["day_of_week"]),
                "fuel_type": row["fuel_type"],
                "province": row["province"],
                "sum_price": round(total_sum, 6),
                "count_days": total_count,
                "min_daily_avg": round(combined_min, 4),
                "max_daily_avg": round(combined_max, 4),
            }
        )

    return pd.DataFrame(result_rows)


def build_day_of_week_stats_from_province_daily_stats(province_daily_df):
    """Build day-of-week aggregates from deduplicated province daily stats."""
    columns = ["day_of_week", "fuel_type", "province", "sum_price", "count_days", "min_daily_avg", "max_daily_avg"]
    if province_daily_df is None or province_daily_df.empty:
        return pd.DataFrame(columns=columns)

    daily_df = province_daily_df.copy()
    daily_df["date"] = pd.to_datetime(daily_df["date"])

    province_daily_patterns = daily_df[["date", "fuel_type", "province", "avg_price"]].copy()
    province_daily_patterns["day_of_week"] = province_daily_patterns["date"].dt.dayofweek

    province_aggregates = (
        province_daily_patterns.groupby(["day_of_week", "fuel_type", "province"], as_index=False)
        .agg(
            sum_price=("avg_price", "sum"),
            count_days=("date", "nunique"),
            min_daily_avg=("avg_price", "min"),
            max_daily_avg=("avg_price", "max"),
        )
        .reset_index(drop=True)
    )

    national_daily_patterns = (
        daily_df.assign(weighted_price=daily_df["avg_price"] * daily_df["station_count"])
        .groupby(["date", "fuel_type"], as_index=False)
        .agg(
            weighted_price=("weighted_price", "sum"),
            station_count=("station_count", "sum"),
        )
    )
    national_daily_patterns = national_daily_patterns[national_daily_patterns["station_count"] > 0].copy()
    national_daily_patterns["avg_price"] = (
        national_daily_patterns["weighted_price"] / national_daily_patterns["station_count"]
    )
    national_daily_patterns["province"] = "__national__"
    national_daily_patterns["day_of_week"] = national_daily_patterns["date"].dt.dayofweek

    national_aggregates = (
        national_daily_patterns.groupby(["day_of_week", "fuel_type", "province"], as_index=False)
        .agg(
            sum_price=("avg_price", "sum"),
            count_days=("date", "nunique"),
            min_daily_avg=("avg_price", "min"),
            max_daily_avg=("avg_price", "max"),
        )
        .reset_index(drop=True)
    )

    result = pd.concat([province_aggregates, national_aggregates], ignore_index=True)
    result["sum_price"] = result["sum_price"].round(6)
    result["count_days"] = result["count_days"].astype(int)
    result["min_daily_avg"] = result["min_daily_avg"].round(4)
    result["max_daily_avg"] = result["max_daily_avg"].round(4)
    return result[columns].sort_values(["day_of_week", "fuel_type", "province"]).reset_index(drop=True)


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

    if all_province_stats:
        province_daily_df = pd.concat(all_province_stats, ignore_index=True)
    else:
        province_daily_df = pd.DataFrame(columns=PROVINCE_DAILY_STATS_COLUMNS)

    if all_ingestion_stats:
        ingestion_stats_df = pd.concat(all_ingestion_stats, ignore_index=True)
    else:
        ingestion_stats_df = pd.DataFrame(columns=DAILY_INGESTION_STATS_COLUMNS)

    dow_stats_df = build_day_of_week_stats_from_province_daily_stats(province_daily_df)
    _log_event(
        logger.info,
        "historical_aggregate_build_complete",
        province_daily_rows=len(province_daily_df),
        day_of_week_rows=len(dow_stats_df),
        ingestion_rows=len(ingestion_stats_df),
    )
    return province_daily_df, dow_stats_df, ingestion_stats_df


def _bootstrap_aggregates(bucket):
    _log_event(logger.info, "bootstrap_aggregation_start")
    parquet_files = _list_raw_parquet_files(bucket)
    _log_event(logger.info, "bootstrap_raw_files_found", count=len(parquet_files))

    if not parquet_files:
        _log_event(logger.warning, "bootstrap_skipped_no_raw_files")
        return

    parquet_files = _latest_raw_file_per_day(parquet_files)
    _log_event(logger.info, "bootstrap_raw_files_selected", count=len(parquet_files))

    province_daily_df, dow_stats_df, ingestion_stats_df = _build_aggregate_dataframes_from_raw_files(
        bucket, parquet_files
    )
    _log_event(
        logger.info,
        "bootstrap_aggregate_frames_ready",
        province_daily_rows=len(province_daily_df),
        day_of_week_rows=len(dow_stats_df),
        ingestion_rows=len(ingestion_stats_df),
    )

    _upload_parquet_to_gcs(bucket, PROVINCE_DAILY_STATS_BLOB, province_daily_df)
    _upload_parquet_to_gcs(bucket, DAILY_INGESTION_STATS_BLOB, ingestion_stats_df)
    _upload_parquet_to_gcs(bucket, DAY_OF_WEEK_STATS_BLOB, dow_stats_df)


def run_aggregation(bucket=None):
    """Run incremental aggregation for today's data."""
    _log_event(logger.info, "aggregation_start", bucket_provided=bucket is not None)
    if bucket is None:
        bucket = _get_bucket()

    missing_aggregate_blobs = [
        blob_name for blob_name in REQUIRED_AGGREGATE_BLOBS if not _blob_exists(bucket, blob_name)
    ]
    if missing_aggregate_blobs:
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

    # Province daily stats: append today's rows
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

    # Daily ingestion stats: append today's row
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

    # Day-of-week stats: rebuild from deduplicated daily province stats
    dow_stats = build_day_of_week_stats_from_province_daily_stats(province_stats)
    _log_event(
        logger.info,
        "day_of_week_stats_rebuilt",
        date=str(snapshot_date),
        **_day_of_week_stats_log_fields(dow_stats),
    )
    _upload_parquet_to_gcs(bucket, DAY_OF_WEEK_STATS_BLOB, dow_stats)

    _log_event(logger.info, "aggregation_complete", file=latest_file, snapshot_date=str(snapshot_date))


if __name__ == "__main__":
    logging.basicConfig(
        format="%(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]",
        level=logging.INFO,
    )
    run_aggregation()
