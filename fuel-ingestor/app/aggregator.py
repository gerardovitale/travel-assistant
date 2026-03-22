import io
import logging
from datetime import datetime
from datetime import timezone

import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)

DATA_DESTINATION_BUCKET = "travel-assistant-spain-fuel-prices"
AGGREGATES_PREFIX = "aggregates/"
PROVINCE_DAILY_STATS_BLOB = f"{AGGREGATES_PREFIX}province_daily_stats.parquet"
DAY_OF_WEEK_STATS_BLOB = f"{AGGREGATES_PREFIX}day_of_week_stats.parquet"

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


def _get_bucket():
    client = storage.Client()
    return client.bucket(DATA_DESTINATION_BUCKET)


def _download_parquet_from_gcs(bucket, blob_name):
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    data = blob.download_as_bytes()
    return pd.read_parquet(io.BytesIO(data))


def _upload_parquet_to_gcs(bucket, blob_name, df):
    blob = bucket.blob(blob_name)
    blob.upload_from_string(df.to_parquet(index=False, compression="snappy"), "application/octet-stream")
    logger.info(f"Uploaded {blob_name} ({len(df)} rows)")


def _get_latest_raw_file(bucket):
    """Find the latest raw parquet file in the bucket."""
    now = datetime.now(timezone.utc)
    for days_ago in range(3):
        date_str = (now - pd.Timedelta(days=days_ago)).strftime("%Y-%m-%d")
        prefix = f"spain_fuel_prices_{date_str}"
        blobs = list(bucket.list_blobs(prefix=prefix))
        parquets = sorted([b.name for b in blobs if b.name.endswith(".parquet")])
        if parquets:
            return parquets[-1]
    return None


def compute_province_daily_stats(raw_df):
    """Compute per-province, per-fuel-type daily stats from a raw snapshot."""
    date_val = pd.to_datetime(raw_df["timestamp"].iloc[0]).date()
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
    return pd.DataFrame(rows)


def compute_day_of_week_stats(raw_df, existing_dow_df=None):
    """Compute or update day-of-week running stats from a raw snapshot."""
    date_val = pd.to_datetime(raw_df["timestamp"].iloc[0]).date()
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


def run_aggregation(bucket=None):
    """Run incremental aggregation for today's data."""
    if bucket is None:
        bucket = _get_bucket()

    latest_file = _get_latest_raw_file(bucket)
    if latest_file is None:
        logger.warning("No raw parquet file found. Skipping aggregation.")
        return

    logger.info(f"Running aggregation for: {latest_file}")
    raw_df = _download_parquet_from_gcs(bucket, latest_file)

    # Province daily stats: append today's rows
    logger.info("Computing province daily stats")
    today_province_stats = compute_province_daily_stats(raw_df)
    existing_province_stats = _download_parquet_from_gcs(bucket, PROVINCE_DAILY_STATS_BLOB)

    if existing_province_stats is not None:
        # Deduplicate: remove existing rows for today's date if re-running
        date_val = pd.Timestamp(today_province_stats["date"].iloc[0])
        existing_province_stats = existing_province_stats[pd.to_datetime(existing_province_stats["date"]) != date_val]
        province_stats = pd.concat([existing_province_stats, today_province_stats], ignore_index=True)
    else:
        province_stats = today_province_stats

    _upload_parquet_to_gcs(bucket, PROVINCE_DAILY_STATS_BLOB, province_stats)

    # Day-of-week stats: rebuild from deduplicated daily province stats
    logger.info("Computing day-of-week stats")
    dow_stats = build_day_of_week_stats_from_province_daily_stats(province_stats)
    _upload_parquet_to_gcs(bucket, DAY_OF_WEEK_STATS_BLOB, dow_stats)

    logger.info("Aggregation complete")


if __name__ == "__main__":
    logging.basicConfig(
        format="%(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]",
        level=logging.INFO,
    )
    run_aggregation()
