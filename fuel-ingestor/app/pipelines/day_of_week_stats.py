from __future__ import annotations

from typing import Any
from typing import Optional

import pandas as pd
from pipeline.base import TaskConfig
from pipeline.gcs import GCSParquetSink
from pipeline.gcs import GCSParquetSource
from pipelines.province_stats import PROVINCE_DAILY_STATS_BLOB
from shared import _rows_with_positive_price
from shared import _snapshot_date
from shared import FUEL_PRICE_COLUMNS

DAY_OF_WEEK_STATS_BLOB = "aggregates/day_of_week_stats.parquet"

DAY_OF_WEEK_STATS_COLUMNS = [
    "day_of_week",
    "fuel_type",
    "province",
    "sum_price",
    "count_days",
    "min_daily_avg",
    "max_daily_avg",
]


def _today_rows_from_snapshot(raw_df: pd.DataFrame, dow: int) -> list:
    rows = []
    for fuel_col in FUEL_PRICE_COLUMNS:
        if fuel_col not in raw_df.columns:
            continue
        valid = _rows_with_positive_price(raw_df, fuel_col)
        if valid.empty:
            continue
        national_avg = valid[fuel_col].mean()
        for province, prov_avg in valid.groupby("province")[fuel_col].mean().items():
            rows.append(
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
        rows.append(
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
    return rows


def _merge_running_stats(existing_dow_df: pd.DataFrame, today_df: pd.DataFrame) -> pd.DataFrame:
    merge_keys = ["day_of_week", "fuel_type", "province"]
    merged = existing_dow_df.merge(today_df, on=merge_keys, how="outer", suffixes=("_old", "_new"))
    rows = []
    for _, row in merged.iterrows():
        old_sum = 0 if pd.isna(row.get("sum_price_old")) else row["sum_price_old"]
        old_count = 0 if pd.isna(row.get("count_days_old")) else row["count_days_old"]
        old_min = row.get("min_daily_avg_old")
        old_max = row.get("max_daily_avg_old")
        new_sum = 0 if pd.isna(row.get("sum_price_new")) else row["sum_price_new"]
        new_count = 0 if pd.isna(row.get("count_days_new")) else row["count_days_new"]
        new_min = row.get("min_daily_avg_new")
        new_max = row.get("max_daily_avg_new")
        rows.append(
            {
                "day_of_week": int(row["day_of_week"]),
                "fuel_type": row["fuel_type"],
                "province": row["province"],
                "sum_price": round(old_sum + new_sum, 6),
                "count_days": int(old_count + new_count),
                "min_daily_avg": round(min(v for v in [old_min, new_min] if pd.notna(v)), 4),
                "max_daily_avg": round(max(v for v in [old_max, new_max] if pd.notna(v)), 4),
            }
        )
    return pd.DataFrame(rows)


def compute_day_of_week_stats(raw_df: pd.DataFrame, existing_dow_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    date_val = _snapshot_date(raw_df)
    today_df = pd.DataFrame(_today_rows_from_snapshot(raw_df, date_val.weekday()))
    if existing_dow_df is None or existing_dow_df.empty:
        return today_df
    return _merge_running_stats(existing_dow_df, today_df)


def _province_day_of_week_aggregates(daily_df: pd.DataFrame) -> pd.DataFrame:
    patterns = daily_df[["date", "fuel_type", "province", "avg_price"]].copy()
    patterns["day_of_week"] = patterns["date"].dt.dayofweek
    return (
        patterns.groupby(["day_of_week", "fuel_type", "province"], as_index=False)
        .agg(
            sum_price=("avg_price", "sum"),
            count_days=("date", "nunique"),
            min_daily_avg=("avg_price", "min"),
            max_daily_avg=("avg_price", "max"),
        )
        .reset_index(drop=True)
    )


def _national_day_of_week_aggregates(daily_df: pd.DataFrame) -> pd.DataFrame:
    national = (
        daily_df.assign(weighted_price=daily_df["avg_price"] * daily_df["station_count"])
        .groupby(["date", "fuel_type"], as_index=False)
        .agg(weighted_price=("weighted_price", "sum"), station_count=("station_count", "sum"))
    )
    national = national[national["station_count"] > 0].copy()
    national["avg_price"] = national["weighted_price"] / national["station_count"]
    national["province"] = "__national__"
    national["day_of_week"] = national["date"].dt.dayofweek
    return (
        national.groupby(["day_of_week", "fuel_type", "province"], as_index=False)
        .agg(
            sum_price=("avg_price", "sum"),
            count_days=("date", "nunique"),
            min_daily_avg=("avg_price", "min"),
            max_daily_avg=("avg_price", "max"),
        )
        .reset_index(drop=True)
    )


def build_day_of_week_stats_from_province_daily_stats(province_daily_df: pd.DataFrame) -> pd.DataFrame:
    if province_daily_df is None or province_daily_df.empty:
        return pd.DataFrame(columns=DAY_OF_WEEK_STATS_COLUMNS)

    daily_df = province_daily_df.copy()
    daily_df["date"] = pd.to_datetime(daily_df["date"])

    result = pd.concat(
        [_province_day_of_week_aggregates(daily_df), _national_day_of_week_aggregates(daily_df)],
        ignore_index=True,
    )
    result["sum_price"] = result["sum_price"].round(6)
    result["count_days"] = result["count_days"].astype(int)
    result["min_daily_avg"] = result["min_daily_avg"].round(4)
    result["max_daily_avg"] = result["max_daily_avg"].round(4)
    return (
        result[DAY_OF_WEEK_STATS_COLUMNS].sort_values(["day_of_week", "fuel_type", "province"]).reset_index(drop=True)
    )


def build_task(bucket: Any) -> TaskConfig:
    """Build the day-of-week stats task.

    Sources from the already-written province_daily_stats blob rather than raw data,
    so this task must run after province_stats in the same TaskRunner call.
    """
    return TaskConfig(
        name="day_of_week_stats",
        description="Day-of-week price patterns (province + national)",
        output_blob=DAY_OF_WEEK_STATS_BLOB,
        source=GCSParquetSource(bucket, PROVINCE_DAILY_STATS_BLOB),
        transformations=[build_day_of_week_stats_from_province_daily_stats],
        sink=GCSParquetSink(bucket, DAY_OF_WEEK_STATS_BLOB),
    )
