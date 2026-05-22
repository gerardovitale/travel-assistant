from __future__ import annotations

from typing import Any
from typing import List

import pandas as pd
from pipeline.base import TaskConfig
from pipeline.gcs import DataFrameSource
from pipeline.gcs import IncrementalGCSParquetSink
from shared import _rows_with_positive_price
from shared import _snapshot_date
from shared import FUEL_PRICE_COLUMNS

PROVINCE_DAILY_STATS_BLOB = "aggregates/province_daily_stats.parquet"

PROVINCE_DAILY_STATS_COLUMNS = [
    "date",
    "province",
    "fuel_type",
    "avg_price",
    "min_price",
    "max_price",
    "station_count",
]


def _aggregate_by_province(valid: pd.DataFrame, fuel_col: str, date_val) -> List[dict]:
    grouped = valid.groupby("province")[fuel_col].agg(["mean", "min", "max", "count"]).reset_index()
    return [
        {
            "date": date_val,
            "province": row["province"],
            "fuel_type": fuel_col,
            "avg_price": round(row["mean"], 4),
            "min_price": round(row["min"], 4),
            "max_price": round(row["max"], 4),
            "station_count": int(row["count"]),
        }
        for _, row in grouped.iterrows()
    ]


def compute_province_daily_stats(raw_df: pd.DataFrame) -> pd.DataFrame:
    date_val = _snapshot_date(raw_df)
    rows = []
    for fuel_col in FUEL_PRICE_COLUMNS:
        if fuel_col not in raw_df.columns:
            continue
        valid = _rows_with_positive_price(raw_df, fuel_col)
        if valid.empty:
            continue
        rows.extend(_aggregate_by_province(valid, fuel_col, date_val))
    return pd.DataFrame(rows, columns=PROVINCE_DAILY_STATS_COLUMNS)


def build_task(bucket: Any, raw_df: pd.DataFrame) -> TaskConfig:
    return TaskConfig(
        name="province_daily_stats",
        description="Province × fuel type daily aggregation",
        output_blob=PROVINCE_DAILY_STATS_BLOB,
        source=DataFrameSource(raw_df),
        transformations=[compute_province_daily_stats],
        sink=IncrementalGCSParquetSink(bucket, PROVINCE_DAILY_STATS_BLOB),
    )
