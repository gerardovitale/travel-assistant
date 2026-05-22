from __future__ import annotations

from typing import List

import pandas as pd
from shared import _snapshot_date
from shared import FUEL_PRICE_COLUMNS

PROVINCE_DAILY_STATS_COLUMNS = [
    "date",
    "province",
    "fuel_type",
    "avg_price",
    "min_price",
    "max_price",
    "station_count",
]


def _rows_with_positive_price(raw_df: pd.DataFrame, fuel_col: str) -> pd.DataFrame:
    return raw_df[raw_df[fuel_col].notna() & (raw_df[fuel_col] > 0)]


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
