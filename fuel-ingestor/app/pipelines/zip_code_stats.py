from __future__ import annotations

from typing import List

import pandas as pd
from shared import _snapshot_date
from shared import FUEL_PRICE_COLUMNS

ZIP_CODE_DAILY_STATS_COLUMNS = [
    "date",
    "zip_code",
    "province",
    "fuel_type",
    "avg_price",
    "min_price",
    "max_price",
    "station_count",
]


def _rows_with_positive_price(zip_df: pd.DataFrame, fuel_col: str) -> pd.DataFrame:
    return zip_df[zip_df[fuel_col].notna() & (zip_df[fuel_col] > 0)]


def _aggregate_by_zip_code(valid: pd.DataFrame, fuel_col: str, date_val) -> List[dict]:
    grouped = valid.groupby(["zip_code", "province"])[fuel_col].agg(["mean", "min", "max", "count"]).reset_index()
    return [
        {
            "date": date_val,
            "zip_code": row["zip_code"],
            "province": row["province"],
            "fuel_type": fuel_col,
            "avg_price": round(row["mean"], 4),
            "min_price": round(row["min"], 4),
            "max_price": round(row["max"], 4),
            "station_count": int(row["count"]),
        }
        for _, row in grouped.iterrows()
    ]


def compute_zip_code_daily_stats(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty or "zip_code" not in raw_df.columns or "province" not in raw_df.columns:
        return pd.DataFrame(columns=ZIP_CODE_DAILY_STATS_COLUMNS)

    date_val = _snapshot_date(raw_df)
    zip_df = raw_df[raw_df["zip_code"].notna() & raw_df["province"].notna()].copy()
    if zip_df.empty:
        return pd.DataFrame(columns=ZIP_CODE_DAILY_STATS_COLUMNS)
    zip_df["zip_code"] = zip_df["zip_code"].astype(str)

    rows = []
    for fuel_col in FUEL_PRICE_COLUMNS:
        if fuel_col not in zip_df.columns:
            continue
        valid = _rows_with_positive_price(zip_df, fuel_col)
        if valid.empty:
            continue
        rows.extend(_aggregate_by_zip_code(valid, fuel_col, date_val))
    return pd.DataFrame(rows, columns=ZIP_CODE_DAILY_STATS_COLUMNS)
