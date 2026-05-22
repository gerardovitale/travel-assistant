from __future__ import annotations

from typing import List

import pandas as pd
from brand_utils import MIN_STATION_COUNT
from brand_utils import normalize_brand
from shared import _snapshot_date
from shared import FUEL_PRICE_COLUMNS

BRAND_DAILY_STATS_COLUMNS = [
    "date",
    "brand",
    "fuel_type",
    "avg_price",
    "min_price",
    "max_price",
    "station_count",
]


def _normalize_brands(raw_df: pd.DataFrame) -> pd.DataFrame:
    brands = raw_df["label"].apply(normalize_brand)
    brand_df = raw_df[brands.notna()].copy()
    brand_df["brand"] = brands[brands.notna()]
    return brand_df


def _rows_with_positive_price(brand_df: pd.DataFrame, fuel_col: str) -> pd.DataFrame:
    return brand_df[brand_df[fuel_col].notna() & (brand_df[fuel_col] > 0)]


def _aggregate_by_brand(valid: pd.DataFrame, fuel_col: str, date_val) -> List[dict]:
    grouped = valid.groupby("brand")[fuel_col].agg(["mean", "min", "max", "count"]).reset_index()
    grouped = grouped[grouped["count"] >= MIN_STATION_COUNT]
    return [
        {
            "date": date_val,
            "brand": row["brand"],
            "fuel_type": fuel_col,
            "avg_price": round(row["mean"], 4),
            "min_price": round(row["min"], 4),
            "max_price": round(row["max"], 4),
            "station_count": int(row["count"]),
        }
        for _, row in grouped.iterrows()
    ]


def compute_brand_daily_stats(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=BRAND_DAILY_STATS_COLUMNS)

    date_val = _snapshot_date(raw_df)
    brand_df = _normalize_brands(raw_df)

    rows = []
    for fuel_col in FUEL_PRICE_COLUMNS:
        if fuel_col not in brand_df.columns:
            continue
        valid = _rows_with_positive_price(brand_df, fuel_col)
        if valid.empty:
            continue
        rows.extend(_aggregate_by_brand(valid, fuel_col, date_val))
    return pd.DataFrame(rows, columns=BRAND_DAILY_STATS_COLUMNS)
