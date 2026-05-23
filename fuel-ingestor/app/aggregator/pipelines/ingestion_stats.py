from __future__ import annotations

from typing import Any

import pandas as pd
from aggregator.pipeline.base import TaskConfig
from aggregator.pipeline.gcs import DataFrameSource
from aggregator.pipeline.gcs import IncrementalGCSParquetSink
from aggregator.shared import _snapshot_date
from aggregator.shared import FUEL_PRICE_COLUMNS

DAILY_INGESTION_STATS_BLOB = "aggregates/daily_ingestion_stats.parquet"

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
    "unique_communities",
    "unique_fuel_types",
]


def _count_unique_localities(raw_df: pd.DataFrame) -> int:
    return len(raw_df[raw_df["locality"].notna()][["province_id", "municipality_id", "locality"]].drop_duplicates())


def _count_active_fuel_types(raw_df: pd.DataFrame) -> int:
    return sum(1 for col in FUEL_PRICE_COLUMNS if col in raw_df.columns and raw_df[col].notna().any())


def compute_daily_ingestion_stats(raw_df: pd.DataFrame) -> pd.DataFrame:
    date_val = _snapshot_date(raw_df)
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
                "unique_localities": _count_unique_localities(raw_df),
                "unique_locality_names": raw_df["locality"].nunique(),
                "unique_communities": raw_df["ccaa_id"].nunique(),
                "unique_fuel_types": _count_active_fuel_types(raw_df),
            }
        ],
        columns=DAILY_INGESTION_STATS_COLUMNS,
    )


def build_task(bucket: Any, raw_df: pd.DataFrame) -> TaskConfig:
    return TaskConfig(
        name="daily_ingestion_stats",
        description="Daily ingestion summary — station and entity counts",
        output_blob=DAILY_INGESTION_STATS_BLOB,
        source=DataFrameSource(raw_df),
        transformations=[compute_daily_ingestion_stats],
        sink=IncrementalGCSParquetSink(bucket, DAILY_INGESTION_STATS_BLOB),
    )
