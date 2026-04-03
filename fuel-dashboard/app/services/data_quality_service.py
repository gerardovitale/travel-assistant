import logging
from datetime import date
from typing import Dict
from typing import List
from typing import Set

import pandas as pd

from data.gcs_client import download_aggregate
from data.gcs_client import list_parquet_files_with_metadata

logger = logging.getLogger(__name__)

INGESTION_STATS_AGGREGATE = "daily_ingestion_stats.parquet"


def get_ingestion_stats() -> pd.DataFrame:
    """Download the daily ingestion stats aggregate."""
    df = download_aggregate(INGESTION_STATS_AGGREGATE)
    if df is None:
        return pd.DataFrame()
    return df


def get_data_inventory(ingestion_stats: pd.DataFrame) -> Dict:
    """Compute data inventory metrics.

    Days/months/years are derived from the ingestion stats aggregate
    (only days with ``record_count > 0`` count) so empty parquet files
    in GCS are excluded.  The approximate storage size still comes from
    the GCS file listing.
    """
    empty: Dict = {
        "num_days": 0,
        "num_months": 0,
        "num_years": 0,
        "total_size_bytes": 0,
        "available_dates": set(),
        "min_date": None,
        "max_date": None,
    }

    # --- storage size from file listing ---
    files = list_parquet_files_with_metadata()
    total_size = sum(f["size_bytes"] for f in files)

    # --- temporal metrics from aggregate ---
    if ingestion_stats.empty:
        empty["total_size_bytes"] = total_size
        return empty

    valid = ingestion_stats[ingestion_stats["record_count"] > 0].copy()
    if valid.empty:
        empty["total_size_bytes"] = total_size
        return empty

    parsed_dates = pd.to_datetime(valid["date"])
    dates = {d.date() for d in parsed_dates}
    months = {(d.year, d.month) for d in dates}
    years = {d.year for d in dates}

    return {
        "num_days": len(dates),
        "num_months": len(months),
        "num_years": len(years),
        "total_size_bytes": total_size,
        "available_dates": dates,
        "min_date": min(dates),
        "max_date": max(dates),
    }


def get_latest_day_stats(ingestion_stats: pd.DataFrame, max_date: date) -> Dict:
    """Extract key metrics from the latest available day in the ingestion stats."""
    empty: Dict = {
        "max_date": None,
        "unique_stations": 0,
        "unique_provinces": 0,
        "unique_communities": 0,
        "unique_localities": 0,
        "unique_fuel_types": 0,
    }
    if ingestion_stats.empty or max_date is None:
        return empty

    dates = pd.to_datetime(ingestion_stats["date"]).dt.date
    latest_rows = ingestion_stats[dates == max_date]
    if latest_rows.empty:
        return empty

    row = latest_rows.iloc[0]
    return {
        "max_date": max_date,
        "unique_stations": int(row.get("unique_stations", 0)),
        "unique_provinces": int(row.get("unique_provinces", 0)),
        "unique_communities": int(row.get("unique_communities", 0)),
        "unique_localities": int(row.get("unique_localities", 0)),
        "unique_fuel_types": int(row.get("unique_fuel_types", 0)),
    }


def get_missing_days(available_dates: Set[date], min_date: date, max_date: date) -> List[str]:
    """Find dates with no ingested data in the range."""
    all_dates = {d.date() for d in pd.date_range(min_date, max_date, freq="D")}
    return [d.isoformat() for d in sorted(all_dates - available_dates)]
