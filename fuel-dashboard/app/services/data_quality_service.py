import logging
from datetime import date

import pandas as pd
from api.schemas import DataInventory
from api.schemas import LatestDayStats
from api.schemas import QualityResponse
from api.schemas import RealtimeStatus

from data.cache import get_realtime_status
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


def get_data_inventory(ingestion_stats: pd.DataFrame) -> dict:
    """Compute data inventory metrics.

    Days/months/years are derived from the ingestion stats aggregate
    (only days with ``record_count > 0`` count) so empty parquet files
    in GCS are excluded.  The approximate storage size still comes from
    the GCS file listing.
    """
    empty: dict = {
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


def get_latest_day_stats(ingestion_stats: pd.DataFrame, max_date: date) -> dict:
    """Extract key metrics from the latest available day in the ingestion stats."""
    empty: dict = {
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


def get_missing_days(available_dates: set[date], min_date: date, max_date: date) -> list[str]:
    """Find dates with no ingested data in the range."""
    all_dates = {d.date() for d in pd.date_range(min_date, max_date, freq="D")}
    return [d.isoformat() for d in sorted(all_dates - available_dates)]


def get_quality_report() -> QualityResponse:
    """Compose the full data-quality/inventory summary (REST `/quality/inventory` and the
    `get_data_quality_inventory` MCP tool share this)."""
    stats = get_ingestion_stats()
    inventory = get_data_inventory(stats)
    max_date: date | None = inventory.get("max_date")
    min_date: date | None = inventory.get("min_date")
    latest = get_latest_day_stats(stats, max_date) if max_date else {}
    available: set[date] = inventory.get("available_dates") or set()
    missing = get_missing_days(available, min_date, max_date) if (min_date and max_date) else []
    return QualityResponse(
        inventory=DataInventory(
            num_days=inventory.get("num_days", 0),
            num_months=inventory.get("num_months", 0),
            num_years=inventory.get("num_years", 0),
            total_size_bytes=inventory.get("total_size_bytes", 0),
            min_date=min_date.isoformat() if min_date else None,
            max_date=max_date.isoformat() if max_date else None,
        ),
        latest_day=LatestDayStats(
            max_date=latest["max_date"].isoformat() if latest.get("max_date") else None,
            unique_stations=latest.get("unique_stations", 0),
            unique_provinces=latest.get("unique_provinces", 0),
            unique_communities=latest.get("unique_communities", 0),
            unique_localities=latest.get("unique_localities", 0),
            unique_fuel_types=latest.get("unique_fuel_types", 0),
        ),
        missing_days=missing,
        realtime=RealtimeStatus(**get_realtime_status()),
    )
