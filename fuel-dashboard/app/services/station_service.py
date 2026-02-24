import logging
from typing import List

import pandas as pd
from api.schemas import FuelType
from api.schemas import StationResult
from api.schemas import TREND_PERIOD_DAYS
from api.schemas import TrendPeriod
from api.schemas import TrendPoint
from api.schemas import ZoneResult
from config import settings
from services.geocoding import geocode_address

from data.duckdb_engine import query_cheapest_by_zip
from data.duckdb_engine import query_cheapest_zones
from data.duckdb_engine import query_nearest_stations
from data.duckdb_engine import query_price_trends
from data.duckdb_engine import query_stations_within_radius
from data.gcs_client import list_parquet_files

logger = logging.getLogger(__name__)


def _df_to_station_results(df: pd.DataFrame, fuel_type: str) -> List[StationResult]:
    results = []
    for _, row in df.iterrows():
        results.append(
            StationResult(
                label=row["label"],
                address=row["address"],
                municipality=row["municipality"],
                province=row["province"],
                zip_code=str(row["zip_code"]),
                latitude=row["latitude"],
                longitude=row["longitude"],
                price=row[fuel_type],
                distance_km=row.get("distance_km"),
                score=row.get("score"),
            )
        )
    return results


def get_cheapest_by_zip(zip_code: str, fuel_type: FuelType, limit: int = 3) -> List[StationResult]:
    df = query_cheapest_by_zip(zip_code, fuel_type.value, limit)
    return _df_to_station_results(df, fuel_type.value)


def get_nearest_by_address(address: str, fuel_type: FuelType, limit: int = 3) -> List[StationResult]:
    coords = geocode_address(address)
    if coords is None:
        return []
    lat, lon = coords
    df = query_nearest_stations(lat, lon, fuel_type.value, limit)
    return _df_to_station_results(df, fuel_type.value)


def get_cheapest_by_address(address: str, fuel_type: FuelType, radius_km: float = None) -> List[StationResult]:
    if radius_km is None:
        radius_km = settings.default_radius_km
    coords = geocode_address(address)
    if coords is None:
        return []
    lat, lon = coords
    df = query_stations_within_radius(lat, lon, fuel_type.value, radius_km)
    if df.empty:
        return []
    df = df.sort_values(fuel_type.value).head(settings.default_limit)
    return _df_to_station_results(df, fuel_type.value)


def get_best_by_address(address: str, fuel_type: FuelType, radius_km: float = None) -> List[StationResult]:
    if radius_km is None:
        radius_km = settings.default_radius_km
    coords = geocode_address(address)
    if coords is None:
        return []
    lat, lon = coords
    df = query_stations_within_radius(lat, lon, fuel_type.value, radius_km)
    if df.empty:
        return []
    df["price_rank"] = df[fuel_type.value].rank(method="min")
    df["distance_rank"] = df["distance_km"].rank(method="min")
    df["score"] = settings.price_weight * df["price_rank"] + settings.distance_weight * df["distance_rank"]
    df = df.sort_values("score").head(settings.default_limit)
    return _df_to_station_results(df, fuel_type.value)


def get_cheapest_zones(province: str, fuel_type: FuelType) -> List[ZoneResult]:
    df = query_cheapest_zones(province, fuel_type.value)
    return [
        ZoneResult(
            zip_code=str(row["zip_code"]),
            avg_price=row["avg_price"],
            min_price=row["min_price"],
            station_count=row["station_count"],
        )
        for _, row in df.iterrows()
    ]


def get_price_trends(zip_code: str, fuel_type: FuelType, period: TrendPeriod) -> List[TrendPoint]:
    days_back = TREND_PERIOD_DAYS[period]
    files = list_parquet_files(days_back=days_back)
    df = query_price_trends(files, zip_code, fuel_type.value)
    return [
        TrendPoint(
            date=str(row["date"]),
            avg_price=row["avg_price"],
            min_price=row["min_price"],
            max_price=row["max_price"],
        )
        for _, row in df.iterrows()
    ]
