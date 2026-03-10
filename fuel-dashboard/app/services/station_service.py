import logging
import re
from typing import List
from typing import Optional

import pandas as pd
from api.schemas import DistrictPriceResult
from api.schemas import FuelType
from api.schemas import ProvincePriceResult
from api.schemas import StationResult
from api.schemas import TREND_PERIOD_DAYS
from api.schemas import TrendPeriod
from api.schemas import TrendPoint
from api.schemas import ZoneResult
from config import settings
from services.geo_utils import assign_districts

from data.duckdb_engine import get_distinct_provinces
from data.duckdb_engine import query_avg_price_by_province
from data.duckdb_engine import query_cheapest_by_zip
from data.duckdb_engine import query_cheapest_zones
from data.duckdb_engine import query_nearest_stations
from data.duckdb_engine import query_price_trends
from data.duckdb_engine import query_stations_by_province
from data.duckdb_engine import query_stations_within_radius
from data.gcs_client import list_parquet_files
from data.geojson_loader import load_madrid_districts
from data.geojson_loader import load_postal_code_boundary

logger = logging.getLogger(__name__)


def _validate_zip_code(zip_code: str) -> str:
    if not re.fullmatch(r"\d{5}", zip_code):
        raise ValueError(f"Invalid zip code: {zip_code!r}. Must be exactly 5 digits.")
    return zip_code


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


def get_zip_code_boundary(zip_code: str) -> Optional[dict]:
    _validate_zip_code(zip_code)
    return load_postal_code_boundary(zip_code)


def get_cheapest_by_zip(zip_code: str, fuel_type: FuelType, limit: int = 3) -> List[StationResult]:
    _validate_zip_code(zip_code)
    df = query_cheapest_by_zip(zip_code, fuel_type.value, limit)
    return _df_to_station_results(df, fuel_type.value)


def get_nearest_by_address(lat: float, lon: float, fuel_type: FuelType, limit: int = 3) -> List[StationResult]:
    df = query_nearest_stations(lat, lon, fuel_type.value, limit)
    return _df_to_station_results(df, fuel_type.value)


def get_cheapest_by_address(
    lat: float, lon: float, fuel_type: FuelType, radius_km: float = None, limit: int = 3
) -> List[StationResult]:
    if radius_km is None:
        radius_km = settings.default_radius_km
    df = query_stations_within_radius(lat, lon, fuel_type.value, radius_km)
    if df.empty:
        return []
    df = df.sort_values(fuel_type.value).head(limit)
    return _df_to_station_results(df, fuel_type.value)


def get_best_by_address(
    lat: float, lon: float, fuel_type: FuelType, radius_km: float = None, limit: int = 3
) -> List[StationResult]:
    if radius_km is None:
        radius_km = settings.default_radius_km
    df = query_stations_within_radius(lat, lon, fuel_type.value, radius_km)
    if df.empty:
        return []
    df["price_rank"] = df[fuel_type.value].rank(method="min")
    df["distance_rank"] = df["distance_km"].rank(method="min")
    df["score"] = settings.price_weight * df["price_rank"] + settings.distance_weight * df["distance_rank"]
    df = df.sort_values("score").head(limit)
    return _df_to_station_results(df, fuel_type.value)


def get_provinces() -> dict[str, str]:
    return get_distinct_provinces()


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


def get_province_price_map(fuel_type: FuelType) -> List[ProvincePriceResult]:
    df = query_avg_price_by_province(fuel_type.value)
    return [
        ProvincePriceResult(
            province=row["province"],
            avg_price=row["avg_price"],
            station_count=row["station_count"],
        )
        for _, row in df.iterrows()
    ]


def get_district_price_map(province: str, fuel_type: FuelType) -> List[DistrictPriceResult]:
    geojson = load_madrid_districts()
    df = query_stations_by_province(province, fuel_type.value)
    if df.empty:
        return []
    aggregated = assign_districts(
        df["latitude"].tolist(),
        df["longitude"].tolist(),
        df["price"].tolist(),
        geojson["features"],
    )
    results = []
    for district, data in aggregated.items():
        results.append(
            DistrictPriceResult(
                district=district,
                avg_price=data["total_price"] / data["count"],
                station_count=int(data["count"]),
            )
        )
    return sorted(results, key=lambda r: r.avg_price)


def get_price_trends(zip_code: str, fuel_type: FuelType, period: TrendPeriod) -> List[TrendPoint]:
    _validate_zip_code(zip_code)
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
