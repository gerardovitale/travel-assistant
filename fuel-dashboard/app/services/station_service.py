import logging
import re
import time
from typing import Dict
from typing import List
from typing import Optional

import pandas as pd
from api.schemas import DistrictPriceResult
from api.schemas import FUEL_GROUP_MEMBERS
from api.schemas import FUEL_GROUP_PRIMARY
from api.schemas import FuelGroup
from api.schemas import FuelType
from api.schemas import ProvincePriceResult
from api.schemas import StationResult
from api.schemas import TREND_PERIOD_DAYS
from api.schemas import TrendPeriod
from api.schemas import TrendPoint
from api.schemas import ZoneResult
from config import settings
from services.geo_utils import assign_districts
from services.geo_utils import point_in_multipolygon
from services.routing import get_road_distances
from services.routing import get_route_geometries

from data.duckdb_engine import get_distinct_provinces
from data.duckdb_engine import is_zip_code_trend_ready
from data.duckdb_engine import query_avg_price_by_province
from data.duckdb_engine import query_cached_group_price_trend
from data.duckdb_engine import query_cached_zip_code_price_trend
from data.duckdb_engine import query_cheapest_by_zip
from data.duckdb_engine import query_cheapest_by_zip_group
from data.duckdb_engine import query_cheapest_zones
from data.duckdb_engine import query_cheapest_zones_by_municipality
from data.duckdb_engine import query_municipalities_by_province
from data.duckdb_engine import query_national_avg_price
from data.duckdb_engine import query_nearest_stations
from data.duckdb_engine import query_nearest_stations_group
from data.duckdb_engine import query_price_trends
from data.duckdb_engine import query_stations_by_province
from data.duckdb_engine import query_stations_within_radius
from data.duckdb_engine import query_stations_within_radius_group
from data.duckdb_engine import query_volatility_by_zone
from data.duckdb_engine import query_zip_codes_by_district
from data.gcs_client import download_aggregate
from data.gcs_client import list_parquet_files
from data.geojson_loader import load_madrid_districts
from data.geojson_loader import load_postal_code_boundary
from data.geojson_loader import load_postal_codes_for_zip_list
from data.geojson_loader import normalize_data_province_name

logger = logging.getLogger(__name__)


def _validate_zip_code(zip_code: str) -> str:
    if not re.fullmatch(r"\d{5}", zip_code):
        raise ValueError(f"Invalid zip code: {zip_code!r}. Must be exactly 5 digits.")
    return zip_code


def _trend_points_from_df(df: pd.DataFrame) -> List[TrendPoint]:
    return [
        TrendPoint(
            date=str(row["date"]),
            avg_price=row["avg_price"],
            min_price=row["min_price"],
            max_price=row["max_price"],
        )
        for _, row in df.iterrows()
    ]


def _df_to_station_results(
    df: pd.DataFrame, fuel_type: str, national_avg: Optional[float] = None
) -> List[StationResult]:
    results = []
    for _, row in df.iterrows():
        pct_vs_avg = None
        if national_avg and national_avg > 0:
            pct_vs_avg = round((row[fuel_type] - national_avg) / national_avg * 100, 1)
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
                estimated_total_cost=row.get("estimated_total_cost"),
                pct_vs_avg=pct_vs_avg,
            )
        )
    return results


def _resolve_fuel_group(fuel_group: FuelGroup):
    primary = FUEL_GROUP_PRIMARY[fuel_group].value
    all_fuels = [ft.value for ft in FUEL_GROUP_MEMBERS[fuel_group]]
    return primary, all_fuels


def _df_to_station_results_group(
    df: pd.DataFrame, primary_fuel: str, all_fuels: List[str], national_avg: Optional[float] = None
) -> List[StationResult]:
    results = []
    for _, row in df.iterrows():
        primary_price = float(row[primary_fuel])
        pct_vs_avg = None
        if national_avg and national_avg > 0:
            pct_vs_avg = round((primary_price - national_avg) / national_avg * 100, 1)
        variant_prices = {}
        for ft in all_fuels:
            val = row.get(ft)
            if val is not None and pd.notna(val) and val > 0:
                variant_prices[ft] = float(val)
        results.append(
            StationResult(
                label=row["label"],
                address=row["address"],
                municipality=row["municipality"],
                province=row["province"],
                zip_code=str(row["zip_code"]),
                latitude=row["latitude"],
                longitude=row["longitude"],
                price=primary_price,
                distance_km=row.get("distance_km"),
                score=row.get("score"),
                estimated_total_cost=row.get("estimated_total_cost"),
                pct_vs_avg=pct_vs_avg,
                variant_prices=variant_prices if variant_prices else None,
            )
        )
    return results


def _enrich_with_road_distances(lat: float, lon: float, df: pd.DataFrame) -> pd.DataFrame:
    if not settings.osrm_enabled or df.empty:
        return df
    destinations = list(zip(df["latitude"].tolist(), df["longitude"].tolist()))
    distances = get_road_distances((lat, lon), destinations)
    if distances is None:
        logger.warning("OSRM enrichment failed, keeping Haversine distances")
        return df
    df = df.copy()
    df["distance_km"] = distances
    df = df.dropna(subset=["distance_km"])
    return df


async def get_route_geometries_for_stations(
    lat: float, lon: float, stations: List[StationResult]
) -> Dict[str, Optional[List[List[float]]]]:
    if not settings.osrm_enabled or not stations:
        return {}
    destinations = [(s.latitude, s.longitude) for s in stations]
    geometries = await get_route_geometries((lat, lon), destinations)
    if geometries is None:
        return {}
    return {s.label: geom for s, geom in zip(stations, geometries)}


def get_zip_code_boundary(zip_code: str) -> Optional[dict]:
    _validate_zip_code(zip_code)
    return load_postal_code_boundary(zip_code)


def get_cheapest_by_zip(zip_code: str, fuel_type: FuelType, limit: int = 5) -> List[StationResult]:
    _validate_zip_code(zip_code)
    df = query_cheapest_by_zip(zip_code, fuel_type.value, limit)
    national_avg = query_national_avg_price(fuel_type.value)
    return _df_to_station_results(df, fuel_type.value, national_avg)


def get_nearest_by_address(lat: float, lon: float, fuel_type: FuelType, limit: int = 5) -> List[StationResult]:
    oversample = limit * 3 if settings.osrm_enabled else limit
    df = query_nearest_stations(lat, lon, fuel_type.value, oversample)
    df = _enrich_with_road_distances(lat, lon, df)
    df = df.sort_values("distance_km").head(limit)
    national_avg = query_national_avg_price(fuel_type.value)
    return _df_to_station_results(df, fuel_type.value, national_avg)


def get_cheapest_by_address(
    lat: float, lon: float, fuel_type: FuelType, radius_km: float = None, limit: int = 5
) -> List[StationResult]:
    if radius_km is None:
        radius_km = settings.default_radius_km
    fetch_radius = radius_km * 1.3 if settings.osrm_enabled else radius_km
    df = query_stations_within_radius(lat, lon, fuel_type.value, fetch_radius)
    if df.empty:
        return []
    df = _enrich_with_road_distances(lat, lon, df)
    if df.empty:
        return []
    df = df[df["distance_km"] <= radius_km]
    df = df.sort_values(fuel_type.value).head(limit)
    national_avg = query_national_avg_price(fuel_type.value)
    return _df_to_station_results(df, fuel_type.value, national_avg)


def get_best_by_address(
    lat: float,
    lon: float,
    fuel_type: FuelType,
    radius_km: Optional[float] = None,
    limit: int = 5,
    consumption_lper100km: Optional[float] = None,
    tank_liters: Optional[float] = None,
) -> List[StationResult]:
    """Rank stations by estimated total cost on a 0-10 scale (10 = cheapest).

    Total cost model
    ----------------
    The score answers: "Which station costs me the least overall, including the
    fuel I burn driving there and back?"

    ``total_cost = price * (tank_liters + 2 * distance_km * consumption / 100)``

    - **price**: station fuel price (EUR/L).
    - **tank_liters**: how many liters the user plans to refill.
    - **2 * distance_km**: round-trip distance to the station.
    - **consumption / 100**: liters burned per km of driving.

    The score maps total-cost savings to a 0-10 scale:

    ``score = round(10 * (max_cost - cost) / (max_cost - min_cost), 1)``

    10 = cheapest total cost, 0 = most expensive.  A single-station result
    always receives 10.0.  No arbitrary weights — the physics of fuel
    consumption determines how price and distance trade off.
    """
    if radius_km is None:
        radius_km = settings.default_radius_km
    if consumption_lper100km is None:
        consumption_lper100km = settings.default_consumption_lper100km
    if tank_liters is None:
        tank_liters = settings.default_refill_liters
    fetch_radius = radius_km * 1.3 if settings.osrm_enabled else radius_km
    df = query_stations_within_radius(lat, lon, fuel_type.value, fetch_radius)
    if df.empty:
        return []
    df = _enrich_with_road_distances(lat, lon, df)
    if df.empty:
        return []
    df = df[df["distance_km"] <= radius_km]
    if df.empty:
        return []

    price_col = fuel_type.value
    trip_liters = 2.0 * df["distance_km"] * consumption_lper100km / 100.0
    df["estimated_total_cost"] = (df[price_col] * (tank_liters + trip_liters)).round(2)

    cost_range = df["estimated_total_cost"].max() - df["estimated_total_cost"].min()
    if cost_range > 0:
        df["score"] = ((df["estimated_total_cost"].max() - df["estimated_total_cost"]) / cost_range * 10.0).round(1)
    else:
        df["score"] = 10.0

    df = df.sort_values("score", ascending=False).head(limit)
    national_avg = query_national_avg_price(fuel_type.value)
    return _df_to_station_results(df, fuel_type.value, national_avg)


def get_provinces() -> dict[str, str]:
    return get_distinct_provinces()


def get_cheapest_by_zip_group(zip_code: str, fuel_group: FuelGroup, limit: int = 5) -> List[StationResult]:
    _validate_zip_code(zip_code)
    primary, all_fuels = _resolve_fuel_group(fuel_group)
    df = query_cheapest_by_zip_group(zip_code, primary, all_fuels, limit)
    national_avg = query_national_avg_price(primary)
    return _df_to_station_results_group(df, primary, all_fuels, national_avg)


def get_nearest_by_address_group(lat: float, lon: float, fuel_group: FuelGroup, limit: int = 5) -> List[StationResult]:
    primary, all_fuels = _resolve_fuel_group(fuel_group)
    oversample = limit * 3 if settings.osrm_enabled else limit
    df = query_nearest_stations_group(lat, lon, primary, all_fuels, oversample)
    df = _enrich_with_road_distances(lat, lon, df)
    df = df.sort_values("distance_km").head(limit)
    national_avg = query_national_avg_price(primary)
    return _df_to_station_results_group(df, primary, all_fuels, national_avg)


def get_cheapest_by_address_group(
    lat: float, lon: float, fuel_group: FuelGroup, radius_km: float = None, limit: int = 5
) -> List[StationResult]:
    primary, all_fuels = _resolve_fuel_group(fuel_group)
    if radius_km is None:
        radius_km = settings.default_radius_km
    fetch_radius = radius_km * 1.3 if settings.osrm_enabled else radius_km
    df = query_stations_within_radius_group(lat, lon, primary, all_fuels, fetch_radius)
    if df.empty:
        return []
    df = _enrich_with_road_distances(lat, lon, df)
    if df.empty:
        return []
    df = df[df["distance_km"] <= radius_km]
    df = df.sort_values([primary, "distance_km"]).head(limit)
    national_avg = query_national_avg_price(primary)
    return _df_to_station_results_group(df, primary, all_fuels, national_avg)


def get_best_by_address_group(
    lat: float,
    lon: float,
    fuel_group: FuelGroup,
    radius_km: Optional[float] = None,
    limit: int = 5,
    consumption_lper100km: Optional[float] = None,
    tank_liters: Optional[float] = None,
) -> List[StationResult]:
    primary, all_fuels = _resolve_fuel_group(fuel_group)
    if radius_km is None:
        radius_km = settings.default_radius_km
    consumption = consumption_lper100km if consumption_lper100km is not None else settings.default_consumption_lper100km
    tank = tank_liters if tank_liters is not None else settings.default_refill_liters
    fetch_radius = radius_km * 1.3 if settings.osrm_enabled else radius_km
    df = query_stations_within_radius_group(lat, lon, primary, all_fuels, fetch_radius)
    if df.empty:
        return []
    df = _enrich_with_road_distances(lat, lon, df)
    if df.empty:
        return []
    df = df[df["distance_km"] <= radius_km]
    trip_liters = 2.0 * df["distance_km"] * consumption / 100.0
    df["estimated_total_cost"] = (df[primary] * (tank + trip_liters)).round(2)
    costs = df["estimated_total_cost"]
    min_cost, max_cost = costs.min(), costs.max()
    cost_range = max_cost - min_cost
    if cost_range > 0:
        df["score"] = ((max_cost - df["estimated_total_cost"]) / cost_range * 10).round(1)
    else:
        df["score"] = 10.0
    df = df.sort_values("score", ascending=False).head(limit)
    national_avg = query_national_avg_price(primary)
    return _df_to_station_results_group(df, primary, all_fuels, national_avg)


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


def get_municipalities(province: str) -> List[str]:
    return query_municipalities_by_province(province)


def get_zip_code_price_map_by_municipality(province: str, fuel_type: FuelType, municipality: str) -> List[ZoneResult]:
    df = query_cheapest_zones_by_municipality(province, fuel_type.value, municipality)
    return [
        ZoneResult(
            zip_code=str(row["zip_code"]),
            avg_price=row["avg_price"],
            min_price=row["min_price"],
            station_count=row["station_count"],
        )
        for _, row in df.iterrows()
    ]


def get_zip_codes_for_district(province: str, fuel_type: FuelType, district_name: str) -> List[str]:
    """Get zip codes that fall within a Madrid district by assigning stations to districts."""
    geojson = load_madrid_districts()
    df = query_zip_codes_by_district(province, fuel_type.value)
    if df.empty:
        return []
    aggregated = assign_districts(
        df["latitude"].tolist(),
        df["longitude"].tolist(),
        df["price"].tolist(),
        geojson["features"],
    )
    if district_name not in aggregated:
        return []
    # Collect zip codes for stations that were assigned to the target district
    target_zips: set = set()
    for feature in geojson["features"]:
        if feature["properties"]["nombre"] == district_name:
            for _, row in df.iterrows():
                if point_in_multipolygon(row["latitude"], row["longitude"], feature["geometry"]):
                    target_zips.add(str(row["zip_code"]))
            break
    return sorted(target_zips)


def get_zip_code_price_map_for_zips(province: str, fuel_type: FuelType, zip_codes: List[str]) -> List[ZoneResult]:
    """Get per-zip-code prices filtered to a specific set of zip codes."""
    all_zones = get_cheapest_zones(province, fuel_type)
    zip_set = set(zip_codes)
    return [z for z in all_zones if z.zip_code in zip_set]


def get_postal_code_geojson(zip_codes: List[str]) -> dict:
    return load_postal_codes_for_zip_list(zip_codes)


def get_price_trends(zip_code: str, fuel_type: FuelType, period: TrendPeriod) -> List[TrendPoint]:
    _validate_zip_code(zip_code)
    days_back = TREND_PERIOD_DAYS[period]
    started = time.perf_counter()
    source = "duckdb_trend_cache"
    if is_zip_code_trend_ready():
        df = query_cached_zip_code_price_trend(zip_code, fuel_type.value, days_back)
    else:
        source = "raw_history_fallback"
        logger.warning("Zip-code trend cache not ready; falling back to raw history for %s", zip_code)
        files = list_parquet_files(days_back=days_back)
        df = query_price_trends(files, zip_code, fuel_type.value)
    trend = _trend_points_from_df(df)
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info(
        "Loaded price trends for %s from %s (%s rows, %.1f ms)",
        zip_code,
        source,
        len(trend),
        duration_ms,
    )
    return trend


def get_group_price_trends(zip_code: str, fuel_group: FuelGroup, period: TrendPeriod) -> Dict[str, List[TrendPoint]]:
    _validate_zip_code(zip_code)
    days_back = TREND_PERIOD_DAYS[period]
    fuel_types = [ft.value for ft in FUEL_GROUP_MEMBERS[fuel_group]]
    started = time.perf_counter()
    result: Dict[str, List[TrendPoint]] = {}
    source = "duckdb_trend_cache"

    if is_zip_code_trend_ready():
        df = query_cached_group_price_trend(zip_code, fuel_types, days_back)
        for fuel_type, group_df in df.groupby("fuel_type"):
            result[str(fuel_type)] = _trend_points_from_df(group_df)
    else:
        source = "raw_history_fallback"
        logger.warning("Zip-code trend cache not ready; falling back to raw history for grouped trends in %s", zip_code)
        files = list_parquet_files(days_back=days_back)
        for fuel_type in fuel_types:
            group_df = query_price_trends(files, zip_code, fuel_type)
            if not group_df.empty:
                result[fuel_type] = _trend_points_from_df(group_df)

    duration_ms = (time.perf_counter() - started) * 1000
    logger.info(
        "Loaded group price trends for %s/%s from %s (%s variants, %.1f ms)",
        zip_code,
        fuel_group.value,
        source,
        len(result),
        duration_ms,
    )
    return result


def get_province_ranking(fuel_type: FuelType, days_back: int) -> pd.DataFrame:
    from data.duckdb_engine import query_province_ranking
    from data.gcs_client import download_aggregate

    agg_df = download_aggregate("province_daily_stats.parquet")
    if agg_df is None:
        return pd.DataFrame()
    return query_province_ranking(agg_df, fuel_type.value, days_back)


def get_day_of_week_pattern(
    fuel_type: FuelType, province: Optional[str] = None, exclude_provinces: Optional[set] = None
) -> pd.DataFrame:
    from data.duckdb_engine import query_day_of_week_pattern
    from data.gcs_client import download_aggregate

    agg_df = download_aggregate("day_of_week_stats.parquet")
    if agg_df is None:
        return pd.DataFrame()
    return query_day_of_week_pattern(agg_df, fuel_type.value, normalize_data_province_name(province), exclude_provinces)


def get_brand_ranking(fuel_type: FuelType, days_back: int, top_n: int = 15) -> pd.DataFrame:
    from data.duckdb_engine import query_brand_ranking
    from data.gcs_client import download_aggregate

    agg_df = download_aggregate("brand_daily_stats.parquet")
    if agg_df is None:
        return pd.DataFrame()
    return query_brand_ranking(agg_df, fuel_type.value, days_back, top_n)


def get_brand_price_trend(fuel_type: FuelType, days_back: int, brands: list) -> pd.DataFrame:
    from data.duckdb_engine import query_brand_price_trend
    from data.gcs_client import download_aggregate

    agg_df = download_aggregate("brand_daily_stats.parquet")
    if agg_df is None:
        return pd.DataFrame()
    return query_brand_price_trend(agg_df, fuel_type.value, days_back, brands)


def get_zone_volatility_ranking(fuel_type: FuelType, days_back: int, mainland_only: bool = True) -> pd.DataFrame:
    agg_df = download_aggregate("zip_code_daily_stats.parquet")
    if agg_df is None:
        return pd.DataFrame()
    return query_volatility_by_zone(agg_df, fuel_type.value, days_back, mainland_only)
