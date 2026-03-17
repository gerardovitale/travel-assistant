import logging
import math
import threading
from typing import List
from typing import Optional

import duckdb
import pandas as pd
from api.schemas import FuelType

from data.gcs_client import download_parquet_as_df
from data.gcs_client import download_parquets_as_df
from data.gcs_client import get_latest_parquet_file

logger = logging.getLogger(__name__)

_VALID_FUEL_COLUMNS: frozenset[str] = frozenset(ft.value for ft in FuelType)


def _validate_fuel_column(fuel_type: str) -> str:
    if fuel_type not in _VALID_FUEL_COLUMNS:
        raise ValueError(f"Invalid fuel column: {fuel_type!r}")
    return fuel_type


_connection: Optional[duckdb.DuckDBPyConnection] = None
_lock = threading.Lock()


def get_connection() -> duckdb.DuckDBPyConnection:
    global _connection
    if _connection is None:
        _connection = duckdb.connect(":memory:")
    return _connection


def refresh_latest_snapshot() -> None:
    latest_file = get_latest_parquet_file()
    if latest_file is None:
        logger.warning("No parquet files found in GCS bucket")
        return
    logger.info(f"Refreshing latest snapshot from {latest_file}")
    df = download_parquet_as_df(latest_file)  # noqa: F841
    with _lock:
        conn = get_connection()
        conn.execute("DROP TABLE IF EXISTS latest_stations")
        conn.execute("CREATE TABLE latest_stations AS SELECT * FROM df")
        count = conn.execute("SELECT COUNT(*) FROM latest_stations").fetchone()[0]
    logger.info(f"Loaded {count} stations into latest_stations table")


def query_cheapest_by_zip(zip_code: str, fuel_type: str, limit: int = 5) -> pd.DataFrame:
    fuel_type = _validate_fuel_column(fuel_type)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT label, address, municipality, province, zip_code, latitude, longitude, {fuel_type}
            FROM latest_stations
            WHERE zip_code = $1 AND {fuel_type} IS NOT NULL AND {fuel_type} > 0
            ORDER BY {fuel_type} ASC
            LIMIT $2
            """,
            [zip_code, limit],
        ).fetchdf()


def query_stations_within_radius(lat: float, lon: float, fuel_type: str, radius_km: float) -> pd.DataFrame:
    fuel_type = _validate_fuel_column(fuel_type)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT * FROM (
                SELECT label, address, municipality, province, zip_code, latitude, longitude, {fuel_type},
                    2 * 6371 * ASIN(SQRT(
                        POWER(SIN(RADIANS(latitude - $1) / 2), 2) +
                        COS(RADIANS($1)) * COS(RADIANS(latitude)) *
                        POWER(SIN(RADIANS(longitude - $2) / 2), 2)
                    )) AS distance_km
                FROM latest_stations
                WHERE {fuel_type} IS NOT NULL AND {fuel_type} > 0
                    AND latitude IS NOT NULL AND longitude IS NOT NULL
            ) WHERE distance_km <= $3
            ORDER BY distance_km ASC
            """,
            [lat, lon, radius_km],
        ).fetchdf()


def query_nearest_stations(lat: float, lon: float, fuel_type: str, limit: int = 5) -> pd.DataFrame:
    fuel_type = _validate_fuel_column(fuel_type)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT label, address, municipality, province, zip_code, latitude, longitude, {fuel_type},
                2 * 6371 * ASIN(SQRT(
                    POWER(SIN(RADIANS(latitude - $1) / 2), 2) +
                    COS(RADIANS($1)) * COS(RADIANS(latitude)) *
                    POWER(SIN(RADIANS(longitude - $2) / 2), 2)
                )) AS distance_km
            FROM latest_stations
            WHERE {fuel_type} IS NOT NULL AND {fuel_type} > 0
                AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY distance_km ASC
            LIMIT $3
            """,
            [lat, lon, limit],
        ).fetchdf()


def query_cheapest_zones(province: str, fuel_type: str) -> pd.DataFrame:
    fuel_type = _validate_fuel_column(fuel_type)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT zip_code,
                AVG({fuel_type}) AS avg_price,
                MIN({fuel_type}) AS min_price,
                COUNT(*) AS station_count
            FROM latest_stations
            WHERE province = $1 AND {fuel_type} IS NOT NULL AND {fuel_type} > 0
            GROUP BY zip_code
            ORDER BY avg_price ASC
            """,
            [province],
        ).fetchdf()


def query_price_trends(blob_names: List[str], zip_code: str, fuel_type: str) -> pd.DataFrame:
    fuel_type = _validate_fuel_column(fuel_type)
    if not blob_names:
        return pd.DataFrame()
    df = download_parquets_as_df(blob_names)  # noqa: F841
    conn = duckdb.connect(":memory:")
    return conn.execute(
        f"""
        SELECT
            CAST(timestamp AS DATE) AS date,
            AVG({fuel_type}) AS avg_price,
            MIN({fuel_type}) AS min_price,
            MAX({fuel_type}) AS max_price
        FROM df
        WHERE zip_code = $1 AND {fuel_type} IS NOT NULL AND {fuel_type} > 0
        GROUP BY CAST(timestamp AS DATE)
        ORDER BY date ASC
        """,
        [zip_code],
    ).fetchdf()


def query_avg_price_by_province(fuel_type: str) -> pd.DataFrame:
    fuel_type = _validate_fuel_column(fuel_type)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT province,
                AVG({fuel_type}) AS avg_price,
                COUNT(*) AS station_count
            FROM latest_stations
            WHERE {fuel_type} IS NOT NULL AND {fuel_type} > 0
            GROUP BY province
            ORDER BY avg_price ASC
            """,
        ).fetchdf()


def query_stations_by_province(province: str, fuel_type: str) -> pd.DataFrame:
    fuel_type = _validate_fuel_column(fuel_type)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT latitude, longitude, {fuel_type} AS price
            FROM latest_stations
            WHERE province = $1
                AND {fuel_type} IS NOT NULL AND {fuel_type} > 0
                AND latitude IS NOT NULL AND longitude IS NOT NULL
            """,
            [province],
        ).fetchdf()


def query_cheapest_zones_by_municipality(province: str, municipality: str, fuel_type: str) -> pd.DataFrame:
    fuel_type = _validate_fuel_column(fuel_type)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT zip_code,
                AVG({fuel_type}) AS avg_price,
                MIN({fuel_type}) AS min_price,
                COUNT(*) AS station_count
            FROM latest_stations
            WHERE province = $1 AND municipality = $2
                AND {fuel_type} IS NOT NULL AND {fuel_type} > 0
            GROUP BY zip_code
            ORDER BY avg_price ASC
            """,
            [province, municipality],
        ).fetchdf()


def query_municipalities_by_province(province: str) -> List[str]:
    with _lock:
        conn = get_connection()
        result = conn.execute(
            "SELECT DISTINCT municipality FROM latest_stations WHERE province = $1 ORDER BY municipality",
            [province],
        ).fetchdf()
    return result["municipality"].tolist()


def query_zip_codes_by_district(province: str, fuel_type: str) -> pd.DataFrame:
    """Query stations in a province with their zip codes (for district-to-zip mapping)."""
    fuel_type = _validate_fuel_column(fuel_type)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT latitude, longitude, zip_code, {fuel_type} AS price
            FROM latest_stations
            WHERE province = $1
                AND {fuel_type} IS NOT NULL AND {fuel_type} > 0
                AND latitude IS NOT NULL AND longitude IS NOT NULL
            """,
            [province],
        ).fetchdf()


def query_stations_along_corridor(
    waypoints: List[tuple],
    fuel_type: str,
    corridor_km: float,
) -> pd.DataFrame:
    """Query stations within a corridor around route waypoints.

    Uses DuckDB cross join + Haversine to efficiently find the closest waypoint
    per station and filter by corridor distance.

    Args:
        waypoints: list of (lat, lon, cumulative_km) tuples.
        fuel_type: validated fuel column name.
        corridor_km: max distance from any waypoint to include a station.

    Returns:
        DataFrame with station data + min_distance_km + closest_waypoint_idx.
    """
    fuel_type = _validate_fuel_column(fuel_type)
    if not waypoints:
        return pd.DataFrame()

    lats = [w[0] for w in waypoints]
    lons = [w[1] for w in waypoints]
    avg_lat_rad = math.radians(sum(lats) / len(lats))
    lon_degree_km = 111.0 * math.cos(avg_lat_rad)
    lat_offset = corridor_km / 111.0
    lon_offset = corridor_km / max(0.01, lon_degree_km)

    min_lat = min(lats) - lat_offset
    max_lat = max(lats) + lat_offset
    min_lon = min(lons) - lon_offset
    max_lon = max(lons) + lon_offset

    wp_df = pd.DataFrame({"wp_idx": range(len(waypoints)), "wp_lat": lats, "wp_lon": lons})  # noqa: F841

    with _lock:
        conn = get_connection()
        df = conn.execute(
            f"""
            WITH corridor_stations AS (
                SELECT label, address, municipality, province, zip_code,
                       latitude, longitude, {fuel_type}
                FROM latest_stations
                WHERE {fuel_type} IS NOT NULL AND {fuel_type} > 0
                    AND latitude IS NOT NULL AND longitude IS NOT NULL
                    AND latitude BETWEEN $1 AND $2
                    AND longitude BETWEEN $3 AND $4
            ),
            station_waypoint_distances AS (
                SELECT s.*,
                    w.wp_idx,
                    2 * 6371 * ASIN(SQRT(
                        POWER(SIN(RADIANS(s.latitude - w.wp_lat) / 2), 2) +
                        COS(RADIANS(w.wp_lat)) * COS(RADIANS(s.latitude)) *
                        POWER(SIN(RADIANS(s.longitude - w.wp_lon) / 2), 2)
                    )) AS dist_km
                FROM corridor_stations s
                CROSS JOIN wp_df w
            )
            SELECT label, address, municipality, province, zip_code,
                   latitude, longitude, {fuel_type},
                   ROUND(MIN(dist_km), 2) AS min_distance_km,
                   ARG_MIN(wp_idx, dist_km) AS closest_waypoint_idx
            FROM station_waypoint_distances
            GROUP BY label, address, municipality, province, zip_code,
                     latitude, longitude, {fuel_type}
            HAVING MIN(dist_km) <= $5
            ORDER BY min_distance_km
            """,
            [min_lat, max_lat, min_lon, max_lon, corridor_km],
        ).fetchdf()

    return df


def get_distinct_provinces() -> dict[str, str]:
    with _lock:
        conn = get_connection()
        result = conn.execute("SELECT DISTINCT province FROM latest_stations ORDER BY province").fetchdf()
    provinces = result["province"].tolist()
    return {p: p.title() for p in provinces}
