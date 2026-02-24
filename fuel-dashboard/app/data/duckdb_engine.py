import logging
from typing import List
from typing import Optional

import duckdb
import pandas as pd

from data.gcs_client import download_parquet_as_df
from data.gcs_client import download_parquets_as_df
from data.gcs_client import get_latest_parquet_file

logger = logging.getLogger(__name__)

_connection: Optional[duckdb.DuckDBPyConnection] = None


def get_connection() -> duckdb.DuckDBPyConnection:
    global _connection
    if _connection is None:
        _connection = duckdb.connect(":memory:")
    return _connection


def refresh_latest_snapshot() -> None:
    conn = get_connection()
    latest_file = get_latest_parquet_file()
    if latest_file is None:
        logger.warning("No parquet files found in GCS bucket")
        return
    logger.info(f"Refreshing latest snapshot from {latest_file}")
    df = download_parquet_as_df(latest_file)  # noqa: F841
    conn.execute("DROP TABLE IF EXISTS latest_stations")
    conn.execute("CREATE TABLE latest_stations AS SELECT * FROM df")
    count = conn.execute("SELECT COUNT(*) FROM latest_stations").fetchone()[0]
    logger.info(f"Loaded {count} stations into latest_stations table")


def query_cheapest_by_zip(zip_code: str, fuel_type: str, limit: int = 3) -> pd.DataFrame:
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


def query_nearest_stations(lat: float, lon: float, fuel_type: str, limit: int = 3) -> pd.DataFrame:
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
    if not blob_names:
        return pd.DataFrame()
    df = download_parquets_as_df(blob_names)  # noqa: F841
    conn = get_connection()
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


def get_distinct_provinces() -> List[str]:
    conn = get_connection()
    result = conn.execute("SELECT DISTINCT province FROM latest_stations ORDER BY province").fetchdf()
    return result["province"].tolist()
