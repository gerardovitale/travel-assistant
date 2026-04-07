import functools
import logging
import math
import threading
import time
from typing import List
from typing import Optional

import duckdb
import pandas as pd
from api.schemas import FuelType
from config import settings

from data.gcs_client import download_aggregate
from data.gcs_client import download_parquet_as_df
from data.gcs_client import download_parquets_as_df
from data.gcs_client import get_latest_parquet_file

logger = logging.getLogger(__name__)

_VALID_FUEL_COLUMNS: frozenset[str] = frozenset(ft.value for ft in FuelType)


def _validate_fuel_column(fuel_type: str) -> str:
    if fuel_type not in _VALID_FUEL_COLUMNS:
        raise ValueError(f"Invalid fuel column: {fuel_type!r}")
    return fuel_type


def _label_filter_clause(labels: Optional[List[str]], next_param_idx: int) -> tuple[str, list]:
    if not labels:
        return "", []
    return f"AND label = ANY(${next_param_idx}::VARCHAR[])", [labels]


_connection: Optional[duckdb.DuckDBPyConnection] = None
_lock = threading.Lock()
_zip_code_trend_ready = threading.Event()
_last_successful_trend_refresh: Optional[float] = None

ZIP_CODE_TREND_TABLE = "zip_code_daily_stats"
ZIP_CODE_TREND_COLUMNS = (
    "date",
    "zip_code",
    "province",
    "fuel_type",
    "avg_price",
    "min_price",
    "max_price",
    "station_count",
)


def _normalize_zip_code_trend_aggregate(aggregate_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if aggregate_df is None:
        return None

    if aggregate_df.empty:
        return aggregate_df

    if "province" not in aggregate_df.columns:
        legacy_columns = set(ZIP_CODE_TREND_COLUMNS) - {"province"}
        if legacy_columns.issubset(aggregate_df.columns):
            normalized_df = aggregate_df.copy()
            normalized_df["province"] = None
            logger.info("Loaded legacy zip-code trend aggregate without province column; filling province with nulls")
            return normalized_df

    return aggregate_df


def get_connection() -> duckdb.DuckDBPyConnection:
    global _connection
    if _connection is None:
        _connection = duckdb.connect(
            ":memory:",
            config={
                "threads": settings.duckdb_threads,
                "memory_limit": settings.duckdb_memory_limit,
            },
        )
    return _connection


def replace_latest_stations(df: pd.DataFrame) -> int:
    """Atomically replace the latest_stations table with the given DataFrame.

    Returns the number of rows loaded.
    """
    with _lock:
        conn = get_connection()
        conn.execute("DROP TABLE IF EXISTS latest_stations")
        conn.execute("CREATE TABLE latest_stations AS SELECT * FROM df")
        count = conn.execute("SELECT COUNT(*) FROM latest_stations").fetchone()[0]
    query_national_avg_price.cache_clear()
    return count


def refresh_latest_snapshot() -> None:
    latest_file = get_latest_parquet_file()
    if latest_file is None:
        logger.warning("No parquet files found in GCS bucket")
        return
    logger.info(f"Refreshing latest snapshot from {latest_file}")
    df = download_parquet_as_df(latest_file)
    count = replace_latest_stations(df)
    logger.info(f"Loaded {count} stations into latest_stations table")


def is_zip_code_trend_ready() -> bool:
    return _zip_code_trend_ready.is_set()


def _log_trend_staleness():
    if _last_successful_trend_refresh is not None:
        stale_seconds = time.time() - _last_successful_trend_refresh
        logger.warning("Last successful trend refresh was %.0f s ago", stale_seconds)


def refresh_zip_code_trend_snapshot() -> bool:
    global _last_successful_trend_refresh

    had_snapshot = _zip_code_trend_ready.is_set()
    started = time.perf_counter()
    aggregate_df = _normalize_zip_code_trend_aggregate(download_aggregate("zip_code_daily_stats.parquet"))
    if aggregate_df is None:
        if had_snapshot:
            logger.warning("Zip-code trend aggregate unavailable; keeping last good trend snapshot")
            _log_trend_staleness()
        else:
            _zip_code_trend_ready.clear()
            logger.warning("Zip-code trend aggregate unavailable; raw-history fallback will be used")
        return False

    expected_columns = set(ZIP_CODE_TREND_COLUMNS)
    if aggregate_df.empty or not expected_columns.issubset(aggregate_df.columns):
        if had_snapshot:
            logger.warning(
                "Zip-code trend aggregate invalid; keeping last good trend snapshot (%s)",
                sorted(aggregate_df.columns.tolist()),
            )
            _log_trend_staleness()
        else:
            _zip_code_trend_ready.clear()
            logger.warning(
                "Zip-code trend aggregate invalid; raw-history fallback will be used (%s)",
                sorted(aggregate_df.columns.tolist()),
            )
        return False

    df = aggregate_df  # noqa: F841
    with _lock:
        conn = get_connection()
        conn.execute(f"CREATE OR REPLACE TABLE {ZIP_CODE_TREND_TABLE} AS SELECT * FROM df")
        row_count = conn.execute(f"SELECT COUNT(*) FROM {ZIP_CODE_TREND_TABLE}").fetchone()[0]
    _zip_code_trend_ready.set()
    _last_successful_trend_refresh = time.time()
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("Refreshed zip-code trend snapshot into DuckDB (%s rows, %.1f ms)", row_count, duration_ms)
    return True


def query_cheapest_by_zip(
    zip_code: str, fuel_type: str, limit: int = 5, labels: Optional[List[str]] = None
) -> pd.DataFrame:
    fuel_type = _validate_fuel_column(fuel_type)
    label_clause, label_params = _label_filter_clause(labels, 3)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT label, address, municipality, province, zip_code, latitude, longitude, {fuel_type}
            FROM latest_stations
            WHERE zip_code = $1 AND {fuel_type} IS NOT NULL AND {fuel_type} > 0
                {label_clause}
            ORDER BY {fuel_type} ASC
            LIMIT $2
            """,
            [zip_code, limit] + label_params,
        ).fetchdf()


def query_stations_within_radius(
    lat: float, lon: float, fuel_type: str, radius_km: float, labels: Optional[List[str]] = None
) -> pd.DataFrame:
    fuel_type = _validate_fuel_column(fuel_type)
    label_clause, label_params = _label_filter_clause(labels, 4)
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
                    {label_clause}
            ) WHERE distance_km <= $3
            ORDER BY distance_km ASC
            """,
            [lat, lon, radius_km] + label_params,
        ).fetchdf()


def query_nearest_stations(
    lat: float, lon: float, fuel_type: str, limit: int = 5, labels: Optional[List[str]] = None
) -> pd.DataFrame:
    fuel_type = _validate_fuel_column(fuel_type)
    label_clause, label_params = _label_filter_clause(labels, 4)
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
                {label_clause}
            ORDER BY distance_km ASC
            LIMIT $3
            """,
            [lat, lon, limit] + label_params,
        ).fetchdf()


def _validate_fuel_columns(fuel_types: List[str]) -> List[str]:
    if not fuel_types:
        raise ValueError("At least one fuel column is required")
    for ft in fuel_types:
        _validate_fuel_column(ft)
    return fuel_types


def _primary_availability_predicate(primary_fuel: str) -> str:
    return f"{primary_fuel} IS NOT NULL AND {primary_fuel} > 0"


def query_cheapest_by_zip_group(
    zip_code: str, primary_fuel: str, all_fuels: List[str], limit: int = 5, labels: Optional[List[str]] = None
) -> pd.DataFrame:
    primary_fuel = _validate_fuel_column(primary_fuel)
    fuel_types = _validate_fuel_columns(all_fuels)
    fuel_cols = ", ".join(fuel_types)
    predicate = _primary_availability_predicate(primary_fuel)
    label_clause, label_params = _label_filter_clause(labels, 3)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT label, address, municipality, province, zip_code, latitude, longitude, {fuel_cols}
            FROM latest_stations
            WHERE zip_code = $1 AND {predicate}
                {label_clause}
            ORDER BY {primary_fuel} ASC
            LIMIT $2
            """,
            [zip_code, limit] + label_params,
        ).fetchdf()


def query_nearest_stations_group(
    lat: float, lon: float, primary_fuel: str, all_fuels: List[str], limit: int = 5, labels: Optional[List[str]] = None
) -> pd.DataFrame:
    primary_fuel = _validate_fuel_column(primary_fuel)
    fuel_types = _validate_fuel_columns(all_fuels)
    fuel_cols = ", ".join(fuel_types)
    predicate = _primary_availability_predicate(primary_fuel)
    label_clause, label_params = _label_filter_clause(labels, 4)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT label, address, municipality, province, zip_code, latitude, longitude, {fuel_cols},
                2 * 6371 * ASIN(SQRT(
                    POWER(SIN(RADIANS(latitude - $1) / 2), 2) +
                    COS(RADIANS($1)) * COS(RADIANS(latitude)) *
                    POWER(SIN(RADIANS(longitude - $2) / 2), 2)
                )) AS distance_km
            FROM latest_stations
            WHERE {predicate}
                AND latitude IS NOT NULL AND longitude IS NOT NULL
                {label_clause}
            ORDER BY distance_km ASC
            LIMIT $3
            """,
            [lat, lon, limit] + label_params,
        ).fetchdf()


def query_stations_within_radius_group(
    lat: float,
    lon: float,
    primary_fuel: str,
    all_fuels: List[str],
    radius_km: float,
    labels: Optional[List[str]] = None,
) -> pd.DataFrame:
    primary_fuel = _validate_fuel_column(primary_fuel)
    fuel_types = _validate_fuel_columns(all_fuels)
    fuel_cols = ", ".join(fuel_types)
    predicate = _primary_availability_predicate(primary_fuel)
    label_clause, label_params = _label_filter_clause(labels, 4)
    with _lock:
        conn = get_connection()
        return conn.execute(
            f"""
            SELECT * FROM (
                SELECT label, address, municipality, province, zip_code, latitude, longitude, {fuel_cols},
                    2 * 6371 * ASIN(SQRT(
                        POWER(SIN(RADIANS(latitude - $1) / 2), 2) +
                        COS(RADIANS($1)) * COS(RADIANS(latitude)) *
                        POWER(SIN(RADIANS(longitude - $2) / 2), 2)
                    )) AS distance_km
                FROM latest_stations
                WHERE {predicate}
                    AND latitude IS NOT NULL AND longitude IS NOT NULL
                    {label_clause}
            ) WHERE distance_km <= $3
            ORDER BY distance_km ASC
            """,
            [lat, lon, radius_km] + label_params,
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
    with _lock:
        conn = get_connection()
        conn.execute("DROP TABLE IF EXISTS _trend_data")
        conn.execute("CREATE TEMP TABLE _trend_data AS SELECT * FROM df")
        try:
            result = conn.execute(
                f"""
                SELECT
                    CAST(timestamp AS DATE) AS date,
                    AVG({fuel_type}) AS avg_price,
                    MIN({fuel_type}) AS min_price,
                    MAX({fuel_type}) AS max_price
                FROM _trend_data
                WHERE zip_code = $1 AND {fuel_type} IS NOT NULL AND {fuel_type} > 0
                GROUP BY CAST(timestamp AS DATE)
                ORDER BY date ASC
                """,
                [zip_code],
            ).fetchdf()
        finally:
            conn.execute("DROP TABLE IF EXISTS _trend_data")
    return result


def query_zip_code_price_trend(
    aggregate_df: pd.DataFrame, zip_code: str, fuel_type: str, days_back: int
) -> pd.DataFrame:
    """Query daily price trend for a zip code from the pre-computed zip aggregate."""
    fuel_type = _validate_fuel_column(fuel_type)
    if aggregate_df is None or aggregate_df.empty:
        return pd.DataFrame(columns=["date", "avg_price", "min_price", "max_price"])
    df = aggregate_df  # noqa: F841
    with _lock:
        conn = get_connection()
        conn.execute("DROP TABLE IF EXISTS _zip_trend_agg")
        conn.execute("CREATE TEMP TABLE _zip_trend_agg AS SELECT * FROM df")
        try:
            result = conn.execute(
                """
                SELECT date, avg_price, min_price, max_price
                FROM _zip_trend_agg
                WHERE zip_code = $1
                    AND fuel_type = $2
                    AND date >= CURRENT_DATE - INTERVAL ($3) DAY
                ORDER BY date ASC
                """,
                [zip_code, fuel_type, days_back],
            ).fetchdf()
        finally:
            conn.execute("DROP TABLE IF EXISTS _zip_trend_agg")
    return result


def query_cached_zip_code_price_trend(zip_code: str, fuel_type: str, days_back: int) -> pd.DataFrame:
    """Query daily price trend for a zip code from the local DuckDB trend cache."""
    fuel_type = _validate_fuel_column(fuel_type)
    if not _zip_code_trend_ready.is_set():
        return pd.DataFrame(columns=["date", "avg_price", "min_price", "max_price"])

    started = time.perf_counter()
    with _lock:
        conn = get_connection()
        result = conn.execute(
            f"""
            SELECT date, avg_price, min_price, max_price
            FROM {ZIP_CODE_TREND_TABLE}
            WHERE zip_code = $1
                AND fuel_type = $2
                AND date >= CURRENT_DATE - INTERVAL ($3) DAY
            ORDER BY date ASC
            """,
            [zip_code, fuel_type, days_back],
        ).fetchdf()
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info(
        "Served zip-code trend query from DuckDB cache (%s/%s, %s days, %s rows, %.1f ms)",
        zip_code,
        fuel_type,
        days_back,
        len(result),
        duration_ms,
    )
    return result


def query_cached_group_price_trend(zip_code: str, fuel_types: List[str], days_back: int) -> pd.DataFrame:
    """Query daily price trend for multiple fuel types in a zip code from the DuckDB trend cache."""
    for ft in fuel_types:
        _validate_fuel_column(ft)
    if not _zip_code_trend_ready.is_set():
        return pd.DataFrame(columns=["date", "fuel_type", "avg_price", "min_price", "max_price"])

    started = time.perf_counter()
    with _lock:
        conn = get_connection()
        result = conn.execute(
            f"""
            SELECT date, fuel_type, avg_price, min_price, max_price
            FROM {ZIP_CODE_TREND_TABLE}
            WHERE zip_code = $1
                AND fuel_type = ANY($2)
                AND date >= CURRENT_DATE - INTERVAL ($3) DAY
            ORDER BY fuel_type, date ASC
            """,
            [zip_code, fuel_types, days_back],
        ).fetchdf()
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info(
        "Served group trend query from DuckDB cache (%s/%s, %s days, %s rows, %.1f ms)",
        zip_code,
        fuel_types,
        days_back,
        len(result),
        duration_ms,
    )
    return result


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
    labels: Optional[List[str]] = None,
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
    label_clause, label_params = _label_filter_clause(labels, 6)
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
                    {label_clause}
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
            [min_lat, max_lat, min_lon, max_lon, corridor_km] + label_params,
        ).fetchdf()

    return df


@functools.lru_cache(maxsize=16)
def query_national_avg_price(fuel_type: str) -> Optional[float]:
    """Return the national average price for a fuel type, or None if unavailable.

    Cached per fuel type; cache is cleared on ``refresh_latest_snapshot()``.
    """
    fuel_type = _validate_fuel_column(fuel_type)
    with _lock:
        conn = get_connection()
        result = conn.execute(
            f"""
            SELECT AVG({fuel_type}) AS avg_price
            FROM latest_stations
            WHERE {fuel_type} IS NOT NULL AND {fuel_type} > 0
            """,
        ).fetchone()
    return result[0] if result and result[0] is not None else None


def get_distinct_provinces() -> dict[str, str]:
    with _lock:
        conn = get_connection()
        result = conn.execute("SELECT DISTINCT province FROM latest_stations ORDER BY province").fetchdf()
    provinces = result["province"].tolist()
    return {p: p.title() for p in provinces}


def get_distinct_labels(top_n: int = 0) -> dict[str, str]:
    """Return distinct station labels as {raw: Title Case} ordered by station count (most popular first).

    If top_n > 0, only return the top N most popular labels.
    """
    limit_clause = f"LIMIT {int(top_n)}" if top_n > 0 else ""
    with _lock:
        conn = get_connection()
        result = conn.execute(
            f"""
            SELECT label, COUNT(*) AS cnt
            FROM latest_stations
            WHERE label IS NOT NULL AND label != ''
            GROUP BY label
            ORDER BY cnt DESC, label ASC
            {limit_clause}
            """
        ).fetchdf()
    labels = result["label"].tolist()
    return {label: label.title() for label in labels}


def query_province_ranking(aggregate_df: pd.DataFrame, fuel_type: str, days_back: int) -> pd.DataFrame:
    """Query province ranking from pre-computed province_daily_stats aggregate."""
    fuel_type = _validate_fuel_column(fuel_type)
    df = aggregate_df  # noqa: F841
    with _lock:
        conn = get_connection()
        conn.execute("DROP TABLE IF EXISTS _province_agg")
        conn.execute("CREATE TEMP TABLE _province_agg AS SELECT * FROM df")
        try:
            result = conn.execute(
                """
                SELECT province,
                    AVG(avg_price) AS avg_price,
                    MIN(min_price) AS min_price,
                    MAX(max_price) AS max_price,
                    CAST(SUM(station_count) AS INTEGER) AS total_observations
                FROM _province_agg
                WHERE fuel_type = $1
                    AND date >= CURRENT_DATE - INTERVAL ($2) DAY
                GROUP BY province
                ORDER BY avg_price ASC
                """,
                [fuel_type, days_back],
            ).fetchdf()
        finally:
            conn.execute("DROP TABLE IF EXISTS _province_agg")
    return result


def query_day_of_week_pattern(
    aggregate_df: pd.DataFrame,
    fuel_type: str,
    province: Optional[str] = None,
    exclude_provinces: Optional[set] = None,
) -> pd.DataFrame:
    """Query day-of-week price patterns from pre-computed day_of_week_stats aggregate."""
    fuel_type = _validate_fuel_column(fuel_type)
    df = aggregate_df  # noqa: F841

    # When a specific province is selected, use it directly
    if province:
        province_filter = province
        use_aggregate = True
    # When excluding provinces (mainland only), aggregate per-province rows
    elif exclude_provinces:
        use_aggregate = False
    else:
        province_filter = "__national__"
        use_aggregate = True

    with _lock:
        conn = get_connection()
        conn.execute("DROP TABLE IF EXISTS _dow_agg")
        conn.execute("CREATE TEMP TABLE _dow_agg AS SELECT * FROM df")
        try:
            if use_aggregate:
                result = conn.execute(
                    """
                    SELECT day_of_week,
                        sum_price / count_days AS avg_price,
                        count_days,
                        min_daily_avg,
                        max_daily_avg
                    FROM _dow_agg
                    WHERE fuel_type = $1 AND province = $2
                    ORDER BY day_of_week ASC
                    """,
                    [fuel_type, province_filter],
                ).fetchdf()
            else:
                exclude_list = list(exclude_provinces | {"__national__"})
                result = conn.execute(
                    """
                    SELECT day_of_week,
                        SUM(sum_price) / SUM(count_days) AS avg_price,
                        MAX(count_days) AS count_days,
                        MIN(min_daily_avg) AS min_daily_avg,
                        MAX(max_daily_avg) AS max_daily_avg
                    FROM _dow_agg
                    WHERE fuel_type = $1 AND province NOT IN (SELECT UNNEST($2::VARCHAR[]))
                    GROUP BY day_of_week
                    ORDER BY day_of_week ASC
                    """,
                    [fuel_type, exclude_list],
                ).fetchdf()
        finally:
            conn.execute("DROP TABLE IF EXISTS _dow_agg")
    return result


def query_brand_ranking(aggregate_df: pd.DataFrame, fuel_type: str, days_back: int, top_n: int = 15) -> pd.DataFrame:
    """Query brand ranking from pre-computed brand_daily_stats aggregate."""
    fuel_type = _validate_fuel_column(fuel_type)
    df = aggregate_df  # noqa: F841
    with _lock:
        conn = get_connection()
        conn.execute("DROP TABLE IF EXISTS _brand_agg")
        conn.execute("CREATE TEMP TABLE _brand_agg AS SELECT * FROM df")
        try:
            result = conn.execute(
                """
                SELECT brand,
                    SUM(avg_price * station_count) / NULLIF(SUM(station_count), 0) AS avg_price,
                    MIN(min_price) AS min_price,
                    MAX(max_price) AS max_price,
                    CAST(SUM(station_count) AS INTEGER) AS total_observations
                FROM _brand_agg
                WHERE fuel_type = $1
                    AND date >= CURRENT_DATE - INTERVAL ($2) DAY
                GROUP BY brand
                ORDER BY avg_price ASC
                LIMIT $3
                """,
                [fuel_type, days_back, top_n],
            ).fetchdf()
        finally:
            conn.execute("DROP TABLE IF EXISTS _brand_agg")
    return result


def query_brand_price_trend(aggregate_df: pd.DataFrame, fuel_type: str, days_back: int, brands: list) -> pd.DataFrame:
    """Query daily price trend for specific brands from brand_daily_stats aggregate."""
    fuel_type = _validate_fuel_column(fuel_type)
    if not brands:
        return pd.DataFrame(columns=["date", "brand", "avg_price"])
    df = aggregate_df  # noqa: F841
    with _lock:
        conn = get_connection()
        conn.execute("DROP TABLE IF EXISTS _brand_trend_agg")
        conn.execute("CREATE TEMP TABLE _brand_trend_agg AS SELECT * FROM df")
        try:
            result = conn.execute(
                """
                SELECT date, brand, avg_price
                FROM _brand_trend_agg
                WHERE fuel_type = $1
                    AND date >= CURRENT_DATE - INTERVAL ($2) DAY
                    AND brand IN (SELECT UNNEST($3::VARCHAR[]))
                ORDER BY date ASC, brand ASC
                """,
                [fuel_type, days_back, brands],
            ).fetchdf()
        finally:
            conn.execute("DROP TABLE IF EXISTS _brand_trend_agg")
    return result


def query_volatility_by_zone(
    aggregate_df: pd.DataFrame,
    fuel_type: str,
    days_back: int,
    mainland_only: bool = True,
) -> pd.DataFrame:
    """Query ZIP-code price volatility from the pre-computed zip_code_daily_stats aggregate."""
    fuel_type = _validate_fuel_column(fuel_type)
    expected_columns = {
        "date",
        "zip_code",
        "province",
        "fuel_type",
        "avg_price",
        "min_price",
        "max_price",
        "station_count",
    }
    if aggregate_df is None or aggregate_df.empty or not expected_columns.issubset(aggregate_df.columns):
        return pd.DataFrame(
            columns=[
                "zip_code",
                "province",
                "avg_price",
                "std_dev_price",
                "coefficient_of_variation",
                "min_price",
                "max_price",
                "price_range",
                "observation_days",
                "avg_station_count",
            ]
        )

    min_observation_days = max(2, math.ceil(days_back * 0.7))
    df = aggregate_df  # noqa: F841
    with _lock:
        conn = get_connection()
        conn.execute("DROP TABLE IF EXISTS _zip_volatility_agg")
        conn.execute("CREATE TEMP TABLE _zip_volatility_agg AS SELECT * FROM df")
        try:
            if mainland_only:
                from data.geojson_loader import _NON_MAINLAND_DATA_NAMES

                result = conn.execute(
                    """
                    SELECT
                        zip_code,
                        province,
                        AVG(avg_price) AS avg_price,
                        STDDEV_SAMP(avg_price) AS std_dev_price,
                        STDDEV_SAMP(avg_price) / NULLIF(AVG(avg_price), 0) AS coefficient_of_variation,
                        MIN(avg_price) AS min_price,
                        MAX(avg_price) AS max_price,
                        MAX(avg_price) - MIN(avg_price) AS price_range,
                        CAST(COUNT(*) AS INTEGER) AS observation_days,
                        AVG(station_count) AS avg_station_count
                    FROM _zip_volatility_agg
                    WHERE fuel_type = $1
                        AND date >= CURRENT_DATE - INTERVAL ($2) DAY
                        AND province NOT IN (SELECT UNNEST($3::VARCHAR[]))
                    GROUP BY zip_code, province
                    HAVING COUNT(*) >= $4
                        AND AVG(station_count) >= 3
                    ORDER BY coefficient_of_variation ASC, std_dev_price ASC, avg_price ASC
                    """,
                    [fuel_type, days_back, list(_NON_MAINLAND_DATA_NAMES), min_observation_days],
                ).fetchdf()
            else:
                result = conn.execute(
                    """
                    SELECT
                        zip_code,
                        province,
                        AVG(avg_price) AS avg_price,
                        STDDEV_SAMP(avg_price) AS std_dev_price,
                        STDDEV_SAMP(avg_price) / NULLIF(AVG(avg_price), 0) AS coefficient_of_variation,
                        MIN(avg_price) AS min_price,
                        MAX(avg_price) AS max_price,
                        MAX(avg_price) - MIN(avg_price) AS price_range,
                        CAST(COUNT(*) AS INTEGER) AS observation_days,
                        AVG(station_count) AS avg_station_count
                    FROM _zip_volatility_agg
                    WHERE fuel_type = $1
                        AND date >= CURRENT_DATE - INTERVAL ($2) DAY
                    GROUP BY zip_code, province
                    HAVING COUNT(*) >= $3
                        AND AVG(station_count) >= 3
                    ORDER BY coefficient_of_variation ASC, std_dev_price ASC, avg_price ASC
                    """,
                    [fuel_type, days_back, min_observation_days],
                ).fetchdf()
        finally:
            conn.execute("DROP TABLE IF EXISTS _zip_volatility_agg")
    return result
