from unittest.mock import patch

import duckdb
import pandas as pd
import pytest

import data.duckdb_engine as duckdb_engine_module
from data.duckdb_engine import _validate_fuel_column
from data.duckdb_engine import query_cached_zip_code_price_trend
from data.duckdb_engine import query_cheapest_by_zip
from data.duckdb_engine import query_cheapest_by_zip_group
from data.duckdb_engine import query_cheapest_zones
from data.duckdb_engine import query_national_avg_price
from data.duckdb_engine import query_nearest_stations
from data.duckdb_engine import query_nearest_stations_group
from data.duckdb_engine import query_stations_within_radius
from data.duckdb_engine import query_stations_within_radius_group
from data.duckdb_engine import refresh_zip_code_trend_snapshot


def _setup_test_table(conn):
    df = pd.DataFrame(  # noqa: F841
        {
            "label": ["station_a", "station_b", "station_c"],
            "address": ["calle a", "calle b", "calle c"],
            "municipality": ["madrid", "madrid", "barcelona"],
            "province": ["madrid", "madrid", "barcelona"],
            "zip_code": ["28001", "28001", "08001"],
            "latitude": [40.4168, 40.4200, 41.3851],
            "longitude": [-3.7038, -3.7000, 2.1734],
            "diesel_a_price": [1.45, 1.50, 1.55],
        }
    )
    conn.execute("DROP TABLE IF EXISTS latest_stations")
    conn.execute("CREATE TABLE latest_stations AS SELECT * FROM df")


def _make_zip_trend_df():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-03-29", "2026-03-30", "2026-03-31"]).date,
            "zip_code": ["28001", "28001", "08001"],
            "province": ["madrid", "madrid", "barcelona"],
            "fuel_type": ["diesel_a_price", "diesel_a_price", "diesel_a_price"],
            "avg_price": [1.45, 1.47, 1.52],
            "min_price": [1.40, 1.42, 1.49],
            "max_price": [1.50, 1.52, 1.55],
            "station_count": [5, 4, 3],
        }
    )


def _make_legacy_zip_trend_df():
    return _make_zip_trend_df().drop(columns=["province"])


@patch("data.duckdb_engine.get_connection")
def test_query_cheapest_by_zip(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_cheapest_by_zip("28001", "diesel_a_price", 3)
    assert len(result) == 2
    assert result.iloc[0]["label"] == "station_a"
    assert result.iloc[0]["diesel_a_price"] == 1.45


@patch("data.duckdb_engine.get_connection")
def test_query_cheapest_by_zip_no_results(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_cheapest_by_zip("99999", "diesel_a_price", 3)
    assert len(result) == 0


@patch("data.duckdb_engine.get_connection")
def test_query_nearest_stations(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_nearest_stations(40.4168, -3.7038, "diesel_a_price", 2)
    assert len(result) == 2
    assert "distance_km" in result.columns
    assert result.iloc[0]["distance_km"] < result.iloc[1]["distance_km"]


@patch("data.duckdb_engine.get_connection")
def test_query_stations_within_radius(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_stations_within_radius(40.4168, -3.7038, "diesel_a_price", 5.0)
    assert len(result) == 2  # Madrid stations only, Barcelona too far
    assert all(result["distance_km"] <= 5.0)


@patch("data.duckdb_engine.get_connection")
def test_query_cheapest_zones(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_cheapest_zones("madrid", "diesel_a_price")
    assert len(result) == 1
    assert result.iloc[0]["zip_code"] == "28001"
    assert result.iloc[0]["station_count"] == 2


@patch("data.duckdb_engine.get_connection")
def test_query_national_avg_price(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_national_avg_price("diesel_a_price")
    assert result is not None
    assert result == pytest.approx(1.50, abs=0.01)


def test_validate_fuel_column_valid():
    assert _validate_fuel_column("diesel_a_price") == "diesel_a_price"


def test_validate_fuel_column_invalid():
    with pytest.raises(ValueError, match="Invalid fuel column"):
        _validate_fuel_column("DROP TABLE; --")


@patch("data.duckdb_engine.get_connection")
def test_query_rejects_invalid_fuel_column(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    with pytest.raises(ValueError, match="Invalid fuel column"):
        query_cheapest_by_zip("28001", "malicious_column", 3)


@patch("data.duckdb_engine.download_aggregate")
@patch("data.duckdb_engine.get_connection")
def test_refresh_zip_code_trend_snapshot_loads_persistent_table(mock_conn, mock_download):
    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    mock_download.return_value = _make_zip_trend_df()
    duckdb_engine_module._zip_code_trend_ready.clear()

    refreshed = refresh_zip_code_trend_snapshot()

    assert refreshed is True
    count = conn.execute("SELECT COUNT(*) FROM zip_code_daily_stats").fetchone()[0]
    assert count == 3
    assert duckdb_engine_module.is_zip_code_trend_ready() is True


@patch("data.duckdb_engine.download_aggregate")
@patch("data.duckdb_engine.get_connection")
def test_refresh_zip_code_trend_snapshot_accepts_legacy_aggregate_without_province(mock_conn, mock_download):
    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    mock_download.return_value = _make_legacy_zip_trend_df()
    duckdb_engine_module._zip_code_trend_ready.clear()

    refreshed = refresh_zip_code_trend_snapshot()

    assert refreshed is True
    result = conn.execute(
        "SELECT province FROM zip_code_daily_stats WHERE zip_code = '28001' ORDER BY date ASC LIMIT 1"
    ).fetchone()
    assert result == (None,)
    assert duckdb_engine_module.is_zip_code_trend_ready() is True


@patch("data.duckdb_engine.download_aggregate")
@patch("data.duckdb_engine.get_connection")
def test_refresh_zip_code_trend_snapshot_keeps_last_good_snapshot_when_download_returns_none(mock_conn, mock_download):
    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    df = _make_zip_trend_df()  # noqa: F841
    conn.execute("CREATE TABLE zip_code_daily_stats AS SELECT * FROM df")
    duckdb_engine_module._zip_code_trend_ready.set()
    mock_download.return_value = None

    refreshed = refresh_zip_code_trend_snapshot()

    assert refreshed is False
    assert duckdb_engine_module.is_zip_code_trend_ready() is True
    count = conn.execute("SELECT COUNT(*) FROM zip_code_daily_stats").fetchone()[0]
    assert count == 3


@patch("data.duckdb_engine.download_aggregate")
@patch("data.duckdb_engine.get_connection")
def test_refresh_zip_code_trend_snapshot_keeps_last_good_snapshot_when_aggregate_is_invalid(mock_conn, mock_download):
    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    df = _make_zip_trend_df()  # noqa: F841
    conn.execute("CREATE TABLE zip_code_daily_stats AS SELECT * FROM df")
    duckdb_engine_module._zip_code_trend_ready.set()
    mock_download.return_value = pd.DataFrame({"zip_code": ["28001"]})

    refreshed = refresh_zip_code_trend_snapshot()

    assert refreshed is False
    assert duckdb_engine_module.is_zip_code_trend_ready() is True
    count = conn.execute("SELECT COUNT(*) FROM zip_code_daily_stats").fetchone()[0]
    assert count == 3


@patch("data.duckdb_engine.get_connection")
def test_query_cached_zip_code_price_trend_reads_from_persistent_table(mock_conn):
    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    df = _make_zip_trend_df()  # noqa: F841
    conn.execute("CREATE TABLE zip_code_daily_stats AS SELECT * FROM df")
    duckdb_engine_module._zip_code_trend_ready.set()

    result = query_cached_zip_code_price_trend("28001", "diesel_a_price", 90)

    assert len(result) == 2
    assert set(result.columns) == {"date", "avg_price", "min_price", "max_price"}
    assert result.iloc[0]["avg_price"] == pytest.approx(1.45, abs=0.001)


def test_query_cached_zip_code_price_trend_returns_empty_when_not_ready():
    duckdb_engine_module._zip_code_trend_ready.clear()

    result = query_cached_zip_code_price_trend("28001", "diesel_a_price", 90)

    assert result.empty


@patch("data.duckdb_engine.get_connection")
def test_query_cached_group_price_trend_returns_multiple_fuel_types(mock_conn):
    from data.duckdb_engine import query_cached_group_price_trend

    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    df = pd.DataFrame(  # noqa: F841
        {
            "date": pd.to_datetime(["2026-03-29", "2026-03-30", "2026-03-29", "2026-03-30"]).date,
            "zip_code": ["28001"] * 4,
            "province": ["madrid"] * 4,
            "fuel_type": ["diesel_a_price", "diesel_a_price", "diesel_premium_price", "diesel_premium_price"],
            "avg_price": [1.45, 1.47, 1.55, 1.57],
            "min_price": [1.40, 1.42, 1.50, 1.52],
            "max_price": [1.50, 1.52, 1.60, 1.62],
            "station_count": [5, 4, 3, 3],
        }
    )  # noqa: F841
    conn.execute("CREATE TABLE zip_code_daily_stats AS SELECT * FROM df")
    duckdb_engine_module._zip_code_trend_ready.set()

    result = query_cached_group_price_trend("28001", ["diesel_a_price", "diesel_premium_price"], 90)

    assert len(result) == 4
    assert set(result.columns) == {"date", "fuel_type", "avg_price", "min_price", "max_price"}
    assert set(result["fuel_type"].unique()) == {"diesel_a_price", "diesel_premium_price"}


def test_query_cached_group_price_trend_returns_empty_when_not_ready():
    from data.duckdb_engine import query_cached_group_price_trend

    duckdb_engine_module._zip_code_trend_ready.clear()

    result = query_cached_group_price_trend("28001", ["diesel_a_price", "diesel_premium_price"], 90)

    assert result.empty
    assert "fuel_type" in result.columns


def test_query_cached_group_price_trend_rejects_invalid_fuel_column():
    from data.duckdb_engine import query_cached_group_price_trend

    duckdb_engine_module._zip_code_trend_ready.set()

    with pytest.raises(ValueError, match="Invalid fuel column"):
        query_cached_group_price_trend("28001", ["diesel_a_price", "malicious_column"], 90)


# --- Multi-column group query tests ---


def _setup_multi_fuel_table(conn):
    df = pd.DataFrame(  # noqa: F841
        {
            "label": ["station_primary_only", "station_variant_only", "station_primary_and_variant", "station_far"],
            "address": ["calle a", "calle b", "calle c", "calle d"],
            "municipality": ["madrid", "madrid", "madrid", "barcelona"],
            "province": ["madrid", "madrid", "madrid", "barcelona"],
            "zip_code": ["28001", "28001", "28001", "08001"],
            "latitude": [40.4168, 40.4172, 40.4180, 41.3851],
            "longitude": [-3.7038, -3.7032, -3.7025, 2.1734],
            "diesel_a_price": [1.50, None, 1.60, 1.55],
            "diesel_b_price": [None, 1.20, 1.10, None],
            "diesel_premium_price": [None, None, 1.30, 1.45],
        }
    )
    conn.execute("DROP TABLE IF EXISTS latest_stations")
    conn.execute("CREATE TABLE latest_stations AS SELECT * FROM df")


@patch("data.duckdb_engine.get_connection")
def test_query_cheapest_by_zip_group_filters_by_primary(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_multi_fuel_table(conn)
    mock_conn.return_value = conn

    result = query_cheapest_by_zip_group("28001", "diesel_a_price", ["diesel_a_price", "diesel_b_price"], 3)
    # station_variant_only has no diesel_a_price, so it's excluded
    assert len(result) == 2
    assert result.iloc[0]["label"] == "station_primary_only"  # 1.50 < 1.60
    assert result.iloc[1]["label"] == "station_primary_and_variant"
    assert "diesel_a_price" in result.columns
    assert "diesel_b_price" in result.columns


@patch("data.duckdb_engine.get_connection")
def test_query_nearest_stations_group_filters_by_primary(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_multi_fuel_table(conn)
    mock_conn.return_value = conn

    result = query_nearest_stations_group(
        40.4168, -3.7038, "diesel_a_price", ["diesel_a_price", "diesel_b_price", "diesel_premium_price"], 3
    )
    # station_variant_only excluded (no diesel_a_price)
    assert len(result) == 3
    assert "station_variant_only" not in result["label"].tolist()
    assert "distance_km" in result.columns
    assert "diesel_a_price" in result.columns
    assert "diesel_premium_price" in result.columns
    assert result.iloc[0]["distance_km"] < result.iloc[1]["distance_km"]


@patch("data.duckdb_engine.get_connection")
def test_query_stations_within_radius_group_filters_by_primary(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_multi_fuel_table(conn)
    mock_conn.return_value = conn

    result = query_stations_within_radius_group(
        40.4168, -3.7038, "diesel_a_price", ["diesel_a_price", "diesel_b_price", "diesel_premium_price"], 5.0
    )
    # Only Madrid stations with diesel_a_price (station_primary_only + station_primary_and_variant)
    assert len(result) == 2
    assert "station_variant_only" not in result["label"].tolist()
    assert all(result["distance_km"] <= 5.0)
    assert "diesel_b_price" in result.columns
    assert "diesel_premium_price" in result.columns
