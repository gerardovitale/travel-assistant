import math
from datetime import date
from datetime import timedelta
from unittest.mock import patch

import duckdb
import numpy as np
import pandas as pd
import pytest

import data.duckdb_engine as duckdb_engine_module
from data.duckdb_engine import _bbox_half_widths
from data.duckdb_engine import _validate_fuel_column
from data.duckdb_engine import filter_public_stations
from data.duckdb_engine import get_distinct_labels
from data.duckdb_engine import get_latest_data_timestamp
from data.duckdb_engine import query_cached_zip_code_price_trend
from data.duckdb_engine import query_cheapest_by_zip
from data.duckdb_engine import query_cheapest_by_zip_group
from data.duckdb_engine import query_cheapest_zones
from data.duckdb_engine import query_national_avg_price
from data.duckdb_engine import query_nearest_stations
from data.duckdb_engine import query_nearest_stations_group
from data.duckdb_engine import query_stations_along_corridor
from data.duckdb_engine import query_stations_within_radius
from data.duckdb_engine import query_stations_within_radius_group
from data.duckdb_engine import refresh_latest_snapshot
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


def _recent_dates(count: int) -> list[date]:
    """`count` consecutive dates ending yesterday, oldest first.

    The zip-code trend queries filter on `date >= CURRENT_DATE - INTERVAL (days_back) DAY`, so fixture
    dates pinned to literals silently fall out of the window as wall-clock advances — the tests
    pass when written and start returning zero rows months later.
    """
    today = date.today()
    return [today - timedelta(days=count - offset) for offset in range(count)]


def _make_zip_trend_df():
    return pd.DataFrame(
        {
            "date": _recent_dates(3),
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
            "date": _recent_dates(2) * 2,
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


# --- Label filter tests ---


@patch("data.duckdb_engine.get_connection")
def test_get_distinct_labels(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = get_distinct_labels()
    assert isinstance(result, dict)
    assert len(result) == 3
    assert result["station_a"] == "Station_A"
    assert result["station_b"] == "Station_B"
    assert result["station_c"] == "Station_C"


@patch("data.duckdb_engine.get_connection")
def test_get_distinct_labels_top_n(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = get_distinct_labels(top_n=2)
    assert isinstance(result, dict)
    assert len(result) == 2


@patch("data.duckdb_engine.get_connection")
def test_query_cheapest_by_zip_with_labels(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_cheapest_by_zip("28001", "diesel_a_price", 3, labels=["station_a"])
    assert len(result) == 1
    assert result.iloc[0]["label"] == "station_a"


@patch("data.duckdb_engine.get_connection")
def test_query_cheapest_by_zip_with_no_labels_returns_all(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_cheapest_by_zip("28001", "diesel_a_price", 3, labels=None)
    assert len(result) == 2


@patch("data.duckdb_engine.get_connection")
def test_query_cheapest_by_zip_with_empty_labels_returns_all(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_cheapest_by_zip("28001", "diesel_a_price", 3, labels=[])
    assert len(result) == 2


@patch("data.duckdb_engine.get_connection")
def test_query_nearest_stations_with_labels(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_nearest_stations(40.4168, -3.7038, "diesel_a_price", 5, labels=["station_a"])
    assert len(result) == 1
    assert result.iloc[0]["label"] == "station_a"


@patch("data.duckdb_engine.get_connection")
def test_query_stations_within_radius_with_labels(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_stations_within_radius(40.4168, -3.7038, "diesel_a_price", 5.0, labels=["station_b"])
    assert len(result) == 1
    assert result.iloc[0]["label"] == "station_b"


@patch("data.duckdb_engine.get_connection")
def test_query_cheapest_by_zip_group_with_labels(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_multi_fuel_table(conn)
    mock_conn.return_value = conn

    result = query_cheapest_by_zip_group(
        "28001", "diesel_a_price", ["diesel_a_price", "diesel_b_price"], 3, labels=["station_primary_only"]
    )
    assert len(result) == 1
    assert result.iloc[0]["label"] == "station_primary_only"


# --- Corridor query label filter tests ---


@patch("data.duckdb_engine.get_connection")
def test_query_stations_along_corridor_with_labels(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    waypoints = [(40.4168, -3.7038, 0.0), (40.4200, -3.7000, 0.5)]
    result = query_stations_along_corridor(waypoints, "diesel_a_price", 50.0, labels=["station_a"])
    assert len(result) >= 1
    assert all(result["label"] == "station_a")


@patch("data.duckdb_engine.get_connection")
def test_query_stations_along_corridor_with_empty_labels(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    waypoints = [(40.4168, -3.7038, 0.0), (40.4200, -3.7000, 0.5)]
    result = query_stations_along_corridor(waypoints, "diesel_a_price", 50.0, labels=[])
    assert len(result) == 2  # station_a and station_b (both in Madrid area)


@patch("data.duckdb_engine.get_connection")
def test_query_stations_along_corridor_with_none_labels(mock_conn):
    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    waypoints = [(40.4168, -3.7038, 0.0), (40.4200, -3.7000, 0.5)]
    result = query_stations_along_corridor(waypoints, "diesel_a_price", 50.0, labels=None)
    assert len(result) == 2  # station_a and station_b (both in Madrid area)


@patch("data.duckdb_engine.get_connection")
def test_get_latest_data_timestamp_returns_max_timestamp(mock_conn):
    conn = duckdb.connect(":memory:")
    df = pd.DataFrame(  # noqa: F841
        {
            "timestamp": [
                "2026-04-16T10:00:00+00:00",
                "2026-04-16T21:56:13+00:00",
                "2026-04-16T08:00:00+00:00",
            ]
        }
    )
    conn.execute("DROP TABLE IF EXISTS latest_stations")
    conn.execute("CREATE TABLE latest_stations AS SELECT * FROM df")
    mock_conn.return_value = conn

    result = get_latest_data_timestamp()
    assert result == "2026-04-16T21:56:13+00:00"


@patch("data.duckdb_engine.get_connection")
def test_get_latest_data_timestamp_returns_none_when_table_missing(mock_conn):
    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn

    result = get_latest_data_timestamp()
    assert result is None


# --- query_national_price_trend province filter tests ---


@patch("data.duckdb_engine.get_connection")
def test_query_national_price_trend_with_province_filters_rows(mock_conn):
    from data.duckdb_engine import query_national_price_trend

    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    df = _make_zip_trend_df()  # noqa: F841
    conn.execute("CREATE TABLE zip_code_daily_stats AS SELECT * FROM df")
    duckdb_engine_module._zip_code_trend_ready.set()

    result = query_national_price_trend("diesel_a_price", 90, province="madrid")

    assert len(result) == 2
    assert set(result.columns) == {"date", "avg_price", "min_price", "max_price"}


@patch("data.duckdb_engine.get_connection")
def test_query_national_price_trend_without_province_returns_all(mock_conn):
    from data.duckdb_engine import query_national_price_trend

    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    df = _make_zip_trend_df()  # noqa: F841
    conn.execute("CREATE TABLE zip_code_daily_stats AS SELECT * FROM df")
    duckdb_engine_module._zip_code_trend_ready.set()

    result = query_national_price_trend("diesel_a_price", 90)

    assert len(result) == 3


@patch("data.duckdb_engine.get_connection")
def test_query_national_price_trend_window_ends_on_the_latest_data_day(mock_conn):
    """A late ingestion must not shorten the window: 30 days back from the last day with data."""
    from data.duckdb_engine import query_latest_trend_date
    from data.duckdb_engine import query_national_price_trend

    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    last = date.today() - timedelta(days=10)
    days = [last - timedelta(days=offset) for offset in range(40)]
    df = pd.DataFrame(  # noqa: F841
        {
            "date": days,
            "zip_code": ["28001"] * 40,
            "province": ["madrid"] * 40,
            "fuel_type": ["diesel_a_price"] * 40,
            "avg_price": [1.5] * 40,
            "min_price": [1.4] * 40,
            "max_price": [1.6] * 40,
            "station_count": [3] * 40,
        }
    )
    conn.execute("CREATE TABLE zip_code_daily_stats AS SELECT * FROM df")
    duckdb_engine_module._zip_code_trend_ready.set()

    result = query_national_price_trend("diesel_a_price", 30)

    assert query_latest_trend_date() == last
    assert len(result) == 30
    assert result["date"].iloc[0].date() == last - timedelta(days=29)
    assert result["date"].iloc[-1].date() == last


def test_query_latest_trend_date_is_none_before_the_trend_cache_loads():
    from data.duckdb_engine import query_latest_trend_date

    duckdb_engine_module._zip_code_trend_ready.clear()
    assert query_latest_trend_date() is None


# --- region (peninsula / islands, Ceuta and Melilla) filter tests ---


def _make_region_trend_df():
    return pd.DataFrame(
        {
            "date": _recent_dates(1) * 4,
            "zip_code": ["28001", "35001", "07001", "51001"],
            "province": ["madrid", "palmas (las)", "balears (illes)", "ceuta"],
            "fuel_type": ["diesel_a_price"] * 4,
            "avg_price": [1.50, 1.30, 1.56, 1.20],
            "min_price": [1.45, 1.25, 1.50, 1.15],
            "max_price": [1.55, 1.35, 1.60, 1.25],
            "station_count": [5, 4, 3, 2],
        }
    )


@pytest.mark.parametrize(
    ("region", "expected_avg"),
    [("all", (1.50 + 1.30 + 1.56 + 1.20) / 4), ("peninsula", 1.50), ("special", (1.30 + 1.56 + 1.20) / 3)],
)
@patch("data.duckdb_engine.get_connection")
def test_query_national_price_trend_filters_by_region(mock_conn, region, expected_avg):
    from data.duckdb_engine import query_national_price_trend

    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    df = _make_region_trend_df()  # noqa: F841
    conn.execute("CREATE TABLE zip_code_daily_stats AS SELECT * FROM df")
    duckdb_engine_module._zip_code_trend_ready.set()

    result = query_national_price_trend("diesel_a_price", 90, region=region)

    assert result["avg_price"].iloc[0] == pytest.approx(expected_avg)


@patch("data.duckdb_engine.get_connection")
def test_query_national_price_trend_province_wins_over_region(mock_conn):
    from data.duckdb_engine import query_national_price_trend

    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    df = _make_region_trend_df()  # noqa: F841
    conn.execute("CREATE TABLE zip_code_daily_stats AS SELECT * FROM df")
    duckdb_engine_module._zip_code_trend_ready.set()

    result = query_national_price_trend("diesel_a_price", 90, province="palmas (las)", region="peninsula")

    assert result["avg_price"].iloc[0] == pytest.approx(1.30)


def test_query_national_price_trend_rejects_unknown_region():
    from data.duckdb_engine import _region_clause

    with pytest.raises(ValueError):
        _region_clause("atlantis", 3)


@patch("data.duckdb_engine.get_connection")
def test_query_national_avg_stats_by_region_matches_raw_uppercase_provinces(mock_conn):
    from data.duckdb_engine import query_national_avg_stats

    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    df = pd.DataFrame(  # noqa: F841
        {"province": ["MADRID", "SANTA CRUZ DE TENERIFE", "MELILLA"], "diesel_a_price": [1.50, 1.30, 1.10]}
    )
    conn.execute("DROP TABLE IF EXISTS latest_stations")
    conn.execute("CREATE TABLE latest_stations AS SELECT * FROM df")
    query_national_avg_stats.cache_clear()
    try:
        assert query_national_avg_stats("diesel_a_price", "peninsula") == (pytest.approx(1.50), 1)
        assert query_national_avg_stats("diesel_a_price", "special") == (pytest.approx(1.20), 2)
        assert query_national_avg_stats("diesel_a_price") == (pytest.approx(1.30), 3)
    finally:
        query_national_avg_stats.cache_clear()


# --- query_national_group_price_trend province filter tests ---


@patch("data.duckdb_engine.get_connection")
def test_query_national_group_price_trend_with_province_filters_rows(mock_conn):
    from data.duckdb_engine import query_national_group_price_trend

    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    df = pd.DataFrame(  # noqa: F841
        {
            "date": _recent_dates(2) * 2,
            "zip_code": ["28001", "28001", "08001", "08001"],
            "province": ["madrid", "madrid", "barcelona", "barcelona"],
            "fuel_type": ["diesel_a_price"] * 4,
            "avg_price": [1.45, 1.47, 1.52, 1.54],
            "min_price": [1.40, 1.42, 1.49, 1.51],
            "max_price": [1.50, 1.52, 1.55, 1.57],
            "station_count": [5, 4, 3, 3],
        }
    )
    conn.execute("CREATE TABLE zip_code_daily_stats AS SELECT * FROM df")
    duckdb_engine_module._zip_code_trend_ready.set()

    result = query_national_group_price_trend(["diesel_a_price"], 90, province="madrid")

    assert len(result) == 2
    assert set(result.columns) == {"date", "fuel_type", "avg_price", "min_price", "max_price"}


@patch("data.duckdb_engine.download_parquet_as_df")
@patch("data.duckdb_engine.get_latest_parquet_file")
@patch("data.duckdb_engine.get_connection")
def test_refresh_latest_snapshot_excludes_non_public_sale_type(mock_conn, mock_latest_file, mock_download):
    conn = duckdb.connect(":memory:")
    mock_conn.return_value = conn
    mock_latest_file.return_value = "some_file.parquet"
    mock_download.return_value = pd.DataFrame(
        {
            "label": ["public_station", "restricted_station"],
            "sale_type": ["p", "r"],
            "diesel_a_price": [1.45, 1.50],
        }
    )

    refresh_latest_snapshot()

    result = conn.execute("SELECT * FROM latest_stations").df()
    assert len(result) == 1
    assert result.iloc[0]["label"] == "public_station"
    assert result.iloc[0]["sale_type"] == "p"


def testfilter_public_stations_excludes_restricted():
    df = pd.DataFrame({"label": ["a", "b", "c"], "sale_type": ["p", "r", "p"]})
    result = filter_public_stations(df)
    assert list(result["label"]) == ["a", "c"]


def testfilter_public_stations_missing_column_returns_df_unchanged(caplog):
    df = pd.DataFrame({"label": ["a", "b"], "diesel_a_price": [1.45, 1.50]})
    result = filter_public_stations(df)
    assert len(result) == 2
    assert "sale_type column missing" in caplog.text


def _setup_random_stations_table(conn, n: int = 3000, seed: int = 7):
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(  # noqa: F841
        {
            "label": [f"brand_{i % 7}" for i in range(n)],
            "address": [f"calle {i}" for i in range(n)],
            "municipality": ["m"] * n,
            "province": ["p"] * n,
            "zip_code": ["28001"] * n,
            "latitude": rng.uniform(36.0, 43.7, n),
            "longitude": rng.uniform(-9.2, 3.3, n),
            "diesel_a_price": rng.uniform(1.3, 1.9, n),
        }
    )
    conn.execute("DROP TABLE IF EXISTS latest_stations")
    conn.execute("CREATE TABLE latest_stations AS SELECT * FROM df")


_HAVERSINE_SQL = """
    2 * 6371 * ASIN(SQRT(
        POWER(SIN(RADIANS(s.latitude - w.lat) / 2), 2) +
        COS(RADIANS(w.lat)) * COS(RADIANS(s.latitude)) *
        POWER(SIN(RADIANS(s.longitude - w.lon) / 2), 2)
    ))
"""


@pytest.mark.parametrize("lat", [0.0, 36.0, 40.4168, 43.7, 70.0])
@pytest.mark.parametrize("radius_km", [0.5, 5.0, 50.0, 400.0])
def test_bbox_half_widths_contain_every_point_within_radius(lat, radius_km):
    dlat, dlon = _bbox_half_widths(lat, radius_km)
    # Walk the circle of exactly radius_km around (lat, 0) and check every point stays in the box.
    angular = radius_km / 6371.0
    lat_r = math.radians(lat)
    for bearing in np.linspace(0, 2 * math.pi, 721):
        plat = math.asin(math.sin(lat_r) * math.cos(angular) + math.cos(lat_r) * math.sin(angular) * math.cos(bearing))
        plon = math.atan2(
            math.sin(bearing) * math.sin(angular) * math.cos(lat_r),
            math.cos(angular) - math.sin(lat_r) * math.sin(plat),
        )
        assert abs(math.degrees(plat) - lat) <= dlat
        assert abs(math.degrees(plon)) <= dlon


@pytest.mark.parametrize("radius_km", [20.0, 75.0, 300.0])
@patch("data.duckdb_engine.get_connection")
def test_query_stations_within_radius_matches_unfiltered_haversine(mock_conn, radius_km):
    conn = duckdb.connect(":memory:")
    _setup_random_stations_table(conn)
    mock_conn.return_value = conn
    lat, lon = 40.4168, -3.7038
    expected = conn.execute(
        f"SELECT s.address FROM latest_stations s, (SELECT $1 AS lat, $2 AS lon) w " f"WHERE {_HAVERSINE_SQL} <= $3",
        [lat, lon, radius_km],
    ).fetchdf()

    result = query_stations_within_radius(lat, lon, "diesel_a_price", radius_km)
    group_result = query_stations_within_radius_group(
        lat, lon, "diesel_a_price", ["diesel_a_price"], radius_km, labels=["brand_1", "brand_2"]
    )

    assert set(result["address"]) == set(expected["address"])
    assert len(result) > 0
    expected_labels = conn.execute(
        "SELECT address FROM latest_stations WHERE label IN ('brand_1', 'brand_2')"
    ).fetchdf()
    assert set(group_result["address"]) == set(expected["address"]) & set(expected_labels["address"])


@pytest.mark.parametrize("corridor_km", [3.0, 15.0, 45.0])
@patch("data.duckdb_engine.get_connection")
def test_query_stations_along_corridor_matches_cross_join(mock_conn, corridor_km):
    conn = duckdb.connect(":memory:")
    _setup_random_stations_table(conn)
    mock_conn.return_value = conn
    # Madrid -> Barcelona, one waypoint roughly every 10 km.
    waypoints = [(40.4168 + 0.9706 * t, -3.7038 + 5.8724 * t, 505.0 * t) for t in np.linspace(0, 1, 51)]
    wp_df = pd.DataFrame(  # noqa: F841
        {"wp_idx": range(len(waypoints)), "lat": [w[0] for w in waypoints], "lon": [w[1] for w in waypoints]}
    )
    expected = conn.execute(
        f"""
        SELECT s.address, ROUND(MIN({_HAVERSINE_SQL}), 2) AS min_distance_km,
               ARG_MIN(w.wp_idx, {_HAVERSINE_SQL}) AS closest_waypoint_idx
        FROM latest_stations s CROSS JOIN wp_df w
        GROUP BY s.address
        HAVING MIN({_HAVERSINE_SQL}) <= $1
        """,
        [corridor_km],
    ).fetchdf()

    result = query_stations_along_corridor(waypoints, "diesel_a_price", corridor_km)

    key = ["address", "min_distance_km", "closest_waypoint_idx"]
    assert len(result) > 0
    pd.testing.assert_frame_equal(
        result[key].sort_values("address").reset_index(drop=True),
        expected[key].sort_values("address").reset_index(drop=True),
        check_dtype=False,
    )


@patch("data.duckdb_engine.get_connection")
def test_query_stations_along_corridor_keeps_stations_north_of_average_latitude(mock_conn):
    # Sevilla -> Barcelona: the old outer box used the route's average latitude for its longitude
    # padding, which is too narrow at Barcelona and dropped this station ~14.9 km east of the end.
    conn = duckdb.connect(":memory:")
    bcn_lat, bcn_lon = 41.3874, 2.1686
    east_lon = bcn_lon + math.degrees(14.9 / (6371.0 * math.cos(math.radians(bcn_lat))))
    df = pd.DataFrame(  # noqa: F841
        {
            "label": ["east_of_bcn"],
            "address": ["calle e"],
            "municipality": ["badalona"],
            "province": ["barcelona"],
            "zip_code": ["08911"],
            "latitude": [bcn_lat],
            "longitude": [east_lon],
            "diesel_a_price": [1.5],
        }
    )
    conn.execute("CREATE TABLE latest_stations AS SELECT * FROM df")
    mock_conn.return_value = conn
    waypoints = [(37.3891 + (bcn_lat - 37.3891) * t, -5.9845 + (bcn_lon + 5.9845) * t, 830.0 * t) for t in (0, 0.5, 1)]

    result = query_stations_along_corridor(waypoints, "diesel_a_price", 15.0)

    assert result["label"].tolist() == ["east_of_bcn"]
    assert result["closest_waypoint_idx"].tolist() == [2]
