from unittest.mock import patch

import duckdb
import pandas as pd

from data.duckdb_engine import query_cheapest_by_zip
from data.duckdb_engine import query_cheapest_zones
from data.duckdb_engine import query_nearest_stations
from data.duckdb_engine import query_stations_within_radius


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
