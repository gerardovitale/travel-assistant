import importlib.util
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest
from api.schemas import FuelType
from api.schemas import ZoneResult

PLOTLY_AVAILABLE = importlib.util.find_spec("plotly") is not None

SAMPLE_FUEL_TYPE = "diesel_a_price"


# ── DuckDB query tests ──────────────────────────────────────────────


def _setup_test_table(conn):
    df = pd.DataFrame(  # noqa: F841
        {
            "label": ["s1", "s2", "s3", "s4"],
            "address": ["a1", "a2", "a3", "a4"],
            "municipality": ["GETAFE", "GETAFE", "LEGANES", "MADRID"],
            "province": ["MADRID", "MADRID", "MADRID", "MADRID"],
            "zip_code": ["28901", "28902", "28911", "28001"],
            "latitude": [40.31, 40.32, 40.33, 40.42],
            "longitude": [-3.73, -3.74, -3.76, -3.70],
            "diesel_a_price": [1.40, 1.45, 1.50, 1.55],
        }
    )
    conn.execute("DROP TABLE IF EXISTS latest_stations")
    conn.execute("CREATE TABLE latest_stations AS SELECT * FROM df")


@patch("data.duckdb_engine.get_connection")
def test_query_municipalities_by_province(mock_conn):
    from data.duckdb_engine import query_municipalities_by_province

    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_municipalities_by_province("MADRID")
    assert result == ["GETAFE", "LEGANES", "MADRID"]


@patch("data.duckdb_engine.get_connection")
def test_query_municipalities_by_province_no_results(mock_conn):
    from data.duckdb_engine import query_municipalities_by_province

    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_municipalities_by_province("NONEXISTENT")
    assert result == []


@patch("data.duckdb_engine.get_connection")
def test_query_cheapest_zones_by_municipality(mock_conn):
    from data.duckdb_engine import query_cheapest_zones_by_municipality

    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_cheapest_zones_by_municipality("MADRID", "GETAFE", SAMPLE_FUEL_TYPE)
    assert len(result) == 2
    assert set(result["zip_code"].tolist()) == {"28901", "28902"}
    assert result.iloc[0]["avg_price"] <= result.iloc[1]["avg_price"]


@patch("data.duckdb_engine.get_connection")
def test_query_cheapest_zones_by_municipality_no_results(mock_conn):
    from data.duckdb_engine import query_cheapest_zones_by_municipality

    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_cheapest_zones_by_municipality("MADRID", "NONEXISTENT", SAMPLE_FUEL_TYPE)
    assert len(result) == 0


@patch("data.duckdb_engine.get_connection")
def test_query_zip_codes_by_district(mock_conn):
    from data.duckdb_engine import query_zip_codes_by_district

    conn = duckdb.connect(":memory:")
    _setup_test_table(conn)
    mock_conn.return_value = conn

    result = query_zip_codes_by_district("MADRID", SAMPLE_FUEL_TYPE)
    assert len(result) == 4
    assert "zip_code" in result.columns
    assert "latitude" in result.columns
    assert "price" in result.columns


# ── Service layer tests ─────────────────────────────────────────────


@patch("services.station_service.query_municipalities_by_province")
def test_get_municipalities(mock_query):
    from services.station_service import get_municipalities

    mock_query.return_value = ["GETAFE", "LEGANES", "MADRID"]
    result = get_municipalities("MADRID")
    assert result == ["GETAFE", "LEGANES", "MADRID"]
    mock_query.assert_called_once_with("MADRID")


@patch("services.station_service.query_cheapest_zones_by_municipality")
def test_get_zip_code_price_map_by_municipality(mock_query):
    from services.station_service import get_zip_code_price_map_by_municipality

    mock_query.return_value = pd.DataFrame(
        {
            "zip_code": ["28901", "28902"],
            "avg_price": [1.40, 1.45],
            "min_price": [1.35, 1.40],
            "station_count": [3, 2],
        }
    )
    results = get_zip_code_price_map_by_municipality("MADRID", FuelType.diesel_a_price, "GETAFE")
    assert len(results) == 2
    assert results[0].zip_code == "28901"
    assert results[0].avg_price == 1.40
    mock_query.assert_called_once_with("MADRID", SAMPLE_FUEL_TYPE, "GETAFE")


@patch("services.station_service.get_cheapest_zones")
def test_get_zip_code_price_map_for_zips(mock_zones):
    from services.station_service import get_zip_code_price_map_for_zips

    mock_zones.return_value = [
        ZoneResult(zip_code="28001", avg_price=1.50, min_price=1.45, station_count=5),
        ZoneResult(zip_code="28002", avg_price=1.55, min_price=1.50, station_count=3),
        ZoneResult(zip_code="28003", avg_price=1.60, min_price=1.55, station_count=2),
    ]
    results = get_zip_code_price_map_for_zips("MADRID", FuelType.diesel_a_price, ["28001", "28003"])
    assert len(results) == 2
    zips = {r.zip_code for r in results}
    assert zips == {"28001", "28003"}


@patch("services.station_service.load_postal_codes_for_zip_list")
def test_get_postal_code_geojson(mock_load):
    from services.station_service import get_postal_code_geojson

    expected = {"type": "FeatureCollection", "features": []}
    mock_load.return_value = expected
    result = get_postal_code_geojson(["28001"])
    assert result == expected
    mock_load.assert_called_once_with(["28001"])


@patch("services.station_service.query_zip_codes_by_district")
@patch("services.station_service.load_madrid_districts")
def test_get_zip_codes_for_district(mock_districts, mock_query):
    from services.station_service import get_zip_codes_for_district

    mock_districts.return_value = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"nombre": "Centro"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-3.72, 40.40], [-3.68, 40.40], [-3.68, 40.44], [-3.72, 40.44], [-3.72, 40.40]]],
                },
            },
            {
                "type": "Feature",
                "properties": {"nombre": "Retiro"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-3.68, 40.40], [-3.64, 40.40], [-3.64, 40.44], [-3.68, 40.44], [-3.68, 40.40]]],
                },
            },
        ],
    }
    mock_query.return_value = pd.DataFrame(
        {
            "latitude": [40.42, 40.42],
            "longitude": [-3.70, -3.66],
            "zip_code": ["28001", "28009"],
            "price": [1.50, 1.55],
        }
    )
    result = get_zip_codes_for_district("MADRID", FuelType.diesel_a_price, "Centro")
    assert result == ["28001"]


@patch("services.station_service.query_zip_codes_by_district")
@patch("services.station_service.load_madrid_districts")
def test_get_zip_codes_for_district_empty(mock_districts, mock_query):
    from services.station_service import get_zip_codes_for_district

    mock_districts.return_value = {"type": "FeatureCollection", "features": []}
    mock_query.return_value = pd.DataFrame()
    result = get_zip_codes_for_district("MADRID", FuelType.diesel_a_price, "Centro")
    assert result == []


# ── Chart tests ──────────────────────────────────────────────────────


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="plotly not installed")
def test_build_zip_code_choropleth():
    from ui.charts import build_zip_code_choropleth

    prices = [
        ZoneResult(zip_code="28001", avg_price=1.50, min_price=1.45, station_count=5),
        ZoneResult(zip_code="28002", avg_price=1.55, min_price=1.50, station_count=3),
    ]
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"COD_POSTAL": "28001"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-3.71, 40.41], [-3.70, 40.41], [-3.70, 40.42], [-3.71, 40.42], [-3.71, 40.41]]],
                },
            },
            {
                "type": "Feature",
                "properties": {"COD_POSTAL": "28002"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-3.70, 40.42], [-3.69, 40.42], [-3.69, 40.43], [-3.70, 40.43], [-3.70, 40.42]]],
                },
            },
        ],
    }
    fig = build_zip_code_choropleth(prices, geojson, "Test title", SAMPLE_FUEL_TYPE)
    assert "Test title" in fig.layout.title.text
    assert "Diesel A" in fig.layout.title.text
    assert len(fig.data) == 1
    assert fig.data[0].featureidkey == "properties.COD_POSTAL"
    assert list(fig.data[0].locations) == ["28001", "28002"]
    assert fig.layout.height == 550


@pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="plotly not installed")
def test_build_zip_code_choropleth_auto_centers():
    from ui.charts import build_zip_code_choropleth

    prices = [ZoneResult(zip_code="28001", avg_price=1.50, min_price=1.45, station_count=5)]
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"COD_POSTAL": "28001"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-3.71, 40.41], [-3.70, 40.41], [-3.70, 40.42], [-3.71, 40.42], [-3.71, 40.41]]],
                },
            },
        ],
    }
    fig = build_zip_code_choropleth(prices, geojson, "Test", SAMPLE_FUEL_TYPE)
    center = fig.layout.mapbox.center
    assert 40.41 <= center.lat <= 40.42
    assert -3.71 <= center.lon <= -3.70
