from unittest.mock import patch

import pandas as pd
from api.schemas import FuelType
from api.schemas import TrendPeriod
from tests.fixture import make_stations_df
from tests.fixture import SAMPLE_FUEL_TYPE


@patch("services.station_service.query_cheapest_by_zip")
def test_get_cheapest_by_zip(mock_query):
    from services.station_service import get_cheapest_by_zip

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 3)
    results = get_cheapest_by_zip("28001", FuelType.diesel_a_price, 3)
    assert len(results) == 3
    assert results[0].price == 1.50
    mock_query.assert_called_once_with("28001", SAMPLE_FUEL_TYPE, 3)


@patch("services.station_service.geocode_address")
@patch("services.station_service.query_nearest_stations")
def test_get_nearest_by_address(mock_query, mock_geocode):
    from services.station_service import get_nearest_by_address

    mock_geocode.return_value = (40.4168, -3.7038)
    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 3)
    results = get_nearest_by_address("Madrid", FuelType.diesel_a_price, 3)
    assert len(results) == 3
    mock_geocode.assert_called_once_with("Madrid")


@patch("services.station_service.geocode_address")
def test_get_nearest_by_address_geocode_fails(mock_geocode):
    from services.station_service import get_nearest_by_address

    mock_geocode.return_value = None
    results = get_nearest_by_address("nonexistent", FuelType.diesel_a_price, 3)
    assert results == []


@patch("services.station_service.geocode_address")
@patch("services.station_service.query_stations_within_radius")
def test_get_best_by_address(mock_query, mock_geocode):
    from services.station_service import get_best_by_address

    mock_geocode.return_value = (40.4168, -3.7038)
    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 5)
    results = get_best_by_address("Madrid", FuelType.diesel_a_price, 5.0)
    assert len(results) == 3  # default limit
    assert all(r.score is not None for r in results)


@patch("services.station_service.query_cheapest_zones")
def test_get_cheapest_zones(mock_query):
    from services.station_service import get_cheapest_zones

    mock_query.return_value = pd.DataFrame(
        {
            "zip_code": ["28001", "28002"],
            "avg_price": [1.45, 1.50],
            "min_price": [1.40, 1.45],
            "station_count": [5, 3],
        }
    )
    results = get_cheapest_zones("madrid", FuelType.diesel_a_price)
    assert len(results) == 2
    assert results[0].zip_code == "28001"


@patch("services.station_service.query_price_trends")
@patch("services.station_service.list_parquet_files")
def test_get_price_trends(mock_list, mock_query):
    from services.station_service import get_price_trends

    mock_list.return_value = ["file1.parquet", "file2.parquet"]
    mock_query.return_value = pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02"],
            "avg_price": [1.45, 1.50],
            "min_price": [1.40, 1.45],
            "max_price": [1.50, 1.55],
        }
    )
    results = get_price_trends("28001", FuelType.diesel_a_price, TrendPeriod.week)
    assert len(results) == 2
    assert results[0].date == "2025-01-01"
