from unittest.mock import patch

import pandas as pd
import pytest
from api.schemas import FuelType
from api.schemas import TrendPeriod
from tests.fixture import make_stations_df
from tests.fixture import SAMPLE_FUEL_TYPE


@pytest.fixture(autouse=True)
def _mock_national_avg():
    """All station service functions call query_national_avg_price; patch it globally."""
    with patch("services.station_service.query_national_avg_price", return_value=1.60):
        yield


@patch("services.station_service.query_cheapest_by_zip")
def test_get_cheapest_by_zip(mock_query):
    from services.station_service import get_cheapest_by_zip

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 3)
    results = get_cheapest_by_zip("28001", FuelType.diesel_a_price, 3)
    assert len(results) == 3
    assert results[0].price == 1.50
    assert results[0].pct_vs_avg is not None
    # pct_vs_avg = (1.50 - 1.60) / 1.60 * 100 = -6.2%
    assert results[0].pct_vs_avg == pytest.approx(-6.2, abs=0.1)
    mock_query.assert_called_once_with("28001", SAMPLE_FUEL_TYPE, 3)


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_nearest_stations")
def test_get_nearest_by_address(mock_query, mock_road):
    from services.station_service import get_nearest_by_address

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 9)
    mock_road.return_value = [1.2, 0.8, 2.5, 1.0, 3.1, 2.0, 4.0, 1.5, 0.9]
    results = get_nearest_by_address(40.4168, -3.7038, FuelType.diesel_a_price, 3)
    assert len(results) == 3
    mock_query.assert_called_once_with(40.4168, -3.7038, SAMPLE_FUEL_TYPE, 9)
    mock_road.assert_called_once()


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_stations_within_radius")
def test_get_best_by_address(mock_query, mock_road):
    from services.station_service import get_best_by_address

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 5)
    mock_road.return_value = [1.0, 1.5, 2.0, 2.5, 3.0]
    results = get_best_by_address(40.4168, -3.7038, FuelType.diesel_a_price, 5.0)
    assert len(results) == 5
    assert all(r.score is not None for r in results)
    assert all(0 <= r.score <= 10 for r in results)
    assert all(r.estimated_total_cost is not None for r in results)
    assert results[0].score >= results[-1].score  # sorted descending by score
    assert results[0].estimated_total_cost <= results[-1].estimated_total_cost  # lowest cost first
    mock_query.assert_called_once_with(40.4168, -3.7038, SAMPLE_FUEL_TYPE, 6.5)


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_stations_within_radius")
def test_get_best_by_address_total_cost_calculation(mock_query, mock_road):
    """Verify the total cost formula: price * (tank + 2 * dist * consumption / 100)."""
    from services.station_service import get_best_by_address

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 1)
    mock_road.return_value = [5.0]
    results = get_best_by_address(
        40.4168, -3.7038, FuelType.diesel_a_price, 10.0, 5, consumption_lper100km=7.0, tank_liters=40.0
    )
    # station_0: price=1.50, distance=5.0km
    # trip_liters = 2 * 5.0 * 7.0 / 100 = 0.7
    # total_cost = 1.50 * (40.0 + 0.7) = 1.50 * 40.7 = 61.05
    assert len(results) == 1
    assert results[0].estimated_total_cost == 61.05


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_stations_within_radius")
def test_get_best_by_address_high_consumption_favors_cheapest(mock_query, mock_road):
    from services.station_service import get_best_by_address

    df = make_stations_df(SAMPLE_FUEL_TYPE, 2)
    # station_0: price=1.50, station_1: price=1.55
    mock_query.return_value = df
    mock_road.return_value = [3.0, 1.0]
    # High consumption: driving far costs a lot, but large tank makes price diff matter more
    results = get_best_by_address(
        40.4168, -3.7038, FuelType.diesel_a_price, 5.0, 2, consumption_lper100km=12.0, tank_liters=60.0
    )
    # station_0: 1.50 * (60 + 2*3*12/100) = 1.50 * 60.72 = 91.08
    # station_1: 1.55 * (60 + 2*1*12/100) = 1.55 * 60.24 = 93.37
    # Cheapest station wins despite being farther
    assert results[0].price == 1.50


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_stations_within_radius")
def test_get_best_by_address_small_tank_favors_closest(mock_query, mock_road):
    from services.station_service import get_best_by_address

    df = make_stations_df(SAMPLE_FUEL_TYPE, 2)
    # station_0: price=1.50, station_1: price=1.55
    mock_query.return_value = df
    mock_road.return_value = [10.0, 1.0]
    # Small tank: price diff yields little savings, but trip cost to far station is high
    results = get_best_by_address(
        40.4168, -3.7038, FuelType.diesel_a_price, 15.0, 2, consumption_lper100km=10.0, tank_liters=10.0
    )
    # station_0: 1.50 * (10 + 2*10*10/100) = 1.50 * 12.0 = 18.00
    # station_1: 1.55 * (10 + 2*1*10/100)  = 1.55 * 10.2 = 15.81
    # Closer station wins despite higher price per liter
    assert results[0].distance_km == 1.0


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_stations_within_radius")
def test_get_best_by_address_equal_prices(mock_query, mock_road):
    from services.station_service import get_best_by_address

    df = make_stations_df(SAMPLE_FUEL_TYPE, 3)
    df[SAMPLE_FUEL_TYPE] = 1.50  # all same price
    mock_query.return_value = df
    mock_road.return_value = [2.0, 1.0, 3.0]
    results = get_best_by_address(40.4168, -3.7038, FuelType.diesel_a_price, 5.0, 3)
    # Same price -> total cost differs only by trip fuel -> closest first
    assert results[0].distance_km == 1.0
    assert results[-1].distance_km == 3.0


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_stations_within_radius")
def test_get_best_by_address_equal_distances(mock_query, mock_road):
    from services.station_service import get_best_by_address

    df = make_stations_df(SAMPLE_FUEL_TYPE, 3)
    mock_query.return_value = df
    mock_road.return_value = [2.0, 2.0, 2.0]  # all same distance
    results = get_best_by_address(40.4168, -3.7038, FuelType.diesel_a_price, 5.0, 3)
    # Same distance -> total cost differs only by price -> cheapest first
    assert results[0].price <= results[1].price <= results[2].price


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_stations_within_radius")
def test_get_best_by_address_single_station(mock_query, mock_road):
    from services.station_service import get_best_by_address

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 1)
    mock_road.return_value = [1.0]
    results = get_best_by_address(40.4168, -3.7038, FuelType.diesel_a_price, 5.0, 5)
    assert len(results) == 1
    assert results[0].score == 10.0


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_stations_within_radius")
def test_get_cheapest_by_address(mock_query, mock_road):
    from services.station_service import get_cheapest_by_address

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 5)
    mock_road.return_value = [1.0, 1.5, 2.0, 2.5, 3.0]
    results = get_cheapest_by_address(40.4168, -3.7038, FuelType.diesel_a_price, 5.0, 3)
    assert len(results) == 3
    assert results[0].price <= results[1].price
    mock_query.assert_called_once_with(40.4168, -3.7038, SAMPLE_FUEL_TYPE, 6.5)


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


@patch("services.station_service.query_zip_code_price_trend")
@patch("services.station_service.download_aggregate")
def test_get_price_trends(mock_download, mock_query):
    from services.station_service import get_price_trends

    mock_download.return_value = pd.DataFrame({"zip_code": ["28001"]})
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
    mock_query.assert_called_once_with(mock_download.return_value, "28001", FuelType.diesel_a_price.value, 7)


@patch("services.station_service.logger")
@patch("services.station_service.perf_counter")
@patch("services.station_service.query_zip_code_price_trend")
@patch("services.station_service.download_aggregate")
def test_get_price_trends_logs_benchmark_for_zip_aggregate(mock_download, mock_query, mock_perf_counter, mock_logger):
    from services.station_service import get_price_trends

    mock_perf_counter.side_effect = [10.0, 10.125]
    mock_download.return_value = pd.DataFrame({"zip_code": ["28001"]})
    mock_query.return_value = pd.DataFrame(
        {
            "date": ["2025-01-01"],
            "avg_price": [1.45],
            "min_price": [1.40],
            "max_price": [1.50],
        }
    )

    get_price_trends("28001", FuelType.diesel_a_price, TrendPeriod.week)

    mock_logger.info.assert_called_once()
    log_args = mock_logger.info.call_args.args
    assert "zone_trend_query_benchmark source=%s" in log_args[0]
    assert log_args[1] == "zip_code_daily_stats"
    assert log_args[6] == 125.0


@patch("services.station_service.query_price_trends")
@patch("services.station_service.list_parquet_files")
@patch("services.station_service.download_aggregate")
def test_get_price_trends_falls_back_to_raw_history(mock_download, mock_list, mock_query):
    from services.station_service import get_price_trends

    mock_download.return_value = None
    mock_list.return_value = ["file1.parquet", "file2.parquet"]
    mock_query.return_value = pd.DataFrame(
        {
            "date": ["2025-01-01"],
            "avg_price": [1.45],
            "min_price": [1.40],
            "max_price": [1.50],
        }
    )

    results = get_price_trends("28001", FuelType.diesel_a_price, TrendPeriod.week)
    assert len(results) == 1
    mock_list.assert_called_once_with(days_back=7)
    mock_query.assert_called_once_with(["file1.parquet", "file2.parquet"], "28001", FuelType.diesel_a_price.value)


@patch("services.station_service.logger")
@patch("services.station_service.perf_counter")
@patch("services.station_service.query_price_trends")
@patch("services.station_service.list_parquet_files")
@patch("services.station_service.download_aggregate")
def test_get_price_trends_logs_benchmark_for_raw_fallback(
    mock_download, mock_list, mock_query, mock_perf_counter, mock_logger
):
    from services.station_service import get_price_trends

    mock_perf_counter.side_effect = [20.0, 20.2501]
    mock_download.return_value = None
    mock_list.return_value = ["file1.parquet"]
    mock_query.return_value = pd.DataFrame(
        {
            "date": ["2025-01-01"],
            "avg_price": [1.45],
            "min_price": [1.40],
            "max_price": [1.50],
        }
    )

    get_price_trends("28001", FuelType.diesel_a_price, TrendPeriod.week)

    mock_logger.info.assert_called_once()
    log_args = mock_logger.info.call_args.args
    assert "zone_trend_query_benchmark source=%s" in log_args[0]
    assert log_args[1] == "raw_parquet_fallback"
    assert log_args[6] == 250.1


@patch("services.station_service.load_postal_code_boundary")
def test_get_zip_code_boundary_valid(mock_load):
    from services.station_service import get_zip_code_boundary

    mock_load.return_value = {"type": "Feature", "properties": {"COD_POSTAL": "28001"}, "geometry": {}}
    result = get_zip_code_boundary("28001")
    assert result is not None
    assert result["properties"]["COD_POSTAL"] == "28001"
    mock_load.assert_called_once_with("28001")


@patch("services.station_service.load_postal_code_boundary")
def test_get_zip_code_boundary_not_found(mock_load):
    from services.station_service import get_zip_code_boundary

    mock_load.return_value = None
    result = get_zip_code_boundary("99999")
    assert result is None


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_nearest_stations")
def test_get_nearest_by_address_osrm_fallback(mock_query, mock_road):
    from services.station_service import get_nearest_by_address

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 9)
    mock_road.return_value = None  # OSRM failure
    results = get_nearest_by_address(40.4168, -3.7038, FuelType.diesel_a_price, 3)
    assert len(results) == 3
    # Falls back to Haversine distances
    assert results[0].distance_km == 1.0


def test_get_zip_code_boundary_invalid_zip():
    from services.station_service import get_zip_code_boundary

    with pytest.raises(ValueError):
        get_zip_code_boundary("abc")
