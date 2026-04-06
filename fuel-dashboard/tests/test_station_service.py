from unittest.mock import call
from unittest.mock import patch

import pandas as pd
import pytest
from api.schemas import FuelGroup
from api.schemas import FuelType
from api.schemas import TrendPeriod
from tests.fixture import make_group_stations_df
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
    mock_query.assert_called_once_with("28001", SAMPLE_FUEL_TYPE, 3, labels=None)


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_nearest_stations")
def test_get_nearest_by_address(mock_query, mock_road):
    from services.station_service import get_nearest_by_address

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 9)
    mock_road.return_value = [1.2, 0.8, 2.5, 1.0, 3.1, 2.0, 4.0, 1.5, 0.9]
    results = get_nearest_by_address(40.4168, -3.7038, FuelType.diesel_a_price, 3)
    assert len(results) == 3
    mock_query.assert_called_once_with(40.4168, -3.7038, SAMPLE_FUEL_TYPE, 9, labels=None)
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
    mock_query.assert_called_once_with(40.4168, -3.7038, SAMPLE_FUEL_TYPE, 6.5, labels=None)


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
def test_get_best_by_address_uses_default_refill_liters(mock_query, mock_road):
    from services.station_service import get_best_by_address

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 1)
    mock_road.return_value = [5.0]
    results = get_best_by_address(40.4168, -3.7038, FuelType.diesel_a_price, 10.0, 5, consumption_lper100km=7.0)
    # default refill liters = 30.0
    # trip_liters = 2 * 5.0 * 7.0 / 100 = 0.7
    # total_cost = 1.50 * (30.0 + 0.7) = 46.05
    assert len(results) == 1
    assert results[0].estimated_total_cost == 46.05


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
    mock_query.assert_called_once_with(40.4168, -3.7038, SAMPLE_FUEL_TYPE, 6.5, labels=None)


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


@patch("services.station_service.query_cached_zip_code_price_trend")
@patch("services.station_service.is_zip_code_trend_ready")
def test_get_price_trends(mock_ready, mock_query):
    from services.station_service import get_price_trends

    mock_ready.return_value = True
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
    mock_query.assert_called_once_with("28001", FuelType.diesel_a_price.value, 7)


@patch("services.station_service.query_price_trends")
@patch("services.station_service.list_parquet_files")
@patch("services.station_service.is_zip_code_trend_ready")
def test_get_price_trends_falls_back_to_raw_history(mock_ready, mock_list, mock_query):
    from services.station_service import get_price_trends

    mock_ready.return_value = False
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


@patch("services.station_service.query_cached_group_price_trend")
@patch("services.station_service.is_zip_code_trend_ready")
def test_get_group_price_trends(mock_ready, mock_query):
    from services.station_service import get_group_price_trends

    mock_ready.return_value = True
    mock_query.return_value = pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02", "2025-01-01", "2025-01-02"],
            "fuel_type": ["diesel_a_price", "diesel_a_price", "diesel_premium_price", "diesel_premium_price"],
            "avg_price": [1.45, 1.47, 1.55, 1.57],
            "min_price": [1.40, 1.42, 1.50, 1.52],
            "max_price": [1.50, 1.52, 1.60, 1.62],
        }
    )
    result = get_group_price_trends("28001", FuelGroup.diesel, TrendPeriod.week)
    assert "diesel_a_price" in result
    assert "diesel_premium_price" in result
    assert len(result["diesel_a_price"]) == 2
    assert result["diesel_a_price"][0].date == "2025-01-01"
    assert result["diesel_a_price"][0].avg_price == 1.45
    mock_query.assert_called_once_with("28001", ["diesel_a_price", "diesel_b_price", "diesel_premium_price"], 7)


@patch("services.station_service.query_cached_group_price_trend")
@patch("services.station_service.is_zip_code_trend_ready")
def test_get_group_price_trends_empty(mock_ready, mock_query):
    from services.station_service import get_group_price_trends

    mock_ready.return_value = True
    mock_query.return_value = pd.DataFrame(columns=["date", "fuel_type", "avg_price", "min_price", "max_price"])
    result = get_group_price_trends("28001", FuelGroup.diesel, TrendPeriod.month)
    assert result == {}


@patch("services.station_service.query_cached_group_price_trend")
@patch("services.station_service.query_price_trends")
@patch("services.station_service.list_parquet_files")
@patch("services.station_service.is_zip_code_trend_ready")
def test_get_group_price_trends_falls_back_to_raw_history(mock_ready, mock_list, mock_query, mock_cached_query):
    from services.station_service import get_group_price_trends

    mock_ready.return_value = False
    mock_list.return_value = ["file1.parquet", "file2.parquet"]
    mock_query.side_effect = [
        pd.DataFrame(
            {
                "date": ["2025-01-01", "2025-01-02"],
                "avg_price": [1.45, 1.47],
                "min_price": [1.40, 1.42],
                "max_price": [1.50, 1.52],
            }
        ),
        pd.DataFrame(columns=["date", "avg_price", "min_price", "max_price"]),
        pd.DataFrame(
            {
                "date": ["2025-01-01"],
                "avg_price": [1.55],
                "min_price": [1.50],
                "max_price": [1.60],
            }
        ),
    ]

    result = get_group_price_trends("28001", FuelGroup.diesel, TrendPeriod.week)

    assert sorted(result) == ["diesel_a_price", "diesel_premium_price"]
    assert len(result["diesel_a_price"]) == 2
    assert result["diesel_premium_price"][0].avg_price == 1.55
    mock_cached_query.assert_not_called()
    mock_list.assert_called_once_with(days_back=7)
    assert mock_query.call_args_list == [
        call(["file1.parquet", "file2.parquet"], "28001", "diesel_a_price"),
        call(["file1.parquet", "file2.parquet"], "28001", "diesel_b_price"),
        call(["file1.parquet", "file2.parquet"], "28001", "diesel_premium_price"),
    ]


# --- Group search service tests ---

ALL_DIESEL_FUELS = ["diesel_a_price", "diesel_b_price", "diesel_premium_price"]


@patch("services.station_service.query_cheapest_by_zip_group")
def test_get_cheapest_by_zip_group(mock_query):
    from services.station_service import get_cheapest_by_zip_group

    mock_query.return_value = pd.DataFrame(
        [
            {
                "label": "station_primary_only",
                "address": "calle 1",
                "municipality": "madrid",
                "province": "madrid",
                "zip_code": "28001",
                "latitude": 40.4168,
                "longitude": -3.7038,
                "diesel_a_price": 1.50,
                "diesel_b_price": None,
                "diesel_premium_price": None,
            },
            {
                "label": "station_primary_and_variant",
                "address": "calle 2",
                "municipality": "madrid",
                "province": "madrid",
                "zip_code": "28001",
                "latitude": 40.4172,
                "longitude": -3.7032,
                "diesel_a_price": 1.60,
                "diesel_b_price": 1.10,
                "diesel_premium_price": 1.70,
            },
        ]
    )
    results = get_cheapest_by_zip_group("28001", FuelGroup.diesel, 3)
    assert len(results) == 2
    # price is the primary fuel price, not the min across variants
    assert results[0].label == "station_primary_only"
    assert results[0].price == 1.50
    assert results[1].label == "station_primary_and_variant"
    assert results[1].price == 1.60
    assert results[1].variant_prices is not None
    assert "diesel_a_price" in results[1].variant_prices
    assert "diesel_premium_price" in results[1].variant_prices
    mock_query.assert_called_once_with("28001", "diesel_a_price", ALL_DIESEL_FUELS, 3, labels=None)


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_nearest_stations_group")
def test_get_nearest_by_address_group(mock_query, mock_road):
    from services.station_service import get_nearest_by_address_group

    mock_query.return_value = make_group_stations_df(n=9)
    mock_road.return_value = [1.2, 0.8, 2.5, 1.0, 3.1, 2.0, 4.0, 1.5, 0.9]
    results = get_nearest_by_address_group(40.4168, -3.7038, FuelGroup.diesel, 3)
    assert len(results) == 3
    assert results[0].variant_prices is not None
    mock_query.assert_called_once_with(40.4168, -3.7038, "diesel_a_price", ALL_DIESEL_FUELS, 9, labels=None)


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_stations_within_radius_group")
def test_get_cheapest_by_address_group(mock_query, mock_road):
    from services.station_service import get_cheapest_by_address_group

    mock_query.return_value = pd.DataFrame(
        [
            {
                "label": "primary_only",
                "address": "calle 1",
                "municipality": "madrid",
                "province": "madrid",
                "zip_code": "28001",
                "latitude": 40.4168,
                "longitude": -3.7038,
                "distance_km": 1.0,
                "diesel_a_price": 1.50,
                "diesel_b_price": None,
                "diesel_premium_price": None,
            },
            {
                "label": "primary_and_variant",
                "address": "calle 2",
                "municipality": "madrid",
                "province": "madrid",
                "zip_code": "28002",
                "latitude": 40.4172,
                "longitude": -3.7032,
                "distance_km": 2.0,
                "diesel_a_price": 1.60,
                "diesel_b_price": 1.10,
                "diesel_premium_price": 1.70,
            },
        ]
    )
    mock_road.return_value = [1.0, 2.0]
    results = get_cheapest_by_address_group(40.4168, -3.7038, FuelGroup.diesel, 5.0, 5)
    assert len(results) == 2
    # sorted by primary fuel price (diesel_a_price)
    assert [r.label for r in results] == ["primary_only", "primary_and_variant"]
    assert [r.price for r in results] == [1.50, 1.60]
    assert results[1].variant_prices is not None
    mock_query.assert_called_once_with(40.4168, -3.7038, "diesel_a_price", ALL_DIESEL_FUELS, 6.5, labels=None)


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_stations_within_radius_group")
def test_get_best_by_address_group(mock_query, mock_road):
    from services.station_service import get_best_by_address_group

    mock_query.return_value = pd.DataFrame(
        [
            {
                "label": "cheap_far",
                "address": "calle 1",
                "municipality": "madrid",
                "province": "madrid",
                "zip_code": "28001",
                "latitude": 40.4168,
                "longitude": -3.7038,
                "distance_km": 3.0,
                "diesel_a_price": 1.40,
                "diesel_b_price": None,
                "diesel_premium_price": 1.55,
            },
            {
                "label": "expensive_close",
                "address": "calle 2",
                "municipality": "madrid",
                "province": "madrid",
                "zip_code": "28002",
                "latitude": 40.4172,
                "longitude": -3.7032,
                "distance_km": 1.0,
                "diesel_a_price": 1.50,
                "diesel_b_price": None,
                "diesel_premium_price": None,
            },
        ]
    )
    mock_road.return_value = [3.0, 1.0]
    results = get_best_by_address_group(
        40.4168,
        -3.7038,
        FuelGroup.diesel,
        5.0,
        5,
        consumption_lper100km=10.0,
        tank_liters=40.0,
    )
    assert len(results) == 2
    assert all(r.score is not None for r in results)
    assert results[0].score >= results[-1].score
    # price is the primary fuel price; cheap_far wins on total cost
    assert results[0].price == 1.40
    assert results[0].label == "cheap_far"


# --- Label filter pass-through tests ---


@patch("services.station_service.query_cheapest_by_zip")
def test_get_cheapest_by_zip_passes_labels(mock_query):
    from services.station_service import get_cheapest_by_zip

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 1)
    results = get_cheapest_by_zip("28001", FuelType.diesel_a_price, 5, labels=["repsol"])
    assert len(results) == 1
    mock_query.assert_called_once_with("28001", SAMPLE_FUEL_TYPE, 5, labels=["repsol"])


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_nearest_stations")
def test_get_nearest_by_address_passes_labels(mock_query, mock_road):
    from services.station_service import get_nearest_by_address

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 3)
    mock_road.return_value = [1.0, 2.0, 3.0]
    results = get_nearest_by_address(40.4168, -3.7038, FuelType.diesel_a_price, 3, labels=["cepsa"])
    assert len(results) == 3
    mock_query.assert_called_once_with(40.4168, -3.7038, SAMPLE_FUEL_TYPE, 9, labels=["cepsa"])


@patch("services.station_service.get_road_distances")
@patch("services.station_service.query_stations_within_radius")
def test_get_cheapest_by_address_passes_labels(mock_query, mock_road):
    from services.station_service import get_cheapest_by_address

    mock_query.return_value = make_stations_df(SAMPLE_FUEL_TYPE, 3)
    mock_road.return_value = [1.0, 2.0, 3.0]
    results = get_cheapest_by_address(40.4168, -3.7038, FuelType.diesel_a_price, 5.0, 3, labels=["bp"])
    assert len(results) == 3
    mock_query.assert_called_once_with(40.4168, -3.7038, SAMPLE_FUEL_TYPE, 6.5, labels=["bp"])
