import pytest
import ui_test_support as ui_test
from api.schemas import FuelGroup
from api.schemas import FuelType
from api.schemas import TrendPeriod
from api.schemas import TripPlanRequest


def test_ui_test_support_happy_path_contracts():
    token = ui_test.push_fixture_set("happy_path")
    try:
        response = ui_test.station_list_response("best_by_address", location="Madrid", labels=["repsol"])
        assert response.query_type == "best_by_address"
        assert len(response.stations) == 1

        trip = ui_test.trip_plan_response(
            TripPlanRequest(
                origin="Madrid",
                destination="Sevilla",
                fuel_type=FuelType.diesel_a_price,
            )
        )
        assert trip.plan.total_distance_km > 0

        trend = ui_test.trend_response("28001", FuelType.diesel_a_price, TrendPeriod.month)
        assert len(trend.trend) == 5

        group = ui_test.group_trend_response("28001", FuelGroup.diesel, TrendPeriod.month)
        assert FuelType.diesel_a_price.value in group.series

        quality = ui_test.quality_response()
        assert quality.inventory.max_date == "2026-04-17"

        geojson = ui_test.zones_province_geojson_response()
        assert geojson.geojson["type"] == "FeatureCollection"
    finally:
        ui_test.pop_fixture_set(token)


def test_fuel_type_history_response_series_carries_margin_pct():
    response = ui_test.fuel_type_history_response(pair_id="vw-golf")
    assert response.series
    for point in response.series:
        # price_ratio on the point is already rounded to 4dp, so recomputing margin_pct from it
        # (rather than from the unrounded ratio the fixture used) only agrees to ~0.1 abs.
        expected = (response.breakeven_ratio - point.price_ratio) / response.breakeven_ratio * 100
        assert point.margin_pct == pytest.approx(expected, abs=0.15)


def test_ui_test_support_special_fixture_states():
    token = ui_test.push_fixture_set("quality_stale")
    try:
        status_code, body = ui_test.health_data_response()
        assert status_code == 503
        assert body["status"] == "stale"
        assert ui_test.quality_response().missing_days
    finally:
        ui_test.pop_fixture_set(token)

    token = ui_test.push_fixture_set("loading")
    try:
        assert ui_test.is_data_ready() is False
    finally:
        ui_test.pop_fixture_set(token)


def test_insights_flags_returns_four_booleans():
    token = ui_test.push_fixture_set("happy_path")
    try:
        result = ui_test.insights_flags()
        assert len(result) == 4
        zones, historical, reportes, fuel_type_report = result
        assert isinstance(zones, bool)
        assert isinstance(historical, bool)
        assert isinstance(reportes, bool)
        assert isinstance(fuel_type_report, bool)
    finally:
        ui_test.pop_fixture_set(token)


def test_insights_flags_enables_reportes_for_insights_all_fixture():
    token = ui_test.push_fixture_set("insights_all")
    try:
        _, _, reportes, _ = ui_test.insights_flags()
        assert reportes is True
    finally:
        ui_test.pop_fixture_set(token)


def test_insights_flags_enables_fuel_type_report_for_insights_all_fixture():
    token = ui_test.push_fixture_set("insights_all")
    try:
        _, _, _, fuel_type_report = ui_test.insights_flags()
        assert fuel_type_report is True
    finally:
        ui_test.pop_fixture_set(token)
