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
