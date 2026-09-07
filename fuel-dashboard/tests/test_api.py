from unittest.mock import patch

import pytest
from api.schemas import HistoricalForecastResponse
from api.schemas import StationResult
from config import settings
from fastapi.testclient import TestClient

# Patch start_cache_refresh for all tests so the lifespan startup doesn't hit GCS
_cache_patcher = patch("data.cache.start_cache_refresh")
_cache_patcher.start()

from main import app  # noqa: E402


def _get_client():
    return TestClient(app, raise_server_exceptions=False)


@patch("api.router.get_cheapest_by_zip")
def test_cheapest_by_zip_endpoint(mock_service):
    mock_service.return_value = [
        StationResult(
            label="station_1",
            address="calle 1",
            municipality="madrid",
            province="madrid",
            zip_code="28001",
            latitude=40.4168,
            longitude=-3.7038,
            price=1.45,
        )
    ]
    client = _get_client()
    response = client.get("/api/v1/stations/cheapest-by-zip?zip_code=28001&fuel_type=diesel_a_price")
    assert response.status_code == 200
    data = response.json()
    assert data["query_type"] == "cheapest_by_zip"
    assert len(data["stations"]) == 1


def test_cheapest_by_zip_missing_params():
    client = _get_client()
    response = client.get("/api/v1/stations/cheapest-by-zip")
    assert response.status_code == 422


def test_invalid_fuel_type():
    client = _get_client()
    response = client.get("/api/v1/stations/cheapest-by-zip?zip_code=28001&fuel_type=invalid")
    assert response.status_code == 422


@patch("api.router.get_cheapest_zones")
def test_cheapest_zones_endpoint(mock_service):
    from api.schemas import ZoneResult

    mock_service.return_value = [ZoneResult(zip_code="28001", avg_price=1.45, min_price=1.40, station_count=5)]
    client = _get_client()
    response = client.get("/api/v1/zones/cheapest?province=madrid&fuel_type=diesel_a_price")
    assert response.status_code == 200
    data = response.json()
    assert len(data["zones"]) == 1


@patch("api.router.geocode_address")
@patch("api.router.get_best_by_address")
def test_best_by_address_with_consumption_and_tank(mock_service, mock_geocode):
    mock_geocode.return_value = (40.4168, -3.7038)
    mock_service.return_value = [
        StationResult(
            label="station_1",
            address="calle 1",
            municipality="madrid",
            province="madrid",
            zip_code="28001",
            latitude=40.4168,
            longitude=-3.7038,
            price=1.45,
            distance_km=1.2,
            score=8.5,
            estimated_total_cost=59.12,
        )
    ]
    client = _get_client()
    response = client.get(
        "/api/v1/stations/best-by-address?address=Madrid&fuel_type=diesel_a_price"
        "&consumption_lper100km=4.5&tank_liters=50"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["query_type"] == "best_by_address"
    assert len(data["stations"]) == 1
    assert data["stations"][0]["estimated_total_cost"] == 59.12
    mock_service.assert_called_once_with(40.4168, -3.7038, mock_service.call_args[0][2], 5.0, 5, 4.5, 50.0, labels=None)


@patch("api.router.geocode_address")
@patch("api.router.get_best_by_address")
def test_best_by_address_uses_default_refill_liters(mock_service, mock_geocode):
    mock_geocode.return_value = (40.4168, -3.7038)
    mock_service.return_value = []
    client = _get_client()
    response = client.get("/api/v1/stations/best-by-address?address=Madrid&fuel_type=diesel_a_price")
    assert response.status_code == 404
    mock_service.assert_called_once_with(40.4168, -3.7038, mock_service.call_args[0][2], 5.0, 5, 7.0, 30.0, labels=None)


@patch("api.router.geocode_address")
@patch("api.router.get_nearest_by_address")
def test_nearest_by_address_returns_search_location(mock_service, mock_geocode):
    mock_geocode.return_value = (40.4168, -3.7038)
    mock_service.return_value = [
        StationResult(
            label="station_1",
            address="calle 1",
            municipality="madrid",
            province="madrid",
            zip_code="28001",
            latitude=40.4168,
            longitude=-3.7038,
            price=1.45,
            distance_km=0.5,
        )
    ]
    client = _get_client()
    response = client.get("/api/v1/stations/nearest-by-address?address=Madrid&fuel_type=diesel_a_price")
    assert response.status_code == 200
    data = response.json()
    assert data["search_location"] == {"latitude": 40.4168, "longitude": -3.7038}


@patch("api.router.geocode_address")
@patch("api.router.get_cheapest_by_address")
def test_cheapest_by_address_returns_search_location(mock_service, mock_geocode):
    mock_geocode.return_value = (40.4168, -3.7038)
    mock_service.return_value = [
        StationResult(
            label="station_1",
            address="calle 1",
            municipality="madrid",
            province="madrid",
            zip_code="28001",
            latitude=40.4168,
            longitude=-3.7038,
            price=1.45,
            distance_km=0.5,
        )
    ]
    client = _get_client()
    response = client.get("/api/v1/stations/cheapest-by-address?address=Madrid&fuel_type=diesel_a_price")
    assert response.status_code == 200
    data = response.json()
    assert data["search_location"] == {"latitude": 40.4168, "longitude": -3.7038}


@patch("api.router.geocode_address")
@patch("api.router.get_best_by_address")
def test_best_by_address_returns_search_location(mock_service, mock_geocode):
    mock_geocode.return_value = (40.4168, -3.7038)
    mock_service.return_value = [
        StationResult(
            label="station_1",
            address="calle 1",
            municipality="madrid",
            province="madrid",
            zip_code="28001",
            latitude=40.4168,
            longitude=-3.7038,
            price=1.45,
            distance_km=0.5,
        )
    ]
    client = _get_client()
    response = client.get("/api/v1/stations/best-by-address?address=Madrid&fuel_type=diesel_a_price")
    assert response.status_code == 200
    data = response.json()
    assert data["search_location"] == {"latitude": 40.4168, "longitude": -3.7038}


@patch("api.router.get_cheapest_by_zip")
def test_cheapest_by_zip_has_no_search_location(mock_service):
    mock_service.return_value = [
        StationResult(
            label="station_1",
            address="calle 1",
            municipality="madrid",
            province="madrid",
            zip_code="28001",
            latitude=40.4168,
            longitude=-3.7038,
            price=1.45,
        )
    ]
    client = _get_client()
    response = client.get("/api/v1/stations/cheapest-by-zip?zip_code=28001&fuel_type=diesel_a_price")
    assert response.status_code == 200
    data = response.json()
    assert data["search_location"] is None


@patch("api.router.get_full_route")
def test_route_endpoint_returns_coordinates(mock_route):
    import pytest

    mock_route.return_value = {
        "coordinates": [[-3.7038, 40.4168], [-3.6900, 40.4200]],
        "distance_km": 1.5,
        "duration_minutes": 3.0,
    }
    client = _get_client()
    response = client.get("/api/v1/route?origin_lat=40.4168&origin_lon=-3.7038&dest_lat=40.4200&dest_lon=-3.6900")
    assert response.status_code == 200
    data = response.json()
    assert "coordinates" in data
    assert len(data["coordinates"]) == 2
    args = mock_route.call_args[0]
    assert args[0] == pytest.approx((40.4168, -3.7038))
    assert args[1] == pytest.approx((40.42, -3.69))


@patch("api.router.get_full_route")
def test_route_endpoint_returns_502_when_osrm_unavailable(mock_route):
    mock_route.return_value = None
    client = _get_client()
    response = client.get("/api/v1/route?origin_lat=40.4168&origin_lon=-3.7038&dest_lat=40.4200&dest_lon=-3.6900")
    assert response.status_code == 502


def test_route_endpoint_missing_params():
    client = _get_client()
    response = client.get("/api/v1/route?origin_lat=40.4168&origin_lon=-3.7038")
    assert response.status_code == 422


@patch("api.router.get_price_trends")
def test_price_trends_endpoint(mock_service):
    from api.schemas import TrendPoint

    mock_service.return_value = [TrendPoint(date="2025-01-01", avg_price=1.45, min_price=1.40, max_price=1.50)]
    client = _get_client()
    response = client.get("/api/v1/trends/price?zip_code=28001&fuel_type=diesel_a_price&period=week")
    assert response.status_code == 200
    data = response.json()
    assert len(data["trend"]) == 1


@patch("api.router.get_price_trends")
def test_price_trends_endpoint_with_province(mock_service):
    from api.schemas import TrendPoint

    mock_service.return_value = [TrendPoint(date="2025-01-01", avg_price=1.45, min_price=1.40, max_price=1.50)]
    client = _get_client()
    response = client.get("/api/v1/trends/price?fuel_type=diesel_a_price&period=week&province=madrid")
    assert response.status_code == 200
    mock_service.assert_called_once()
    _, kwargs = mock_service.call_args
    assert kwargs.get("province") == "madrid"


@patch("api.router.get_historical_forecast")
def test_historical_forecast_endpoint(mock_service):
    mock_service.return_value = HistoricalForecastResponse(
        geography_type="zip_code",
        geography_value="28001",
        source="zip_code",
        coverage_days=90,
        transition_observations=89,
        current_date="2026-04-17",
        current_avg_price=1.452,
        current_regime="cheap",
        next_day_probabilities={"cheap": 0.6, "normal": 0.3, "expensive": 0.1},
        cheaper_within_3d=0.0,
        cheaper_within_7d=0.0,
        expected_days_in_current_regime=2.5,
        confidence=0.72,
        recommendation="Reposta hoy",
        explanation="Forecast explanation",
        insufficient_data=False,
        transition_matrix={
            "cheap": {"cheap": 0.6, "normal": 0.3, "expensive": 0.1},
            "normal": {"cheap": 0.2, "normal": 0.5, "expensive": 0.3},
            "expensive": {"cheap": 0.1, "normal": 0.4, "expensive": 0.5},
        },
    )

    client = _get_client()
    response = client.get("/api/v1/historical/forecast?zip_code=28001&fuel_type=diesel_a_price")

    assert response.status_code == 200
    data = response.json()
    assert data["geography_type"] == "zip_code"
    assert data["recommendation"] == "Reposta hoy"
    mock_service.assert_called_once()


def test_historical_forecast_requires_geography():
    client = _get_client()
    response = client.get("/api/v1/historical/forecast?fuel_type=diesel_a_price")

    assert response.status_code == 422


@patch("api.router.get_historical_forecast")
def test_historical_forecast_returns_400_on_value_error(mock_service):
    mock_service.side_effect = ValueError("zip_code or province is required")

    client = _get_client()
    response = client.get("/api/v1/historical/forecast?zip_code=28001&fuel_type=diesel_a_price")

    assert response.status_code == 400
    assert "zip_code or province is required" in response.json()["detail"]


@patch("api.router.get_address_suggestions")
def test_address_suggestions_success(mock_service):
    mock_service.return_value = [{"display_name": "Madrid, Comunidad de Madrid", "lat": 40.4168, "lon": -3.7038}]
    client = _get_client()
    response = client.get("/api/v1/address-suggestions?q=Madr")
    assert response.status_code == 200
    data = response.json()
    assert len(data["suggestions"]) == 1
    assert data["suggestions"][0]["display_name"] == "Madrid, Comunidad de Madrid"


def test_address_suggestions_query_too_short():
    client = _get_client()
    response = client.get("/api/v1/address-suggestions?q=Ma")
    assert response.status_code == 422


@patch("api.router.get_address_suggestions")
def test_address_suggestions_graceful_empty(mock_service):
    mock_service.return_value = []
    client = _get_client()
    response = client.get("/api/v1/address-suggestions?q=xyz")
    assert response.status_code == 200
    assert response.json()["suggestions"] == []


# ---- /reportes endpoints ----------------------------------------------------


@patch("api.router.get_brand_win_rate_report")
def test_reportes_win_rate_returns_200_with_data(mock_service):
    mock_service.return_value = [
        {"brand": "ballenoil", "win_rate_pct": 64.04, "appearances": 559359, "confidence": "high"}
    ]
    client = _get_client()
    response = client.get("/api/v1/reportes/win-rate?fuel_type=gasoline_95_e5_price&direction=cheapest")
    assert response.status_code == 200
    data = response.json()
    assert data[0]["brand"] == "ballenoil"
    assert data[0]["win_rate_pct"] == 64.04
    assert data[0]["confidence"] == "high"


@patch("api.router.get_brand_win_rate_report")
def test_reportes_win_rate_returns_404_when_aggregate_missing(mock_service):
    mock_service.return_value = None
    client = _get_client()
    response = client.get("/api/v1/reportes/win-rate?fuel_type=gasoline_95_e5_price&direction=cheapest")
    assert response.status_code == 404


def test_reportes_win_rate_rejects_invalid_direction():
    response = _get_client().get("/api/v1/reportes/win-rate?fuel_type=gasoline_95_e5_price&direction=sideways")
    assert response.status_code == 422


def test_reportes_win_rate_rejects_invalid_fuel_type():
    response = _get_client().get("/api/v1/reportes/win-rate?fuel_type=jet_fuel&direction=cheapest")
    assert response.status_code == 422


@patch("api.router.get_brand_price_comparison_report")
def test_reportes_price_comparison_returns_200_with_data(mock_service):
    mock_service.return_value = [
        {
            "brand": "ballenoil",
            "price_delta_pct": -5.71,
            "days_below_market_pct": 92.96,
            "appearances": 559359,
            "confidence": "high",
            "brand_avg_price": 1.4520,
            "market_avg_price": 1.5400,
        }
    ]
    client = _get_client()
    response = client.get("/api/v1/reportes/price-comparison?fuel_type=gasoline_95_e5_price")
    assert response.status_code == 200
    data = response.json()
    assert data[0]["price_delta_pct"] == -5.71
    assert data[0]["confidence"] == "high"
    assert data[0]["brand_avg_price"] == 1.4520
    assert data[0]["market_avg_price"] == 1.5400


@patch("api.router.get_brand_price_comparison_report")
def test_reportes_price_comparison_returns_404_when_aggregate_missing(mock_service):
    mock_service.return_value = None
    response = _get_client().get("/api/v1/reportes/price-comparison?fuel_type=gasoline_95_e5_price")
    assert response.status_code == 404


@patch("api.router.get_brand_coverage_report")
def test_reportes_coverage_returns_200_with_data(mock_service):
    mock_service.return_value = [
        {"brand": "ballenoil", "zip_codes": 250, "localities": 180, "municipalities": 120, "total_observations": 559359}
    ]
    client = _get_client()
    response = client.get("/api/v1/reportes/coverage?fuel_type=gasoline_95_e5_price")
    assert response.status_code == 200
    data = response.json()
    assert data[0]["brand"] == "ballenoil"
    assert data[0]["zip_codes"] == 250


@patch("api.router.get_brand_coverage_report")
def test_reportes_coverage_returns_404_when_aggregate_missing(mock_service):
    mock_service.return_value = None
    response = _get_client().get("/api/v1/reportes/coverage?fuel_type=gasoline_95_e5_price")
    assert response.status_code == 404


@patch("api.router.get_brand_win_rate_report")
def test_reportes_win_rate_passes_selected_brands_to_service(mock_service):
    mock_service.return_value = []
    client = _get_client()
    response = client.get(
        "/api/v1/reportes/win-rate?fuel_type=gasoline_95_e5_price&direction=cheapest&brands=repsol&brands=cepsa"
    )
    assert response.status_code == 200
    # service receives the selected brands (3rd positional arg)
    assert mock_service.call_args.args[2] == ["repsol", "cepsa"]


def test_reportes_win_rate_rejects_more_than_four_brands():
    response = _get_client().get(
        "/api/v1/reportes/win-rate?fuel_type=gasoline_95_e5_price&direction=cheapest"
        "&brands=a&brands=b&brands=c&brands=d&brands=e"
    )
    assert response.status_code == 422


@patch("api.router.get_report_available_brands")
def test_reportes_brands_returns_200_with_options(mock_service):
    mock_service.return_value = ["bp", "repsol", "cepsa"]
    response = _get_client().get("/api/v1/reportes/brands?fuel_type=gasoline_95_e5_price")
    assert response.status_code == 200
    data = response.json()
    assert data["brands"] == ["bp", "repsol", "cepsa"]
    assert isinstance(data["default"], list)


@patch("api.router.get_report_available_brands")
def test_reportes_brands_returns_404_when_aggregate_missing(mock_service):
    mock_service.return_value = None
    response = _get_client().get("/api/v1/reportes/brands?fuel_type=gasoline_95_e5_price")
    assert response.status_code == 404


# ---- /reportes/fuel-type endpoints ----
# These routes are gated on report_fuel_type_enabled, which ships False.


@pytest.fixture
def fuel_type_on():
    with patch.object(settings, "report_fuel_type_enabled", True):
        yield


def _cost_row(vehicle_id="vw-golf-tsi", energy_type="gasoline", consumption=5.7, price=1.509):
    return {
        "vehicle_id": vehicle_id,
        "pair_id": "vw-golf",
        "label": "Volkswagen Golf 1.5 TSI 130 CV",
        "model": "Volkswagen Golf",
        "variant": "1.5 TSI 130 CV",
        "segment": "compacto",
        "energy_type": energy_type,
        "consumption": consumption,
        "consumption_unit": "l/100km",
        "price_per_unit": price,
        "price_date": "2026-08-14",
        "cost_per_100km": round(consumption * price, 2),
    }


def test_fuel_type_report_vehicles_returns_200_from_the_committed_catalog(fuel_type_on):
    # No service patch: the catalog is shipped in the repo, so this endpoint never depends on GCS.
    response = _get_client().get("/api/v1/reportes/fuel-type/vehicles")
    assert response.status_code == 200
    data = response.json()
    assert data["pairs"]
    # The endpoint serves whatever the committed catalog carries, empty or not -- it doesn't
    # editorialize on provenance itself (see test_vehicle_catalog.py for that guarantee).
    assert "source_url" in data
    # Electricity has no price source yet and must be declared as such rather than hidden.
    assert "electric" in data["unpriceable_energy_types"]


@patch("api.router.get_vehicle_costs")
def test_fuel_type_report_cost_returns_200_with_data(mock_service, fuel_type_on):
    mock_service.return_value = [_cost_row()]
    response = _get_client().get("/api/v1/reportes/fuel-type/cost?vehicle_ids=vw-golf-tsi&province=madrid")
    assert response.status_code == 200
    assert response.json()[0]["cost_per_100km"] == 8.6


@patch("api.router.get_vehicle_costs")
def test_fuel_type_report_cost_returns_404_when_aggregate_missing(mock_service, fuel_type_on):
    mock_service.return_value = None
    response = _get_client().get("/api/v1/reportes/fuel-type/cost?vehicle_ids=vw-golf-tsi")
    assert response.status_code == 404


@patch("api.router.get_vehicle_costs")
def test_fuel_type_report_cost_passes_vehicle_ids_and_province(mock_service, fuel_type_on):
    mock_service.return_value = []
    _get_client().get("/api/v1/reportes/fuel-type/cost?vehicle_ids=a&vehicle_ids=b&province=madrid")
    assert mock_service.call_args.args[0] == ["a", "b"]
    assert mock_service.call_args.args[1] == "madrid"


def test_fuel_type_report_cost_rejects_more_than_six_vehicles(fuel_type_on):
    query = "&".join(f"vehicle_ids={i}" for i in range(7))
    response = _get_client().get(f"/api/v1/reportes/fuel-type/cost?{query}")
    assert response.status_code == 422


def test_fuel_type_report_cost_requires_vehicle_ids(fuel_type_on):
    assert _get_client().get("/api/v1/reportes/fuel-type/cost").status_code == 422


@patch("api.router.get_pair_breakeven")
def test_fuel_type_report_breakeven_returns_200_with_data(mock_service, fuel_type_on):
    mock_service.return_value = {
        "pair_id": "vw-golf",
        "model": "Volkswagen Golf",
        "segment": "compacto",
        "province": "madrid",
        "gasoline": _cost_row(),
        "diesel": _cost_row("vw-golf-tdi", "diesel", 4.6, 1.449),
        "price_ratio": 0.96,
        "breakeven_ratio": 1.2391,
        "breakeven_diesel_price": 1.87,
        "diesel_headroom_eur_l": 0.421,
        "breakeven_diesel_consumption": 5.94,
        "margin_pct": 22.5,
        "winner": "diesel",
        "cost_gasoline_per_100km": 8.6,
        "cost_diesel_per_100km": 6.67,
        "cost_gap_per_100km": 1.94,
        "price_date": "2026-08-14",
    }
    response = _get_client().get("/api/v1/reportes/fuel-type/breakeven?pair_id=vw-golf&province=madrid")
    assert response.status_code == 200
    assert response.json()["winner"] == "diesel"


@patch("api.router.get_pair_breakeven")
def test_fuel_type_report_breakeven_returns_404_for_unknown_pair(mock_service, fuel_type_on):
    mock_service.return_value = None
    response = _get_client().get("/api/v1/reportes/fuel-type/breakeven?pair_id=does-not-exist")
    assert response.status_code == 404


def test_fuel_type_report_breakeven_rejects_overlong_pair_id(fuel_type_on):
    response = _get_client().get(f"/api/v1/reportes/fuel-type/breakeven?pair_id={'x' * 65}")
    assert response.status_code == 422


@patch("api.router.get_breakeven_by_province")
def test_fuel_type_report_provinces_returns_200_with_data(mock_service, fuel_type_on):
    mock_service.return_value = {
        "pair_id": "vw-golf",
        "model": "Volkswagen Golf",
        "breakeven_ratio": 1.2391,
        "rows": [],
        "provinces_dropped": 2,
    }
    response = _get_client().get("/api/v1/reportes/fuel-type/provinces?pair_id=vw-golf")
    assert response.status_code == 200
    assert response.json()["provinces_dropped"] == 2


@patch("api.router.get_breakeven_by_province")
def test_fuel_type_report_provinces_defaults_to_mainland_only(mock_service, fuel_type_on):
    mock_service.return_value = {
        "pair_id": "vw-golf",
        "model": "Volkswagen Golf",
        "breakeven_ratio": 1.2391,
        "rows": [],
        "provinces_dropped": 0,
    }
    _get_client().get("/api/v1/reportes/fuel-type/provinces?pair_id=vw-golf")
    assert mock_service.call_args.args[1] is True


@patch("api.router.get_breakeven_history")
def test_fuel_type_report_history_returns_200_with_data(mock_service, fuel_type_on):
    mock_service.return_value = {
        "pair_id": "vw-golf",
        "model": "Volkswagen Golf",
        "province": None,
        "breakeven_ratio": 1.2391,
        "pct_days_diesel_wins": 100.0,
        "pct_days_tie": 0.0,
        "days": 365,
        "flips": 0,
        "crossovers": [],
        "series": [],
    }
    response = _get_client().get("/api/v1/reportes/fuel-type/history?pair_id=vw-golf&period=year")
    assert response.status_code == 200
    assert response.json()["flips"] == 0


@patch("api.router.get_breakeven_history")
def test_fuel_type_report_history_maps_period_to_days(mock_service, fuel_type_on):
    mock_service.return_value = None
    _get_client().get("/api/v1/reportes/fuel-type/history?pair_id=vw-golf&period=quarter")
    assert mock_service.call_args.args[2] == 90


def test_fuel_type_report_history_rejects_invalid_period(fuel_type_on):
    response = _get_client().get("/api/v1/reportes/fuel-type/history?pair_id=vw-golf&period=decade")
    assert response.status_code == 422


# ---- fuel-type report feature gate ----


def test_fuel_type_report_endpoints_404_while_the_flag_is_off():
    # Hiding the tab is not enough: the catalog this serves declares itself unverified, so "off"
    # has to mean off at the API too.
    client = _get_client()
    paths = [
        "/api/v1/reportes/fuel-type/vehicles",
        "/api/v1/reportes/fuel-type/cost?vehicle_ids=vw-golf-tsi",
        "/api/v1/reportes/fuel-type/breakeven?pair_id=vw-golf",
        "/api/v1/reportes/fuel-type/provinces?pair_id=vw-golf",
        "/api/v1/reportes/fuel-type/history?pair_id=vw-golf",
    ]
    for path in paths:
        assert client.get(path).status_code == 404, path


def test_fuel_type_report_vehicles_serves_once_the_flag_is_on():
    with patch.object(settings, "report_fuel_type_enabled", True):
        response = _get_client().get("/api/v1/reportes/fuel-type/vehicles")
    assert response.status_code == 200
    assert response.json()["pairs"]
