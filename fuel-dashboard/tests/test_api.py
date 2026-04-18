from unittest.mock import patch

from api.schemas import HistoricalForecastResponse
from api.schemas import StationResult
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
