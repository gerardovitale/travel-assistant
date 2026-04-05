from unittest.mock import patch

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
    mock_service.assert_called_once_with(40.4168, -3.7038, mock_service.call_args[0][2], 5.0, 5, 4.5, 50.0)


@patch("api.router.geocode_address")
@patch("api.router.get_best_by_address")
def test_best_by_address_uses_default_refill_liters(mock_service, mock_geocode):
    mock_geocode.return_value = (40.4168, -3.7038)
    mock_service.return_value = []
    client = _get_client()
    response = client.get("/api/v1/stations/best-by-address?address=Madrid&fuel_type=diesel_a_price")
    assert response.status_code == 404
    mock_service.assert_called_once_with(40.4168, -3.7038, mock_service.call_args[0][2], 5.0, 5, 7.0, 30.0)


@patch("api.router.get_price_trends")
def test_price_trends_endpoint(mock_service):
    from api.schemas import TrendPoint

    mock_service.return_value = [TrendPoint(date="2025-01-01", avg_price=1.45, min_price=1.40, max_price=1.50)]
    client = _get_client()
    response = client.get("/api/v1/trends/price?zip_code=28001&fuel_type=diesel_a_price&period=week")
    assert response.status_code == 200
    data = response.json()
    assert len(data["trend"]) == 1
