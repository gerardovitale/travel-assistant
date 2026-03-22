import asyncio
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

from services.routing import get_road_distances
from services.routing import get_route_geometries


@patch("services.routing._get_sync_client")
def test_successful_response(mock_get_client):
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"code": "Ok", "distances": [[1500.0, 3200.5]]}
    mock_client.get.return_value = mock_response
    mock_get_client.return_value = mock_client

    result = get_road_distances((40.0, -3.0), [(40.1, -3.1), (40.2, -3.2)])
    assert result == [1.5, 3.2]


@patch("services.routing._get_sync_client")
def test_osrm_request_failure(mock_get_client):
    mock_client = MagicMock()
    mock_client.get.side_effect = Exception("connection error")
    mock_get_client.return_value = mock_client

    result = get_road_distances((40.0, -3.0), [(40.1, -3.1)])
    assert result is None


@patch("services.routing._get_sync_client")
def test_non_ok_response_code(mock_get_client):
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"code": "InvalidQuery"}
    mock_client.get.return_value = mock_response
    mock_get_client.return_value = mock_client

    result = get_road_distances((40.0, -3.0), [(40.1, -3.1)])
    assert result is None


def test_empty_destinations():
    result = get_road_distances((40.0, -3.0), [])
    assert result == []


@patch("services.routing._get_sync_client")
def test_unreachable_destination(mock_get_client):
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"code": "Ok", "distances": [[1500.0, None]]}
    mock_client.get.return_value = mock_response
    mock_get_client.return_value = mock_client

    result = get_road_distances((40.0, -3.0), [(40.1, -3.1), (40.2, -3.2)])
    assert result == [1.5, None]


@patch("services.routing._get_sync_client")
def test_http_error_status(mock_get_client):
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_client.get.return_value = mock_response
    mock_get_client.return_value = mock_client

    result = get_road_distances((40.0, -3.0), [(40.1, -3.1)])
    assert result is None


# --- get_route_geometries tests ---


def _make_async_route_response(coords):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"code": "Ok", "routes": [{"geometry": {"coordinates": coords}}]}
    return resp


def _mock_async_client(mock_client_cls, mock_get):
    mock_instance = MagicMock()
    mock_instance.get = mock_get
    mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
    mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=None)


@patch("services.routing.httpx.AsyncClient")
def test_route_geometries_success(mock_client_cls):
    coords_a = [[-3.0, 40.0], [-3.05, 40.05], [-3.1, 40.1]]
    coords_b = [[-3.0, 40.0], [-3.15, 40.15], [-3.2, 40.2]]

    async def mock_get(url):
        if "-3.1,40.1" in url:
            return _make_async_route_response(coords_a)
        return _make_async_route_response(coords_b)

    _mock_async_client(mock_client_cls, mock_get)

    result = asyncio.run(get_route_geometries((40.0, -3.0), [(40.1, -3.1), (40.2, -3.2)]))
    assert result == [coords_a, coords_b]


@patch("services.routing.httpx.AsyncClient")
def test_route_geometries_partial_failure(mock_client_cls):
    coords_a = [[-3.0, 40.0], [-3.1, 40.1]]
    fail_resp = MagicMock()
    fail_resp.status_code = 200
    fail_resp.json.return_value = {"code": "NoRoute"}

    async def mock_get(url):
        if "-3.1,40.1" in url:
            return _make_async_route_response(coords_a)
        return fail_resp

    _mock_async_client(mock_client_cls, mock_get)

    result = asyncio.run(get_route_geometries((40.0, -3.0), [(40.1, -3.1), (40.2, -3.2)]))
    assert result == [coords_a, None]


def test_route_geometries_empty_destinations():
    result = asyncio.run(get_route_geometries((40.0, -3.0), []))
    assert result == []


@patch("services.routing.httpx.AsyncClient")
def test_route_geometries_non_ok_status(mock_client_cls):
    fail_resp = MagicMock()
    fail_resp.status_code = 500

    async def mock_get(url):
        return fail_resp

    _mock_async_client(mock_client_cls, mock_get)

    result = asyncio.run(get_route_geometries((40.0, -3.0), [(40.1, -3.1)]))
    assert result == [None]
