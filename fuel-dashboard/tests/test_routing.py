import asyncio
import threading
import time
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import services.routing as routing
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


def test_get_sync_client_concurrent_first_calls_create_one_client(monkeypatch):
    """Round trips fetch both routes in parallel; the first calls must share one client."""
    created = []

    def slow_client(*args, **kwargs):
        time.sleep(0.05)  # widen the check-then-create window
        client = MagicMock(is_closed=False)
        created.append(client)
        return client

    monkeypatch.setattr(routing, "_sync_client", None)
    monkeypatch.setattr(routing.httpx, "Client", slow_client)
    results = []
    threads = [threading.Thread(target=lambda: results.append(routing._get_sync_client())) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(created) == 1
    assert all(r is created[0] for r in results)


def _osrm_routes_response(routes: list[dict]) -> MagicMock:
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"code": "Ok", "routes": routes}
    return response


@patch("services.routing._get_sync_client")
def test_get_route_options_parses_every_route(mock_get_client):
    routing._cached_route_options.cache_clear()
    mock_client = MagicMock()
    mock_client.get.return_value = _osrm_routes_response(
        [
            {
                "geometry": {"coordinates": [[-3.7, 40.4], [-5.9, 37.4]]},
                "distance": 533000,
                "duration": 18720,
                "legs": [{"summary": "A-4"}],
            },
            {
                "geometry": {"coordinates": [[-3.7, 40.4], [-6.1, 38.9]]},
                "distance": 561400,
                "duration": 20280,
                "legs": [{"summary": ""}],
            },
        ]
    )
    mock_get_client.return_value = mock_client

    routes = routing.get_route_options((40.4, -3.7), (37.4, -5.9), 2)

    url = mock_client.get.call_args.args[0]
    assert "alternatives=2" in url
    assert "steps=true" in url
    assert routes == (
        {
            "coordinates": [[-3.7, 40.4], [-5.9, 37.4]],
            "distance_km": 533.0,
            "duration_minutes": 312.0,
            "summary": "A-4",
        },
        {"coordinates": [[-3.7, 40.4], [-6.1, 38.9]], "distance_km": 561.4, "duration_minutes": 338.0, "summary": ""},
    )


@patch("services.routing._get_sync_client")
def test_get_full_route_returns_main_route_without_alternatives(mock_get_client):
    routing._cached_route_options.cache_clear()
    mock_client = MagicMock()
    mock_client.get.return_value = _osrm_routes_response(
        [{"geometry": {"coordinates": [[0, 0], [1, 1]]}, "distance": 1000, "duration": 60, "legs": []}]
    )
    mock_get_client.return_value = mock_client

    route = routing.get_full_route((10.0, 20.0), (11.0, 21.0))

    url = mock_client.get.call_args.args[0]
    assert "alternatives" not in url
    assert "steps" not in url
    assert route == {"coordinates": [[0, 0], [1, 1]], "distance_km": 1.0, "duration_minutes": 1.0, "summary": ""}


@patch("services.routing._get_sync_client")
def test_get_route_options_returns_empty_on_error(mock_get_client):
    routing._cached_route_options.cache_clear()
    mock_client = MagicMock()
    mock_client.get.return_value = MagicMock(status_code=200, json=MagicMock(return_value={"code": "NoRoute"}))
    mock_get_client.return_value = mock_client

    assert routing.get_route_options((12.0, 22.0), (13.0, 23.0), 2) == ()
    assert routing.get_full_route((12.0, 22.0), (13.0, 23.0)) is None


@patch("services.routing._get_sync_client")
def test_get_route_options_does_not_cache_failures(mock_get_client):
    routing._cached_route_options.cache_clear()
    ok = _osrm_routes_response(
        [{"geometry": {"coordinates": [[0, 0], [1, 1]]}, "distance": 1000, "duration": 60, "legs": []}]
    )
    mock_client = MagicMock()
    mock_client.get.side_effect = [MagicMock(status_code=503), ok]
    mock_get_client.return_value = mock_client

    assert routing.get_route_options((14.0, 24.0), (15.0, 25.0), 2) == ()
    assert len(routing.get_route_options((14.0, 24.0), (15.0, 25.0), 2)) == 1
    # Success is cached: no third OSRM call.
    assert len(routing.get_route_options((14.0, 24.0), (15.0, 25.0), 2)) == 1
    assert mock_client.get.call_count == 2
