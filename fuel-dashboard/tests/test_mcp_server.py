import importlib
import json
from unittest.mock import patch

import config
import pytest
from api.schemas import HistoricalForecastResponse
from api.schemas import StationResult
from fastapi.testclient import TestClient

# Patch start_cache_refresh for all tests so the lifespan startup doesn't hit GCS (mirrors
# test_api.py's module-level patch).
_cache_patcher = patch("data.cache.start_cache_refresh")
_cache_patcher.start()

_MCP_TEST_KEY = "test-mcp-secret"

# main.py decides whether to mount /mcp once, at import time, from settings.mcp_api_key — so the
# key must be set (mutating the shared settings singleton every module reads) *before* main is
# (re)imported. Module-scoped so it survives for every test below; never undone, matching
# test_api.py's own never-stopped _cache_patcher.
_module_monkeypatch = pytest.MonkeyPatch()
_module_monkeypatch.setattr(config.settings, "mcp_api_key", _MCP_TEST_KEY)
_module_monkeypatch.setattr(config.settings, "mcp_rate_limit", "1000/minute")

import main  # noqa: E402

importlib.reload(main)
assert main._mcp_route is not None, "MCP layer failed to mount with a test API key configured"

EXPECTED_TOOL_NAMES = {
    "find_cheapest_by_zip",
    "find_cheapest_by_address",
    "get_price_trend",
    "forecast_price_regime",
    "list_report_brands",
    "get_brand_win_rate",
    "get_brand_price_comparison",
    "get_data_quality_inventory",
}


@pytest.fixture(scope="module")
def mcp_client():
    # `mcp.server.streamable_http_manager.StreamableHTTPSessionManager.run()` may only be entered
    # once per process, so all tests in this file share one TestClient/lifespan (one entry),
    # each opening its own MCP session via its own initialize handshake.
    with TestClient(main.app, raise_server_exceptions=False) as client:
        yield client


def _mcp_headers(session_id: str | None = None) -> dict:
    headers = {
        "X-MCP-API-Key": _MCP_TEST_KEY,
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }
    if session_id:
        headers["mcp-session-id"] = session_id
    return headers


def _mcp_initialize(client) -> dict:
    response = client.post(
        "/mcp",
        headers=_mcp_headers(),
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"},
            },
        },
    )
    assert response.status_code == 200
    headers = _mcp_headers(response.headers["mcp-session-id"])
    notif = client.post("/mcp", headers=headers, json={"jsonrpc": "2.0", "method": "notifications/initialized"})
    assert notif.status_code == 202
    return headers


def _call_tool(client, headers, name: str, arguments: dict, request_id: int) -> dict:
    response = client.post(
        "/mcp",
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
    )
    assert response.status_code == 200
    line = next(line_ for line_ in response.text.splitlines() if line_.startswith("data:"))
    return json.loads(line[len("data:") :])["result"]  # noqa: E203


def test_mcp_requires_api_key(mcp_client):
    response = mcp_client.get("/mcp")
    assert response.status_code == 401


def test_mcp_rejects_wrong_api_key(mcp_client):
    response = mcp_client.get("/mcp", headers={"X-MCP-API-Key": "wrong"})
    assert response.status_code == 401


def test_mcp_lists_expected_tools(mcp_client):
    headers = _mcp_initialize(mcp_client)
    response = mcp_client.post("/mcp", headers=headers, json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    line = next(line_ for line_ in response.text.splitlines() if line_.startswith("data:"))
    tools = json.loads(line[len("data:") :])["result"]["tools"]  # noqa: E203
    assert {t["name"] for t in tools} == EXPECTED_TOOL_NAMES
    assert all(t["description"] for t in tools)


@patch("mcp_layer.server.get_cheapest_by_zip")
def test_find_cheapest_by_zip_tool_call(mock_service, mcp_client):
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
    headers = _mcp_initialize(mcp_client)
    result = _call_tool(
        mcp_client, headers, "find_cheapest_by_zip", {"zip_code": "28001", "fuel_type": "diesel_a_price"}, 3
    )
    assert result.get("isError") is not True
    stations = result["structuredContent"]["result"]
    assert stations == [
        {
            "label": "station_1",
            "address": "calle 1",
            "municipality": "madrid",
            "province": "madrid",
            "zip_code": "28001",
            "latitude": 40.4168,
            "longitude": -3.7038,
            "price": 1.45,
            "distance_km": None,
            "score": None,
            "estimated_total_cost": None,
            "route_km": None,
            "detour_minutes": None,
            "pct_vs_avg": None,
            "variant_prices": None,
        }
    ]


@patch("mcp_layer.server.get_historical_forecast")
def test_forecast_price_regime_tool_call(mock_service, mcp_client):
    mock_service.return_value = HistoricalForecastResponse(
        geography_type="zip_code",
        geography_value="28001",
        source="zip_code",
        recommendation="Reposta hoy",
        explanation="test explanation",
    )
    headers = _mcp_initialize(mcp_client)
    result = _call_tool(
        mcp_client, headers, "forecast_price_regime", {"fuel_type": "diesel_a_price", "zip_code": "28001"}, 4
    )
    assert result.get("isError") is not True
    assert result["structuredContent"]["recommendation"] == "Reposta hoy"


def test_forecast_price_regime_requires_zip_or_province(mcp_client):
    headers = _mcp_initialize(mcp_client)
    result = _call_tool(mcp_client, headers, "forecast_price_regime", {"fuel_type": "diesel_a_price"}, 5)
    assert result.get("isError") is True


@patch("mcp_layer.server.get_brand_win_rate_report")
def test_get_brand_win_rate_coerces_numpy_types(mock_service, mcp_client):
    import numpy as np

    mock_service.return_value = [
        {
            "brand": "repsol",
            "win_rate_pct": np.float64(43.29),
            "appearances": np.int64(3331315),
            "confidence": "high",
        }
    ]
    headers = _mcp_initialize(mcp_client)
    result = _call_tool(
        mcp_client,
        headers,
        "get_brand_win_rate",
        {"fuel_type": "gasoline_95_e5_price", "direction": "cheapest"},
        6,
    )
    assert result.get("isError") is not True
    row = result["structuredContent"]["result"][0]
    assert row == {"brand": "repsol", "win_rate_pct": 43.29, "appearances": 3331315, "confidence": "high"}


@patch("api.router.get_provinces")
def test_rest_endpoints_unaffected_by_mcp_mount(mock_service, mcp_client):
    mock_service.return_value = {"madrid": "Madrid"}
    response = mcp_client.get("/api/v1/provinces")
    assert response.status_code == 200


def test_mcp_rate_limit_returns_429(monkeypatch, mcp_client):
    import mcp_layer.rate_limit as rate_limit_module

    monkeypatch.setattr(config.settings, "mcp_rate_limit", "1/minute")
    monkeypatch.setattr(rate_limit_module, "_limiter", None)

    first = mcp_client.get("/mcp", headers={"X-MCP-API-Key": _MCP_TEST_KEY})
    second = mcp_client.get("/mcp", headers={"X-MCP-API-Key": _MCP_TEST_KEY})
    assert first.status_code != 429
    assert second.status_code == 429

    # REST's own (unrelated) slowapi limiter is untouched by the /mcp-scoped limiter above.
    rest_response = mcp_client.get("/health")
    assert rest_response.status_code == 200


def test_mcp_not_mounted_under_ui_test_mode(monkeypatch):
    # Regression test: ui_test_mode + a configured mcp_api_key used to mount /mcp anyway, but
    # lifespan()'s ui_test_mode branch returns before starting the session manager — any request
    # then 500'd with "Task group is not initialized". The MCP tools also have no fixture-backed
    # path (unlike every REST route), so this mode now skips mounting /mcp entirely.
    monkeypatch.setattr(config.settings, "ui_test_mode", True)
    importlib.reload(main)
    try:
        assert main._mcp_route is None
    finally:
        monkeypatch.setattr(config.settings, "ui_test_mode", False)
        importlib.reload(main)
        assert main._mcp_route is not None
