import datetime as _dt
from unittest.mock import patch

import numpy as np
import pandas as pd
from config import settings
from fastapi.testclient import TestClient

_cache_patcher = patch("data.cache.start_cache_refresh")
_cache_patcher.start()

from main import app  # noqa: E402


def _get_client():
    return TestClient(app, raise_server_exceptions=False)


# ── Page templates ──────────────────────────────────────────────────


def test_robots_txt_returns_plain_text_and_disallows_api():
    resp = _get_client().get("/robots.txt")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    assert "User-agent: *" in resp.text
    assert "Disallow: /api/" in resp.text
    assert "Disallow: /health/" in resp.text
    assert "Disallow: /health\n" not in resp.text  # must include trailing slash


def test_robots_txt_omits_sitemap_when_public_url_unset():
    resp = _get_client().get("/robots.txt")
    assert "Sitemap:" not in resp.text


def test_robots_txt_includes_sitemap_when_public_url_set():
    with patch.object(settings, "public_url", "https://fuelprecision.es"):
        resp = _get_client().get("/robots.txt")
    assert "Sitemap: https://fuelprecision.es/sitemap.xml" in resp.text


def test_sitemap_xml_returns_xml_with_all_pages():
    with patch.object(settings, "public_url", "https://fuelprecision.es"):
        resp = _get_client().get("/sitemap.xml")
    assert resp.status_code == 200
    assert "application/xml" in resp.headers["content-type"]
    assert "<loc>https://fuelprecision.es/</loc>" in resp.text
    assert "<loc>https://fuelprecision.es/trip</loc>" in resp.text
    assert "<loc>https://fuelprecision.es/insights</loc>" in resp.text


def test_sitemap_xml_without_public_url_still_returns_200():
    resp = _get_client().get("/sitemap.xml")
    assert resp.status_code == 200
    assert "application/xml" in resp.headers["content-type"]


def test_analytics_script_not_injected_by_default():
    with patch("main.is_data_ready", return_value=True):
        resp = _get_client().get("/")
    assert "plausible.io" not in resp.text


def test_analytics_script_injected_when_enabled():
    with patch.object(settings, "analytics_enabled", True), patch.object(
        settings, "analytics_domain", "fuelprecision.es"
    ), patch.object(settings, "ui_test_mode", False), patch("main.is_data_ready", return_value=True):
        resp = _get_client().get("/")
    assert 'data-domain="fuelprecision.es"' in resp.text
    assert "plausible.io" in resp.text


def test_canonical_and_og_url_use_public_url():
    with patch.object(settings, "public_url", "https://fuelprecision.es"), patch(
        "main.is_data_ready", return_value=True
    ):
        resp = _get_client().get("/")
    assert 'rel="canonical" href="https://fuelprecision.es/"' in resp.text
    assert 'rel="alternate" hreflang="es-ES" href="https://fuelprecision.es/"' in resp.text
    assert 'content="https://fuelprecision.es/"' in resp.text
    assert "127.0.0.1" not in resp.text


def test_json_ld_omits_url_when_public_url_unset():
    with patch("main.is_data_ready", return_value=True):
        resp = _get_client().get("/")
    assert '"url": ""' not in resp.text


def test_json_ld_includes_url_when_public_url_set():
    with patch.object(settings, "public_url", "https://fuelprecision.es"), patch(
        "main.is_data_ready", return_value=True
    ):
        resp = _get_client().get("/")
    assert '"url": "https://fuelprecision.es"' in resp.text


def test_page_search_renders():
    with patch("main.is_data_ready", return_value=True):
        resp = _get_client().get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "/static/js/search.js" in resp.text
    assert 'id="theme-toggle"' not in resp.text


def test_page_trip_renders():
    with patch("main.is_data_ready", return_value=True):
        resp = _get_client().get("/trip")
    assert resp.status_code == 200
    assert "/static/js/trip.js" in resp.text


def test_page_insights_renders():
    with patch("main.is_data_ready", return_value=True):
        resp = _get_client().get("/insights")
    assert resp.status_code == 200
    assert "/static/js/insights.js" in resp.text


def test_pages_wait_for_data_before_rendering():
    with patch("main.is_data_ready", return_value=False):
        for path in ("/", "/trip", "/insights"):
            resp = _get_client().get(path)
            assert resp.status_code == 503
            assert resp.headers["retry-after"] == "5"
            assert "Cargando la instantánea de estaciones" in resp.text
            assert "window.setTimeout(() => window.location.reload(), 3000);" in resp.text


def test_page_search_renders_config_defaults():
    with patch("main.is_data_ready", return_value=True):
        resp = _get_client().get("/")
    assert f'value="{settings.default_consumption_lper100km}"' in resp.text
    assert f'value="{settings.default_radius_km}"' in resp.text
    assert f'value="{settings.default_refill_liters}"' in resp.text


def test_page_trip_renders_config_defaults():
    with patch("main.is_data_ready", return_value=True):
        resp = _get_client().get("/trip")
    assert f'value="{settings.default_tank_liters}"' in resp.text
    assert f'value="{settings.default_max_detour_minutes}"' in resp.text
    assert f'value="{settings.default_consumption_lper100km}"' in resp.text


def test_static_token_css_served():
    resp = _get_client().get("/static/css/tokens.css")
    assert resp.status_code == 200


# ── New API endpoints ───────────────────────────────────────────────


def test_fuel_catalog_endpoint():
    resp = _get_client().get("/api/v1/fuel/catalog")
    assert resp.status_code == 200
    data = resp.json()
    assert "groups" in data and "primary" in data and "singletons" in data


@patch("api.router.get_provinces")
def test_provinces_endpoint(mock_service):
    mock_service.return_value = {"madrid": "Madrid"}
    resp = _get_client().get("/api/v1/provinces")
    assert resp.status_code == 200
    assert resp.json()["provinces"] == {"madrid": "Madrid"}


@patch("api.router.get_station_labels")
def test_labels_endpoint(mock_service):
    mock_service.return_value = {"repsol": "Repsol", "cepsa": "Cepsa"}
    resp = _get_client().get("/api/v1/labels")
    assert resp.status_code == 200
    assert resp.json()["labels"] == {"repsol": "Repsol", "cepsa": "Cepsa"}


@patch("api.router.get_national_avg_stats")
def test_national_avg_endpoint(mock_service):
    mock_service.return_value = (1.52, 8421)
    resp = _get_client().get("/api/v1/stats/national-avg?fuel_type=diesel_a_price")
    assert resp.status_code == 200
    data = resp.json()
    assert data["avg_price"] == 1.52
    assert data["station_count"] == 8421


@patch("api.router.get_province_ranking")
def test_zones_provinces_endpoint(mock_service):
    mock_service.return_value = pd.DataFrame([{"province": "madrid", "avg_price": 1.45, "station_count": 100}])
    resp = _get_client().get("/api/v1/zones/provinces?fuel_type=diesel_a_price&period=quarter")
    assert resp.status_code == 200
    assert resp.json()["rows"][0]["province"] == "madrid"


@patch("api.router.get_district_price_map")
def test_zones_districts_endpoint(mock_service):
    from api.schemas import DistrictPriceResult

    mock_service.return_value = [DistrictPriceResult(district="Centro", avg_price=1.50, station_count=10)]
    resp = _get_client().get("/api/v1/zones/districts?province=madrid&fuel_type=diesel_a_price")
    assert resp.status_code == 200
    assert resp.json()["items"][0]["district"] == "Centro"


@patch("api.router.get_province_price_map_filtered")
def test_zones_province_map_endpoint(mock_service):
    from api.schemas import ProvincePriceResult

    mock_service.return_value = [ProvincePriceResult(province="madrid", avg_price=1.45, station_count=100)]
    resp = _get_client().get("/api/v1/zones/province-map?fuel_type=diesel_a_price&mainland_only=true")
    assert resp.status_code == 200
    assert resp.json()["items"][0]["province"] == "madrid"


@patch("api.router.get_province_price_geojson")
def test_zones_province_geojson_endpoint(mock_service):
    mock_service.return_value = {"type": "FeatureCollection", "features": [{"properties": {"province": "Madrid"}}]}
    resp = _get_client().get("/api/v1/zones/province-geojson?fuel_type=diesel_a_price&mainland_only=true")
    assert resp.status_code == 200
    assert resp.json()["geojson"]["features"][0]["properties"]["province"] == "Madrid"


@patch("api.router.get_district_price_geojson")
def test_zones_district_geojson_endpoint(mock_service):
    mock_service.return_value = {"type": "FeatureCollection", "features": [{"properties": {"district": "Centro"}}]}
    resp = _get_client().get("/api/v1/zones/district-geojson?province=madrid&fuel_type=diesel_a_price")
    assert resp.status_code == 200
    assert resp.json()["geojson"]["features"][0]["properties"]["district"] == "Centro"


@patch("api.router.get_municipalities")
def test_zones_municipalities_endpoint(mock_service):
    mock_service.return_value = ["Getafe", "Leganés"]
    resp = _get_client().get("/api/v1/zones/municipalities?province=madrid")
    assert resp.status_code == 200
    assert resp.json()["municipalities"] == ["Getafe", "Leganés"]


@patch("api.router.get_zip_code_price_map_by_municipality")
def test_zones_municipality_zips_endpoint(mock_service):
    from api.schemas import ZoneResult

    mock_service.return_value = [ZoneResult(zip_code="28901", avg_price=1.45, min_price=1.40, station_count=5)]
    resp = _get_client().get(
        "/api/v1/zones/municipality-zips?province=madrid&municipality=getafe&fuel_type=diesel_a_price"
    )
    assert resp.status_code == 200
    assert resp.json()["zones"][0]["zip_code"] == "28901"


@patch("api.router.get_zip_code_price_map_for_zips")
@patch("api.router.get_zip_codes_for_district")
def test_zones_district_zips_endpoint(mock_zip_codes, mock_service):
    from api.schemas import ZoneResult

    mock_zip_codes.return_value = ["28001"]
    mock_service.return_value = [ZoneResult(zip_code="28001", avg_price=1.50, min_price=1.47, station_count=4)]
    resp = _get_client().get("/api/v1/zones/district-zips?province=madrid&district=Centro&fuel_type=diesel_a_price")
    assert resp.status_code == 200
    assert resp.json()["zones"][0]["zip_code"] == "28001"


@patch("api.router.get_day_of_week_pattern")
def test_historical_day_of_week_endpoint(mock_service):
    mock_service.return_value = pd.DataFrame([{"day_of_week": i, "avg_price": 1.4 + i * 0.01} for i in range(7)])
    resp = _get_client().get("/api/v1/historical/day-of-week?fuel_type=diesel_a_price")
    assert resp.status_code == 200
    assert len(resp.json()["rows"]) == 7


@patch("api.router.get_brand_price_trend")
@patch("api.router.get_brand_ranking")
def test_historical_brands_endpoint(mock_rank, mock_trend):
    mock_rank.return_value = pd.DataFrame([{"brand": "repsol", "avg_price": 1.45}])
    mock_trend.return_value = pd.DataFrame([{"date": "2026-04-01", "brand": "repsol", "avg_price": 1.45}])
    resp = _get_client().get("/api/v1/historical/brands?fuel_type=diesel_a_price&period=quarter")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ranking"][0]["brand"] == "repsol"
    assert data["trend"][0]["brand"] == "repsol"


@patch("api.router.get_zone_volatility_ranking")
def test_historical_volatility_endpoint(mock_service):
    mock_service.return_value = pd.DataFrame(
        [{"zip_code": "28001", "coefficient_of_variation": 0.01, "volatility_pct": 1.0}]
    )
    resp = _get_client().get("/api/v1/historical/volatility?fuel_type=diesel_a_price&period=quarter")
    assert resp.status_code == 200
    assert resp.json()["rows"][0]["zip_code"] == "28001"


@patch("api.router.geocode_address")
def test_geocode_endpoint(mock_service):
    mock_service.return_value = (40.4168, -3.7038)
    resp = _get_client().get("/api/v1/geocode?address=madrid")
    assert resp.status_code == 200
    assert resp.json() == {"lat": 40.4168, "lon": -3.7038}


@patch("api.router.geocode_address")
def test_geocode_endpoint_not_found(mock_service):
    mock_service.return_value = None
    resp = _get_client().get("/api/v1/geocode?address=zzzz")
    assert resp.status_code == 404


@patch("api.router.plan_trip")
def test_trip_plan_endpoint(mock_service):
    mock_service.return_value = {
        "stops": [],
        "total_fuel_cost": 0.0,
        "total_distance_km": 500.0,
        "duration_minutes": 300.0,
        "total_fuel_liters": 30.0,
        "savings_eur": 0.0,
        "route_coordinates": [[-3.7, 40.4], [-5.9, 37.4]],
        "candidate_stations": [],
        "origin_coords": [40.4, -3.7],
        "destination_coords": [37.4, -5.9],
        "fuel_at_destination_pct": 20.0,
        "alternative_plans": [],
    }
    body = {
        "origin": "Madrid",
        "destination": "Sevilla",
        "fuel_type": "diesel_a_price",
        "consumption_lper100km": 6.5,
        "tank_liters": 50,
        "fuel_level_pct": 30,
        "max_detour_minutes": 15,
    }
    resp = _get_client().post("/api/v1/trip/plan", json=body)
    assert resp.status_code == 200
    assert resp.json()["plan"]["total_distance_km"] == 500.0


@patch("api.router.plan_trip")
def test_trip_plan_uses_settings_defaults_when_fields_omitted(mock_service):
    mock_service.return_value = {
        "stops": [],
        "total_fuel_cost": 0.0,
        "total_distance_km": 100.0,
        "duration_minutes": 60.0,
        "total_fuel_liters": 7.0,
        "savings_eur": 0.0,
        "route_coordinates": [],
        "candidate_stations": [],
        "origin_coords": [40.4, -3.7],
        "destination_coords": [41.4, -2.7],
        "fuel_at_destination_pct": 50.0,
        "alternative_plans": [],
    }
    body = {"origin": "Madrid", "destination": "Zaragoza", "fuel_type": "diesel_a_price"}
    resp = _get_client().post("/api/v1/trip/plan", json=body)
    assert resp.status_code == 200
    _, kwargs = mock_service.call_args
    assert kwargs["tank_liters"] == settings.default_tank_liters
    assert kwargs["consumption_lper100km"] == settings.default_consumption_lper100km
    assert kwargs["fuel_level_pct"] == settings.default_fuel_level_pct
    assert kwargs["max_detour_minutes"] == settings.default_max_detour_minutes


@patch("api.router.plan_trip")
def test_trip_plan_bad_request(mock_service):
    mock_service.side_effect = ValueError("unreachable destination")
    body = {
        "origin": "Madrid",
        "destination": "???",
        "fuel_type": "diesel_a_price",
    }
    resp = _get_client().post("/api/v1/trip/plan", json=body)
    assert resp.status_code == 400


@patch("api.router.get_realtime_status")
@patch("api.router.get_missing_days")
@patch("api.router.get_latest_day_stats")
@patch("api.router.get_data_inventory")
@patch("api.router.get_ingestion_stats")
def test_quality_inventory_endpoint(mock_stats, mock_inv, mock_latest, mock_missing, mock_rt):
    mock_stats.return_value = {}
    mock_inv.return_value = {
        "num_days": 30,
        "num_months": 1,
        "num_years": 1,
        "total_size_bytes": 1024,
        "min_date": _dt.date(2026, 3, 1),
        "max_date": _dt.date(2026, 3, 30),
        "available_dates": set(),
    }
    mock_latest.return_value = {"max_date": _dt.date(2026, 3, 30), "unique_stations": 8000}
    mock_missing.return_value = []
    mock_rt.return_value = {"realtime_enabled": True, "realtime_active": True, "last_realtime_refresh": None}

    resp = _get_client().get("/api/v1/quality/inventory")
    assert resp.status_code == 200
    data = resp.json()
    assert data["inventory"]["num_days"] == 30
    assert data["inventory"]["max_date"] == "2026-03-30"
    assert data["latest_day"]["max_date"] == "2026-03-30"
    assert data["realtime"]["realtime_active"] is True


@patch("api.router.get_realtime_status")
@patch("api.router.get_missing_days")
@patch("api.router.get_latest_day_stats")
@patch("api.router.get_data_inventory")
@patch("api.router.get_ingestion_stats")
def test_quality_inventory_endpoint_no_dates(mock_stats, mock_inv, mock_latest, mock_missing, mock_rt):
    mock_stats.return_value = {}
    mock_inv.return_value = {
        "num_days": 0,
        "num_months": 0,
        "num_years": 0,
        "total_size_bytes": 0,
        "min_date": None,
        "max_date": None,
        "available_dates": set(),
    }
    mock_latest.return_value = {}
    mock_missing.return_value = []
    mock_rt.return_value = {"realtime_enabled": False, "realtime_active": False, "last_realtime_refresh": None}

    resp = _get_client().get("/api/v1/quality/inventory")
    assert resp.status_code == 200
    data = resp.json()
    assert data["inventory"]["min_date"] is None
    assert data["inventory"]["max_date"] is None
    assert data["missing_days"] == []


@patch("api.router.load_postal_codes_for_zip_list")
def test_zones_postal_geojson_endpoint(mock_load):
    mock_load.return_value = {"type": "FeatureCollection", "features": [{"id": "28001"}]}
    resp = _get_client().get("/api/v1/zones/postal-geojson?zip_codes=28001&zip_codes=08001")
    assert resp.status_code == 200
    assert resp.json()["geojson"]["features"][0]["id"] == "28001"


@patch("api.router.load_postal_codes_for_zip_list")
def test_zones_postal_geojson_endpoint_empty(mock_load):
    mock_load.return_value = None
    resp = _get_client().get("/api/v1/zones/postal-geojson?zip_codes=99999")
    assert resp.status_code == 200
    assert resp.json()["geojson"] == {"type": "FeatureCollection", "features": []}


@patch("api.router.load_postal_code_boundary")
def test_zones_zip_boundary_endpoint(mock_load):
    mock_load.return_value = {"type": "Feature", "properties": {"zip_code": "28001"}}
    resp = _get_client().get("/api/v1/zones/zip-boundary?zip_code=28001")
    assert resp.status_code == 200
    assert resp.json()["geojson"]["properties"]["zip_code"] == "28001"


@patch("api.router.load_postal_code_boundary")
def test_zones_zip_boundary_endpoint_not_found(mock_load):
    mock_load.return_value = None
    resp = _get_client().get("/api/v1/zones/zip-boundary?zip_code=99999")
    assert resp.status_code == 404


@patch("api.router.get_group_price_trends")
def test_group_trends_endpoint(mock_service):
    from api.schemas import TrendPoint

    mock_service.return_value = {
        "diesel_a_price": [TrendPoint(date="2026-04-01", avg_price=1.45, min_price=1.40, max_price=1.50)],
    }
    resp = _get_client().get("/api/v1/trends/group?zip_code=28001&fuel_group=diesel&period=month")
    assert resp.status_code == 200
    data = resp.json()
    assert data["fuel_group"] == "diesel"
    assert data["series"]["diesel_a_price"][0]["avg_price"] == 1.45


@patch("api.router.get_group_price_trends")
def test_group_trends_endpoint_with_province(mock_service):
    from api.schemas import TrendPoint

    mock_service.return_value = {
        "diesel_a_price": [TrendPoint(date="2026-04-01", avg_price=1.45, min_price=1.40, max_price=1.50)],
    }
    resp = _get_client().get("/api/v1/trends/group?fuel_group=diesel&period=month&province=madrid")
    assert resp.status_code == 200
    mock_service.assert_called_once()
    _, kwargs = mock_service.call_args
    assert kwargs.get("province") == "madrid"


@patch("api.router.get_brand_price_trend")
@patch("api.router.get_brand_ranking")
def test_historical_brands_empty_ranking(mock_rank, mock_trend):
    mock_rank.return_value = pd.DataFrame()
    resp = _get_client().get("/api/v1/historical/brands?fuel_type=diesel_a_price&period=quarter")
    assert resp.status_code == 200
    assert resp.json() == {"ranking": [], "trend": []}
    mock_trend.assert_not_called()


def test_rows_handles_nan_and_timestamp():
    from api.router import _rows

    df = pd.DataFrame(
        [
            {
                "a": np.int64(1),
                "b": np.float64(1.5),
                "c": np.nan,
                "d": pd.Timestamp("2026-04-15"),
                "e": "x",
            }
        ]
    )
    result = _rows(df)
    assert result == [
        {"a": 1, "b": 1.5, "c": None, "d": "2026-04-15T00:00:00.000", "e": "x"},
    ]


def test_rows_handles_none_and_empty():
    from api.router import _rows

    assert _rows(None) == []
    assert _rows(pd.DataFrame()) == []


def test_trip_plan_rejects_out_of_bounds_consumption():
    body = {
        "origin": "Madrid",
        "destination": "Sevilla",
        "fuel_type": "diesel_a_price",
        "consumption_lper100km": -1.0,
    }
    resp = _get_client().post("/api/v1/trip/plan", json=body)
    assert resp.status_code == 422


# ── /health/data ─────────────────────────────────────────────────────

_RT_ACTIVE = {"realtime_enabled": True, "realtime_active": True, "last_realtime_refresh": 1.0}
_RT_INACTIVE = {"realtime_enabled": True, "realtime_active": False, "last_realtime_refresh": None}
_GCS_FILE = "spain_fuel_prices_2026-04-16T120000Z.parquet"
_STALE_FILE = "spain_fuel_prices_2025-01-01T120000Z.parquet"


@patch("main.get_latest_data_timestamp")
@patch("main.get_latest_parquet_file")
@patch("main.get_realtime_status")
def test_health_data_realtime_returns_prices_timestamp(mock_rt, mock_file, mock_ts):
    mock_rt.return_value = _RT_ACTIVE
    mock_file.return_value = _GCS_FILE
    mock_ts.return_value = "2026-04-16T21:56:13+00:00"
    resp = _get_client().get("/health/data")
    assert resp.status_code == 200
    data = resp.json()
    assert data["source"] == "realtime"
    assert data["data_datetime"] == "2026-04-16T21:56:13+00:00"


@patch("main.datetime")
@patch("main.get_latest_data_timestamp")
@patch("main.get_latest_parquet_file")
@patch("main.get_realtime_status")
def test_health_data_gcs_returns_prices_timestamp(mock_rt, mock_file, mock_ts, mock_dt):
    mock_dt.now.return_value = _dt.datetime(2026, 4, 16, tzinfo=_dt.timezone.utc)
    mock_rt.return_value = _RT_INACTIVE
    mock_file.return_value = _GCS_FILE
    mock_ts.return_value = "2026-04-16T10:00:00+00:00"
    resp = _get_client().get("/health/data")
    assert resp.status_code == 200
    data = resp.json()
    assert data["source"] == "gcs"
    assert data["data_datetime"] == "2026-04-16T10:00:00+00:00"


@patch("main.datetime")
@patch("main.get_latest_data_timestamp")
@patch("main.get_latest_parquet_file")
@patch("main.get_realtime_status")
def test_health_data_falls_back_to_file_date_when_snapshot_unavailable(mock_rt, mock_file, mock_ts, mock_dt):
    mock_dt.now.return_value = _dt.datetime(2026, 4, 16, tzinfo=_dt.timezone.utc)
    mock_rt.return_value = _RT_INACTIVE
    mock_file.return_value = _GCS_FILE
    mock_ts.return_value = None
    resp = _get_client().get("/health/data")
    assert resp.status_code == 200
    data = resp.json()
    assert data["data_datetime"] == "2026-04-16"


@patch("main.get_latest_data_timestamp")
@patch("main.get_latest_parquet_file")
@patch("main.get_realtime_status")
def test_health_data_stale_includes_data_datetime(mock_rt, mock_file, mock_ts):
    mock_rt.return_value = _RT_INACTIVE
    mock_file.return_value = _STALE_FILE
    mock_ts.return_value = "2025-01-01T10:00:00+00:00"
    resp = _get_client().get("/health/data")
    assert resp.status_code == 503
    data = resp.json()
    assert data["status"] == "stale"
    assert data["data_datetime"] == "2025-01-01T10:00:00+00:00"


@patch("main.get_latest_parquet_file")
@patch("main.get_realtime_status")
def test_health_data_no_file_no_realtime_returns_error(mock_rt, mock_file):
    mock_rt.return_value = {"realtime_enabled": False, "realtime_active": False, "last_realtime_refresh": None}
    mock_file.return_value = None
    resp = _get_client().get("/health/data")
    assert resp.status_code == 503
    data = resp.json()
    assert data["status"] == "error"
