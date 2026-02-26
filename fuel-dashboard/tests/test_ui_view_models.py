import pytest
from api.schemas import StationResult
from api.schemas import TrendPoint
from api.schemas import ZoneResult
from ui.view_models import format_delta
from ui.view_models import format_distance
from ui.view_models import format_price
from ui.view_models import fuel_label
from ui.view_models import search_mode_metadata
from ui.view_models import search_summary_cards
from ui.view_models import station_summary
from ui.view_models import trend_kpis
from ui.view_models import zone_kpis


def test_search_mode_metadata_radius_by_mode():
    zip_meta = search_mode_metadata("cheapest_by_zip")
    assert zip_meta.requires_radius is False
    address_meta = search_mode_metadata("best_by_address")
    assert address_meta.requires_radius is True


def test_fuel_label_uses_known_mappings_and_fallback():
    assert fuel_label("diesel_a_price") == "Diesel A"
    assert fuel_label("unknown_fuel_price") == "Unknown Fuel"


def test_station_summary_and_cards_include_distance():
    stations = [
        StationResult(
            label="s1",
            address="a1",
            municipality="madrid",
            province="madrid",
            zip_code="28001",
            latitude=40.1,
            longitude=-3.7,
            price=1.50,
            distance_km=4.0,
        ),
        StationResult(
            label="s2",
            address="a2",
            municipality="madrid",
            province="madrid",
            zip_code="28001",
            latitude=40.2,
            longitude=-3.8,
            price=1.45,
            distance_km=2.5,
        ),
    ]
    summary = station_summary(stations)
    assert summary["count"] == 2
    assert summary["best_price"] == 1.45
    assert summary["min_distance_km"] == 2.5
    cards = search_summary_cards(summary, "cheapest_by_address")
    assert len(cards) == 4
    assert cards[0]["value"] == "2"


def test_trend_kpis_and_formatters():
    trend = [
        TrendPoint(date="2025-01-01", avg_price=1.60, min_price=1.55, max_price=1.65),
        TrendPoint(date="2025-01-02", avg_price=1.56, min_price=1.50, max_price=1.61),
        TrendPoint(date="2025-01-03", avg_price=1.58, min_price=1.52, max_price=1.62),
    ]
    metrics = trend_kpis(trend)
    assert metrics["current_avg_price"] == 1.58
    assert metrics["min_price"] == 1.50
    assert metrics["max_price"] == 1.65
    assert metrics["delta_avg_price"] == pytest.approx(-0.02)
    assert format_price(metrics["current_avg_price"]) == "1.580 EUR/L"
    assert format_distance(3.456) == "3.46 km"
    assert format_delta(metrics["delta_avg_price"]) == "-0.020 EUR/L"


def test_zone_kpis_values():
    zones = [
        ZoneResult(zip_code="28001", avg_price=1.50, min_price=1.45, station_count=5),
        ZoneResult(zip_code="28002", avg_price=1.47, min_price=1.41, station_count=3),
    ]
    metrics = zone_kpis(zones)
    assert metrics["zone_count"] == 2
    assert metrics["cheapest_zip"] == "28002"
    assert metrics["cheapest_avg_price"] == 1.47
    assert metrics["province_avg_price"] == pytest.approx(1.485)
