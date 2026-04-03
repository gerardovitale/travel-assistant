from datetime import date

import pandas as pd
import pytest
from api.schemas import StationResult
from api.schemas import TrendPoint
from api.schemas import ZoneResult
from ui.view_models import best_day_advice
from ui.view_models import format_delta
from ui.view_models import format_distance
from ui.view_models import format_price
from ui.view_models import fuel_label
from ui.view_models import latest_day_kpis
from ui.view_models import search_mode_metadata
from ui.view_models import search_summary_cards
from ui.view_models import station_summary
from ui.view_models import trend_kpis
from ui.view_models import volatility_kpis
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


def test_best_day_advice_returns_tip():
    df = pd.DataFrame(
        {
            "day_of_week": list(range(7)),
            "avg_price": [1.50, 1.48, 1.49, 1.51, 1.52, 1.53, 1.50],
            "count_days": [10] * 7,
            "min_daily_avg": [1.45] * 7,
            "max_daily_avg": [1.55] * 7,
        }
    )
    tip = best_day_advice(df)
    assert tip is not None
    assert "martes" in tip.lower()
    assert "sabado" in tip.lower()


def test_best_day_advice_returns_none_for_empty():
    assert best_day_advice(pd.DataFrame()) is None


def test_format_data_size_megabytes():
    from ui.view_models import format_data_size

    assert format_data_size(500 * 1024 * 1024) == "500.0 MB"


def test_format_data_size_gigabytes():
    from ui.view_models import format_data_size

    assert format_data_size(2 * 1024 * 1024 * 1024) == "2.0 GB"


def test_data_inventory_kpis():
    from ui.view_models import data_inventory_kpis

    inventory = {"num_days": 365, "num_months": 12, "num_years": 1, "total_size_bytes": 500 * 1024 * 1024}
    cards = data_inventory_kpis(inventory)
    assert len(cards) == 4
    assert cards[0]["value"] == "365"
    assert cards[1]["value"] == "12"
    assert cards[2]["value"] == "1"
    assert "MB" in cards[3]["value"]


def test_missing_days_kpis_with_gaps():
    from ui.view_models import missing_days_kpis

    cards = missing_days_kpis(["2026-01-02", "2026-01-04"])
    assert cards[0]["value"] == "2"
    assert cards[0]["color"] == "text-red-600"
    assert cards[1]["value"] == "2026-01-04"


def test_missing_days_kpis_no_gaps():
    from ui.view_models import missing_days_kpis

    cards = missing_days_kpis([])
    assert cards[0]["value"] == "0"
    assert cards[0]["color"] == "text-green-600"
    assert cards[1]["value"] == "-"


def test_latest_day_kpis():
    stats = {
        "max_date": date(2026, 3, 22),
        "unique_stations": 12000,
        "unique_provinces": 52,
        "unique_communities": 19,
        "unique_localities": 8500,
        "unique_fuel_types": 10,
    }
    cards = latest_day_kpis(stats)
    assert len(cards) == 6
    assert cards[0]["value"] == "2026-03-22"
    assert cards[1]["value"] == "12000"
    assert cards[2]["value"] == "52"
    assert cards[3]["value"] == "19"
    assert cards[4]["value"] == "8500"
    assert cards[5]["value"] == "10"


def test_latest_day_kpis_no_data():
    stats = {
        "max_date": None,
        "unique_stations": 0,
        "unique_provinces": 0,
        "unique_communities": 0,
        "unique_localities": 0,
        "unique_fuel_types": 0,
    }
    cards = latest_day_kpis(stats)
    assert cards[0]["value"] == "-"
    assert cards[1]["value"] == "0"


def test_best_day_advice_returns_none_for_tiny_diff():
    df = pd.DataFrame(
        {
            "day_of_week": list(range(7)),
            "avg_price": [1.500] * 7,
            "count_days": [10] * 7,
            "min_daily_avg": [1.50] * 7,
            "max_daily_avg": [1.50] * 7,
        }
    )
    assert best_day_advice(df) is None


def test_volatility_kpis_populated():
    df = pd.DataFrame(
        {
            "zip_code": ["28001", "41001", "50001"],
            "province": ["madrid", "sevilla", "zaragoza"],
            "coefficient_of_variation": [0.0010, 0.0040, 0.0020],
        }
    )

    cards = volatility_kpis(df)

    assert len(cards) == 4
    assert cards[0]["value"] == "28001"
    assert "Madrid" in cards[0]["description"]
    assert cards[3]["value"] == "3"


def test_volatility_kpis_empty():
    assert volatility_kpis(pd.DataFrame()) == []
