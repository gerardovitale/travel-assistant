from unittest.mock import patch

import pandas as pd
import pytest
from api.schemas import BrandKind
from api.schemas import BrandReportFuelType
from api.schemas import Region
from services.brand_service import get_brand_scorecard

GAS = BrandReportFuelType.gasoline_95_e5_price


def _row(province, brand, stations, station_days, price_gap_sum, zip_days, below, wins, fuel="gasoline_95_e5"):
    return {
        "province": province,
        "brand": brand,
        "fuel_type": fuel,
        "stations": stations,
        "station_days": station_days,
        "price_gap_sum": price_gap_sum,
        "zip_days": zip_days,
        "below_market_days": below,
        "win_days": wins,
        "window_start": "2026-05-18",
        "window_end": "2026-08-15",
        "days_covered": 90,
    }


REPORT = pd.DataFrame(
    [
        # Ballenoil: 10 + 2 stations on the peninsula, 5 on the islands.
        _row("madrid", "ballenoil", 10, 900, -90.0, 450, 400, 300),
        _row("soria", "ballenoil", 2, 100, -30.0, 50, 50, 50),
        _row("balears (illes)", "ballenoil", 5, 400, -8.0, 200, 120, 40),
        _row("madrid", "repsol", 40, 3000, 150.0, 1000, 100, 50),
        # Too few zip-days with competitors to rank.
        _row("madrid", "costco", 12, 20, -4.0, 20, 20, 20),
        _row("madrid", "ballenoil", 10, 900, -72.0, 450, 400, 300, fuel="diesel_a"),
    ]
)


@pytest.fixture
def report():
    with patch("services.brand_service.download_aggregate", return_value=REPORT) as download:
        yield download


def test_a_region_sums_its_provinces_before_dividing(report):
    scorecard = get_brand_scorecard([GAS], region=Region.peninsula)

    brands = {b.brand: b for b in scorecard.fuels[GAS.value]}
    assert list(brands) == ["ballenoil", "repsol"]  # cheapest first; costco lacks zip-days
    ballenoil = brands["ballenoil"]
    assert ballenoil.stations == 12
    assert ballenoil.price_gap == -0.12  # (-90 - 30) / (900 + 100), weighted by station-days
    assert ballenoil.below_market_pct == 90.0
    assert ballenoil.win_pct == 70.0
    assert ballenoil.kind == BrandKind.low_cost and ballenoil.name == "Ballenoil"
    assert (scorecard.window_start, scorecard.window_end, scorecard.days_covered) == ("2026-05-18", "2026-08-15", 90)


def test_islands_are_their_own_scope_with_the_region_station_minimum(report):
    # Five island stations are below the 10 a region needs.
    assert get_brand_scorecard([GAS], region=Region.special).fuels[GAS.value] == []


def test_a_province_matches_its_map_name_and_needs_fewer_stations(report):
    scorecard = get_brand_scorecard([GAS], province="Illes Balears")

    [ballenoil] = scorecard.fuels[GAS.value]
    assert ballenoil.stations == 5
    assert ballenoil.price_gap == -0.02


def test_each_fuel_is_scored_from_its_own_rows(report):
    scorecard = get_brand_scorecard([GAS, BrandReportFuelType.diesel_a_price], province="madrid")

    assert [b.brand for b in scorecard.fuels["diesel_a_price"]] == ["ballenoil"]
    assert scorecard.fuels["diesel_a_price"][0].price_gap == -0.08


def test_missing_report_returns_none():
    with patch("services.brand_service.download_aggregate", return_value=None):
        assert get_brand_scorecard([GAS]) is None
