from unittest.mock import patch

import pandas as pd
import pytest
from services.vehicle_cost_service import get_breakeven_by_province
from services.vehicle_cost_service import get_breakeven_history
from services.vehicle_cost_service import get_pair_breakeven
from services.vehicle_cost_service import get_vehicle_costs

# VW Golf (IDAE WLTP, MY26): 5.4 l/100km gasoline, 4.4 l/100km diesel -> breakeven ratio 1.2272...
PAIR_ID = "vw-golf"
GASOLINE_ID = "vw-golf-gasoline"
DIESEL_ID = "vw-golf-diesel"
GASOLINE_CONSUMPTION = 5.4
DIESEL_CONSUMPTION = 4.4
BREAKEVEN_RATIO = GASOLINE_CONSUMPTION / DIESEL_CONSUMPTION

GASOLINE_COLUMN = "gasoline_95_e5_price"
DIESEL_COLUMN = "diesel_a_price"


def _prices(rows: list[tuple[str, str, str, float, int]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["date", "province", "fuel_type", "avg_price", "station_count"])


def _flat_prices(gasoline: float, diesel: float, province: str = "madrid", date: str = "2026-08-01") -> pd.DataFrame:
    return _prices(
        [
            (date, province, GASOLINE_COLUMN, gasoline, 100),
            (date, province, DIESEL_COLUMN, diesel, 100),
        ]
    )


def _patch_prices(df: pd.DataFrame | None):
    return patch("services.vehicle_cost_service.download_aggregate", return_value=df)


# --- get_vehicle_costs -----------------------------------------------------------------


def test_vehicle_costs_multiplies_consumption_by_price():
    with _patch_prices(_flat_prices(gasoline=1.50, diesel=1.45)):
        rows = get_vehicle_costs([GASOLINE_ID, DIESEL_ID], province="madrid")

    by_id = {r["vehicle_id"]: r for r in rows}
    assert by_id[GASOLINE_ID]["cost_per_100km"] == pytest.approx(GASOLINE_CONSUMPTION * 1.50, abs=0.01)
    assert by_id[DIESEL_ID]["cost_per_100km"] == pytest.approx(DIESEL_CONSUMPTION * 1.45, abs=0.01)
    assert by_id[DIESEL_ID]["price_per_unit"] == pytest.approx(1.45)
    assert by_id[DIESEL_ID]["price_date"] == "2026-08-01"


def test_vehicle_costs_skips_unknown_vehicle_ids():
    with _patch_prices(_flat_prices(gasoline=1.50, diesel=1.45)):
        rows = get_vehicle_costs(["does-not-exist", GASOLINE_ID])
    assert [r["vehicle_id"] for r in rows] == [GASOLINE_ID]


def test_vehicle_costs_uses_latest_date():
    prices = pd.concat(
        [_flat_prices(1.40, 1.30, date="2026-07-01"), _flat_prices(1.60, 1.50, date="2026-08-01")],
        ignore_index=True,
    )
    with _patch_prices(prices):
        rows = get_vehicle_costs([GASOLINE_ID], province="madrid")
    assert rows[0]["price_per_unit"] == pytest.approx(1.60)


def test_vehicle_costs_national_average_is_station_count_weighted():
    prices = _prices(
        [
            ("2026-08-01", "madrid", GASOLINE_COLUMN, 1.50, 300),
            ("2026-08-01", "soria", GASOLINE_COLUMN, 1.70, 100),
        ]
    )
    with _patch_prices(prices):
        rows = get_vehicle_costs([GASOLINE_ID], province=None)
    # (1.50*300 + 1.70*100) / 400 = 1.55, not the unweighted 1.60
    assert rows[0]["price_per_unit"] == pytest.approx(1.55)


def test_vehicle_costs_returns_none_when_aggregate_missing():
    with _patch_prices(None):
        assert get_vehicle_costs([GASOLINE_ID]) is None


def test_vehicle_costs_returns_none_when_aggregate_empty():
    with _patch_prices(pd.DataFrame()):
        assert get_vehicle_costs([GASOLINE_ID]) is None


def test_vehicle_costs_reports_unpriceable_vehicle_without_crashing():
    # Province with no diesel row at all -> price is None, the row still describes the vehicle.
    prices = _prices([("2026-08-01", "madrid", GASOLINE_COLUMN, 1.50, 100)])
    with _patch_prices(prices):
        rows = get_vehicle_costs([DIESEL_ID], province="madrid")
    assert rows[0]["cost_per_100km"] is None
    assert rows[0]["consumption"] == DIESEL_CONSUMPTION


# --- get_pair_breakeven ----------------------------------------------------------------


def test_pair_breakeven_ratio_comes_from_consumption():
    with _patch_prices(_flat_prices(gasoline=1.50, diesel=1.45)):
        result = get_pair_breakeven(PAIR_ID, province="madrid")
    assert result["breakeven_ratio"] == pytest.approx(BREAKEVEN_RATIO, abs=0.0001)


def test_pair_breakeven_diesel_price_is_where_costs_meet():
    with _patch_prices(_flat_prices(gasoline=1.50, diesel=1.45)):
        result = get_pair_breakeven(PAIR_ID, province="madrid")
    breakeven_price = result["breakeven_diesel_price"]
    assert breakeven_price == pytest.approx(1.50 * BREAKEVEN_RATIO, abs=0.001)
    # At that price the two vehicles cost the same per 100 km.
    assert GASOLINE_CONSUMPTION * 1.50 == pytest.approx(DIESEL_CONSUMPTION * breakeven_price, abs=0.01)


def test_pair_breakeven_diesel_wins_when_price_ratio_below_breakeven():
    with _patch_prices(_flat_prices(gasoline=1.50, diesel=1.45)):
        result = get_pair_breakeven(PAIR_ID, province="madrid")
    assert result["winner"] == "diesel"
    assert result["margin_pct"] > 0
    assert result["diesel"]["cost_per_100km"] < result["gasoline"]["cost_per_100km"]


def test_pair_breakeven_gasoline_wins_when_diesel_price_above_breakeven():
    with _patch_prices(_flat_prices(gasoline=1.50, diesel=1.95)):
        result = get_pair_breakeven(PAIR_ID, province="madrid")
    assert result["winner"] == "gasoline"
    assert result["margin_pct"] < 0


def test_pair_breakeven_reports_tie_inside_the_noise_band():
    # Diesel priced exactly at breakeven: the honest answer is "no diferencia", not a winner.
    with _patch_prices(_flat_prices(gasoline=1.50, diesel=1.50 * BREAKEVEN_RATIO)):
        result = get_pair_breakeven(PAIR_ID, province="madrid")
    assert result["winner"] == "tie"
    assert result["margin_pct"] == pytest.approx(0.0, abs=0.1)


def test_pair_breakeven_reports_headroom_and_real_world_consumption():
    with _patch_prices(_flat_prices(gasoline=1.50, diesel=1.45)):
        result = get_pair_breakeven(PAIR_ID, province="madrid")
    assert result["diesel_headroom_eur_l"] == pytest.approx(1.50 * BREAKEVEN_RATIO - 1.45, abs=0.001)
    assert result["breakeven_diesel_consumption"] == pytest.approx(GASOLINE_CONSUMPTION * 1.50 / 1.45, abs=0.01)


def test_pair_breakeven_returns_none_for_unknown_pair():
    with _patch_prices(_flat_prices(gasoline=1.50, diesel=1.45)):
        assert get_pair_breakeven("does-not-exist", province="madrid") is None


def test_pair_breakeven_returns_none_when_province_has_no_prices():
    with _patch_prices(_flat_prices(gasoline=1.50, diesel=1.45, province="madrid")):
        assert get_pair_breakeven(PAIR_ID, province="soria") is None


def test_pair_breakeven_returns_none_when_aggregate_missing():
    with _patch_prices(None):
        assert get_pair_breakeven(PAIR_ID) is None


# --- get_breakeven_by_province ---------------------------------------------------------


def test_breakeven_by_province_ranks_by_margin_descending():
    prices = pd.concat(
        [
            _flat_prices(1.50, 1.45, province="madrid"),
            _flat_prices(1.50, 1.95, province="soria"),
            _flat_prices(1.50, 1.60, province="cadiz"),
        ],
        ignore_index=True,
    )
    with _patch_prices(prices):
        result = get_breakeven_by_province(PAIR_ID)

    rows = result["rows"]
    assert [r["province"] for r in rows] == ["madrid", "cadiz", "soria"]
    assert rows[0]["winner"] == "diesel"
    assert rows[-1]["winner"] == "gasoline"
    margins = [r["margin_pct"] for r in rows]
    assert margins == sorted(margins, reverse=True)


def test_breakeven_by_province_headroom_sign_matches_winner():
    prices = pd.concat(
        [_flat_prices(1.50, 1.45, province="madrid"), _flat_prices(1.50, 1.95, province="soria")],
        ignore_index=True,
    )
    with _patch_prices(prices):
        rows = {r["province"]: r for r in get_breakeven_by_province(PAIR_ID)["rows"]}

    assert rows["madrid"]["diesel_headroom_eur_l"] > 0
    assert rows["soria"]["diesel_headroom_eur_l"] < 0


def test_breakeven_by_province_reports_breakeven_real_world_consumption():
    with _patch_prices(_flat_prices(1.50, 1.45, province="madrid")):
        row = get_breakeven_by_province(PAIR_ID)["rows"][0]
    # Diesel ties once it actually burns c_g * P_g / P_d litres.
    expected = GASOLINE_CONSUMPTION * 1.50 / 1.45
    assert row["breakeven_diesel_consumption"] == pytest.approx(expected, abs=0.01)
    assert row["breakeven_diesel_consumption"] > DIESEL_CONSUMPTION


def test_breakeven_by_province_uses_latest_day_per_province():
    prices = pd.concat(
        [
            _flat_prices(1.40, 1.30, province="madrid", date="2026-07-01"),
            _flat_prices(1.60, 1.50, province="madrid", date="2026-08-01"),
        ],
        ignore_index=True,
    )
    with _patch_prices(prices):
        row = get_breakeven_by_province(PAIR_ID)["rows"][0]
    assert row["diesel_price"] == pytest.approx(1.50)
    assert row["price_date"] == "2026-08-01"


def test_breakeven_by_province_drops_and_counts_provinces_missing_a_fuel():
    prices = pd.concat(
        [
            _flat_prices(1.50, 1.45, province="madrid"),
            _prices([("2026-08-01", "soria", GASOLINE_COLUMN, 1.60, 50)]),
        ],
        ignore_index=True,
    )
    with _patch_prices(prices):
        result = get_breakeven_by_province(PAIR_ID)
    assert [r["province"] for r in result["rows"]] == ["madrid"]
    assert result["provinces_dropped"] == 1


def test_breakeven_by_province_excludes_non_mainland_by_default():
    prices = pd.concat(
        [
            _flat_prices(1.50, 1.45, province="madrid"),
            _flat_prices(1.30, 1.20, province="santa cruz de tenerife"),
        ],
        ignore_index=True,
    )
    with _patch_prices(prices):
        mainland = get_breakeven_by_province(PAIR_ID)
        everywhere = get_breakeven_by_province(PAIR_ID, mainland_only=False)

    assert [r["province"] for r in mainland["rows"]] == ["madrid"]
    assert "santa cruz de tenerife" in [r["province"] for r in everywhere["rows"]]


def test_breakeven_by_province_reports_smallest_station_count():
    prices = _prices(
        [
            ("2026-08-01", "madrid", GASOLINE_COLUMN, 1.50, 300),
            ("2026-08-01", "madrid", DIESEL_COLUMN, 1.45, 120),
        ]
    )
    with _patch_prices(prices):
        row = get_breakeven_by_province(PAIR_ID)["rows"][0]
    assert row["station_count"] == 120


def test_breakeven_by_province_returns_none_for_unknown_pair():
    with _patch_prices(_flat_prices(1.50, 1.45)):
        assert get_breakeven_by_province("does-not-exist") is None


# --- get_breakeven_history -------------------------------------------------------------


def _history_prices(pairs: list[tuple[str, float, float]]) -> pd.DataFrame:
    return pd.concat(
        [_flat_prices(gasoline, diesel, date=date) for date, gasoline, diesel in pairs],
        ignore_index=True,
    )


def test_breakeven_history_counts_days_diesel_wins():
    prices = _history_prices(
        [
            ("2026-08-01", 1.50, 1.45),  # diesel wins
            ("2026-08-02", 1.50, 1.45),  # diesel wins
            ("2026-08-03", 1.50, 1.95),  # gasoline wins
            ("2026-08-04", 1.50, 1.95),  # gasoline wins
        ]
    )
    with _patch_prices(prices):
        result = get_breakeven_history(PAIR_ID, province="madrid")

    assert result["days"] == 4
    assert result["pct_days_diesel_wins"] == pytest.approx(50.0)
    assert result["breakeven_ratio"] == pytest.approx(BREAKEVEN_RATIO, abs=0.0001)


def test_breakeven_history_reports_crossovers_not_every_day():
    prices = _history_prices(
        [
            ("2026-08-01", 1.50, 1.45),
            ("2026-08-02", 1.50, 1.45),
            ("2026-08-03", 1.50, 1.95),
            ("2026-08-04", 1.50, 1.45),
        ]
    )
    with _patch_prices(prices):
        result = get_breakeven_history(PAIR_ID, province="madrid")

    assert result["crossovers"] == [
        {"date": "2026-08-03", "winner": "gasoline"},
        {"date": "2026-08-04", "winner": "diesel"},
    ]


def test_breakeven_history_has_no_crossover_when_verdict_never_flips():
    prices = _history_prices([("2026-08-01", 1.50, 1.45), ("2026-08-02", 1.50, 1.44)])
    with _patch_prices(prices):
        result = get_breakeven_history(PAIR_ID, province="madrid")
    assert result["crossovers"] == []
    assert result["flips"] == 0
    assert result["pct_days_diesel_wins"] == pytest.approx(100.0)


def test_breakeven_history_limits_to_requested_window():
    prices = _history_prices([("2026-06-01", 1.50, 1.95), ("2026-08-01", 1.50, 1.45), ("2026-08-02", 1.50, 1.45)])
    with _patch_prices(prices):
        result = get_breakeven_history(PAIR_ID, province="madrid", days=2)
    assert [point["date"] for point in result["series"]] == ["2026-08-01", "2026-08-02"]


def test_breakeven_history_series_carries_both_costs():
    with _patch_prices(_flat_prices(1.50, 1.45)):
        result = get_breakeven_history(PAIR_ID, province="madrid")
    point = result["series"][0]
    assert point["cost_gasoline_per_100km"] == pytest.approx(GASOLINE_CONSUMPTION * 1.50, abs=0.01)
    assert point["cost_diesel_per_100km"] == pytest.approx(DIESEL_CONSUMPTION * 1.45, abs=0.01)
    assert point["price_ratio"] == pytest.approx(1.45 / 1.50, abs=0.0001)


def test_breakeven_history_returns_none_for_unknown_pair():
    with _patch_prices(_flat_prices(1.50, 1.45)):
        assert get_breakeven_history("does-not-exist") is None


def test_breakeven_history_returns_none_when_a_fuel_is_missing():
    prices = _prices([("2026-08-01", "madrid", GASOLINE_COLUMN, 1.50, 100)])
    with _patch_prices(prices):
        assert get_breakeven_history(PAIR_ID, province="madrid") is None


# --- date alignment (regression) --------------------------------------------------------


def _stale_diesel_prices() -> pd.DataFrame:
    """Diesel stops reporting a month before gasoline does."""
    rows = []
    for day in pd.date_range("2026-07-01", "2026-07-31"):
        rows.append((day.date().isoformat(), "madrid", DIESEL_COLUMN, 1.45, 100))
    for day in pd.date_range("2026-07-01", "2026-08-30"):
        rows.append((day.date().isoformat(), "madrid", GASOLINE_COLUMN, 1.90, 100))
    return _prices(rows)


def test_pair_breakeven_uses_a_day_both_fuels_reported():
    with _patch_prices(_stale_diesel_prices()):
        result = get_pair_breakeven(PAIR_ID, province="madrid")
    # 2026-07-31 is the last day diesel reported; gasoline runs on to 08-30.
    assert result["price_date"] == "2026-07-31"
    assert result["gasoline"]["price_date"] == "2026-07-31"
    assert result["diesel"]["price_date"] == "2026-07-31"


def test_pair_breakeven_never_dates_itself_newer_than_the_stalest_fuel():
    with _patch_prices(_stale_diesel_prices()):
        result = get_pair_breakeven(PAIR_ID, province="madrid")
    assert result["price_date"] <= "2026-07-31"
    assert result["gasoline"]["price_date"] == result["diesel"]["price_date"]


def test_breakeven_by_province_uses_a_day_both_fuels_reported():
    with _patch_prices(_stale_diesel_prices()):
        row = get_breakeven_by_province(PAIR_ID)["rows"][0]
    assert row["price_date"] == "2026-07-31"
    # The gasoline price must be that day's, not the newer 08-30 reading.
    assert row["gasoline_price"] == pytest.approx(1.90)


def test_breakeven_by_province_drops_provinces_with_no_overlapping_day():
    # Each fuel reports, but never on the same day — there is nothing honest to compare.
    prices = _prices(
        [
            ("2026-08-01", "madrid", GASOLINE_COLUMN, 1.50, 100),
            ("2026-08-02", "madrid", DIESEL_COLUMN, 1.45, 100),
        ]
    )
    with _patch_prices(prices):
        result = get_breakeven_by_province(PAIR_ID)
    assert result["rows"] == []
    assert result["provinces_dropped"] == 1


def test_breakeven_by_province_picks_the_latest_overlapping_day_per_province():
    prices = pd.concat(
        [
            _flat_prices(1.40, 1.30, province="madrid", date="2026-07-01"),
            _flat_prices(1.60, 1.50, province="madrid", date="2026-08-01"),
            _prices([("2026-08-05", "madrid", GASOLINE_COLUMN, 1.99, 100)]),  # gasoline-only, ignored
        ],
        ignore_index=True,
    )
    with _patch_prices(prices):
        row = get_breakeven_by_province(PAIR_ID)["rows"][0]
    assert row["price_date"] == "2026-08-01"
    assert row["gasoline_price"] == pytest.approx(1.60)


def test_pair_breakeven_returns_none_when_fuels_never_overlap():
    prices = _prices(
        [
            ("2026-08-01", "madrid", GASOLINE_COLUMN, 1.50, 100),
            ("2026-08-02", "madrid", DIESEL_COLUMN, 1.45, 100),
        ]
    )
    with _patch_prices(prices):
        assert get_pair_breakeven(PAIR_ID, province="madrid") is None


def test_vehicle_costs_price_every_vehicle_on_one_shared_day():
    # These rows render beside the verdict card, so they must not mix days either.
    with _patch_prices(_stale_diesel_prices()):
        rows = get_vehicle_costs([GASOLINE_ID, DIESEL_ID], province="madrid")
    dates = {r["price_date"] for r in rows}
    assert dates == {"2026-07-31"}


def test_vehicle_costs_shared_day_matches_the_pair_verdict():
    with _patch_prices(_stale_diesel_prices()):
        rows = get_vehicle_costs([GASOLINE_ID, DIESEL_ID], province="madrid")
        verdict = get_pair_breakeven(PAIR_ID, province="madrid")
    assert {r["price_date"] for r in rows} == {verdict["price_date"]}


def test_vehicle_costs_single_vehicle_uses_its_own_latest_day():
    # Nothing to align against, so the freshest reading for that fuel is the honest one.
    with _patch_prices(_stale_diesel_prices()):
        rows = get_vehicle_costs([GASOLINE_ID], province="madrid")
    assert rows[0]["price_date"] == "2026-08-30"


def test_vehicle_costs_leave_prices_unset_when_fuels_never_overlap():
    prices = _prices(
        [
            ("2026-08-01", "madrid", GASOLINE_COLUMN, 1.50, 100),
            ("2026-08-02", "madrid", DIESEL_COLUMN, 1.45, 100),
        ]
    )
    with _patch_prices(prices):
        rows = get_vehicle_costs([GASOLINE_ID, DIESEL_ID], province="madrid")
    assert all(r["cost_per_100km"] is None for r in rows)


def test_vehicle_costs_unpriceable_vehicle_does_not_null_out_the_others():
    # Diesel has no rows at all here — the stand-in for an energy type with no price source, like
    # electricity. It must not be pulled into the date intersection and blank out the gasoline row.
    prices = _prices([("2026-08-01", "madrid", GASOLINE_COLUMN, 1.50, 100)])
    with _patch_prices(prices):
        rows = {r["vehicle_id"]: r for r in get_vehicle_costs([GASOLINE_ID, DIESEL_ID], province="madrid")}
    assert rows[GASOLINE_ID]["cost_per_100km"] == pytest.approx(GASOLINE_CONSUMPTION * 1.50, abs=0.01)
    assert rows[GASOLINE_ID]["price_date"] == "2026-08-01"
    assert rows[DIESEL_ID]["cost_per_100km"] is None


# --- tie band consistency (regression) --------------------------------------------------


def test_breakeven_history_applies_the_same_tie_band_as_the_verdict():
    # Diesel priced exactly at breakeven every day: the verdict card calls that a draw, so the
    # history must not simultaneously report it as a 100% diesel win streak.
    tie_price = 1.50 * BREAKEVEN_RATIO
    prices = _history_prices([("2026-08-01", 1.50, tie_price), ("2026-08-02", 1.50, tie_price)])
    with _patch_prices(prices):
        history = get_breakeven_history(PAIR_ID, province="madrid")
        verdict = get_pair_breakeven(PAIR_ID, province="madrid")

    assert verdict["winner"] == "tie"
    assert history["pct_days_diesel_wins"] == pytest.approx(0.0)
    assert history["pct_days_tie"] == pytest.approx(100.0)


def test_breakeven_history_percentages_account_for_every_day():
    prices = _history_prices(
        [
            ("2026-08-01", 1.50, 1.45),  # diesel
            ("2026-08-02", 1.50, 1.95),  # gasoline
            ("2026-08-03", 1.50, 1.50 * BREAKEVEN_RATIO),  # tie
        ]
    )
    with _patch_prices(prices):
        result = get_breakeven_history(PAIR_ID, province="madrid")

    gasoline_pct = 100.0 - result["pct_days_diesel_wins"] - result["pct_days_tie"]
    assert result["pct_days_diesel_wins"] == pytest.approx(33.3, abs=0.1)
    assert result["pct_days_tie"] == pytest.approx(33.3, abs=0.1)
    # The remainder is gasoline's; each figure is rounded to 1dp so the three need not sum exactly.
    assert gasoline_pct == pytest.approx(33.3, abs=0.2)


def test_breakeven_history_crossovers_track_ties_as_their_own_state():
    prices = _history_prices(
        [
            ("2026-08-01", 1.50, 1.45),  # diesel
            ("2026-08-02", 1.50, 1.50 * BREAKEVEN_RATIO),  # tie
            ("2026-08-03", 1.50, 1.95),  # gasoline
        ]
    )
    with _patch_prices(prices):
        result = get_breakeven_history(PAIR_ID, province="madrid")

    assert [c["winner"] for c in result["crossovers"]] == ["tie", "gasoline"]
    assert result["flips"] == 2
