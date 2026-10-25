import duckdb
import pandas as pd
from aggregator.reports.brand_scorecard import BRAND_SCORECARD_COLUMNS
from aggregator.reports.brand_scorecard import compute_brand_scorecard

FUEL = "gasoline_95_e5_price"


def _station(day, zip_code, eess_id, label, price, province="madrid", sale_type="p"):
    return {
        "timestamp": f"2026-08-{day:02d}T06:00:00+00:00",
        "zip_code": zip_code,
        "province": province,
        "eess_id": eess_id,
        "label": label,
        "sale_type": sale_type,
        FUEL: price,
    }


def _con(rows):
    con = duckdb.connect()
    con.register("df", pd.DataFrame(rows))
    con.execute("create table fuel_prices as select * from df")
    return con


def _scorecard(rows, **kwargs):
    kwargs.setdefault("min_national_stations", 1)
    return compute_brand_scorecard(_con(rows), fuel_cols=[FUEL], **kwargs).set_index("brand")


def test_price_gap_is_station_weighted_against_the_rest_of_the_zip():
    rows = [
        # Two cheap stations at 1.40 against one rival at 1.70: the brand never counts in its own market.
        _station(1, "28001", "1", "Cheap", 1.40),
        _station(1, "28001", "2", "Cheap", 1.40),
        _station(1, "28001", "3", "Rival", 1.70),
        # Alone in its zip: no market to beat, so it adds a station but no comparison.
        _station(1, "28002", "4", "Cheap", 1.20),
        # Restricted-sale stations are not open to drivers and never set the market.
        _station(1, "28001", "5", "Fleet", 1.00, sale_type="r"),
        # Without a province the dashboard could not scope the row, so it is left out.
        _station(1, "28004", "6", "Nowhere", 1.30, province=None),
        _station(1, "28004", "7", "Rival", 1.50, province=None),
    ]
    result = _scorecard(rows)

    cheap = result.loc["cheap"]
    assert cheap["stations"] == 3
    assert cheap["station_days"] == 2
    assert cheap["zip_days"] == 1
    assert round(cheap["price_gap_sum"] / cheap["station_days"], 4) == -0.30
    assert cheap["below_market_days"] == 1
    assert cheap["win_days"] == 1
    assert "fleet" not in result.index
    assert "nowhere" not in result.index


def test_ties_split_the_win_and_unbranded_stations_compete():
    rows = [
        _station(1, "28001", "1", "Alpha", 1.40),
        _station(1, "28001", "2", "Beta", 1.40),
        _station(1, "28001", "3", "Gamma", 1.60),
        # An unlabelled station is the cheapest here, so no brand wins this zip.
        _station(1, "28003", "4", "Alpha", 1.50),
        _station(1, "28003", "5", "Nº 10.935", 1.45),
    ]
    result = _scorecard(rows)

    assert result.loc["alpha", "win_days"] == 0.5
    assert result.loc["alpha", "zip_days"] == 2
    assert result.loc["beta", "win_days"] == 0.5
    assert result.loc["gamma", "win_days"] == 0


def test_only_the_last_window_days_count_and_stations_come_from_the_last_day():
    rows = [
        # Before the 3-day window ending on 2026-08-10: ignored.
        _station(7, "28001", "1", "Brand", 1.00),
        _station(7, "28001", "2", "Rival", 2.00),
        _station(9, "28001", "1", "Brand", 1.40),
        _station(9, "28001", "2", "Rival", 1.60),
        # A station that closed before the last day is not in the footprint.
        _station(9, "08001", "3", "Brand", 1.40, province="barcelona"),
        _station(9, "08001", "4", "Rival", 1.60, province="barcelona"),
        _station(10, "28001", "1", "Brand", 1.40),
        _station(10, "28001", "2", "Rival", 1.60),
    ]
    result = compute_brand_scorecard(_con(rows), fuel_cols=[FUEL], window_days=3, min_national_stations=1)

    brand = result[result["brand"] == "brand"].set_index("province")
    assert brand.loc["madrid", "zip_days"] == 2
    assert round(brand.loc["madrid", "price_gap_sum"], 4) == -0.4
    assert brand.loc["madrid", "stations"] == 1
    assert brand.loc["barcelona", "stations"] == 0
    assert brand.loc["barcelona", "zip_days"] == 1
    assert set(result["window_start"]) == {"2026-08-08"}
    assert set(result["window_end"]) == {"2026-08-10"}
    assert set(result["days_covered"]) == {2}


def test_brands_below_the_national_station_minimum_are_dropped():
    rows = [
        _station(1, "28001", "1", "Chain", 1.40),
        _station(1, "08001", "2", "Chain", 1.40, province="barcelona"),
        _station(1, "28001", "3", "Solo", 1.60),
    ]
    result = compute_brand_scorecard(_con(rows), fuel_cols=[FUEL], min_national_stations=2)

    assert set(result["brand"]) == {"chain"}
    assert list(result.columns) == BRAND_SCORECARD_COLUMNS


def test_empty_input_keeps_the_contract_columns():
    con = duckdb.connect()
    con.execute(
        "create table fuel_prices (timestamp varchar, zip_code varchar, province varchar, eess_id varchar,"
        " label varchar, sale_type varchar, gasoline_95_e5_price double)"
    )
    result = compute_brand_scorecard(con, fuel_cols=[FUEL])
    assert result.empty
    assert list(result.columns) == BRAND_SCORECARD_COLUMNS


def test_a_chain_owning_most_of_a_zip_is_compared_with_its_rivals():
    rows = [_station(1, "28001", str(i), "Big", 1.60) for i in range(3)] + [_station(1, "28001", "9", "Rival", 1.50)]
    result = _scorecard(rows)

    big = result.loc["big"]
    # An average including Big's own stations (1.575) would hide most of the 0.10 €/l gap.
    assert round(big["price_gap_sum"] / big["station_days"], 4) == 0.10
    assert big["below_market_days"] == 0
