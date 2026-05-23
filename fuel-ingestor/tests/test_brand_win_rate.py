from datetime import date
from datetime import timedelta
from unittest import TestCase

import duckdb
import pandas as pd
from aggregator.reports.brand_win_rate import BRAND_WIN_RATE_COLUMNS
from aggregator.reports.brand_win_rate import compute_brand_win_rate


def _make_duckdb_con():
    """Return an in-memory DuckDB connection pre-loaded with 2-day test data.

    Day 1: ballenoil cheapest + repsol priciest in 28001
    Day 2: repsol cheapest + ballenoil priciest in 28001 (roles flipped)
    Both brands appear every day → 2 appearances each.
    """
    rows = [
        # Day 1
        {
            "timestamp": "2026-01-01T05:00:00",
            "zip_code": "28001",
            "locality": "Centro",
            "municipality": "Madrid",
            "label": "ballenoil",
            "gasoline_95_e5_price": 1.40,
            "diesel_a_price": 1.30,
        },
        {
            "timestamp": "2026-01-01T05:00:00",
            "zip_code": "28001",
            "locality": "Centro",
            "municipality": "Madrid",
            "label": "repsol",
            "gasoline_95_e5_price": 1.60,
            "diesel_a_price": 1.50,
        },
        {
            "timestamp": "2026-01-01T05:00:00",
            "zip_code": "28001",
            "locality": "Centro",
            "municipality": "Madrid",
            "label": "other",
            "gasoline_95_e5_price": 1.50,
            "diesel_a_price": 1.40,
        },
        # Day 2 — roles flipped
        {
            "timestamp": "2026-01-02T05:00:00",
            "zip_code": "28001",
            "locality": "Centro",
            "municipality": "Madrid",
            "label": "ballenoil",
            "gasoline_95_e5_price": 1.60,
            "diesel_a_price": 1.50,
        },
        {
            "timestamp": "2026-01-02T05:00:00",
            "zip_code": "28001",
            "locality": "Centro",
            "municipality": "Madrid",
            "label": "repsol",
            "gasoline_95_e5_price": 1.40,
            "diesel_a_price": 1.30,
        },
        {
            "timestamp": "2026-01-02T05:00:00",
            "zip_code": "28001",
            "locality": "Centro",
            "municipality": "Madrid",
            "label": "other",
            "gasoline_95_e5_price": 1.50,
            "diesel_a_price": 1.40,
        },
    ]
    df = pd.DataFrame(rows)
    con = duckdb.connect()
    con.register("fuel_prices", df)
    # brand_win_rate uses a persisted table
    con.execute("create table fuel_prices as select * from fuel_prices")
    return con


class TestComputeBrandWinRate(TestCase):

    def setUp(self):
        self.con = _make_duckdb_con()

    def tearDown(self):
        self.con.close()

    def test_returns_dataframe(self):
        result = compute_brand_win_rate(
            self.con,
            brands=["ballenoil", "repsol"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            directions=["cheapest"],
            min_appearances=1,
        )
        self.assertIsInstance(result, pd.DataFrame)

    def test_output_has_correct_columns(self):
        result = compute_brand_win_rate(
            self.con,
            brands=["ballenoil"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            directions=["cheapest"],
            min_appearances=1,
        )
        for col in BRAND_WIN_RATE_COLUMNS:
            self.assertIn(col, result.columns)

    def test_cheapest_direction_win_rate_50_percent(self):
        # Day 1: ballenoil cheapest; Day 2: ballenoil NOT cheapest → 50% win rate
        result = compute_brand_win_rate(
            self.con,
            brands=["ballenoil"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            directions=["cheapest"],
            min_appearances=1,
        )
        row = result[(result["brand"] == "ballenoil") & (result["direction"] == "cheapest")]
        self.assertEqual(len(row), 1)
        self.assertAlmostEqual(row["win_rate_pct"].iloc[0], 50.0)

    def test_priciest_direction_win_rate_50_percent(self):
        # Day 1: repsol priciest; Day 2: repsol NOT priciest → 50% priciest win rate
        result = compute_brand_win_rate(
            self.con,
            brands=["repsol"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            directions=["priciest"],
            min_appearances=1,
        )
        row = result[(result["brand"] == "repsol") & (result["direction"] == "priciest")]
        self.assertEqual(len(row), 1)
        self.assertAlmostEqual(row["win_rate_pct"].iloc[0], 50.0)

    def test_both_directions_returned_when_requested(self):
        result = compute_brand_win_rate(
            self.con,
            brands=["ballenoil"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            directions=["cheapest", "priciest"],
            min_appearances=1,
        )
        self.assertIn("cheapest", result["direction"].values)
        self.assertIn("priciest", result["direction"].values)

    def test_min_appearances_filter_excludes_low_count_rows(self):
        # With min_appearances=3, both brands have only 2 days → empty result
        result = compute_brand_win_rate(
            self.con,
            brands=["ballenoil", "repsol"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            directions=["cheapest"],
            min_appearances=3,
        )
        self.assertEqual(len(result), 0)

    def test_fuel_type_stripped_of_price_suffix(self):
        result = compute_brand_win_rate(
            self.con,
            brands=["ballenoil"],
            fuel_cols=["gasoline_95_e5_price", "diesel_a_price"],
            geo_cols=["zip_code"],
            directions=["cheapest"],
            min_appearances=1,
        )
        self.assertIn("gasoline_95_e5", result["fuel_type"].values)
        self.assertIn("diesel_a", result["fuel_type"].values)

    def test_last_updated_equals_max_data_date(self):
        # last_updated must reflect the latest date in the data (2026-01-02), not the run date
        result = compute_brand_win_rate(
            self.con,
            brands=["ballenoil"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            directions=["cheapest"],
            min_appearances=1,
        )
        self.assertTrue((result["last_updated"] == "2026-01-02").all())

    def test_returns_empty_dataframe_when_no_brands_match(self):
        result = compute_brand_win_rate(
            self.con,
            brands=["nonexistent_brand"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            directions=["cheapest"],
            min_appearances=1,
        )
        self.assertEqual(len(result), 0)
        for col in BRAND_WIN_RATE_COLUMNS:
            self.assertIn(col, result.columns)

    def test_priciest_100_when_always_most_expensive(self):
        # Build a dataset where ballenoil is ALWAYS the most expensive on every day
        base = date(2026, 1, 1)
        rows = []
        for day in range(35):
            dt = (base + timedelta(days=day)).isoformat() + "T05:00:00"
            rows += [
                {
                    "timestamp": dt,
                    "zip_code": "99001",
                    "locality": "Test",
                    "municipality": "Test",
                    "label": "ballenoil",
                    "gasoline_95_e5_price": 2.00,
                },
                {
                    "timestamp": dt,
                    "zip_code": "99001",
                    "locality": "Test",
                    "municipality": "Test",
                    "label": "other",
                    "gasoline_95_e5_price": 1.50,
                },
            ]
        con = duckdb.connect()
        con.register("fuel_prices", pd.DataFrame(rows))
        con.execute("create table fuel_prices as select * from fuel_prices")
        try:
            result = compute_brand_win_rate(
                con,
                brands=["ballenoil"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                directions=["priciest"],
                min_appearances=30,
            )
            row = result[(result["brand"] == "ballenoil") & (result["direction"] == "priciest")]
            self.assertEqual(len(row), 1)
            self.assertAlmostEqual(row["win_rate_pct"].iloc[0], 100.0)
        finally:
            con.close()

    def test_tie_win_credit_split_equally(self):
        # Both ballenoil and repsol have the same price every day → they always tie.
        # Each brand should receive 50% win credit per day, not 100%.
        # Sum of win_rate_pct across both brands must equal 100, not 200.
        rows = [
            {
                "timestamp": f"2026-01-0{d}T05:00:00",
                "zip_code": "11001",
                "locality": "Tie",
                "municipality": "Tie",
                "label": brand,
                "gasoline_95_e5_price": 1.40,
            }
            for d in range(1, 6)
            for brand in ["ballenoil", "repsol"]
        ]
        con = duckdb.connect()
        con.register("fuel_prices", pd.DataFrame(rows))
        con.execute("create table fuel_prices as select * from fuel_prices")
        try:
            result = compute_brand_win_rate(
                con,
                brands=["ballenoil", "repsol"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                directions=["cheapest"],
                min_appearances=1,
            )
            subset = result[result["direction"] == "cheapest"]
            total_win_rate = subset["win_rate_pct"].sum()
            # Each brand gets 50%; sum must be 100, not 200
            self.assertAlmostEqual(total_win_rate, 100.0, places=1)
            for _, row in subset.iterrows():
                self.assertAlmostEqual(row["win_rate_pct"], 50.0, places=1)
        finally:
            con.close()

    def test_zero_price_records_excluded_from_boundary(self):
        # A station with price=0.0 must NOT drive boundary_price to 0 for cheapest direction.
        # ballenoil at 1.40 should still win cheapest (boundary = 1.40 after filtering zeros).
        rows = [
            {
                "timestamp": "2026-01-01T05:00:00",
                "zip_code": "22001",
                "locality": "ZeroTest",
                "municipality": "ZeroTest",
                "label": "ballenoil",
                "gasoline_95_e5_price": 1.40,
            },
            {
                "timestamp": "2026-01-01T05:00:00",
                "zip_code": "22001",
                "locality": "ZeroTest",
                "municipality": "ZeroTest",
                "label": "other",
                "gasoline_95_e5_price": 1.60,
            },
            # zero-price record — should be excluded before boundary is computed
            {
                "timestamp": "2026-01-01T05:00:00",
                "zip_code": "22001",
                "locality": "ZeroTest",
                "municipality": "ZeroTest",
                "label": "ghost",
                "gasoline_95_e5_price": 0.0,
            },
        ]
        con = duckdb.connect()
        con.register("fuel_prices", pd.DataFrame(rows))
        con.execute("create table fuel_prices as select * from fuel_prices")
        try:
            result = compute_brand_win_rate(
                con,
                brands=["ballenoil"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                directions=["cheapest"],
                min_appearances=1,
            )
            row = result[result["brand"] == "ballenoil"]
            self.assertEqual(len(row), 1)
            self.assertAlmostEqual(row["win_rate_pct"].iloc[0], 100.0)
        finally:
            con.close()

    def test_multi_station_brand_appearances_counts_days_not_stations(self):
        # ballenoil has 3 stations in the same zip on each of 5 days.
        # appearances must be 5 (days), not 15 (station-days).
        rows = [
            {
                "timestamp": f"2026-01-0{d}T05:00:00",
                "zip_code": "33001",
                "locality": "Multi",
                "municipality": "Multi",
                "label": "ballenoil",
                "gasoline_95_e5_price": 1.40 + i * 0.01,  # slightly different prices per station
            }
            for d in range(1, 6)
            for i in range(3)  # 3 stations
        ]
        con = duckdb.connect()
        con.register("fuel_prices", pd.DataFrame(rows))
        con.execute("create table fuel_prices as select * from fuel_prices")
        try:
            result = compute_brand_win_rate(
                con,
                brands=["ballenoil"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                directions=["cheapest"],
                min_appearances=1,
            )
            row = result[result["brand"] == "ballenoil"]
            self.assertEqual(len(row), 1)
            self.assertEqual(row["appearances"].iloc[0], 5)
        finally:
            con.close()

    def test_confidence_level_column_present_and_low_for_small_sample(self):
        # With only 2 appearances, confidence_level must be 'low'
        result = compute_brand_win_rate(
            self.con,
            brands=["ballenoil"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            directions=["cheapest"],
            min_appearances=1,
        )
        self.assertIn("confidence_level", result.columns)
        row = result[result["brand"] == "ballenoil"]
        self.assertEqual(row["confidence_level"].iloc[0], "low")
