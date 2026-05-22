from unittest import TestCase

import duckdb
import pandas as pd
from reports.brand_comparison import BRAND_COMPARISON_COLUMNS
from reports.brand_comparison import compute_brand_price_comparison


def _make_duckdb_con_with_data(rows):
    con = duckdb.connect()
    con.register("df", pd.DataFrame(rows))
    con.execute("create table fuel_prices as select * from df")
    return con


def _two_day_data(brand_prices, market_prices):
    """Build 2-day data: brand has brand_prices[i], market average is market_prices[i].

    One other station sets the market avg. brand_prices and market_prices are lists of length 2.
    For simplicity, zip_code=28001, one brand station, one 'other' station per day.
    The 'other' station price is computed to achieve the target market avg:
      market_avg = (brand_price + other_price) / 2
      other_price = 2 * market_avg - brand_price
    """
    rows = []
    for i, (bp, mp) in enumerate(zip(brand_prices, market_prices)):
        other_price = 2 * mp - bp
        dt = f"2026-01-{i + 1:02d}T05:00:00"
        rows += [
            {
                "timestamp": dt,
                "zip_code": "28001",
                "locality": "Centro",
                "municipality": "Madrid",
                "label": "testbrand",
                "gasoline_95_e5_price": bp,
            },
            {
                "timestamp": dt,
                "zip_code": "28001",
                "locality": "Centro",
                "municipality": "Madrid",
                "label": "other",
                "gasoline_95_e5_price": other_price,
            },
        ]
    return rows


class TestComputeBrandPriceComparison(TestCase):

    def test_returns_dataframe(self):
        rows = _two_day_data([1.40, 1.45], [1.50, 1.55])
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
                today="2026-01-03",
            )
            self.assertIsInstance(result, pd.DataFrame)
        finally:
            con.close()

    def test_output_has_correct_columns(self):
        rows = _two_day_data([1.40, 1.45], [1.50, 1.55])
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
                today="2026-01-03",
            )
            for col in BRAND_COMPARISON_COLUMNS:
                self.assertIn(col, result.columns)
        finally:
            con.close()

    def test_brand_cheaper_than_market_has_negative_delta(self):
        # Brand always below market → price_delta_pct should be negative
        rows = _two_day_data([1.40, 1.42], [1.50, 1.52])
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
                today="2026-01-03",
            )
            row = result[result["brand"] == "testbrand"]
            self.assertEqual(len(row), 1)
            self.assertLess(row["price_delta_pct"].iloc[0], 0)
        finally:
            con.close()

    def test_brand_more_expensive_than_market_has_positive_delta(self):
        # Brand always above market → price_delta_pct should be positive
        rows = _two_day_data([1.60, 1.62], [1.50, 1.52])
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
                today="2026-01-03",
            )
            row = result[result["brand"] == "testbrand"]
            self.assertEqual(len(row), 1)
            self.assertGreater(row["price_delta_pct"].iloc[0], 0)
        finally:
            con.close()

    def test_days_below_market_100_when_always_cheaper(self):
        rows = _two_day_data([1.40, 1.42], [1.50, 1.52])
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
                today="2026-01-03",
            )
            row = result[result["brand"] == "testbrand"]
            self.assertAlmostEqual(row["days_below_market_pct"].iloc[0], 100.0)
        finally:
            con.close()

    def test_days_below_market_0_when_always_more_expensive(self):
        rows = _two_day_data([1.60, 1.62], [1.50, 1.52])
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
                today="2026-01-03",
            )
            row = result[result["brand"] == "testbrand"]
            self.assertAlmostEqual(row["days_below_market_pct"].iloc[0], 0.0)
        finally:
            con.close()

    def test_min_appearances_filter(self):
        # 2 days of data; min_appearances=3 should exclude all rows
        rows = _two_day_data([1.40, 1.42], [1.50, 1.52])
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=3,
                today="2026-01-03",
            )
            self.assertEqual(len(result), 0)
        finally:
            con.close()

    def test_multiple_brands_returned(self):
        rows = []
        for i in range(12):
            dt = f"2026-01-{i + 1:02d}T05:00:00"
            rows += [
                {
                    "timestamp": dt,
                    "zip_code": "28001",
                    "locality": "Centro",
                    "municipality": "Madrid",
                    "label": "brandalpha",
                    "gasoline_95_e5_price": 1.40,
                },
                {
                    "timestamp": dt,
                    "zip_code": "28001",
                    "locality": "Centro",
                    "municipality": "Madrid",
                    "label": "brandbeta",
                    "gasoline_95_e5_price": 1.60,
                },
                {
                    "timestamp": dt,
                    "zip_code": "28001",
                    "locality": "Centro",
                    "municipality": "Madrid",
                    "label": "other",
                    "gasoline_95_e5_price": 1.50,
                },
            ]
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["brandalpha", "brandbeta"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=10,
                today="2026-01-13",
            )
            brands_in_result = set(result["brand"].unique())
            self.assertIn("brandalpha", brands_in_result)
            self.assertIn("brandbeta", brands_in_result)
        finally:
            con.close()

    def test_fuel_type_suffix_stripped(self):
        rows = _two_day_data([1.40, 1.42], [1.50, 1.52])
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
                today="2026-01-03",
            )
            self.assertIn("gasoline_95_e5", result["fuel_type"].values)
        finally:
            con.close()

    def test_returns_empty_dataframe_when_brand_absent(self):
        rows = _two_day_data([1.40, 1.42], [1.50, 1.52])
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["costco"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
                today="2026-01-03",
            )
            self.assertEqual(len(result), 0)
            for col in BRAND_COMPARISON_COLUMNS:
                self.assertIn(col, result.columns)
        finally:
            con.close()

    def test_price_delta_calculation(self):
        # brand_avg=1.40, market_avg=1.50 → delta = (1.40-1.50)/1.50*100 = -6.67%
        rows = []
        for i in range(15):
            dt = f"2026-01-{i + 1:02d}T05:00:00"
            rows += [
                {
                    "timestamp": dt,
                    "zip_code": "28001",
                    "locality": "C",
                    "municipality": "M",
                    "label": "testbrand",
                    "gasoline_95_e5_price": 1.40,
                },
                {
                    "timestamp": dt,
                    "zip_code": "28001",
                    "locality": "C",
                    "municipality": "M",
                    "label": "other",
                    "gasoline_95_e5_price": 1.60,
                },
            ]
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=10,
                today="2026-01-16",
            )
            row = result[result["brand"] == "testbrand"]
            self.assertEqual(len(row), 1)
            # brand=1.40, market=(1.40+1.60)/2=1.50 → delta=(1.40-1.50)/1.50*100 ≈ -6.67
            self.assertAlmostEqual(row["price_delta_pct"].iloc[0], -6.67, places=1)
        finally:
            con.close()
