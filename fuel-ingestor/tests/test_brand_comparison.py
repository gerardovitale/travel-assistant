from unittest import TestCase

import duckdb
import pandas as pd
from aggregator.reports.brand_comparison import BRAND_COMPARISON_COLUMNS
from aggregator.reports.brand_comparison import compute_brand_price_comparison


def _make_duckdb_con_with_data(rows):
    con = duckdb.connect()
    con.register("df", pd.DataFrame(rows))
    con.execute("create table fuel_prices as select * from df")
    return con


def _make_empty_duckdb_con():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE fuel_prices (
            timestamp VARCHAR,
            zip_code VARCHAR,
            locality VARCHAR,
            municipality VARCHAR,
            label VARCHAR,
            gasoline_95_e5_price DOUBLE
        )
        """
    )
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
            )
            row = result[result["brand"] == "testbrand"]
            self.assertEqual(len(row), 1)
            # brand=1.40, market=(1.40+1.60)/2=1.50 → delta=(1.40-1.50)/1.50*100 ≈ -6.67
            self.assertAlmostEqual(row["price_delta_pct"].iloc[0], -6.67, places=1)
        finally:
            con.close()

    # --- new tests covering fixes from quant review ---

    def test_empty_table_returns_empty_dataframe_with_correct_columns(self):
        con = _make_empty_duckdb_con()
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
            )
            self.assertEqual(len(result), 0)
            for col in BRAND_COMPARISON_COLUMNS:
                self.assertIn(col, result.columns)
        finally:
            con.close()

    def test_zero_price_stations_excluded_from_market_average(self):
        # One zero-price row injected alongside normal rows; zero must not affect market_avg_price.
        rows = []
        for i in range(5):
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
                # zero-price sentinel — must be excluded from market average
                {
                    "timestamp": dt,
                    "zip_code": "28001",
                    "locality": "C",
                    "municipality": "M",
                    "label": "zeroed",
                    "gasoline_95_e5_price": 0.0,
                },
            ]
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
            )
            row = result[result["brand"] == "testbrand"]
            self.assertEqual(len(row), 1)
            # market_avg_price should be (1.40+1.60)/2=1.50, not (1.40+1.60+0.0)/3≈1.00
            self.assertAlmostEqual(row["market_avg_price"].iloc[0], 1.50, places=2)
        finally:
            con.close()

    def test_last_updated_equals_max_dt_in_data(self):
        # 5 days of data; last_updated must be the max date in the dataset, not the run date.
        rows = []
        for i in range(5):
            dt = f"2026-03-{i + 1:02d}T05:00:00"
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
                min_appearances=1,
            )
            row = result[result["brand"] == "testbrand"]
            self.assertEqual(len(row), 1)
            self.assertEqual(row["last_updated"].iloc[0], "2026-03-05")
        finally:
            con.close()

    def test_confidence_level_reflects_appearances(self):
        # With 2 appearances → 'low'; with 384+ → 'high'.
        rows_low = _two_day_data([1.40, 1.42], [1.50, 1.52])
        con = _make_duckdb_con_with_data(rows_low)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
            )
            self.assertEqual(result["confidence_level"].iloc[0], "low")
        finally:
            con.close()

        # Build 400-day dataset for 'high' confidence
        import datetime as _dt

        rows_high = []
        for i in range(400):
            day = (_dt.date(2025, 1, 1) + _dt.timedelta(days=i)).isoformat()
            rows_high += [
                {
                    "timestamp": f"{day}T05:00:00",
                    "zip_code": "28001",
                    "locality": "C",
                    "municipality": "M",
                    "label": "testbrand",
                    "gasoline_95_e5_price": 1.40,
                },
                {
                    "timestamp": f"{day}T05:00:00",
                    "zip_code": "28001",
                    "locality": "C",
                    "municipality": "M",
                    "label": "other",
                    "gasoline_95_e5_price": 1.60,
                },
            ]
        con2 = _make_duckdb_con_with_data(rows_high)
        try:
            result2 = compute_brand_price_comparison(
                con2,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
            )
            self.assertEqual(result2["confidence_level"].iloc[0], "high")
        finally:
            con2.close()

    def test_price_delta_null_when_all_prices_filtered(self):
        # All brand prices are zero → filtered out → brand_daily empty → no output rows.
        # This verifies the zero-filter + NULLIF path does not crash.
        rows = [
            {
                "timestamp": "2026-01-01T05:00:00",
                "zip_code": "28001",
                "locality": "C",
                "municipality": "M",
                "label": "testbrand",
                "gasoline_95_e5_price": 0.0,
            },
            {
                "timestamp": "2026-01-01T05:00:00",
                "zip_code": "28001",
                "locality": "C",
                "municipality": "M",
                "label": "other",
                "gasoline_95_e5_price": 0.0,
            },
        ]
        con = _make_duckdb_con_with_data(rows)
        try:
            result = compute_brand_price_comparison(
                con,
                brands=["testbrand"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
            )
            # All prices zero → filtered → no rows (not a crash)
            self.assertEqual(len(result), 0)
            for col in BRAND_COMPARISON_COLUMNS:
                self.assertIn(col, result.columns)
        finally:
            con.close()
