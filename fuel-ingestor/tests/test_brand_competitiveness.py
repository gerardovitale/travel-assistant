import os
import shutil
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
from brand_competitiveness import BRAND_COMPETITIVENESS_BLOB
from brand_competitiveness import BRAND_COMPETITIVENESS_MONTHLY_BLOB
from brand_competitiveness import compute_brand_competitiveness
from brand_competitiveness import run_brand_analytics
from reports.brand_comparison import BRAND_COMPARISON_BLOB
from reports.brand_win_rate import BRAND_WIN_RATE_BLOB


def _make_test_parquet_dir():
    """Write a minimal multi-day test parquet to a temp directory."""
    rows = []
    # Day 1: ballenoil cheapest in 28001, repsol cheapest in 08001
    rows += [
        {
            "timestamp": "2026-01-01T05:00:00",
            "zip_code": "28001",
            "locality": "Madrid Centro",
            "municipality": "Madrid",
            "label": "ballenoil",
            "gasoline_95_e5_price": 1.50,
            "diesel_a_price": 1.40,
        },
        {
            "timestamp": "2026-01-01T05:00:00",
            "zip_code": "28001",
            "locality": "Madrid Centro",
            "municipality": "Madrid",
            "label": "repsol",
            "gasoline_95_e5_price": 1.60,
            "diesel_a_price": 1.50,
        },
        {
            "timestamp": "2026-01-01T05:00:00",
            "zip_code": "28001",
            "locality": "Madrid Centro",
            "municipality": "Madrid",
            "label": "other",
            "gasoline_95_e5_price": 1.70,
            "diesel_a_price": 1.60,
        },
        {
            "timestamp": "2026-01-01T05:00:00",
            "zip_code": "08001",
            "locality": "Barcelona Port",
            "municipality": "Barcelona",
            "label": "ballenoil",
            "gasoline_95_e5_price": 1.55,
            "diesel_a_price": 1.45,
        },
        {
            "timestamp": "2026-01-01T05:00:00",
            "zip_code": "08001",
            "locality": "Barcelona Port",
            "municipality": "Barcelona",
            "label": "repsol",
            "gasoline_95_e5_price": 1.45,
            "diesel_a_price": 1.35,
        },
        {
            "timestamp": "2026-01-01T05:00:00",
            "zip_code": "08001",
            "locality": "Barcelona Port",
            "municipality": "Barcelona",
            "label": "other",
            "gasoline_95_e5_price": 1.65,
            "diesel_a_price": 1.55,
        },
    ]
    # Day 2: flip — repsol cheapest in 28001, ballenoil cheapest in 08001
    rows += [
        {
            "timestamp": "2026-01-02T05:00:00",
            "zip_code": "28001",
            "locality": "Madrid Centro",
            "municipality": "Madrid",
            "label": "ballenoil",
            "gasoline_95_e5_price": 1.55,
            "diesel_a_price": 1.45,
        },
        {
            "timestamp": "2026-01-02T05:00:00",
            "zip_code": "28001",
            "locality": "Madrid Centro",
            "municipality": "Madrid",
            "label": "repsol",
            "gasoline_95_e5_price": 1.45,
            "diesel_a_price": 1.35,
        },
        {
            "timestamp": "2026-01-02T05:00:00",
            "zip_code": "28001",
            "locality": "Madrid Centro",
            "municipality": "Madrid",
            "label": "other",
            "gasoline_95_e5_price": 1.65,
            "diesel_a_price": 1.55,
        },
        {
            "timestamp": "2026-01-02T05:00:00",
            "zip_code": "08001",
            "locality": "Barcelona Port",
            "municipality": "Barcelona",
            "label": "ballenoil",
            "gasoline_95_e5_price": 1.45,
            "diesel_a_price": 1.35,
        },
        {
            "timestamp": "2026-01-02T05:00:00",
            "zip_code": "08001",
            "locality": "Barcelona Port",
            "municipality": "Barcelona",
            "label": "repsol",
            "gasoline_95_e5_price": 1.55,
            "diesel_a_price": 1.45,
        },
        {
            "timestamp": "2026-01-02T05:00:00",
            "zip_code": "08001",
            "locality": "Barcelona Port",
            "municipality": "Barcelona",
            "label": "other",
            "gasoline_95_e5_price": 1.65,
            "diesel_a_price": 1.55,
        },
    ]
    df = pd.DataFrame(rows)
    tmp_dir = tempfile.mkdtemp(prefix="test_brand_competitiveness_")
    df.to_parquet(os.path.join(tmp_dir, "test_snapshot.parquet"), index=False)
    return tmp_dir


class TestComputeBrandCompetitiveness(TestCase):

    def setUp(self):
        self.tmp_dir = _make_test_parquet_dir()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_returns_overall_and_monthly_dataframes(self):
        overall, monthly = compute_brand_competitiveness(
            self.tmp_dir,
            brands=["ballenoil", "repsol"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            min_appearances=1,
        )
        self.assertIsInstance(overall, pd.DataFrame)
        self.assertIsInstance(monthly, pd.DataFrame)

    def test_overall_columns(self):
        overall, _ = compute_brand_competitiveness(
            self.tmp_dir,
            brands=["ballenoil", "repsol"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            min_appearances=1,
        )
        for col in ["brand", "geo_level", "geo_value", "fuel_type", "appearances", "win_rate_pct", "last_updated"]:
            self.assertIn(col, overall.columns)

    def test_monthly_columns(self):
        _, monthly = compute_brand_competitiveness(
            self.tmp_dir,
            brands=["ballenoil", "repsol"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            min_appearances=1,
        )
        for col in ["year_month", "brand", "geo_level", "fuel_type", "appearances", "win_rate_pct"]:
            self.assertIn(col, monthly.columns)

    def test_win_rate_ballenoil_zip_28001(self):
        # Day 1: ballenoil cheapest (50%), Day 2: ballenoil NOT cheapest (50%) → 50% win rate
        overall, _ = compute_brand_competitiveness(
            self.tmp_dir,
            brands=["ballenoil", "repsol"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            min_appearances=1,
        )
        row = overall[(overall["brand"] == "ballenoil") & (overall["geo_value"] == "28001")]
        self.assertEqual(len(row), 1)
        self.assertAlmostEqual(row["win_rate_pct"].iloc[0], 50.0)

    def test_win_rate_ballenoil_zip_08001(self):
        # Day 1: ballenoil NOT cheapest, Day 2: ballenoil cheapest → 50% win rate
        overall, _ = compute_brand_competitiveness(
            self.tmp_dir,
            brands=["ballenoil", "repsol"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            min_appearances=1,
        )
        row = overall[(overall["brand"] == "ballenoil") & (overall["geo_value"] == "08001")]
        self.assertEqual(len(row), 1)
        self.assertAlmostEqual(row["win_rate_pct"].iloc[0], 50.0)

    def test_geo_level_is_set_correctly(self):
        overall, _ = compute_brand_competitiveness(
            self.tmp_dir,
            brands=["ballenoil"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code", "locality"],
            min_appearances=1,
        )
        self.assertIn("zip_code", overall["geo_level"].values)
        self.assertIn("locality", overall["geo_level"].values)

    def test_fuel_type_strips_price_suffix(self):
        overall, _ = compute_brand_competitiveness(
            self.tmp_dir,
            brands=["ballenoil"],
            fuel_cols=["gasoline_95_e5_price", "diesel_a_price"],
            geo_cols=["zip_code"],
            min_appearances=1,
        )
        self.assertIn("gasoline_95_e5", overall["fuel_type"].values)
        self.assertIn("diesel_a", overall["fuel_type"].values)

    def test_min_appearances_filter(self):
        # With min_appearances=3, no rows should pass (only 2 days of data per geo_value)
        overall, _ = compute_brand_competitiveness(
            self.tmp_dir,
            brands=["ballenoil"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            min_appearances=3,
        )
        self.assertEqual(len(overall), 0)

    def test_monthly_win_rate_aggregated_across_geo_values(self):
        # Monthly should NOT have geo_value column — aggregated across all geo values
        _, monthly = compute_brand_competitiveness(
            self.tmp_dir,
            brands=["ballenoil"],
            fuel_cols=["gasoline_95_e5_price"],
            geo_cols=["zip_code"],
            min_appearances=1,
        )
        self.assertNotIn("geo_value", monthly.columns)

    def test_schema_evolution_across_files(self):
        # Simulates the real GCS case: early files have a legacy "date" column
        # that was later removed. DuckDB glob must handle this via union_by_name.
        schema_dir = tempfile.mkdtemp(prefix="test_schema_evolution_")
        try:
            legacy = pd.DataFrame(
                [
                    {
                        "timestamp": "2024-10-11T11:50:49",
                        "date": "2024-10-11",  # legacy column — dropped in later files
                        "zip_code": "28001",
                        "locality": "Madrid Centro",
                        "municipality": "Madrid",
                        "label": "ballenoil",
                        "gasoline_95_e5_price": 1.50,
                        "diesel_a_price": 1.40,
                    }
                ]
            )
            current = pd.DataFrame(
                [
                    {
                        "timestamp": "2026-01-01T05:00:00",
                        "zip_code": "28001",
                        "locality": "Madrid Centro",
                        "municipality": "Madrid",
                        "label": "ballenoil",
                        "gasoline_95_e5_price": 1.45,
                        "diesel_a_price": 1.35,
                    }
                ]
            )
            legacy.to_parquet(os.path.join(schema_dir, "legacy.parquet"), index=False)
            current.to_parquet(os.path.join(schema_dir, "current.parquet"), index=False)

            overall, _ = compute_brand_competitiveness(
                schema_dir,
                brands=["ballenoil"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
            )
            self.assertGreater(len(overall), 0)
        finally:
            shutil.rmtree(schema_dir, ignore_errors=True)

    def test_returns_empty_dataframes_on_empty_input(self):
        empty_dir = tempfile.mkdtemp(prefix="empty_parquets_")
        try:
            # Create an empty parquet file with explicit dtypes so DuckDB can infer schema
            pd.DataFrame(
                {
                    "timestamp": pd.Series([], dtype="str"),
                    "zip_code": pd.Series([], dtype="str"),
                    "label": pd.Series([], dtype="str"),
                    "gasoline_95_e5_price": pd.Series([], dtype="float64"),
                }
            ).to_parquet(os.path.join(empty_dir, "empty.parquet"), index=False)
            overall, monthly = compute_brand_competitiveness(
                empty_dir,
                brands=["ballenoil"],
                fuel_cols=["gasoline_95_e5_price"],
                geo_cols=["zip_code"],
                min_appearances=1,
            )
            self.assertEqual(len(overall), 0)
            self.assertEqual(len(monthly), 0)
        finally:
            shutil.rmtree(empty_dir, ignore_errors=True)


class TestRunBrandAnalytics(TestCase):

    @patch("brand_competitiveness._download_parquets_to_dir")
    @patch("brand_competitiveness._upload_parquet_to_gcs")
    @patch("brand_competitiveness._list_raw_parquet_files")
    @patch("brand_competitiveness._latest_raw_file_per_day")
    def test_uploads_both_legacy_aggregates(self, mock_dedup, mock_list, mock_upload, mock_download):
        tmp_dir = _make_test_parquet_dir()
        try:
            mock_list.return_value = ["spain_fuel_prices_2026-01-01T05:00:00.parquet"]
            mock_dedup.return_value = ["spain_fuel_prices_2026-01-01T05:00:00.parquet"]
            mock_download.return_value = tmp_dir

            bucket = MagicMock()
            run_brand_analytics(bucket=bucket)

            uploaded_blobs = [call.args[1] for call in mock_upload.call_args_list]
            self.assertIn(BRAND_COMPETITIVENESS_BLOB, uploaded_blobs)
            self.assertIn(BRAND_COMPETITIVENESS_MONTHLY_BLOB, uploaded_blobs)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @patch("brand_competitiveness._download_parquets_to_dir")
    @patch("brand_competitiveness._upload_parquet_to_gcs")
    @patch("brand_competitiveness._list_raw_parquet_files")
    @patch("brand_competitiveness._latest_raw_file_per_day")
    def test_uploads_new_report_blobs(self, mock_dedup, mock_list, mock_upload, mock_download):
        tmp_dir = _make_test_parquet_dir()
        try:
            mock_list.return_value = ["spain_fuel_prices_2026-01-01T05:00:00.parquet"]
            mock_dedup.return_value = ["spain_fuel_prices_2026-01-01T05:00:00.parquet"]
            mock_download.return_value = tmp_dir

            bucket = MagicMock()
            results = run_brand_analytics(bucket=bucket)

            output_blobs = {r.output_blob for r in results}
            self.assertIn(BRAND_WIN_RATE_BLOB, output_blobs)
            self.assertIn(BRAND_COMPARISON_BLOB, output_blobs)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @patch("brand_competitiveness._download_parquets_to_dir")
    @patch("brand_competitiveness._upload_parquet_to_gcs")
    @patch("brand_competitiveness._list_raw_parquet_files")
    @patch("brand_competitiveness._latest_raw_file_per_day")
    def test_returns_pipeline_results_for_all_tasks(self, mock_dedup, mock_list, mock_upload, mock_download):
        tmp_dir = _make_test_parquet_dir()
        try:
            mock_list.return_value = ["spain_fuel_prices_2026-01-01T05:00:00.parquet"]
            mock_dedup.return_value = ["spain_fuel_prices_2026-01-01T05:00:00.parquet"]
            mock_download.return_value = tmp_dir

            bucket = MagicMock()
            results = run_brand_analytics(bucket=bucket)

            self.assertIsInstance(results, list)
            result_names = {r.name for r in results}
            self.assertIn("brand_competitiveness", result_names)
            self.assertIn("brand_competitiveness_monthly", result_names)
            self.assertIn("brand_win_rate", result_names)
            self.assertIn("brand_price_comparison", result_names)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @patch("brand_competitiveness._list_raw_parquet_files")
    def test_skips_when_no_raw_files(self, mock_list):
        mock_list.return_value = []
        bucket = MagicMock()
        results = run_brand_analytics(bucket=bucket)
        self.assertEqual(results, [])
        bucket.blob.assert_not_called()
