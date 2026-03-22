from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
from aggregator import build_day_of_week_stats_from_province_daily_stats
from aggregator import compute_day_of_week_stats
from aggregator import compute_province_daily_stats
from aggregator import run_aggregation


def _make_raw_df():
    """Create a minimal raw DataFrame simulating a daily snapshot."""
    return pd.DataFrame(
        {
            "timestamp": ["2026-03-22T05:48:06"] * 6,
            "province": ["madrid", "madrid", "madrid", "barcelona", "barcelona", "barcelona"],
            "diesel_a_price": [1.45, 1.50, 1.48, 1.42, 1.44, 1.46],
            "gasoline_95_e5_price": [1.55, 1.60, 1.58, 1.52, 1.54, 1.56],
            "hydrogen_price": [None, None, None, None, None, None],
        }
    )


def _make_province_daily_df():
    return pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-03-22").date(), pd.Timestamp("2026-03-22").date()],
            "province": ["madrid", "barcelona"],
            "fuel_type": ["diesel_a_price", "diesel_a_price"],
            "avg_price": [1.4767, 1.44],
            "min_price": [1.45, 1.42],
            "max_price": [1.5, 1.46],
            "station_count": [3, 3],
        }
    )


class TestComputeProvinceDailyStats(TestCase):

    def test_computes_stats_per_province_and_fuel_type(self):
        raw_df = _make_raw_df()
        result = compute_province_daily_stats(raw_df)

        self.assertIn("date", result.columns)
        self.assertIn("province", result.columns)
        self.assertIn("fuel_type", result.columns)
        self.assertIn("avg_price", result.columns)
        self.assertIn("min_price", result.columns)
        self.assertIn("max_price", result.columns)
        self.assertIn("station_count", result.columns)

        # 2 provinces x 2 fuel types (hydrogen has no valid data)
        self.assertEqual(len(result), 4)

    def test_province_stats_values(self):
        raw_df = _make_raw_df()
        result = compute_province_daily_stats(raw_df)

        madrid_diesel = result[(result["province"] == "madrid") & (result["fuel_type"] == "diesel_a_price")]
        self.assertEqual(len(madrid_diesel), 1)
        row = madrid_diesel.iloc[0]
        self.assertAlmostEqual(row["avg_price"], round((1.45 + 1.50 + 1.48) / 3, 4), places=4)
        self.assertAlmostEqual(row["min_price"], 1.45)
        self.assertAlmostEqual(row["max_price"], 1.50)
        self.assertEqual(row["station_count"], 3)

    def test_skips_null_and_zero_prices(self):
        raw_df = _make_raw_df()
        result = compute_province_daily_stats(raw_df)

        hydrogen_rows = result[result["fuel_type"] == "hydrogen_price"]
        self.assertEqual(len(hydrogen_rows), 0)

    def test_date_extracted_from_timestamp(self):
        raw_df = _make_raw_df()
        result = compute_province_daily_stats(raw_df)

        import datetime

        expected_date = datetime.date(2026, 3, 22)
        self.assertTrue((result["date"] == expected_date).all())


class TestComputeDayOfWeekStats(TestCase):

    def test_initial_computation_without_existing(self):
        raw_df = _make_raw_df()
        result = compute_day_of_week_stats(raw_df)

        self.assertIn("day_of_week", result.columns)
        self.assertIn("fuel_type", result.columns)
        self.assertIn("province", result.columns)
        self.assertIn("sum_price", result.columns)
        self.assertIn("count_days", result.columns)

        # 2026-03-22 is a Sunday = 6
        self.assertTrue((result["day_of_week"] == 6).all())
        self.assertTrue((result["count_days"] == 1).all())

    def test_includes_national_aggregate(self):
        raw_df = _make_raw_df()
        result = compute_day_of_week_stats(raw_df)

        national = result[result["province"] == "__national__"]
        self.assertGreater(len(national), 0)

    def test_incremental_update_different_day(self):
        raw_df = _make_raw_df()
        first = compute_day_of_week_stats(raw_df)

        # Change to a Monday (2026-03-23)
        raw_df_monday = raw_df.copy()
        raw_df_monday["timestamp"] = "2026-03-23T05:48:06"
        result = compute_day_of_week_stats(raw_df_monday, first)

        # Should have entries for both day_of_week 6 (Sunday) and 0 (Monday)
        days = result["day_of_week"].unique()
        self.assertIn(6, days)
        self.assertIn(0, days)


class TestBuildDayOfWeekStatsFromProvinceDailyStats(TestCase):

    def test_builds_province_and_national_rows(self):
        province_daily_df = _make_province_daily_df()

        result = build_day_of_week_stats_from_province_daily_stats(province_daily_df)

        madrid_row = result[
            (result["province"] == "madrid") & (result["fuel_type"] == "diesel_a_price") & (result["day_of_week"] == 6)
        ].iloc[0]
        national_row = result[
            (result["province"] == "__national__")
            & (result["fuel_type"] == "diesel_a_price")
            & (result["day_of_week"] == 6)
        ].iloc[0]

        self.assertAlmostEqual(madrid_row["sum_price"], 1.4767, places=6)
        self.assertEqual(madrid_row["count_days"], 1)
        self.assertAlmostEqual(national_row["sum_price"], 1.45835, places=6)
        self.assertEqual(national_row["count_days"], 1)


class TestRunAggregation(TestCase):

    @patch("aggregator._upload_parquet_to_gcs")
    @patch("aggregator._download_parquet_from_gcs")
    @patch("aggregator._get_latest_raw_file")
    def test_run_aggregation_creates_new_aggregates(self, mock_latest, mock_download, mock_upload):
        raw_df = _make_raw_df()
        mock_latest.return_value = "spain_fuel_prices_2026-03-22T05:48:06.parquet"
        mock_download.side_effect = [raw_df, None, None]

        bucket = MagicMock()
        run_aggregation(bucket)

        self.assertEqual(mock_upload.call_count, 2)

    @patch("aggregator._upload_parquet_to_gcs")
    @patch("aggregator._download_parquet_from_gcs")
    @patch("aggregator._get_latest_raw_file")
    def test_run_aggregation_skips_when_no_raw_file(self, mock_latest, mock_download, mock_upload):
        mock_latest.return_value = None

        bucket = MagicMock()
        run_aggregation(bucket)

        mock_upload.assert_not_called()

    @patch("aggregator._upload_parquet_to_gcs")
    @patch("aggregator._download_parquet_from_gcs")
    @patch("aggregator._get_latest_raw_file")
    def test_run_aggregation_rebuilds_dow_from_deduplicated_daily_rows(self, mock_latest, mock_download, mock_upload):
        raw_df = _make_raw_df()
        today_stats = compute_province_daily_stats(raw_df)
        stale_today_stats = today_stats.copy()
        stale_today_stats["avg_price"] = stale_today_stats["avg_price"] + 0.1
        previous_day_stats = today_stats.copy()
        previous_day_stats["date"] = pd.Timestamp("2026-03-21").date()

        mock_latest.return_value = "spain_fuel_prices_2026-03-22T05:48:06.parquet"
        mock_download.side_effect = [raw_df, pd.concat([previous_day_stats, stale_today_stats], ignore_index=True)]

        bucket = MagicMock()
        run_aggregation(bucket)

        province_daily_df = mock_upload.call_args_list[0].args[2]
        dow_stats_df = mock_upload.call_args_list[1].args[2]

        self.assertEqual(len(province_daily_df), len(previous_day_stats) + len(today_stats))

        madrid_sunday = dow_stats_df[
            (dow_stats_df["province"] == "madrid")
            & (dow_stats_df["fuel_type"] == "diesel_a_price")
            & (dow_stats_df["day_of_week"] == 6)
        ].iloc[0]
        self.assertAlmostEqual(madrid_sunday["sum_price"], 1.4767, places=6)
        self.assertEqual(madrid_sunday["count_days"], 1)
