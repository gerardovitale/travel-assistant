from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
from aggregator import BRAND_DAILY_STATS_BLOB
from aggregator import build_day_of_week_stats_from_province_daily_stats
from aggregator import compute_daily_ingestion_stats
from aggregator import compute_day_of_week_stats
from aggregator import compute_province_daily_stats
from aggregator import compute_zip_code_daily_stats
from aggregator import DAILY_INGESTION_STATS_BLOB
from aggregator import PROVINCE_DAILY_STATS_BLOB
from aggregator import run_aggregation
from aggregator import ZIP_CODE_DAILY_STATS_BLOB


def _logged_messages(mock_calls):
    return [call.args[0] for call in mock_calls]


def _make_raw_df():
    """Create a minimal raw DataFrame simulating a daily snapshot."""
    return pd.DataFrame(
        {
            "timestamp": ["2026-03-22T05:48:06"] * 6,
            "eess_id": ["1", "2", "3", "4", "5", "6"],
            "municipality_id": ["101", "101", "102", "201", "201", "202"],
            "province_id": ["28", "28", "28", "08", "08", "08"],
            "label": ["Station A", "Station B", "Station C", "Station D", "Station E", "Station F"],
            "province": ["madrid", "madrid", "madrid", "barcelona", "barcelona", "barcelona"],
            "municipality": ["Madrid", "Madrid", "Getafe", "Barcelona", "Barcelona", "Hospitalet"],
            "locality": ["Madrid", "Madrid", "Getafe", "Barcelona", "Barcelona", "Hospitalet"],
            "zip_code": ["28001", "28001", "28901", "08001", "08001", "08901"],
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


def _make_ingestion_stats_raw_df():
    return pd.DataFrame(
        {
            "timestamp": ["2026-03-22T05:48:06"] * 6,
            "eess_id": ["1", "2", "3", "4", "5", "6"],
            "municipality_id": ["101", "101", "102", "201", "201", "202"],
            "province_id": ["28", "28", "28", "08", "08", "08"],
            "label": ["repsol", "repsol", "shell", "repsol", "bp", "bp"],
            "province": ["madrid", "madrid", "madrid", "barcelona", "barcelona", "barcelona"],
            "municipality": ["Madrid", "Madrid", "Getafe", "Madrid", "Barcelona", "Barcelona"],
            "locality": ["Centro", "Centro", "Centro", "Centro", "Port", "Port"],
            "zip_code": ["28001", "28001", "28901", "28001", "08001", "08001"],
            "diesel_a_price": [1.45, 1.50, 1.48, 1.42, 1.44, 1.46],
        }
    )


def _make_brand_raw_df():
    rows = []
    for index in range(12):
        rows.append(
            {
                "timestamp": "2026-03-22T05:48:06",
                "eess_id": f"r{index}",
                "municipality_id": "101",
                "province_id": "28",
                "label": "repsol",
                "province": "madrid",
                "municipality": "Madrid",
                "locality": "Madrid",
                "zip_code": "28001",
                "diesel_a_price": 1.45 + index * 0.001,
            }
        )
    for index in range(12):
        rows.append(
            {
                "timestamp": "2026-03-22T05:48:06",
                "eess_id": f"s{index}",
                "municipality_id": "201",
                "province_id": "08",
                "label": "shell",
                "province": "barcelona",
                "municipality": "Barcelona",
                "locality": "Barcelona",
                "zip_code": "08001",
                "diesel_a_price": 1.5 + index * 0.001,
            }
        )
    return pd.DataFrame(rows)


def _make_download_side_effect(raw_df=None, aggregate_overrides=None):
    """Build a side_effect function for _download_parquet_from_gcs based on blob names.

    *raw_df* is returned for any raw parquet file. *aggregate_overrides* is an
    optional dict mapping aggregate blob names to DataFrames (or None).
    Unspecified aggregates default to None.
    """
    overrides = aggregate_overrides or {}

    def _side_effect(_bucket, blob_name, columns=None):
        if blob_name in overrides:
            return overrides[blob_name]
        if blob_name in (
            PROVINCE_DAILY_STATS_BLOB,
            DAILY_INGESTION_STATS_BLOB,
            BRAND_DAILY_STATS_BLOB,
            ZIP_CODE_DAILY_STATS_BLOB,
        ):
            return None
        return raw_df

    return _side_effect


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


class TestComputeZipCodeDailyStats(TestCase):

    def test_computes_stats_per_zip_code_and_fuel_type(self):
        raw_df = _make_raw_df()
        result = compute_zip_code_daily_stats(raw_df)

        self.assertIn("zip_code", result.columns)
        self.assertIn("fuel_type", result.columns)
        self.assertIn("avg_price", result.columns)
        self.assertIn("station_count", result.columns)
        self.assertEqual(len(result), 8)

    def test_skips_invalid_prices(self):
        raw_df = _make_raw_df()
        raw_df.loc[0, "diesel_a_price"] = 0
        raw_df.loc[1, "diesel_a_price"] = None

        result = compute_zip_code_daily_stats(raw_df)
        zip_row = result[(result["zip_code"] == "28001") & (result["fuel_type"] == "diesel_a_price")]

        self.assertEqual(len(zip_row), 0)

    def test_aggregates_duplicate_zip_codes(self):
        raw_df = _make_raw_df()
        result = compute_zip_code_daily_stats(raw_df)

        madrid_row = result[(result["zip_code"] == "28001") & (result["fuel_type"] == "diesel_a_price")].iloc[0]
        self.assertAlmostEqual(madrid_row["avg_price"], 1.475, places=4)
        self.assertAlmostEqual(madrid_row["min_price"], 1.45, places=4)
        self.assertAlmostEqual(madrid_row["max_price"], 1.50, places=4)
        self.assertEqual(madrid_row["station_count"], 2)


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
        mock_download.side_effect = _make_download_side_effect(raw_df)

        bucket = MagicMock()
        with patch("aggregator.logger") as mock_logger:
            run_aggregation(bucket)

        self.assertEqual(mock_upload.call_count, 5)
        info_messages = _logged_messages(mock_logger.info.call_args_list)
        self.assertTrue(
            any(
                "aggregation_mode_selected" in message and "run_type='incremental'" in message
                for message in info_messages
            )
        )
        self.assertTrue(any("raw_snapshot_loaded" in message for message in info_messages))
        self.assertTrue(any("province_daily_stats_computed" in message for message in info_messages))
        self.assertTrue(any("daily_ingestion_stats_computed" in message for message in info_messages))
        self.assertTrue(any("day_of_week_stats_rebuilt" in message for message in info_messages))
        self.assertTrue(any("brand_daily_stats_computed" in message for message in info_messages))
        self.assertTrue(any("zip_code_daily_stats_computed" in message for message in info_messages))
        self.assertTrue(any("aggregation_complete" in message for message in info_messages))

    @patch("aggregator._upload_parquet_to_gcs")
    @patch("aggregator._download_parquet_from_gcs")
    @patch("aggregator._get_latest_raw_file")
    def test_run_aggregation_skips_when_no_raw_file(self, mock_latest, mock_download, mock_upload):
        mock_latest.return_value = None

        bucket = MagicMock()
        with patch("aggregator.logger") as mock_logger:
            run_aggregation(bucket)

        mock_upload.assert_not_called()
        warning_messages = _logged_messages(mock_logger.warning.call_args_list)
        self.assertTrue(any("aggregation_skipped_no_raw_file" in message for message in warning_messages))

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
        mock_download.side_effect = _make_download_side_effect(
            raw_df,
            aggregate_overrides={
                PROVINCE_DAILY_STATS_BLOB: pd.concat([previous_day_stats, stale_today_stats], ignore_index=True),
            },
        )

        bucket = MagicMock()
        with patch("aggregator.logger") as mock_logger:
            run_aggregation(bucket)

        province_daily_df = mock_upload.call_args_list[0].args[2]
        dow_stats_df = mock_upload.call_args_list[2].args[2]

        self.assertEqual(len(province_daily_df), len(previous_day_stats) + len(today_stats))

        madrid_sunday = dow_stats_df[
            (dow_stats_df["province"] == "madrid")
            & (dow_stats_df["fuel_type"] == "diesel_a_price")
            & (dow_stats_df["day_of_week"] == 6)
        ].iloc[0]
        self.assertAlmostEqual(madrid_sunday["sum_price"], 1.4767, places=6)
        self.assertEqual(madrid_sunday["count_days"], 1)
        info_messages = _logged_messages(mock_logger.info.call_args_list)
        self.assertTrue(
            any("province_daily_stats_updated" in message and "removed_rows=4" in message for message in info_messages)
        )
        self.assertTrue(any("daily_ingestion_stats_initialized" in message for message in info_messages))
        self.assertTrue(any("zip_code_daily_stats_initialized" in message for message in info_messages))

    @patch("aggregator._upload_parquet_to_gcs")
    @patch("aggregator._download_parquet_from_gcs")
    @patch("aggregator._list_raw_parquet_files")
    @patch("aggregator._blob_exists")
    @patch("aggregator._get_latest_raw_file")
    def test_run_aggregation_bootstraps_missing_aggregates_from_historical_files(
        self,
        mock_latest,
        mock_blob_exists,
        mock_list_raw,
        mock_download,
        mock_upload,
    ):
        first_day_raw_df = _make_raw_df().copy()
        first_day_raw_df["timestamp"] = "2026-03-22T08:00:00"

        second_day_raw_df = _make_raw_df().copy()
        second_day_raw_df["timestamp"] = "2026-03-23T05:48:06"

        mock_blob_exists.return_value = False
        mock_list_raw.return_value = [
            "spain_fuel_prices_2026-03-22T05:00:00.parquet",
            "spain_fuel_prices_2026-03-22T08:00:00.parquet",
            "spain_fuel_prices_2026-03-23T05:48:06.parquet",
        ]
        raw_files_map = {
            "spain_fuel_prices_2026-03-22T08:00:00.parquet": first_day_raw_df,
            "spain_fuel_prices_2026-03-23T05:48:06.parquet": second_day_raw_df,
        }

        def _bootstrap_download(_bucket, blob_name, columns=None):
            return raw_files_map.get(blob_name)

        mock_download.side_effect = _bootstrap_download

        bucket = MagicMock()
        with patch("aggregator.logger") as mock_logger:
            run_aggregation(bucket)

        mock_latest.assert_not_called()
        self.assertEqual(mock_upload.call_count, 5)

        province_daily_df = mock_upload.call_args_list[0].args[2]
        ingestion_stats_df = mock_upload.call_args_list[1].args[2]
        zip_code_daily_df = mock_upload.call_args_list[4].args[2]

        self.assertEqual(len(province_daily_df), 8)
        self.assertEqual(len(ingestion_stats_df), 2)
        self.assertFalse(zip_code_daily_df.empty)
        info_messages = _logged_messages(mock_logger.info.call_args_list)
        self.assertTrue(
            any(
                "aggregation_mode_selected" in message and "run_type='bootstrap'" in message
                for message in info_messages
            )
        )
        self.assertTrue(
            any("bootstrap_raw_files_found" in message and "count=3" in message for message in info_messages)
        )
        self.assertTrue(
            any("raw_files_deduplicated_by_day" in message and "unique_days=2" in message for message in info_messages)
        )
        self.assertTrue(any("historical_aggregate_build_complete" in message for message in info_messages))
        self.assertTrue(any("bootstrap_aggregate_frames_ready" in message for message in info_messages))

    @patch("aggregator._upload_parquet_to_gcs")
    @patch("aggregator._download_parquet_from_gcs")
    @patch("aggregator._list_raw_parquet_files")
    @patch("aggregator._blob_exists")
    @patch("aggregator._get_latest_raw_file")
    def test_run_aggregation_bootstraps_when_any_aggregate_is_missing(
        self,
        mock_latest,
        mock_blob_exists,
        mock_list_raw,
        mock_download,
        mock_upload,
    ):
        raw_df = _make_raw_df()
        mock_blob_exists.side_effect = [False, True, True, True, True]
        mock_list_raw.return_value = ["spain_fuel_prices_2026-03-22T05:48:06.parquet"]
        mock_download.return_value = raw_df

        bucket = MagicMock()
        run_aggregation(bucket)

        mock_latest.assert_not_called()
        self.assertEqual(mock_upload.call_count, 5)

    @patch("aggregator._upload_parquet_to_gcs")
    @patch("aggregator._download_parquet_from_gcs")
    @patch("aggregator._list_raw_parquet_files")
    @patch("aggregator._blob_exists")
    @patch("aggregator._get_latest_raw_file")
    def test_run_aggregation_backfills_brand_blob_without_rebuilding_other_aggregates(
        self,
        mock_latest,
        mock_blob_exists,
        mock_list_raw,
        mock_download,
        mock_upload,
    ):
        mock_blob_exists.side_effect = [True, True, True, False, True]
        mock_list_raw.return_value = ["spain_fuel_prices_2026-03-22T05:48:06.parquet"]
        mock_download.return_value = _make_brand_raw_df()

        bucket = MagicMock()
        with patch("aggregator.logger") as mock_logger:
            run_aggregation(bucket)

        mock_latest.assert_not_called()
        self.assertEqual(mock_upload.call_count, 1)
        self.assertEqual(mock_upload.call_args.args[1], BRAND_DAILY_STATS_BLOB)
        self.assertFalse(mock_upload.call_args.args[2].empty)

        info_messages = _logged_messages(mock_logger.info.call_args_list)
        self.assertTrue(
            any(
                "aggregation_mode_selected" in message and "run_type='brand_backfill'" in message
                for message in info_messages
            )
        )
        self.assertTrue(any("brand_backfill_frame_ready" in message for message in info_messages))

    @patch("aggregator._upload_parquet_to_gcs")
    @patch("aggregator._download_parquet_from_gcs")
    @patch("aggregator._list_raw_parquet_files")
    @patch("aggregator._blob_exists")
    @patch("aggregator._get_latest_raw_file")
    def test_run_aggregation_backfills_zip_code_blob_without_rebuilding_other_aggregates(
        self,
        mock_latest,
        mock_blob_exists,
        mock_list_raw,
        mock_download,
        mock_upload,
    ):
        mock_blob_exists.side_effect = [True, True, True, True, False]
        mock_list_raw.return_value = ["spain_fuel_prices_2026-03-22T05:48:06.parquet"]
        mock_download.return_value = _make_raw_df()

        bucket = MagicMock()
        with patch("aggregator.logger") as mock_logger:
            run_aggregation(bucket)

        mock_latest.assert_not_called()
        self.assertEqual(mock_upload.call_count, 1)
        self.assertEqual(mock_upload.call_args.args[1], ZIP_CODE_DAILY_STATS_BLOB)
        self.assertFalse(mock_upload.call_args.args[2].empty)

        info_messages = _logged_messages(mock_logger.info.call_args_list)
        self.assertTrue(
            any(
                "aggregation_mode_selected" in message and "run_type='zip_code_daily_backfill'" in message
                for message in info_messages
            )
        )
        self.assertTrue(any("zip_code_daily_backfill_frame_ready" in message for message in info_messages))

    @patch("aggregator._upload_parquet_to_gcs")
    @patch("aggregator._download_parquet_from_gcs")
    @patch("aggregator._get_latest_raw_file")
    def test_run_aggregation_prunes_zip_code_stats_older_than_retention(self, mock_latest, mock_download, mock_upload):
        raw_df = _make_raw_df().copy()
        raw_df["timestamp"] = "2026-03-31T05:48:06"

        existing_zip_stats = pd.DataFrame(
            {
                "date": [pd.Timestamp("2025-03-31").date(), pd.Timestamp("2025-04-01").date()],
                "zip_code": ["28001", "28001"],
                "fuel_type": ["diesel_a_price", "diesel_a_price"],
                "avg_price": [1.4, 1.41],
                "min_price": [1.35, 1.36],
                "max_price": [1.45, 1.46],
                "station_count": [2, 2],
            }
        )
        mock_latest.return_value = "spain_fuel_prices_2026-03-31T05:48:06.parquet"
        mock_download.side_effect = _make_download_side_effect(
            raw_df,
            aggregate_overrides={ZIP_CODE_DAILY_STATS_BLOB: existing_zip_stats},
        )

        bucket = MagicMock()
        with patch("aggregator.logger") as mock_logger:
            run_aggregation(bucket)

        zip_code_daily_df = mock_upload.call_args_list[4].args[2]
        retained_dates = set(pd.to_datetime(zip_code_daily_df["date"]).dt.date)

        self.assertNotIn(pd.Timestamp("2025-03-31").date(), retained_dates)
        self.assertIn(pd.Timestamp("2025-04-01").date(), retained_dates)
        self.assertIn(pd.Timestamp("2026-03-31").date(), retained_dates)

        info_messages = _logged_messages(mock_logger.info.call_args_list)
        self.assertTrue(
            any("zip_code_daily_stats_updated" in message and "pruned_rows=1" in message for message in info_messages)
        )

    @patch("aggregator._upload_parquet_to_gcs")
    @patch("aggregator._list_raw_parquet_files")
    @patch("aggregator._blob_exists")
    @patch("aggregator._get_latest_raw_file")
    def test_run_aggregation_skips_bootstrap_when_no_raw_history_exists(
        self,
        mock_latest,
        mock_blob_exists,
        mock_list_raw,
        mock_upload,
    ):
        mock_blob_exists.return_value = False
        mock_list_raw.return_value = []

        bucket = MagicMock()
        with patch("aggregator.logger") as mock_logger:
            run_aggregation(bucket)

        mock_latest.assert_not_called()
        mock_upload.assert_not_called()
        info_messages = _logged_messages(mock_logger.info.call_args_list)
        warning_messages = _logged_messages(mock_logger.warning.call_args_list)
        self.assertTrue(
            any(
                "aggregation_mode_selected" in message and "run_type='bootstrap'" in message
                for message in info_messages
            )
        )
        self.assertTrue(any("bootstrap_skipped_no_raw_files" in message for message in warning_messages))

    @patch("aggregator._upload_parquet_to_gcs")
    @patch("aggregator._download_parquet_from_gcs")
    @patch("aggregator._get_latest_raw_file")
    def test_run_aggregation_skips_when_selected_raw_file_disappears(self, mock_latest, mock_download, mock_upload):
        mock_latest.return_value = "spain_fuel_prices_2026-03-22T05:48:06.parquet"
        mock_download.return_value = None

        bucket = MagicMock()
        with patch("aggregator.logger") as mock_logger:
            run_aggregation(bucket)

        mock_upload.assert_not_called()
        warning_messages = _logged_messages(mock_logger.warning.call_args_list)
        self.assertTrue(any("raw_snapshot_missing_after_selection" in message for message in warning_messages))


class TestComputeDailyIngestionStats(TestCase):

    def test_computes_identifier_and_name_metrics(self):
        raw_df = _make_ingestion_stats_raw_df()
        result = compute_daily_ingestion_stats(raw_df)

        self.assertEqual(len(result), 1)
        row = result.iloc[0]
        self.assertEqual(row["record_count"], 6)
        self.assertEqual(row["unique_stations"], 6)
        self.assertEqual(row["unique_station_labels"], 3)
        self.assertEqual(row["unique_provinces"], 2)
        self.assertEqual(row["unique_municipalities"], 4)
        self.assertEqual(row["unique_municipality_names"], 3)
        self.assertEqual(row["unique_localities"], 5)
        self.assertEqual(row["unique_locality_names"], 2)

    def test_extracts_date_from_timestamp(self):
        import datetime

        raw_df = _make_ingestion_stats_raw_df()
        result = compute_daily_ingestion_stats(raw_df)
        self.assertEqual(result.iloc[0]["date"], datetime.date(2026, 3, 22))
