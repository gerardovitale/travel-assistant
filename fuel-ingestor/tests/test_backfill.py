from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
from backfill import backfill


def _make_raw_df(timestamp: str, diesel_price: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": [timestamp],
            "province": ["madrid"],
            "diesel_a_price": [diesel_price],
        }
    )


class TestBackfill(TestCase):

    @patch("backfill._upload_parquet_to_gcs")
    @patch("backfill.pd.read_parquet")
    @patch("backfill._get_bucket")
    def test_backfill_collapses_duplicate_raw_files_per_day(self, mock_get_bucket, mock_read_parquet, mock_upload):
        bucket = MagicMock()
        mock_get_bucket.return_value = bucket

        file_names = [
            "spain_fuel_prices_2026-03-22T05:00:00.parquet",
            "spain_fuel_prices_2026-03-22T08:00:00.parquet",
            "spain_fuel_prices_2026-03-23T05:00:00.parquet",
        ]
        list_blobs = []
        for name in file_names:
            blob_ref = MagicMock()
            blob_ref.name = name
            list_blobs.append(blob_ref)
        bucket.list_blobs.return_value = list_blobs

        blobs_by_name = {}
        for name in file_names:
            blob = MagicMock()
            blob.download_as_bytes.return_value = b"parquet-bytes"
            blobs_by_name[name] = blob
        bucket.blob.side_effect = lambda name: blobs_by_name[name]

        mock_read_parquet.side_effect = [
            _make_raw_df("2026-03-22T08:00:00", 1.55),
            _make_raw_df("2026-03-23T05:00:00", 1.6),
        ]

        backfill()

        province_daily_df = mock_upload.call_args_list[0].args[2]
        dow_stats_df = mock_upload.call_args_list[1].args[2]

        self.assertEqual(mock_read_parquet.call_count, 2)
        self.assertEqual(
            [call.args[0] for call in bucket.blob.call_args_list],
            [
                "spain_fuel_prices_2026-03-22T08:00:00.parquet",
                "spain_fuel_prices_2026-03-23T05:00:00.parquet",
            ],
        )
        self.assertEqual(len(province_daily_df), 2)

        madrid_sunday = dow_stats_df[
            (dow_stats_df["province"] == "madrid")
            & (dow_stats_df["fuel_type"] == "diesel_a_price")
            & (dow_stats_df["day_of_week"] == 6)
        ].iloc[0]
        self.assertAlmostEqual(madrid_sunday["sum_price"], 1.55, places=6)
        self.assertEqual(madrid_sunday["count_days"], 1)
