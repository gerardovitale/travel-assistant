from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
from backfill import backfill


class TestBackfill(TestCase):

    @patch("backfill._upload_parquet_to_gcs")
    @patch("backfill._build_aggregate_dataframes_from_raw_files")
    @patch("backfill._get_bucket")
    def test_backfill_collapses_duplicate_raw_files_per_day(self, mock_get_bucket, mock_build_aggregates, mock_upload):
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

        mock_build_aggregates.return_value = (
            pd.DataFrame({"date": ["2026-03-22", "2026-03-23"]}),
            pd.DataFrame({"day_of_week": [6, 0]}),
            pd.DataFrame({"date": ["2026-03-22", "2026-03-23"]}),
        )

        backfill()

        self.assertEqual(
            mock_build_aggregates.call_args.args[1],
            [
                "spain_fuel_prices_2026-03-22T08:00:00.parquet",
                "spain_fuel_prices_2026-03-23T05:00:00.parquet",
            ],
        )
        self.assertEqual(mock_upload.call_count, 3)
