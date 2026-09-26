from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
from commodity_api import get_expected_columns
from google.api_core.exceptions import ServiceUnavailable
from ingestor.commodity_price import validate_commodity_dataframe
from ingestor.commodity_price import write_commodity_prices_data_as_parquet


def _make_valid_df(n_rows=2):
    data = {
        "date": [f"2026-01-0{i + 1}" for i in range(n_rows)],
        "series_id": ["DCOILBRENTEU"] * n_rows,
        "commodity": ["brent_crude"] * n_rows,
        "value": [82.5 + i for i in range(n_rows)],
        "unit": ["USD/bbl"] * n_rows,
        "source": ["FRED"] * n_rows,
    }
    return pd.DataFrame(data)


class TestWriteWithRetry(TestCase):

    def setUp(self):
        logger_patch = patch("ingestor.commodity_price.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

    @patch("ingestor.commodity_price.time.sleep")
    @patch("ingestor.commodity_price.storage.Client")
    def test_write_retries_on_gcs_error(self, mock_client, mock_sleep):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_blob.upload_from_string.side_effect = [ServiceUnavailable("temporarily unavailable"), None]
        mock_bucket.blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket

        write_commodity_prices_data_as_parquet(_make_valid_df())

        self.assertEqual(mock_blob.upload_from_string.call_count, 2)
        mock_sleep.assert_called_once_with(2)

    @patch("ingestor.commodity_price.time.sleep")
    @patch("ingestor.commodity_price.storage.Client")
    def test_write_raises_after_all_retries_exhausted(self, mock_client, mock_sleep):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_blob.upload_from_string.side_effect = ServiceUnavailable("unavailable")
        mock_bucket.blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket

        with self.assertRaises(ServiceUnavailable):
            write_commodity_prices_data_as_parquet(_make_valid_df())

        self.assertEqual(mock_blob.upload_from_string.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)


class TestValidateCommodityDataframe(TestCase):

    def setUp(self):
        logger_patch = patch("ingestor.commodity_price.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

    def test_empty_dataframe_raises(self):
        df = pd.DataFrame(columns=get_expected_columns())
        with self.assertRaises(ValueError):
            validate_commodity_dataframe(df)

    def test_missing_columns_raises(self):
        df = pd.DataFrame({"wrong_col": [1, 2, 3]})
        with self.assertRaises(ValueError):
            validate_commodity_dataframe(df)

    def test_valid_dataframe_passes(self):
        validate_commodity_dataframe(_make_valid_df())

    def test_out_of_range_value_warns(self):
        df = _make_valid_df()
        df.loc[0, "value"] = 500.0
        validate_commodity_dataframe(df)
        warning_calls = [str(c) for c in self.mock_logger.warning.call_args_list]
        self.assertTrue(any("outside" in w for w in warning_calls))

    def test_duplicate_date_series_warns(self):
        df = pd.concat([_make_valid_df(1), _make_valid_df(1)], ignore_index=True)
        validate_commodity_dataframe(df)
        warning_calls = [str(c) for c in self.mock_logger.warning.call_args_list]
        self.assertTrue(any("duplicate" in w for w in warning_calls))
