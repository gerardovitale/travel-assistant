from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
from google.api_core.exceptions import ServiceUnavailable
from ingestor.spain_fuel_price import validate_dataframe
from ingestor.spain_fuel_price import write_spain_fuel_prices_data_as_parquet
from spain_fuel_api import get_expected_columns
from spain_fuel_api import get_float_columns


class TestWriteWithRetry(TestCase):

    def setUp(self):
        logger_patch = patch("ingestor.spain_fuel_price.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

    @patch("ingestor.spain_fuel_price.time.sleep")
    @patch("ingestor.spain_fuel_price.storage.Client")
    def test_upload_retries_on_gcs_error(self, mock_client, mock_sleep):
        mock_blob = MagicMock()
        mock_blob.upload_from_string.side_effect = [
            ServiceUnavailable("temporarily unavailable"),
            None,
        ]
        mock_client.return_value.bucket.return_value.blob.return_value = mock_blob

        df = pd.DataFrame({"col": [1, 2]})
        write_spain_fuel_prices_data_as_parquet(df)

        self.assertEqual(mock_blob.upload_from_string.call_count, 2)
        mock_sleep.assert_called_once_with(2)

    @patch("ingestor.spain_fuel_price.time.sleep")
    @patch("ingestor.spain_fuel_price.storage.Client")
    def test_upload_raises_after_all_retries_exhausted(self, mock_client, mock_sleep):
        mock_blob = MagicMock()
        mock_blob.upload_from_string.side_effect = ServiceUnavailable("unavailable")
        mock_client.return_value.bucket.return_value.blob.return_value = mock_blob

        df = pd.DataFrame({"col": [1, 2]})
        with self.assertRaises(ServiceUnavailable):
            write_spain_fuel_prices_data_as_parquet(df)

        self.assertEqual(mock_blob.upload_from_string.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)


class TestValidateDataframe(TestCase):

    def setUp(self):
        logger_patch = patch("ingestor.spain_fuel_price.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

    def _make_valid_df(self, n_rows=100):
        float_cols = set(get_float_columns())
        data = {}
        for col in get_expected_columns():
            if col in float_cols:
                data[col] = [1.5]
            else:
                data[col] = ["test"]
        data["latitude"] = [40.0]
        data["longitude"] = [-3.5]
        df = pd.DataFrame(data)
        return pd.concat([df] * n_rows, ignore_index=True)

    def test_empty_dataframe_raises(self):
        df = pd.DataFrame(columns=get_expected_columns())
        with self.assertRaises(ValueError, msg="empty"):
            validate_dataframe(df)

    def test_missing_columns_raises(self):
        df = pd.DataFrame({"wrong_col": [1, 2, 3]})
        with self.assertRaises(ValueError, msg="missing required columns"):
            validate_dataframe(df)

    def test_low_station_count_warns(self):
        df = self._make_valid_df(n_rows=100)
        validate_dataframe(df)
        warning_calls = [str(c) for c in self.mock_logger.warning.call_args_list]
        self.assertTrue(any("Low station count" in w for w in warning_calls))

    def test_out_of_range_prices_warns(self):
        df = self._make_valid_df(n_rows=100)
        df.loc[0, "diesel_a_price"] = 10.0
        validate_dataframe(df)
        warning_calls = [str(c) for c in self.mock_logger.warning.call_args_list]
        self.assertTrue(any("diesel_a_price" in w and "outside" in w for w in warning_calls))

    def test_out_of_range_coordinates_warns(self):
        df = self._make_valid_df(n_rows=100)
        df.loc[0, "latitude"] = 60.0
        validate_dataframe(df)
        warning_calls = [str(c) for c in self.mock_logger.warning.call_args_list]
        self.assertTrue(any("latitude" in w for w in warning_calls))
