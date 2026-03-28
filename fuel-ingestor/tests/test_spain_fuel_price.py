import json
import subprocess
from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
from entity import get_expected_columns
from google.api_core.exceptions import ServiceUnavailable
from spain_fuel_price import _validate_api_response
from spain_fuel_price import create_spain_fuel_dataframe
from spain_fuel_price import extract_fuel_prices_raw_data
from spain_fuel_price import validate_dataframe
from spain_fuel_price import write_spain_fuel_prices_data_as_parquet
from tests.fixture import get_response_raw_data


class TestFuelPrice(TestCase):

    def setUp(self):
        logger_patch = patch("spain_fuel_price.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

    @patch("spain_fuel_price.subprocess.run")
    def test_extract_fuel_prices_raw_data(self, mock_run):
        expected_data = get_response_raw_data()
        mock_run.return_value = MagicMock(stdout=json.dumps(expected_data))
        result = extract_fuel_prices_raw_data()
        mock_run.assert_called_once()
        self.assertEqual(result, expected_data)

    @patch("spain_fuel_price.time.sleep")
    @patch("spain_fuel_price.subprocess.run")
    def test_extract_fuel_prices_raw_data_retries_on_error(self, mock_run, mock_sleep):
        expected_data = get_response_raw_data()
        mock_run.side_effect = [
            subprocess.CalledProcessError(22, "curl"),
            MagicMock(stdout=json.dumps(expected_data)),
        ]
        result = extract_fuel_prices_raw_data()
        self.assertEqual(mock_run.call_count, 2)
        mock_sleep.assert_called_once_with(10)
        self.assertEqual(result, expected_data)

    @patch("spain_fuel_price.time.sleep")
    @patch("spain_fuel_price.subprocess.run")
    def test_extract_fuel_prices_raw_data_raises_after_all_retries_exhausted(self, mock_run, mock_sleep):
        mock_run.side_effect = subprocess.CalledProcessError(22, "curl")
        with self.assertRaises(subprocess.CalledProcessError):
            extract_fuel_prices_raw_data()
        self.assertEqual(mock_run.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_create_spain_fuel_dataframe(self):
        test_data = get_response_raw_data()
        expected_data = [
            (
                "2024-10-09t20:12:15",
                "02250",
                "4375",
                "07",
                "52",
                "02",
                "p",
                "nº 10.935",
                "avenida castilla la mancha, 26",
                "abengibre",
                "albacete",
                "abengibre",
                "39,211417",
                "-1,539167",
                "",
                "",
                "",
                "",
                "",
                "1,299",
                "0,899",
                "",
                "",
                "1,399",
                "",
                "",
                "",
                "",
            ),
            (
                "2024-10-09t20:12:15",
                "02152",
                "5122",
                "07",
                "53",
                "02",
                "p",
                "repsol",
                "cr cm-332, 46,4",
                "alatoz",
                "albacete",
                "alatoz",
                "39,100389",
                "-1,346083",
                "",
                "",
                "",
                "",
                "",
                "1,589",
                "",
                "1,709",
                "",
                "1,699",
                "",
                "",
                "",
                "",
            ),
        ]
        expected_df = pd.DataFrame(expected_data, columns=get_expected_columns())

        actual_df = create_spain_fuel_dataframe(test_data)

        self.assertIsInstance(actual_df, pd.DataFrame)
        self.assertEqual(list(actual_df.columns), list(expected_df.columns))


class TestValidateApiResponse(TestCase):

    def test_valid_response_passes(self):
        raw_data = get_response_raw_data()
        _validate_api_response(raw_data)

    def test_non_ok_status_raises(self):
        raw_data = get_response_raw_data()
        raw_data["ResultadoConsulta"] = "ERROR"
        with self.assertRaises(ValueError, msg="non-OK status"):
            _validate_api_response(raw_data)

    def test_missing_lista_raises(self):
        raw_data = get_response_raw_data()
        raw_data["ListaEESSPrecio"] = None
        with self.assertRaises(ValueError, msg="missing or empty"):
            _validate_api_response(raw_data)

    def test_empty_lista_raises(self):
        raw_data = get_response_raw_data()
        raw_data["ListaEESSPrecio"] = []
        with self.assertRaises(ValueError, msg="missing or empty"):
            _validate_api_response(raw_data)

    def test_missing_fecha_raises(self):
        raw_data = get_response_raw_data()
        del raw_data["Fecha"]
        with self.assertRaises(ValueError, msg="missing 'Fecha'"):
            _validate_api_response(raw_data)

    def test_bad_fecha_format_raises(self):
        raw_data = get_response_raw_data()
        raw_data["Fecha"] = "2024-10-09"
        with self.assertRaises(ValueError, msg="unexpected format"):
            _validate_api_response(raw_data)


class TestWriteWithRetry(TestCase):

    def setUp(self):
        logger_patch = patch("spain_fuel_price.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

    @patch("spain_fuel_price.time.sleep")
    @patch("spain_fuel_price.storage.Client")
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

    @patch("spain_fuel_price.time.sleep")
    @patch("spain_fuel_price.storage.Client")
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
        logger_patch = patch("spain_fuel_price.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

    def _make_valid_df(self, n_rows=100):
        from entity import get_float_columns

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
