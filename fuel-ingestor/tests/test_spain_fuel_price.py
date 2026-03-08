import json
import subprocess
from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
from entity import get_expected_columns
from spain_fuel_price import create_spain_fuel_dataframe
from spain_fuel_price import extract_fuel_prices_raw_data
from tests.fixture import get_response_raw_data


class TestFuelPrice(TestCase):

    def setUp(self):
        logger_patch = patch("spain_fuel_price.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

    @patch("spain_fuel_price.subprocess.run")
    def test_extract_fuel_prices_raw_data(self, mock_run):
        expected_data = {"ResultadoConsulta": "OK", "ListaEESSPrecio": []}
        mock_run.return_value = MagicMock(stdout=json.dumps(expected_data))
        result = extract_fuel_prices_raw_data()
        mock_run.assert_called_once()
        self.assertEqual(result, expected_data)

    @patch("spain_fuel_price.time.sleep")
    @patch("spain_fuel_price.subprocess.run")
    def test_extract_fuel_prices_raw_data_retries_on_error(self, mock_run, mock_sleep):
        expected_data = {"ResultadoConsulta": "OK", "ListaEESSPrecio": []}
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
