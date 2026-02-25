from unittest import TestCase
from unittest.mock import patch

import pandas as pd
from entity import get_expected_columns
from spain_fuel_price import create_spain_fuel_dataframe
from spain_fuel_price import extract_fuel_prices_raw_data
from tests.fixture import get_response_raw_data


class TestFuelPrice(TestCase):

    def setUp(self):
        requests_patch = patch("spain_fuel_price.requests")
        self.addCleanup(requests_patch.stop)
        self.mock_requests = requests_patch.start()

        logger_patch = patch("spain_fuel_price.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

    def test_extract_fuel_prices_raw_data(self):
        mock_session = self.mock_requests.Session.return_value
        mock_session.get.return_value.status_code = 200
        _ = extract_fuel_prices_raw_data()
        mock_session.mount.assert_called_once()
        mock_session.get.assert_called_once()
        mock_session.get().json.assert_called_once()

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
