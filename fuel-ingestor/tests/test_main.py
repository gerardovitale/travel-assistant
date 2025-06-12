from unittest.mock import Mock
from unittest.mock import patch

import pytest

from main import main


@patch("main.logging")
@patch("main.extract_fuel_prices_raw_data")
@patch("main.create_spain_fuel_dataframe")
@patch("main.write_spain_fuel_prices_data_as_csv")
def test_main(
        mock_write_spain_fuel_prices_data_as_csv: Mock,
        mock_create_spain_fuel_dataframe: Mock,
        mock_extract_fuel_prices_raw_data: Mock,
        mock_logging: Mock,
):
    main()
    mock_extract_fuel_prices_raw_data.assert_called_once()
    mock_create_spain_fuel_dataframe.assert_called_once()
    mock_write_spain_fuel_prices_data_as_csv.assert_called_once()
