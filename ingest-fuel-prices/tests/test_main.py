from unittest.mock import Mock
from unittest.mock import patch

import flask
import main
import pytest


@pytest.fixture(scope="module")
def fake_app():
    return flask.Flask(__name__)


@patch("main.logging")
@patch("main.create_spain_fuel_dataframe")
@patch("main.extract_fuel_prices_raw_data")
@patch("main.map_raw_data_into_spain_fuel_price")
@patch("main.write_spain_fuel_prices_data_as_csv")
def test_ingest_fuel_prices(
    mock_write_spain_fuel_prices_data_as_csv: Mock,
    mock_map_raw_data_into_spain_fuel_price: Mock,
    mock_extract_fuel_prices_raw_data: Mock,
    mock_create_spain_fuel_dataframe: Mock,
    mock_logging: Mock,
    fake_app,
):
    with fake_app.test_request_context():
        res = main.ingest_fuel_prices(flask.request)
        assert "OK" in res
