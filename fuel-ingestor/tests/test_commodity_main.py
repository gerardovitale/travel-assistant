import os
from unittest.mock import Mock
from unittest.mock import patch

from ingestor.commodity_main import main


@patch.dict(os.environ, {"FRED_API_KEY": "test-key"})
@patch("ingestor.commodity_main.logging")
@patch("ingestor.commodity_main.fetch_commodity_prices")
@patch("ingestor.commodity_main.validate_commodity_dataframe")
@patch("ingestor.commodity_main.write_commodity_prices_data_as_parquet")
def test_main(
    mock_write_commodity_prices_data_as_parquet: Mock,
    mock_validate_commodity_dataframe: Mock,
    mock_fetch_commodity_prices: Mock,
    mock_logging: Mock,
):
    main()
    mock_fetch_commodity_prices.assert_called_once()
    mock_validate_commodity_dataframe.assert_called_once()
    mock_write_commodity_prices_data_as_parquet.assert_called_once()


@patch("ingestor.commodity_main.fetch_commodity_prices")
def test_main_raises_when_api_key_missing(mock_fetch_commodity_prices):
    env_without_key = {k: v for k, v in os.environ.items() if k != "FRED_API_KEY"}
    with patch.dict(os.environ, env_without_key, clear=True):
        try:
            main()
            raised = False
        except KeyError:
            raised = True
    assert raised
    mock_fetch_commodity_prices.assert_not_called()
