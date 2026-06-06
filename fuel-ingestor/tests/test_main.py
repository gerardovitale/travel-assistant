from unittest.mock import Mock
from unittest.mock import patch

from ingestor.main import main


@patch("ingestor.main.logging")
@patch("ingestor.main.fetch_fuel_stations")
@patch("ingestor.main.validate_dataframe")
@patch("ingestor.main.write_spain_fuel_prices_data_as_parquet")
def test_main(
    mock_write_spain_fuel_prices_data_as_parquet: Mock,
    mock_validate_dataframe: Mock,
    mock_fetch_fuel_stations: Mock,
    mock_logging: Mock,
):
    main()
    mock_fetch_fuel_stations.assert_called_once()
    mock_validate_dataframe.assert_called_once()
    mock_write_spain_fuel_prices_data_as_parquet.assert_called_once()
