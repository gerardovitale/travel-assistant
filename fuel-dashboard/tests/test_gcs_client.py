from datetime import datetime
from datetime import timezone
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch


@patch("data.gcs_client._get_bucket")
@patch("data.gcs_client.utcnow")
def test_list_parquet_files_filters_days_back_without_datetime_type_errors(mock_utcnow, mock_get_bucket):
    from data.gcs_client import list_parquet_files

    mock_utcnow.return_value = datetime(2025, 1, 10, 12, 0, 0, tzinfo=timezone.utc)

    mock_bucket = Mock()
    mock_bucket.list_blobs.return_value = [
        SimpleNamespace(name="spain_fuel_prices_2025-01-02T01-00-00.parquet"),
        SimpleNamespace(name="spain_fuel_prices_2025-01-03T01-00-00.parquet"),
        SimpleNamespace(name="spain_fuel_prices_2025-01-10T01-00-00.parquet"),
        SimpleNamespace(name="spain_fuel_prices_2025-01-11T01-00-00.parquet"),
        SimpleNamespace(name="spain_fuel_prices_2025-01-10T01-00-00.csv"),
    ]
    mock_get_bucket.return_value = mock_bucket

    files = list_parquet_files(days_back=7)

    assert files == [
        "spain_fuel_prices_2025-01-03T01-00-00.parquet",
        "spain_fuel_prices_2025-01-10T01-00-00.parquet",
    ]


@patch("data.gcs_client._get_bucket")
def test_list_parquet_files_accepts_naive_start_end_dates(mock_get_bucket):
    from data.gcs_client import list_parquet_files

    mock_bucket = Mock()
    mock_bucket.list_blobs.return_value = [
        SimpleNamespace(name="spain_fuel_prices_2025-01-01T01-00-00.parquet"),
        SimpleNamespace(name="spain_fuel_prices_2025-01-03T01-00-00.parquet"),
    ]
    mock_get_bucket.return_value = mock_bucket

    files = list_parquet_files(
        start_date=datetime(2025, 1, 1, 0, 0, 0),
        end_date=datetime(2025, 1, 2, 23, 59, 59),
    )

    assert files == ["spain_fuel_prices_2025-01-01T01-00-00.parquet"]
