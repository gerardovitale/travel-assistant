from datetime import date
from unittest.mock import patch

import pandas as pd
from services.data_quality_service import get_data_inventory
from services.data_quality_service import get_ingestion_stats
from services.data_quality_service import get_missing_days


@patch("services.data_quality_service.download_aggregate")
def test_get_ingestion_stats_returns_dataframe(mock_agg):
    df = pd.DataFrame({"date": ["2026-03-01"], "record_count": [100]})
    mock_agg.return_value = df
    result = get_ingestion_stats()
    assert len(result) == 1
    mock_agg.assert_called_once_with("daily_ingestion_stats.parquet")


@patch("services.data_quality_service.download_aggregate")
def test_get_ingestion_stats_returns_empty_when_none(mock_agg):
    mock_agg.return_value = None
    result = get_ingestion_stats()
    assert result.empty


@patch("services.data_quality_service.list_parquet_files_with_metadata")
def test_get_data_inventory_computes_metrics_from_aggregate(mock_list):
    mock_list.return_value = [
        {"name": "spain_fuel_prices_2026-01-01T05.parquet", "date": "2026-01-01", "size_bytes": 1_000_000},
        {"name": "spain_fuel_prices_2026-01-02T05.parquet", "date": "2026-01-02", "size_bytes": 1_200_000},
        {"name": "spain_fuel_prices_2026-02-01T05.parquet", "date": "2026-02-01", "size_bytes": 1_100_000},
    ]
    stats = pd.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02", "2026-02-01"],
            "record_count": [100, 150, 120],
        }
    )
    result = get_data_inventory(stats)
    assert result["num_days"] == 3
    assert result["num_months"] == 2
    assert result["num_years"] == 1
    assert result["total_size_bytes"] == 3_300_000
    assert result["min_date"] == date(2026, 1, 1)
    assert result["max_date"] == date(2026, 2, 1)


@patch("services.data_quality_service.list_parquet_files_with_metadata")
def test_get_data_inventory_excludes_empty_records(mock_list):
    mock_list.return_value = [
        {"name": "a.parquet", "date": "2026-01-01", "size_bytes": 1_000_000},
        {"name": "b.parquet", "date": "2026-01-02", "size_bytes": 500},
    ]
    stats = pd.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02"],
            "record_count": [100, 0],
        }
    )
    result = get_data_inventory(stats)
    assert result["num_days"] == 1
    assert result["total_size_bytes"] == 1_000_500


@patch("services.data_quality_service.list_parquet_files_with_metadata")
def test_get_data_inventory_empty_aggregate(mock_list):
    mock_list.return_value = [
        {"name": "a.parquet", "date": "2026-01-01", "size_bytes": 1_000},
    ]
    result = get_data_inventory(pd.DataFrame())
    assert result["num_days"] == 0
    assert result["total_size_bytes"] == 1_000
    assert result["min_date"] is None


def test_get_missing_days_finds_gaps():
    available = {date(2026, 1, 1), date(2026, 1, 3), date(2026, 1, 5)}
    result = get_missing_days(available, date(2026, 1, 1), date(2026, 1, 5))
    assert result == ["2026-01-02", "2026-01-04"]


def test_get_missing_days_no_gaps():
    available = {date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)}
    result = get_missing_days(available, date(2026, 1, 1), date(2026, 1, 3))
    assert result == []
