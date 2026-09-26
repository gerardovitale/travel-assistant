import datetime
from unittest.mock import patch

import pandas as pd
from api.schemas import FuelType


def _make_commodity_df(days: int = 30, start_value: float = 82.0) -> pd.DataFrame:
    # query_commodity_price_trend filters on "date >= CURRENT_DATE - INTERVAL (days_back) DAY",
    # so fixture dates must be anchored to today, not a fixed calendar date.
    start = datetime.date.today() - datetime.timedelta(days=days - 1)
    rows = []
    for offset in range(days):
        rows.append(
            {
                "date": start + datetime.timedelta(days=offset),
                "series_id": "DCOILBRENTEU",
                "commodity": "brent_crude",
                "value": start_value + offset * 0.1,
                "unit": "USD/bbl",
                "source": "FRED",
            }
        )
    return pd.DataFrame(rows)


def _make_fuel_trend_df(days: int = 30, start_price: float = 1.40) -> pd.DataFrame:
    # query_national_price_trend returns dates as pandas Timestamps (DuckDB fetchdf's DATE
    # columns come back as datetime64), not python date objects — match that dtype here so
    # the pd.merge in get_fuel_vs_brent_correlation joins cleanly against the real
    # query_commodity_price_trend output below, same as it would in production. Anchored to
    # today so the date range overlaps _make_commodity_df's (also today-anchored) range.
    start = pd.Timestamp(datetime.date.today() - datetime.timedelta(days=days - 1))
    rows = []
    for offset in range(days):
        rows.append(
            {
                "date": start + pd.Timedelta(days=offset),
                "avg_price": start_price + offset * 0.002,
                "min_price": start_price,
                "max_price": start_price + 0.05,
            }
        )
    return pd.DataFrame(rows)


@patch("services.commodity_service.download_aggregate")
def test_get_commodity_trend_returns_series_from_aggregate(mock_download):
    mock_download.return_value = _make_commodity_df(days=10)
    from services.commodity_service import get_commodity_trend

    result = get_commodity_trend(days_back=90)

    assert result.commodity == "brent_crude"
    assert result.unit == "USD/bbl"
    assert len(result.series) == 10


@patch("services.commodity_service.download_aggregate")
def test_get_commodity_trend_uses_brent_unit_when_aggregate_has_other_series(mock_download):
    # Regression: unit must come from the DCOILBRENTEU rows specifically, not whichever
    # series happens to be first in the aggregate once a second series_id is added.
    brent = _make_commodity_df(days=5)
    other_series = brent.copy()
    other_series["series_id"] = "EURUSD"
    other_series["unit"] = "USD/EUR"
    combined = pd.concat([other_series, brent], ignore_index=True)
    mock_download.return_value = combined
    from services.commodity_service import get_commodity_trend

    result = get_commodity_trend(days_back=90)

    assert result.unit == "USD/bbl"


@patch("services.commodity_service.download_aggregate")
def test_get_commodity_trend_handles_missing_aggregate(mock_download):
    mock_download.return_value = None
    from services.commodity_service import get_commodity_trend

    result = get_commodity_trend(days_back=90)

    assert result.series == []
    assert result.unit == ""


@patch("services.commodity_service.download_aggregate")
@patch("services.commodity_service.query_national_price_trend")
def test_get_fuel_vs_brent_correlation_computes_pearson(mock_fuel_trend, mock_download):
    mock_fuel_trend.return_value = _make_fuel_trend_df(days=30)
    mock_download.return_value = _make_commodity_df(days=30)

    from services.commodity_service import get_fuel_vs_brent_correlation

    result = get_fuel_vs_brent_correlation(FuelType.diesel_a_price, days_back=90)

    assert result.fuel_type == FuelType.diesel_a_price.value
    assert result.commodity == "brent_crude"
    assert result.insufficient_data is False
    assert result.observations == 30
    # Both series rise linearly with the same date offset, so they should correlate near 1.
    assert result.correlation > 0.95


@patch("services.commodity_service.download_aggregate")
@patch("services.commodity_service.query_national_price_trend")
def test_get_fuel_vs_brent_correlation_reports_insufficient_data_below_floor(mock_fuel_trend, mock_download):
    mock_fuel_trend.return_value = _make_fuel_trend_df(days=5)
    mock_download.return_value = _make_commodity_df(days=5)

    from services.commodity_service import get_fuel_vs_brent_correlation

    result = get_fuel_vs_brent_correlation(FuelType.diesel_a_price, days_back=90)

    assert result.insufficient_data is True
    assert result.correlation is None


@patch("services.commodity_service.download_aggregate")
@patch("services.commodity_service.query_national_price_trend")
def test_get_fuel_vs_brent_correlation_handles_missing_commodity_aggregate(mock_fuel_trend, mock_download):
    mock_fuel_trend.return_value = _make_fuel_trend_df(days=30)
    mock_download.return_value = None

    from services.commodity_service import get_fuel_vs_brent_correlation

    result = get_fuel_vs_brent_correlation(FuelType.diesel_a_price, days_back=90)

    assert result.insufficient_data is True
    assert result.correlation is None
    assert result.observations == 0
