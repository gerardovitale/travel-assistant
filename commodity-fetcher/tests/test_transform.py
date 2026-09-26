from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from commodity_api.client import fetch_commodity_prices
from commodity_api.schema import get_expected_columns
from commodity_api.transform import transform_to_dataframe
from tests.fixture import get_response_raw_data


class TestTransformToDataframe:
    def test_produces_expected_columns(self):
        df = transform_to_dataframe(get_response_raw_data(), series_id="DCOILBRENTEU")
        assert list(df.columns) == get_expected_columns()

    def test_drops_non_trading_day_rows(self):
        df = transform_to_dataframe(get_response_raw_data(), series_id="DCOILBRENTEU")
        # Fixture has 4 observations, 2 of which are "."
        assert len(df) == 2
        assert {str(d) for d in df["date"]} == {"2026-01-02", "2026-01-05"}

    def test_value_parsed_as_float(self):
        raw = get_response_raw_data([("2026-01-02", "82.87")])
        df = transform_to_dataframe(raw, series_id="DCOILBRENTEU")
        assert df["value"].iloc[0] == pytest.approx(82.87)

    def test_attaches_series_metadata(self):
        raw = get_response_raw_data([("2026-01-02", "82.87")])
        df = transform_to_dataframe(raw, series_id="DCOILBRENTEU")
        assert df["series_id"].iloc[0] == "DCOILBRENTEU"
        assert df["commodity"].iloc[0] == "brent_crude"
        assert df["unit"].iloc[0] == "USD/bbl"
        assert df["source"].iloc[0] == "FRED"

    def test_all_non_trading_days_yields_empty_dataframe(self):
        raw = get_response_raw_data([("2026-01-03", "."), ("2026-01-04", ".")])
        df = transform_to_dataframe(raw, series_id="DCOILBRENTEU")
        assert df.empty
        assert list(df.columns) == get_expected_columns()


class TestFetchCommodityPrices:
    @patch("commodity_api.fetch.requests.get")
    def test_end_to_end_returns_dataframe(self, mock_get):
        raw = get_response_raw_data()
        mock_get.return_value = MagicMock(json=lambda: raw)
        df = fetch_commodity_prices(api_key="test-key", series_id="DCOILBRENTEU")
        assert list(df.columns) == get_expected_columns()
        assert len(df) == 2

    @patch("commodity_api.fetch.requests.get")
    def test_missing_observations_raises(self, mock_get):
        mock_get.return_value = MagicMock(json=lambda: {"observations": []})
        with pytest.raises(ValueError, match="missing or empty"):
            fetch_commodity_prices(api_key="test-key")
