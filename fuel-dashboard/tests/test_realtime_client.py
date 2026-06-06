import subprocess
from unittest.mock import patch

import pandas as pd
from spain_fuel_api import get_expected_columns

from data.realtime_client import fetch_realtime_stations


def _make_df(n: int) -> pd.DataFrame:
    row = {col: ("test" if col != "timestamp" else "2026-04-04T08:30:00+00:00") for col in get_expected_columns()}
    return pd.DataFrame([row] * n)


class TestFetchRealtimeStations:
    @patch("data.realtime_client.MIN_EXPECTED_STATIONS", 1)
    @patch("data.realtime_client.fetch_fuel_stations")
    def test_successful_fetch(self, mock_fetch):
        mock_fetch.return_value = _make_df(3)
        df = fetch_realtime_stations(curl_timeout=10)
        assert df is not None
        assert len(df) == 3

    @patch("data.realtime_client.MIN_EXPECTED_STATIONS", 10)
    @patch("data.realtime_client.fetch_fuel_stations")
    def test_undersized_payload_returns_none(self, mock_fetch):
        mock_fetch.return_value = _make_df(9)
        assert fetch_realtime_stations(curl_timeout=10) is None

    @patch("data.realtime_client.fetch_fuel_stations")
    def test_curl_failure_returns_none(self, mock_fetch):
        mock_fetch.side_effect = subprocess.CalledProcessError(1, "curl")
        assert fetch_realtime_stations(curl_timeout=10) is None

    @patch("data.realtime_client.fetch_fuel_stations")
    def test_curl_timeout_returns_none(self, mock_fetch):
        mock_fetch.side_effect = subprocess.TimeoutExpired("curl", 10)
        assert fetch_realtime_stations(curl_timeout=10) is None

    @patch("data.realtime_client.fetch_fuel_stations")
    def test_bad_api_status_returns_none(self, mock_fetch):
        mock_fetch.side_effect = ValueError("API returned non-OK status: 'ERROR'")
        assert fetch_realtime_stations(curl_timeout=10) is None
