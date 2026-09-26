from unittest.mock import MagicMock
from unittest.mock import patch

import requests
from commodity_api.fetch import fetch_raw_data
from tests.fixture import get_response_raw_data


class TestFetchRawData:
    @patch("commodity_api.fetch.requests.get")
    def test_returns_parsed_json(self, mock_get):
        expected = get_response_raw_data()
        mock_get.return_value = MagicMock(json=lambda: expected)
        result = fetch_raw_data(api_key="test-key", series_id="DCOILBRENTEU")
        mock_get.assert_called_once()
        assert result == expected

    @patch("commodity_api.fetch.requests.get")
    def test_passes_series_id_and_api_key_as_params(self, mock_get):
        mock_get.return_value = MagicMock(json=lambda: get_response_raw_data())
        fetch_raw_data(api_key="test-key", series_id="DCOILBRENTEU")
        _, kwargs = mock_get.call_args
        assert kwargs["params"]["series_id"] == "DCOILBRENTEU"
        assert kwargs["params"]["api_key"] == "test-key"

    @patch("commodity_api.fetch.requests.get")
    def test_passes_observation_window(self, mock_get):
        mock_get.return_value = MagicMock(json=lambda: get_response_raw_data())
        fetch_raw_data(api_key="test-key", observation_start="2026-01-01", observation_end="2026-01-31")
        _, kwargs = mock_get.call_args
        assert kwargs["params"]["observation_start"] == "2026-01-01"
        assert kwargs["params"]["observation_end"] == "2026-01-31"

    @patch("commodity_api.fetch.time.sleep")
    @patch("commodity_api.fetch.requests.get")
    def test_retries_on_error_then_succeeds(self, mock_get, mock_sleep):
        expected = get_response_raw_data()
        ok_response = MagicMock(json=lambda: expected)
        mock_get.side_effect = [requests.exceptions.ConnectionError("boom"), ok_response]
        result = fetch_raw_data(api_key="test-key")
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(10)
        assert result == expected

    @patch("commodity_api.fetch.time.sleep")
    @patch("commodity_api.fetch.requests.get")
    def test_exponential_backoff_delays(self, mock_get, mock_sleep):
        mock_get.side_effect = requests.exceptions.ConnectionError("boom")
        try:
            fetch_raw_data(api_key="test-key", max_retries=3, retry_base_delay=10)
        except requests.exceptions.ConnectionError:
            pass
        assert [c.args[0] for c in mock_sleep.call_args_list] == [10, 20]

    @patch("commodity_api.fetch.time.sleep")
    @patch("commodity_api.fetch.requests.get")
    def test_fixed_backoff_delays(self, mock_get, mock_sleep):
        mock_get.side_effect = requests.exceptions.ConnectionError("boom")
        try:
            fetch_raw_data(api_key="test-key", max_retries=3, retry_base_delay=10, exponential_backoff=False)
        except requests.exceptions.ConnectionError:
            pass
        assert [c.args[0] for c in mock_sleep.call_args_list] == [10, 10]

    @patch("commodity_api.fetch.time.sleep")
    @patch("commodity_api.fetch.requests.get")
    def test_raises_after_all_retries_exhausted(self, mock_get, mock_sleep):
        mock_get.side_effect = requests.exceptions.ConnectionError("boom")
        try:
            fetch_raw_data(api_key="test-key")
            raised = False
        except requests.exceptions.ConnectionError:
            raised = True
        assert raised
        assert mock_get.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("commodity_api.fetch.time.sleep")
    @patch("commodity_api.fetch.requests.get")
    def test_raises_on_http_error_exhausted(self, mock_get, mock_sleep):
        bad_response = MagicMock()
        bad_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
        mock_get.return_value = bad_response
        try:
            fetch_raw_data(api_key="test-key")
            raised = False
        except requests.exceptions.HTTPError:
            raised = True
        assert raised
        assert mock_get.call_count == 3
