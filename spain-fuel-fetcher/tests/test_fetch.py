import json
import subprocess
from unittest.mock import MagicMock
from unittest.mock import patch

from spain_fuel_api.fetch import fetch_raw_data
from tests.fixture import get_response_raw_data


class TestCurlAvailability:
    def test_curl_is_available(self):
        """curl binary required for API data extraction."""
        result = subprocess.run(["curl", "--version"], capture_output=True, text=True)
        assert result.returncode == 0


class TestFetchRawData:
    @patch("spain_fuel_api.fetch.subprocess.run")
    def test_returns_parsed_json(self, mock_run):
        expected = get_response_raw_data()
        mock_run.return_value = MagicMock(stdout=json.dumps(expected))
        result = fetch_raw_data()
        mock_run.assert_called_once()
        assert result == expected

    @patch("spain_fuel_api.fetch.time.sleep")
    @patch("spain_fuel_api.fetch.subprocess.run")
    def test_retries_on_error_then_succeeds(self, mock_run, mock_sleep):
        expected = get_response_raw_data()
        mock_run.side_effect = [
            subprocess.CalledProcessError(22, "curl"),
            MagicMock(stdout=json.dumps(expected)),
        ]
        result = fetch_raw_data()
        assert mock_run.call_count == 2
        mock_sleep.assert_called_once_with(10)
        assert result == expected

    @patch("spain_fuel_api.fetch.time.sleep")
    @patch("spain_fuel_api.fetch.subprocess.run")
    def test_exponential_backoff_delays(self, mock_run, mock_sleep):
        mock_run.side_effect = subprocess.CalledProcessError(22, "curl")
        try:
            fetch_raw_data(max_retries=3, retry_base_delay=10)
        except subprocess.CalledProcessError:
            pass
        assert [c.args[0] for c in mock_sleep.call_args_list] == [10, 20]

    @patch("spain_fuel_api.fetch.time.sleep")
    @patch("spain_fuel_api.fetch.subprocess.run")
    def test_fixed_backoff_delays(self, mock_run, mock_sleep):
        mock_run.side_effect = subprocess.CalledProcessError(22, "curl")
        try:
            fetch_raw_data(max_retries=3, retry_base_delay=10, exponential_backoff=False)
        except subprocess.CalledProcessError:
            pass
        assert [c.args[0] for c in mock_sleep.call_args_list] == [10, 10]

    @patch("spain_fuel_api.fetch.time.sleep")
    @patch("spain_fuel_api.fetch.subprocess.run")
    def test_raises_after_all_retries_exhausted(self, mock_run, mock_sleep):
        mock_run.side_effect = subprocess.CalledProcessError(22, "curl")
        try:
            fetch_raw_data()
            raised = False
        except subprocess.CalledProcessError:
            raised = True
        assert raised
        assert mock_run.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("spain_fuel_api.fetch.time.sleep")
    @patch("spain_fuel_api.fetch.subprocess.run")
    def test_raises_on_timeout_exhausted(self, mock_run, mock_sleep):
        mock_run.side_effect = subprocess.TimeoutExpired("curl", 10)
        try:
            fetch_raw_data()
            raised = False
        except subprocess.TimeoutExpired:
            raised = True
        assert raised
        assert mock_run.call_count == 3
