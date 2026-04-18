import time
from unittest.mock import patch

import pandas as pd
import pytest


def _make_stations_df(n=3):
    return pd.DataFrame(
        {
            "label": [f"station_{i}" for i in range(n)],
            "address": [f"calle {i}" for i in range(n)],
            "municipality": ["madrid"] * n,
            "province": ["madrid"] * n,
            "zip_code": [f"2800{i}" for i in range(n)],
            "latitude": [40.4168 + i * 0.01 for i in range(n)],
            "longitude": [-3.7038 + i * 0.01 for i in range(n)],
            "diesel_a_price": [1.45 + i * 0.05 for i in range(n)],
        }
    )


@pytest.fixture(autouse=True)
def _reset_cache_state():
    import data.cache as cache_module

    cache_module._last_realtime_refresh = None
    yield
    cache_module._last_realtime_refresh = None


@patch("data.cache.replace_latest_stations")
@patch("data.cache.fetch_realtime_stations")
def test_realtime_refresh_loop_filters_non_public_stations(mock_fetch, mock_replace):
    import data.cache as cache_module

    df = pd.DataFrame(
        {
            "label": ["public", "restricted"],
            "sale_type": ["p", "r"],
            "diesel_a_price": [1.45, 1.50],
        }
    )
    mock_fetch.return_value = df
    mock_replace.return_value = 1

    def fake_sleep(seconds):
        raise StopIteration("break loop")

    with patch.object(cache_module.time, "sleep", side_effect=fake_sleep):
        try:
            cache_module._realtime_refresh_loop()
        except StopIteration:
            pass

    called_df = mock_replace.call_args[0][0]
    assert len(called_df) == 1
    assert called_df.iloc[0]["label"] == "public"


@patch("data.cache.replace_latest_stations", return_value=5000)
@patch("data.cache.fetch_realtime_stations")
def test_realtime_refresh_loop_loads_data(mock_fetch, mock_replace):
    import data.cache as cache_module

    df = _make_stations_df(3)
    mock_fetch.return_value = df

    def fake_sleep(seconds):
        raise StopIteration("break loop")

    with patch.object(cache_module.time, "sleep", side_effect=fake_sleep):
        try:
            cache_module._realtime_refresh_loop()
        except StopIteration:
            pass

    mock_fetch.assert_called_once()
    mock_replace.assert_called_once()
    assert cache_module._last_realtime_refresh is not None
    assert cache_module._is_realtime_active() is True


@patch("data.cache.replace_latest_stations")
@patch("data.cache.fetch_realtime_stations", return_value=None)
def test_realtime_refresh_loop_skips_on_none(mock_fetch, mock_replace):
    import data.cache as cache_module

    def fake_sleep(seconds):
        raise StopIteration("break loop")

    with patch.object(cache_module.time, "sleep", side_effect=fake_sleep):
        try:
            cache_module._realtime_refresh_loop()
        except StopIteration:
            pass

    mock_fetch.assert_called_once()
    mock_replace.assert_not_called()
    assert cache_module._is_realtime_active() is False


@patch("data.cache.settings")
def test_start_cache_refresh_skips_realtime_when_disabled(mock_settings):
    import data.cache as cache_module

    mock_settings.realtime_enabled = False
    mock_settings.cache_ttl_seconds = 86400
    mock_settings.parquet_cache_max_age_hours = 2

    cache_module._realtime_refresh_thread = None

    with (
        patch.object(cache_module, "_snapshot_refresh_thread", None),
        patch.object(cache_module, "_trend_refresh_thread", None),
        patch("data.cache.threading.Thread") as mock_thread,
    ):
        mock_thread.return_value.is_alive.return_value = False
        cache_module.start_cache_refresh()

        thread_targets = [call.kwargs.get("target") or call[1].get("target") for call in mock_thread.call_args_list]
        assert cache_module._realtime_refresh_loop not in thread_targets


def test_get_realtime_status_returns_dict():
    from data.cache import get_realtime_status

    status = get_realtime_status()
    assert "realtime_enabled" in status
    assert "realtime_active" in status
    assert "last_realtime_refresh" in status


def test_is_realtime_active_false_when_no_refresh():
    import data.cache as cache_module

    assert cache_module._is_realtime_active() is False


@patch("data.cache.settings")
def test_is_realtime_active_true_when_recent(mock_settings):
    import data.cache as cache_module

    mock_settings.realtime_refresh_seconds = 600
    cache_module._last_realtime_refresh = time.time() - 300
    assert cache_module._is_realtime_active() is True


@patch("data.cache.settings")
def test_is_realtime_active_false_when_stale(mock_settings):
    import data.cache as cache_module

    mock_settings.realtime_refresh_seconds = 600
    cache_module._last_realtime_refresh = time.time() - 1500
    assert cache_module._is_realtime_active() is False


@patch("data.cache.settings")
def test_snapshot_sleep_seconds_uses_cache_ttl_when_realtime_inactive(mock_settings):
    import data.cache as cache_module

    mock_settings.cache_ttl_seconds = 86400
    mock_settings.realtime_refresh_seconds = 600

    assert cache_module._snapshot_sleep_seconds() == 86400


@patch("data.cache.settings")
def test_snapshot_sleep_seconds_waits_for_realtime_expiry(mock_settings):
    import data.cache as cache_module

    mock_settings.cache_ttl_seconds = 86400
    mock_settings.realtime_refresh_seconds = 600
    cache_module._last_realtime_refresh = 1000

    with patch.object(cache_module.time, "time", return_value=2100):
        assert cache_module._snapshot_sleep_seconds() == 100


@patch("data.cache._snapshot_sleep_seconds", return_value=42)
@patch("data.cache._is_realtime_active", return_value=True)
@patch("data.cache.refresh_latest_snapshot")
def test_snapshot_loop_skips_when_realtime_active(mock_refresh, mock_active, mock_sleep_seconds):
    import data.cache as cache_module

    def fake_sleep(seconds):
        raise StopIteration("break loop")

    with patch.object(cache_module.time, "sleep", side_effect=fake_sleep) as mock_sleep:
        try:
            cache_module._snapshot_refresh_loop()
        except StopIteration:
            pass

    mock_refresh.assert_not_called()
    mock_sleep.assert_called_once_with(42)


@patch("data.cache._is_realtime_active", return_value=False)
@patch("data.cache.refresh_latest_snapshot")
def test_snapshot_loop_refreshes_when_realtime_inactive(mock_refresh, mock_active):
    import data.cache as cache_module

    def fake_sleep(seconds):
        raise StopIteration("break loop")

    with patch.object(cache_module.time, "sleep", side_effect=fake_sleep):
        try:
            cache_module._snapshot_refresh_loop()
        except StopIteration:
            pass

    mock_refresh.assert_called_once()
