from unittest.mock import patch


@patch("data.cache.get_aggregate_cache_age_seconds")
def test_initial_trend_refresh_delay_is_zero_without_skip(mock_age):
    from data.cache import _initial_trend_refresh_delay_seconds

    mock_age.return_value = 120

    assert _initial_trend_refresh_delay_seconds(False) == 0


@patch("data.cache.get_aggregate_cache_age_seconds")
@patch("data.cache.settings.parquet_cache_max_age_hours", 2)
def test_initial_trend_refresh_delay_uses_remaining_cache_ttl(mock_age):
    from data.cache import _initial_trend_refresh_delay_seconds

    mock_age.return_value = 60 * 60 * 1.5

    assert _initial_trend_refresh_delay_seconds(True) == 1800


@patch("data.cache.get_aggregate_cache_age_seconds")
@patch("data.cache.settings.parquet_cache_max_age_hours", 2)
def test_initial_trend_refresh_delay_is_zero_when_cache_age_is_missing(mock_age):
    from data.cache import _initial_trend_refresh_delay_seconds

    mock_age.return_value = None

    assert _initial_trend_refresh_delay_seconds(True) == 0
