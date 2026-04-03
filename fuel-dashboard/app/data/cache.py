import logging
import threading
import time

from config import settings

from data.duckdb_engine import refresh_latest_snapshot
from data.duckdb_engine import refresh_zip_code_trend_snapshot
from data.gcs_client import get_aggregate_cache_age_seconds

logger = logging.getLogger(__name__)

_snapshot_refresh_thread: threading.Thread = None
_trend_refresh_thread: threading.Thread = None
_data_ready = threading.Event()


def is_data_ready() -> bool:
    return _data_ready.is_set()


def _snapshot_refresh_loop():
    while True:
        try:
            logger.info("Refreshing data cache")
            refresh_latest_snapshot()
            _data_ready.set()
        except Exception as e:
            logger.error(f"Error refreshing cache: {e}")
        time.sleep(settings.cache_ttl_seconds)


def _trend_refresh_loop(initial_delay_seconds: int = 0):
    if initial_delay_seconds > 0:
        time.sleep(initial_delay_seconds)
    refresh_interval_seconds = max(1, settings.parquet_cache_max_age_hours * 3600)
    while True:
        try:
            logger.info("Refreshing zip-code trend cache")
            refresh_zip_code_trend_snapshot()
        except Exception as e:
            logger.error(f"Error refreshing zip-code trend cache: {e}")
        time.sleep(refresh_interval_seconds)


def _initial_trend_refresh_delay_seconds(skip_initial_trend_refresh: bool) -> int:
    if not skip_initial_trend_refresh:
        return 0

    refresh_interval_seconds = max(1, settings.parquet_cache_max_age_hours * 3600)
    cache_age_seconds = get_aggregate_cache_age_seconds("zip_code_daily_stats.parquet")
    if cache_age_seconds is None:
        return 0
    return max(0, int(refresh_interval_seconds - cache_age_seconds))


def start_cache_refresh(skip_initial_trend_refresh: bool = False):
    global _snapshot_refresh_thread, _trend_refresh_thread

    if _snapshot_refresh_thread is None or not _snapshot_refresh_thread.is_alive():
        _snapshot_refresh_thread = threading.Thread(target=_snapshot_refresh_loop, daemon=True)
        _snapshot_refresh_thread.start()
        logger.info(f"Cache refresh started (TTL: {settings.cache_ttl_seconds}s) — initial load in background")

    if _trend_refresh_thread is None or not _trend_refresh_thread.is_alive():
        initial_delay_seconds = _initial_trend_refresh_delay_seconds(skip_initial_trend_refresh)
        _trend_refresh_thread = threading.Thread(
            target=_trend_refresh_loop,
            kwargs={"initial_delay_seconds": initial_delay_seconds},
            daemon=True,
        )
        _trend_refresh_thread.start()
        logger.info(
            "Zip-code trend refresh started (interval: %ss)%s",
            settings.parquet_cache_max_age_hours * 3600,
            f" — initial refresh deferred by {initial_delay_seconds}s" if initial_delay_seconds > 0 else "",
        )
