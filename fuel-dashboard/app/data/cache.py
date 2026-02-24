import logging
import threading
import time

from config import settings

from data.duckdb_engine import refresh_latest_snapshot

logger = logging.getLogger(__name__)

_refresh_thread: threading.Thread = None
_data_ready = threading.Event()


def is_data_ready() -> bool:
    return _data_ready.is_set()


def _refresh_loop():
    while True:
        try:
            logger.info("Refreshing data cache")
            refresh_latest_snapshot()
            _data_ready.set()
        except Exception as e:
            logger.error(f"Error refreshing cache: {e}")
        time.sleep(settings.cache_ttl_seconds)


def start_cache_refresh():
    global _refresh_thread
    if _refresh_thread is not None and _refresh_thread.is_alive():
        return
    _refresh_thread = threading.Thread(target=_refresh_loop, daemon=True)
    _refresh_thread.start()
    logger.info(f"Cache refresh started (TTL: {settings.cache_ttl_seconds}s)")
    logger.info("Waiting for initial data load...")
    if _data_ready.wait(timeout=60):
        logger.info("Initial data load complete")
    else:
        logger.warning("Initial data load timed out — queries will fail until data is available")
