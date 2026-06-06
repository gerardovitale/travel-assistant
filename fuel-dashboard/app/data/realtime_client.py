import logging

import pandas as pd
from spain_fuel_api import fetch_fuel_stations

MIN_EXPECTED_STATIONS = 5000

logger = logging.getLogger(__name__)


def fetch_realtime_stations(
    curl_timeout: int = 120,
    connect_timeout: int = 10,
    max_retries: int = 3,
    retry_base_delay: int = 10,
) -> pd.DataFrame | None:
    """Fetch real-time fuel prices from the Spain government API.

    Thin wrapper over spain_fuel_api.fetch_fuel_stations adding dashboard-specific
    graceful degradation: returns None on any failure (never raises) and rejects
    undersized payloads below MIN_EXPECTED_STATIONS.
    """
    try:
        df = fetch_fuel_stations(
            curl_timeout=curl_timeout,
            connect_timeout=connect_timeout,
            max_retries=max_retries,
            retry_base_delay=retry_base_delay,
        )
        if len(df) < MIN_EXPECTED_STATIONS:
            raise ValueError(f"Real-time fetch returned only {len(df)} stations (expected >= {MIN_EXPECTED_STATIONS})")
        return df
    except Exception:
        logger.exception("Real-time fuel price fetch failed")
        return None
