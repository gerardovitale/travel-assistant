import json
import logging
import subprocess
import time
from datetime import datetime
from datetime import timezone
from typing import Optional

import pandas as pd
import pytz

from data.entity_maps import get_expected_columns
from data.entity_maps import get_float_columns
from data.entity_maps import get_renaming_map

DATA_SOURCE_URL = (
    "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
)
DATA_SOURCE_DATETIME_FORMAT = "%d/%m/%Y %H:%M:%S"
DATA_SOURCE_TIMEZONE = pytz.timezone("Europe/Madrid")

MIN_EXPECTED_STATIONS = 5000

logger = logging.getLogger(__name__)


def fetch_realtime_stations(curl_timeout: int = 120) -> Optional[pd.DataFrame]:
    """Fetch fuel prices from the Spain government API and return a transformed DataFrame.

    Uses curl subprocess to bypass TLS fingerprinting issues with Python's OpenSSL 3.x.
    Returns None on any failure (never raises).
    """
    try:
        raw_data = _fetch_raw_data(curl_timeout)
        _validate_api_response(raw_data)
        df = _transform_to_dataframe(raw_data)
        if len(df) < MIN_EXPECTED_STATIONS:
            raise ValueError(f"Real-time fetch returned only {len(df)} stations (expected >= {MIN_EXPECTED_STATIONS})")
        return df

    except Exception:
        logger.exception("Real-time fuel price fetch failed")
        return None


def _fetch_raw_data(curl_timeout: int) -> dict:
    max_attempts = 3
    retry_delay = 10
    for attempt in range(1, max_attempts + 1):
        try:
            result = subprocess.run(
                [
                    "curl",
                    "-s",
                    "-f",
                    "--connect-timeout",
                    "10",
                    "--max-time",
                    str(curl_timeout),
                    "--tlsv1.2",
                    "--tls-max",
                    "1.2",
                    "-H",
                    "Accept: application/json",
                    DATA_SOURCE_URL,
                ],
                capture_output=True,
                text=True,
                check=True,
                timeout=curl_timeout + 10,
            )
            return json.loads(result.stdout)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            if attempt < max_attempts:
                logger.warning(
                    "Real-time fetch attempt %d/%d failed: %s. Retrying in %ds...",
                    attempt,
                    max_attempts,
                    exc,
                    retry_delay,
                )
                time.sleep(retry_delay)
            else:
                raise
    raise RuntimeError("All fetch attempts exhausted")


def _validate_api_response(raw_data: dict) -> None:
    status = raw_data.get("ResultadoConsulta")
    if status != "OK":
        raise ValueError(f"API returned non-OK status: {status!r}")

    stations = raw_data.get("ListaEESSPrecio")
    if not stations or not isinstance(stations, list):
        raise ValueError(f"API response missing or empty 'ListaEESSPrecio' (got {type(stations).__name__})")

    fecha = raw_data.get("Fecha")
    if not fecha:
        raise ValueError("API response missing 'Fecha' field")
    try:
        datetime.strptime(fecha, DATA_SOURCE_DATETIME_FORMAT)
    except ValueError:
        raise ValueError(f"API 'Fecha' field has unexpected format: {fecha!r}")


def _transform_to_dataframe(raw_data: dict) -> pd.DataFrame:
    data = raw_data["ListaEESSPrecio"]
    renaming_map = get_renaming_map()
    float_columns = get_float_columns()

    df = pd.DataFrame(data).rename(columns=renaming_map)[list(renaming_map.values())]

    for column in df.columns:
        df[column] = df[column].str.lower().str.strip()
        if column in float_columns:
            df[column] = pd.to_numeric(df[column].str.replace(",", "."), errors="coerce")

    fecha_str = raw_data["Fecha"]
    datetime_obj = datetime.strptime(fecha_str, DATA_SOURCE_DATETIME_FORMAT)
    utc_datetime_obj = DATA_SOURCE_TIMEZONE.localize(datetime_obj).astimezone(timezone.utc)
    df["timestamp"] = utc_datetime_obj.isoformat()

    return df[get_expected_columns()]
