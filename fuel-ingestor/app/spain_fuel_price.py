import json
import logging
import subprocess
import time
from datetime import datetime
from datetime import timezone

import pandas as pd
import pytz
from entity import get_expected_columns
from entity import get_float_columns
from entity import get_renaming_map
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage

DATA_SOURCE_URL = (
    "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
)
DATA_SOURCE_DATETIME_FORMAT = "%d/%m/%Y %H:%M:%S"
DATA_SOURCE_TIMEZONE = pytz.timezone("Europe/Madrid")
DATA_DESTINATION_BUCKET = "travel-assistant-spain-fuel-prices"

logger = logging.getLogger(__name__)


def extract_fuel_prices_raw_data() -> dict:
    """Fetch fuel price data from the Spanish government API using curl.

    Python's ssl module (OpenSSL 3.x) is blocked by the server's TLS fingerprinting,
    while curl's TLS handshake is accepted. We use subprocess + curl to bypass this.
    """
    logger.info(f"Getting fuel price raw data from {DATA_SOURCE_URL}")

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
                    "120",
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
                timeout=130,
            )
            break
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            if attempt < max_attempts:
                logger.warning(f"Attempt {attempt}/{max_attempts} failed: {exc}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"All {max_attempts} attempts failed. Last error: {exc}")
                raise

    raw_data = json.loads(result.stdout)
    _validate_api_response(raw_data)
    logger.info("Response status: {0}".format(raw_data.get("ResultadoConsulta")))
    return raw_data


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


def create_spain_fuel_dataframe(raw_data_response: dict) -> pd.DataFrame:
    logger.info("Creating Spain Fuel DataFrame")
    data = raw_data_response.get("ListaEESSPrecio")
    logger.info("Number of Data Points: {0}".format(len(data)))
    logger.info("Existing keys: {0}".format(list(data[0].keys())))
    renaming_map, float_columns = get_renaming_map(), get_float_columns()
    fuel_df = pd.DataFrame(data).rename(columns=renaming_map)[list(renaming_map.values())]

    logger.info("Processing columns")
    for column in fuel_df.columns:
        fuel_df[column] = fuel_df[column].str.lower().str.strip()
        if column in float_columns:
            fuel_df[column] = pd.to_numeric(fuel_df[column].str.replace(",", "."), errors="coerce")

    logger.info("Adding datetime column")
    sting_datetime = raw_data_response.get("Fecha")
    datetime_obj = datetime.strptime(sting_datetime, DATA_SOURCE_DATETIME_FORMAT)
    utc_datetime_obj = DATA_SOURCE_TIMEZONE.localize(datetime_obj).astimezone(timezone.utc)
    fuel_df["timestamp"] = utc_datetime_obj.isoformat()

    return fuel_df[get_expected_columns()]


def write_spain_fuel_prices_data_as_parquet(spain_fuel_prices_df: pd.DataFrame) -> None:
    logger.info(f"Writing Spain Fuel Price Data to: {DATA_DESTINATION_BUCKET}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(DATA_DESTINATION_BUCKET)
    timestamp = datetime.now().isoformat(timespec="seconds")
    blob = bucket.blob(f"spain_fuel_prices_{timestamp}.parquet")
    parquet_data = spain_fuel_prices_df.to_parquet(index=False, compression="snappy")

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            blob.upload_from_string(parquet_data, "application/octet-stream")
            logger.info(f"Successfully uploaded {blob.name} ({len(spain_fuel_prices_df)} rows)")
            return
        except (GoogleAPIError, ConnectionError, TimeoutError) as exc:
            if attempt < max_attempts:
                delay = 2**attempt
                logger.warning(f"GCS upload attempt {attempt}/{max_attempts} failed: {exc}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"GCS upload failed after {max_attempts} attempts: {exc}")
                raise


MIN_EXPECTED_STATIONS = 5000
PRICE_MIN = 0.5
PRICE_MAX = 3.0
LATITUDE_RANGE = (27.0, 44.0)
LONGITUDE_RANGE = (-19.0, 5.0)


def validate_dataframe(df: pd.DataFrame) -> None:
    row_count = len(df)
    if row_count == 0:
        raise ValueError("DataFrame is empty — no fuel station data to upload")
    if row_count < MIN_EXPECTED_STATIONS:
        logger.warning(f"Low station count: {row_count} (expected >= {MIN_EXPECTED_STATIONS})")

    expected_columns = set(get_expected_columns())
    missing = expected_columns - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    float_columns = get_float_columns()
    price_columns = [c for c in float_columns if c.endswith("_price")]
    for col in price_columns:
        non_null = df[col].dropna()
        if non_null.empty:
            continue
        out_of_range = non_null[(non_null < PRICE_MIN) | (non_null > PRICE_MAX)]
        if len(out_of_range) > 0:
            logger.warning(f"{col}: {len(out_of_range)} values outside [{PRICE_MIN}, {PRICE_MAX}] range")
        null_pct = df[col].isna().mean() * 100
        if null_pct > 0:
            logger.info(f"{col}: {null_pct:.1f}% null values")

    if "latitude" in df.columns and "longitude" in df.columns:
        lat = df["latitude"].dropna()
        lon = df["longitude"].dropna()
        lat_out = lat[(lat < LATITUDE_RANGE[0]) | (lat > LATITUDE_RANGE[1])]
        lon_out = lon[(lon < LONGITUDE_RANGE[0]) | (lon > LONGITUDE_RANGE[1])]
        if len(lat_out) > 0:
            logger.warning(f"latitude: {len(lat_out)} values outside {LATITUDE_RANGE}")
        if len(lon_out) > 0:
            logger.warning(f"longitude: {len(lon_out)} values outside {LONGITUDE_RANGE}")
