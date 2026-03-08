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
    logger.info("Response status: {0}".format(raw_data.get("ResultadoConsulta")))
    return raw_data


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
    logger.info("Writing Parquet data")
    blob.upload_from_string(
        spain_fuel_prices_df.to_parquet(index=False, compression="snappy"), "application/octet-stream"
    )
