import logging
import ssl
import time
from datetime import datetime
from datetime import timezone

import pandas as pd
import pytz
import requests
from entity import get_expected_columns
from entity import get_float_columns
from entity import get_renaming_map
from google.cloud import storage
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout
from urllib3.util.retry import Retry
from urllib3.util.ssl_ import create_urllib3_context

DATA_SOURCE_URL = (
    "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
)
DATA_SOURCE_DATETIME_FORMAT = "%d/%m/%Y %H:%M:%S"
DATA_SOURCE_TIMEZONE = pytz.timezone("Europe/Madrid")
DATA_DESTINATION_BUCKET = "travel-assistant-spain-fuel-prices"

logger = logging.getLogger(__name__)


class TLSv12Adapter(HTTPAdapter):
    """HTTPS adapter that forces TLS 1.2 for servers that don't support TLS 1.3."""

    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_default_certs()
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


def extract_fuel_prices_raw_data() -> dict:
    logger.info(f"Getting fuel price raw data from {DATA_SOURCE_URL}")
    retry_strategy = Retry(
        total=5,
        backoff_factor=3,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = TLSv12Adapter(max_retries=retry_strategy)
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FuelPriceBot/1.0)"})
    session.mount("https://", adapter)

    max_attempts = 3
    retry_delay = 30
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(DATA_SOURCE_URL, timeout=30)
            break
        except (RequestsConnectionError, RequestsTimeout) as exc:
            if attempt < max_attempts:
                logger.warning(f"Attempt {attempt}/{max_attempts} failed: {exc}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"All {max_attempts} attempts failed. Last error: {exc}")
                raise
    if response.status_code != 200:
        logger.error(f"Error getting fuel price raw data with code: {response.status_code}")
        logger.error(f"Error message: {response.json()}")
        raise requests.exceptions.HTTPError
    logger.info("Response code: {0}".format(response.status_code))
    logger.info("Response headers: {0}".format(response.headers))
    raw_data = response.json()
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
