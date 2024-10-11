import logging
from datetime import datetime
from datetime import timezone

import pandas as pd
import pytz
import requests
from google.cloud import storage

from src.entity import get_expected_columns
from src.entity import get_float_columns
from src.entity import get_renaming_map

DATA_SOURCE_URL = (
    "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
)
DATA_SOURCE_DATETIME_FORMAT = "%d/%m/%Y %H:%M:%S"
DATA_SOURCE_TIMEZONE = pytz.timezone("Europe/Madrid")
DATA_DESTINATION_BUCKET = "spain-fuel-prices"

logger = logging.getLogger(__name__)


def extract_fuel_prices_raw_data() -> dict:
    logger.info(f"Getting fuel price raw data from {DATA_SOURCE_URL}")
    response = requests.get(DATA_SOURCE_URL)
    if response.status_code != 200:
        logger.error(f"Error getting fuel price raw data with code: {response.status_code}")
        logger.error(f"Error message: {response.json()}")
        raise requests.exceptions.HTTPError
    raw_data = response.json()
    logger.info("Status = {0}".format(raw_data.get("ResultadoConsulta")))
    return raw_data


def create_spain_fuel_dataframe(raw_data_response: dict) -> pd.DataFrame:
    logger.info("Creating Spain Fuel DataFrame")
    renaming_map, float_columns = get_renaming_map(), get_float_columns()
    fuel_df = (
        pd.DataFrame(raw_data_response.get("ListaEESSPrecio"))
        .rename(columns=renaming_map)
        [list(renaming_map.values())]
    )

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
    fuel_df["date"] = utc_datetime_obj.date().isoformat()

    return fuel_df[get_expected_columns()]


def write_spain_fuel_prices_data_as_csv(spain_fuel_prices_df: pd.DataFrame) -> None:
    logger.info(f"Writing Spain Fuel Price Data to: {DATA_DESTINATION_BUCKET}")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(DATA_DESTINATION_BUCKET)
    timestamp = datetime.now().isoformat()
    blob = bucket.blob(f"spain_fuel_prices_{timestamp}.csv")
    blob.upload_from_string(spain_fuel_prices_df.to_csv(index=False), "text/csv")
