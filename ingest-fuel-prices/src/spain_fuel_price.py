import logging
from datetime import datetime
from datetime import timezone

import pandas as pd
import pytz
import requests
from google.cloud import storage

from src.entity import SpainFuelPrice

DATA_SOURCE_URL = (
    "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
)
DATA_SOURCE_DATETIME_FORMAT = "%d/%m/%Y %H:%M:%S"
DATA_SOURCE_TIMEZONE = pytz.timezone("Europe/Madrid")
DATA_DESTINATION_BUCKET = "gs://travel-assistant-417315-spain-fuel-prices"

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


def map_raw_data_into_spain_fuel_price(raw_fuel_data: dict):
    def _map_spain_fuel_price_wrapped_func(record):
        return map_spain_fuel_price(record, utc_datetime_obj)

    logger.info("Mapping Spain fuel data")
    sting_datetime = raw_fuel_data.get("Fecha")
    datetime_obj = datetime.strptime(sting_datetime, DATA_SOURCE_DATETIME_FORMAT)
    utc_datetime_obj = DATA_SOURCE_TIMEZONE.localize(datetime_obj).astimezone(timezone.utc)
    logger.info(
        "Datetime = {0}, Date = {1}, Hour = {2}".format(
            datetime_obj.isoformat(), datetime_obj.date().isoformat(), datetime_obj.hour
        )
    )
    raw_fuel_data_list = raw_fuel_data.get("ListaEESSPrecio")
    return map(_map_spain_fuel_price_wrapped_func, raw_fuel_data_list)


def map_spain_fuel_price(record: dict, utc_datetime_obj: datetime) -> SpainFuelPrice:
    def _format_string(string: str) -> str:
        return string.lower().strip()

    def _format_float(string: str) -> Optional[str]:
        formatted_string = _format_string(string)
        if formatted_string == "":
            return None
        return formatted_string.replace(",", ".")

    return SpainFuelPrice(
        timestamp=utc_datetime_obj.isoformat(),
        date=utc_datetime_obj.date().isoformat(),
        hour=utc_datetime_obj.hour,
        zip_code=_format_string(record.get("C.P.", "")),
        municipality_id=_format_string(record.get("IDMunicipio", "")),
        province_id=_format_string(record.get("IDProvincia", "")),
        sale_type=_format_string(record.get("Tipo Venta", "")),
        label=_format_string(record.get("Rótulo", "")),
        address=_format_string(record.get("Dirección", "")),
        municipality=_format_string(record.get("Municipio", "")),
        province=_format_string(record.get("Provincia", "")),
        locality=_format_string(record.get("Localidad", "")),
        latitude=_format_float(record.get("Latitud", "")),
        longitude=_format_float(record.get("Longitud (WGS84)", "")),
        biodiesel_price=_format_float(record.get("Precio Biodiesel", "")),
        bioethanol_price=_format_float(record.get("Precio Bioetanol", "")),
        compressed_natural_gas_price=_format_float(record.get("Precio Gas Natural Comprimido", "")),
        liquefied_natural_gas_price=_format_float(record.get("Precio Gas Natural Licuado", "")),
        liquefied_petroleum_gases_price=_format_float(record.get("Precio Gases licuados del petróleo", "")),
        diesel_a_price=_format_float(record.get("Precio Gasoleo A", "")),
        diesel_b_price=_format_float(record.get("Precio Gasoleo B", "")),
        diesel_premium_price=_format_float(record.get("Precio Gasoleo Premium", "")),
        gasoline_95_e10_price=_format_float(record.get("Precio Gasolina 95 E10", "")),
        gasoline_95_e5_price=_format_float(record.get("Precio Gasolina 95 E5", "")),
        gasoline_95_e5_premium_price=_format_float(record.get("Precio Gasolina 95 E5 Premium", "")),
        gasoline_98_e10_price=_format_float(record.get("Precio Gasolina 98 E10", "")),
        gasoline_98_e5_price=_format_float(record.get("Precio Gasolina 98 E5", "")),
        hydrogen_price=_format_float(record.get("Precio Hidrogeno", "")),
    )


def create_spain_fuel_dataframe(spain_fuel_price_list) -> pd.DataFrame:
    return pd.DataFrame(spain_fuel_price_list)


def write_spain_fuel_prices_data_as_csv(spain_fuel_prices_df: pd.DataFrame) -> None:
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(DATA_DESTINATION_BUCKET)
    timestamp = datetime.now().isoformat()
    blob = bucket.blob(f'spain_fuel_prices_{timestamp}.csv')
    blob.upload_from_string(spain_fuel_prices_df.to_csv(index=False), 'text/csv')
