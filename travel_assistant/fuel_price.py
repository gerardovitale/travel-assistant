import logging
from datetime import datetime
from datetime import timezone
from typing import Iterable
from typing import Optional

import pytz
import requests
from pyspark.sql import DataFrame
from pyspark.sql import Row

from travel_assistant.config import Config
from travel_assistant.entity import SpainFuelPrice
from travel_assistant.quality import data_quality_metrics
from travel_assistant.schema import SPAIN_FUEL_PRICES_SCHEMA

DATA_SOURCE_TIMEZONE = pytz.timezone("Europe/Madrid")
DATA_SOURCE_DATETIME_FORMAT = "%d/%m/%Y %H:%M:%S"

logger = logging.getLogger(__name__)


def update_spain_fuel_price_table(config: Config) -> None:
    response_data = get_spain_fuel_price_raw_data(config)
    spain_fuel_data = map_spain_fuel_data(response_data)
    spain_fuel_df = create_spain_fuel_dataframe(config, spain_fuel_data)
    logger.info("Appending Spain Fuel Table to: {0}".format(config.DESTINATION_PATH))
    (
        # ToDo: what about when the actual data already exist in the table?
        spain_fuel_df.write.format("delta")
        .mode("append")
        .partitionBy(config.PARTITION_COLS)
        .option("mergeSchema", "true")
        .save(config.DESTINATION_PATH)
    )


def get_spain_fuel_price_raw_data(config: Config) -> dict:
    logger.info("Getting fuel price data")
    response = requests.get(config.DATA_SOURCE_URL)
    data = response.json()
    logger.info("Status = {0}".format(data.get("ResultadoConsulta")))
    return data


def map_spain_fuel_data(spain_fuel_data: dict):
    def _map_spain_fuel_price_wrapped_func(record):
        return map_spain_fuel_price(record, utc_datetime_obj)

    logger.info("Mapping Spain fuel data")
    sting_datetime = spain_fuel_data.get("Fecha")
    datetime_obj = datetime.strptime(sting_datetime, DATA_SOURCE_DATETIME_FORMAT)
    utc_datetime_obj = DATA_SOURCE_TIMEZONE.localize(datetime_obj).astimezone(timezone.utc)
    logger.info(
        "Datetime = {0}, Date = {1}, Hour = {2}".format(
            datetime_obj.isoformat(), datetime_obj.date().isoformat(), datetime_obj.hour
        )
    )
    spain_fuel_data_list = spain_fuel_data.get("ListaEESSPrecio")
    return map(_map_spain_fuel_price_wrapped_func, spain_fuel_data_list)


@data_quality_metrics("spain-fuel-price")
def create_spain_fuel_dataframe(config: Config, spain_fuel_price_list: Iterable[SpainFuelPrice]) -> DataFrame:
    def _map_spark_rows(spain_fuel_price: SpainFuelPrice) -> Row:
        spain_fuel_price_dict = spain_fuel_price.model_dump()
        spain_fuel_price_dict["timestamp"] = spain_fuel_price_dict["timestamp"].isoformat()
        spain_fuel_price_dict["date"] = spain_fuel_price_dict["date"].isoformat()
        return Row(**spain_fuel_price_dict)

    logger.info("Creating Spain fuel dataframe")
    row_list = map(_map_spark_rows, spain_fuel_price_list)
    spain_fuel_df = config.get_spark_session().createDataFrame(row_list, SPAIN_FUEL_PRICES_SCHEMA)
    if spain_fuel_df.limit(1).rdd.isEmpty():
        logger.error("The created dataframe is empty")
        raise EmptyDataframe()
    return spain_fuel_df


def map_spain_fuel_price(record: dict, utc_datetime_obj: datetime) -> SpainFuelPrice:
    def _format_string(string: str) -> str:
        return string.lower().strip()

    def _format_float(string: str) -> Optional[str]:
        formatted_string = _format_string(string)
        if formatted_string == "":
            return None
        return formatted_string.replace(",", ".")

    return SpainFuelPrice(
        timestamp=utc_datetime_obj,
        date=utc_datetime_obj.date(),
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


class EmptyDataframe(Exception):
    ...
