import logging
from datetime import datetime
from datetime import timezone
from typing import Iterable
from typing import Optional

import requests
from pyspark.sql import DataFrame
from pyspark.sql import Row

from travel_assistant import CET_TIMEZONE
from travel_assistant import get_spark_session
from travel_assistant.entity import SpainFuelPrice
from travel_assistant.schema import SPAIN_FUEL_PRICES_SCHEMA

logger = logging.getLogger(__name__)


def update_spain_fuel_price_table() -> None:
    response_data = get_spain_fuel_price_raw_data()
    spain_fuel_data = map_spain_fuel_data(response_data)
    spain_fuel_df = create_spain_fuel_dataframe(spain_fuel_data)
    spain_fuel_df.write.format("delta").mode("append").save("../data/spain-fuel-price")


def get_spain_fuel_price_raw_data() -> dict:
    logger.info("Getting data from the Ministry")
    url = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
    response = requests.get(url)
    data = response.json()
    return data


def map_spain_fuel_data(spain_fuel_data):
    def _map_spain_fuel_price_wrapped_func(record):
        return map_spain_fuel_price(record, spain_fuel_data.get("Fecha"))

    logger.info("Mapping Spain fuel data")
    spain_fuel_data_list = spain_fuel_data.get("ListaEESSPrecio")
    return map(_map_spain_fuel_price_wrapped_func, spain_fuel_data_list)


def create_spain_fuel_dataframe(spain_fuel_price_list: Iterable[SpainFuelPrice]) -> DataFrame:
    def _map_spark_rows(spain_fuel_price: SpainFuelPrice) -> Row:
        spain_fuel_price_dict = spain_fuel_price.dict()
        spain_fuel_price_dict["date"] = spain_fuel_price_dict["date"].isoformat()
        return Row(**spain_fuel_price_dict)

    logger.info("Creating Spain fuel dataframe")
    row_list = map(_map_spark_rows, spain_fuel_price_list)
    return get_spark_session().createDataFrame(row_list, SPAIN_FUEL_PRICES_SCHEMA)


def map_spain_fuel_price(record, sting_datetime) -> SpainFuelPrice:
    def _format_string(string: str) -> str:
        return string.lower().strip()

    def _format_float(string: str) -> Optional[str]:
        formatted_string = _format_string(string)
        if formatted_string == "":
            return None
        return formatted_string.replace(",", ".")

    datetime_obj = datetime.strptime(sting_datetime, "%d/%m/%Y %H:%M:%S")
    utc_datetime_obj = CET_TIMEZONE.localize(datetime_obj).astimezone(timezone.utc)
    return SpainFuelPrice(
        **{
            "date": utc_datetime_obj,
            "zip_code": _format_string(record.get("C.P.", "")),
            "municipality_id": _format_string(record.get("IDMunicipio", "")),
            "province_id": _format_string(record.get("IDProvincia", "")),
            "sale_type": _format_string(record.get("Tipo Venta", "")),
            "label": _format_string(record.get("Rótulo", "")),
            "address": _format_string(record.get("Dirección", "")),
            "municipality": _format_string(record.get("Municipio", "")),
            "province": _format_string(record.get("Provincia", "")),
            "locality": _format_string(record.get("Localidad", "")),
            "latitude": _format_float(record.get("Latitud", "")),
            "longitude": _format_float(record.get("Longitud (WGS84)", "")),
            "biodiesel_price": _format_float(record.get("Precio Biodiesel", "")),
            "bioethanol_price": _format_float(record.get("Precio Bioetanol", "")),
            "compressed_natural_gas_price": _format_float(record.get("Precio Gas Natural Comprimido", "")),
            "liquefied_natural_gas_price": _format_float(record.get("Precio Gas Natural Licuado", "")),
            "liquefied_petroleum_gases_price": _format_float(record.get("Precio Gases licuados del petróleo", "")),
            "diesel_a_price": _format_float(record.get("Precio Gasoleo A", "")),
            "diesel_b_price": _format_float(record.get("Precio Gasoleo B", "")),
            "diesel_premium_price": _format_float(record.get("Precio Gasoleo Premium", "")),
            "gasoline_95_e10_price": _format_float(record.get("Precio Gasolina 95 E10", "")),
            "gasoline_95_e5_price": _format_float(record.get("Precio Gasolina 95 E5", "")),
            "gasoline_95_e5_premium_price": _format_float(record.get("Precio Gasolina 95 E5 Premium", "")),
            "gasoline_98_e10_price": _format_float(record.get("Precio Gasolina 98 E10", "")),
            "gasoline_98_e5_price": _format_float(record.get("Precio Gasolina 98 E5", "")),
            "hydrogen_price": _format_float(record.get("Precio Hidrogeno", "")),
        }
    )
