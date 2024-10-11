from datetime import date
from datetime import datetime
from typing import Dict
from typing import Optional

from pydantic import BaseModel


class SpainFuelPrice(BaseModel):
    timestamp: datetime
    date: date
    hour: int
    zip_code: int
    municipality_id: int
    municipality: str
    province_id: int
    province: str
    locality: str
    sale_type: str
    label: str
    address: str
    latitude: float
    longitude: float
    biodiesel_price: Optional[float]
    bioethanol_price: Optional[float]
    compressed_natural_gas_price: Optional[float]
    liquefied_natural_gas_price: Optional[float]
    liquefied_petroleum_gases_price: Optional[float]
    diesel_a_price: Optional[float]
    diesel_b_price: Optional[float]
    diesel_premium_price: Optional[float]
    gasoline_95_e10_price: Optional[float]
    gasoline_95_e5_price: Optional[float]
    gasoline_95_e5_premium_price: Optional[float]
    gasoline_98_e10_price: Optional[float]
    gasoline_98_e5_price: Optional[float]
    hydrogen_price: Optional[float]


def get_renaming_map() -> Dict[str, str]:
    return {
        "C.P.": "zip_code",
        "IDEESS": "eess_id",
        "IDCCAA": "ccaa_id",
        "IDMunicipio": "municipality_id",
        "IDProvincia": "province_id",
        "Tipo Venta": "sale_type",
        "Rótulo": "label",
        "Dirección": "address",
        "Municipio": "municipality",
        "Provincia": "province",
        "Localidad": "locality",
        "Latitud": "latitude",
        "Longitud (WGS84)": "longitude",
        "Precio Biodiesel": "biodiesel_price",
        "Precio Bioetanol": "bioethanol_price",
        "Precio Gas Natural Comprimido": "compressed_natural_gas_price",
        "Precio Gas Natural Licuado": "liquefied_natural_gas_price",
        "Precio Gases licuados del petróleo": "liquefied_petroleum_gases_price",
        "Precio Gasoleo A": "diesel_a_price",
        "Precio Gasoleo B": "diesel_b_price",
        "Precio Gasoleo Premium": "diesel_premium_price",
        "Precio Gasolina 95 E10": "gasoline_95_e10_price",
        "Precio Gasolina 95 E5": "gasoline_95_e5_price",
        "Precio Gasolina 95 E5 Premium": "gasoline_95_e5_premium_price",
        "Precio Gasolina 98 E10": "gasoline_98_e10_price",
        "Precio Gasolina 98 E5": "gasoline_98_e5_price",
        "Precio Hidrogeno": "hydrogen_price",
    }


def get_float_columns():
    return [
        "latitude",
        "longitude",
        "biodiesel_price",
        "bioethanol_price",
        "compressed_natural_gas_price",
        "liquefied_natural_gas_price",
        "liquefied_petroleum_gases_price",
        "diesel_a_price",
        "diesel_b_price",
        "diesel_premium_price",
        "gasoline_95_e10_price",
        "gasoline_95_e5_price",
        "gasoline_95_e5_premium_price",
        "gasoline_98_e10_price",
        "gasoline_98_e5_price",
        "hydrogen_price",
    ]


def get_expected_columns():
    return [
        "timestamp",
        "date",
        "zip_code",
        "eess_id",
        "ccaa_id",
        "municipality_id",
        "province_id",
        "sale_type",
        "label",
        "address",
        "municipality",
        "province",
        "locality",
        "latitude",
        "longitude",
        "biodiesel_price",
        "bioethanol_price",
        "compressed_natural_gas_price",
        "liquefied_natural_gas_price",
        "liquefied_petroleum_gases_price",
        "diesel_a_price",
        "diesel_b_price",
        "diesel_premium_price",
        "gasoline_95_e10_price",
        "gasoline_95_e5_price",
        "gasoline_95_e5_premium_price",
        "gasoline_98_e10_price",
        "gasoline_98_e5_price",
        "hydrogen_price",
    ]
