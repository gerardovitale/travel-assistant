from datetime import date
from datetime import datetime
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
