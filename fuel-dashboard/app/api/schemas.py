from enum import Enum
from typing import List
from typing import Optional

from pydantic import BaseModel


class FuelType(str, Enum):
    diesel_a_price = "diesel_a_price"
    diesel_b_price = "diesel_b_price"
    diesel_premium_price = "diesel_premium_price"
    gasoline_95_e5_price = "gasoline_95_e5_price"
    gasoline_95_e10_price = "gasoline_95_e10_price"
    gasoline_95_e5_premium_price = "gasoline_95_e5_premium_price"
    gasoline_98_e5_price = "gasoline_98_e5_price"
    gasoline_98_e10_price = "gasoline_98_e10_price"
    biodiesel_price = "biodiesel_price"
    bioethanol_price = "bioethanol_price"
    compressed_natural_gas_price = "compressed_natural_gas_price"
    liquefied_natural_gas_price = "liquefied_natural_gas_price"
    liquefied_petroleum_gases_price = "liquefied_petroleum_gases_price"
    hydrogen_price = "hydrogen_price"


class TrendPeriod(str, Enum):
    week = "week"
    month = "month"
    quarter = "quarter"


TREND_PERIOD_DAYS = {
    TrendPeriod.week: 7,
    TrendPeriod.month: 30,
    TrendPeriod.quarter: 90,
}


class StationResult(BaseModel):
    label: str
    address: str
    municipality: str
    province: str
    zip_code: str
    latitude: float
    longitude: float
    price: float
    distance_km: Optional[float] = None
    score: Optional[float] = None


class ZoneResult(BaseModel):
    zip_code: str
    avg_price: float
    min_price: float
    station_count: int


class TrendPoint(BaseModel):
    date: str
    avg_price: float
    min_price: float
    max_price: float


class StationListResponse(BaseModel):
    stations: List[StationResult]
    fuel_type: str
    query_type: str


class ZoneListResponse(BaseModel):
    zones: List[ZoneResult]
    province: str
    fuel_type: str


class TrendResponse(BaseModel):
    trend: List[TrendPoint]
    zip_code: str
    fuel_type: str
    period: str
