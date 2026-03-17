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
    estimated_total_cost: Optional[float] = None
    route_km: Optional[float] = None
    detour_minutes: Optional[float] = None


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


class ProvincePriceResult(BaseModel):
    province: str
    avg_price: float
    station_count: int


class DistrictPriceResult(BaseModel):
    district: str
    avg_price: float
    station_count: int


class TripStop(BaseModel):
    station: StationResult
    route_km: float
    detour_minutes: float
    fuel_at_arrival_pct: float
    liters_to_fill: float
    cost_eur: float
    reasoning: Optional[str] = None


class AlternativePlan(BaseModel):
    strategy_name: str
    strategy_description: str
    stops: List[TripStop]
    total_fuel_cost: float
    total_fuel_liters: float
    total_detour_minutes: float
    fuel_at_destination_pct: float = 0.0

    @property
    def num_stops(self) -> int:
        return len(self.stops)


class TripPlan(BaseModel):
    stops: List[TripStop]
    total_fuel_cost: float
    total_distance_km: float
    duration_minutes: float
    total_fuel_liters: float
    savings_eur: float
    route_coordinates: List[List[float]]
    candidate_stations: List[StationResult]
    origin_coords: List[float]
    destination_coords: List[float]
    fuel_at_destination_pct: float = 0.0
    alternative_plans: List[AlternativePlan] = []


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
