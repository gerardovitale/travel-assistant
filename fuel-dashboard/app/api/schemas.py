from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from config import settings
from pydantic import BaseModel
from pydantic import Field


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


class FuelGroup(str, Enum):
    diesel = "diesel"
    gasoline_95 = "gasoline_95"
    gasoline_98 = "gasoline_98"
    biofuel = "biofuel"
    natural_gas = "natural_gas"


FUEL_GROUP_MEMBERS: Dict[FuelGroup, List[FuelType]] = {
    FuelGroup.diesel: [
        FuelType.diesel_a_price,
        FuelType.diesel_b_price,
        FuelType.diesel_premium_price,
    ],
    FuelGroup.gasoline_95: [
        FuelType.gasoline_95_e5_price,
        FuelType.gasoline_95_e10_price,
        FuelType.gasoline_95_e5_premium_price,
    ],
    FuelGroup.gasoline_98: [
        FuelType.gasoline_98_e5_price,
        FuelType.gasoline_98_e10_price,
    ],
    FuelGroup.biofuel: [
        FuelType.biodiesel_price,
        FuelType.bioethanol_price,
    ],
    FuelGroup.natural_gas: [
        FuelType.compressed_natural_gas_price,
        FuelType.liquefied_natural_gas_price,
    ],
}


FUEL_GROUP_PRIMARY: Dict[FuelGroup, FuelType] = {
    FuelGroup.diesel: FuelType.diesel_a_price,
    FuelGroup.gasoline_95: FuelType.gasoline_95_e5_price,
    FuelGroup.gasoline_98: FuelType.gasoline_98_e5_price,
    FuelGroup.biofuel: FuelType.biodiesel_price,
    FuelGroup.natural_gas: FuelType.compressed_natural_gas_price,
}

# Fuel types that don't belong to any group (shown as standalone in search)
FUEL_SINGLETONS: List[FuelType] = [
    FuelType.liquefied_petroleum_gases_price,
    FuelType.hydrogen_price,
]


class TrendPeriod(str, Enum):
    week = "week"
    month = "month"
    quarter = "quarter"
    half_year = "half_year"
    year = "year"


TREND_PERIOD_DAYS = {
    TrendPeriod.week: 7,
    TrendPeriod.month: 30,
    TrendPeriod.quarter: 90,
    TrendPeriod.half_year: 180,
    TrendPeriod.year: 365,
}


class HistoricalPeriod(str, Enum):
    quarter = "quarter"
    half_year = "half_year"
    year = "year"


HISTORICAL_PERIOD_DAYS = {
    HistoricalPeriod.quarter: 90,
    HistoricalPeriod.half_year: 180,
    HistoricalPeriod.year: 365,
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
    pct_vs_avg: Optional[float] = None
    variant_prices: Optional[Dict[str, float]] = None


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


class SearchLocation(BaseModel):
    latitude: float
    longitude: float


class StationListResponse(BaseModel):
    stations: List[StationResult]
    fuel_type: str
    query_type: str
    search_location: Optional[SearchLocation] = None


class ZoneListResponse(BaseModel):
    zones: List[ZoneResult]
    province: str
    fuel_type: str


class TrendResponse(BaseModel):
    trend: List[TrendPoint]
    zip_code: Optional[str] = None
    fuel_type: str
    period: str


class GroupTrendResponse(BaseModel):
    series: Dict[str, List[TrendPoint]]
    zip_code: Optional[str] = None
    fuel_group: str
    period: str


class HistoricalForecastResponse(BaseModel):
    geography_type: str
    geography_value: str
    source: str
    coverage_days: int = 0
    transition_observations: int = 0
    current_date: Optional[str] = None
    current_avg_price: Optional[float] = None
    current_regime: Optional[str] = None
    next_day_probabilities: Dict[str, float] = Field(default_factory=dict)
    cheaper_within_3d: Optional[float] = None
    cheaper_within_7d: Optional[float] = None
    expected_days_in_current_regime: Optional[float] = None
    confidence: float = 0.0
    recommendation: str
    explanation: str
    insufficient_data: bool = False
    transition_matrix: Dict[str, Dict[str, float]] = Field(default_factory=dict)


class TripPlanRequest(BaseModel):
    origin: str = Field(..., min_length=2, max_length=200)
    destination: str = Field(..., min_length=2, max_length=200)
    fuel_type: FuelType
    consumption_lper100km: float = Field(
        default_factory=lambda: settings.default_consumption_lper100km, ge=1.0, le=30.0
    )
    tank_liters: float = Field(default_factory=lambda: settings.default_tank_liters, ge=5.0, le=120.0)
    fuel_level_pct: float = Field(default_factory=lambda: settings.default_fuel_level_pct, ge=0.0, le=100.0)
    max_detour_minutes: float = Field(default_factory=lambda: settings.default_max_detour_minutes, ge=0.0, le=180.0)
    labels: Optional[List[str]] = None


class NationalAvgResponse(BaseModel):
    fuel_type: str
    avg_price: Optional[float]
    station_count: int


class LabelsResponse(BaseModel):
    labels: Dict[str, str]


class ProvincesResponse(BaseModel):
    provinces: Dict[str, str]


class MunicipalitiesResponse(BaseModel):
    province: str
    municipalities: List[str]


class FuelCatalogResponse(BaseModel):
    groups: Dict[str, List[str]]
    primary: Dict[str, str]
    singletons: List[str]


class DataFrameResponse(BaseModel):
    rows: List[Dict[str, Any]]


class ProvinceMapResponse(BaseModel):
    items: List[ProvincePriceResult]
    fuel_type: str


class DistrictMapResponse(BaseModel):
    items: List[DistrictPriceResult]
    province: str
    fuel_type: str


class AddressSuggestion(BaseModel):
    display_name: str
    lat: float
    lon: float


class AddressSuggestionsResponse(BaseModel):
    suggestions: List[AddressSuggestion]


class GeocodeResponse(BaseModel):
    lat: float
    lon: float


class GeoJSONResponse(BaseModel):
    geojson: Dict[str, Any]


class TripPlanResponse(BaseModel):
    plan: TripPlan


class BrandHistoricalResponse(BaseModel):
    ranking: List[Dict[str, Any]]
    trend: List[Dict[str, Any]]


class DataInventory(BaseModel):
    num_days: int
    num_months: int
    num_years: int
    total_size_bytes: int
    min_date: Optional[str]
    max_date: Optional[str]


class LatestDayStats(BaseModel):
    max_date: Optional[str]
    unique_stations: int = 0
    unique_provinces: int = 0
    unique_communities: int = 0
    unique_localities: int = 0
    unique_fuel_types: int = 0


class RealtimeStatus(BaseModel):
    realtime_enabled: bool
    realtime_active: bool
    last_realtime_refresh: Optional[float]


class QualityResponse(BaseModel):
    inventory: DataInventory
    latest_day: LatestDayStats
    missing_days: List[str]
    realtime: RealtimeStatus


class RouteResponse(BaseModel):
    coordinates: List[List[float]]
