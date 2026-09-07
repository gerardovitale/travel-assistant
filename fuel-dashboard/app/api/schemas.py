from enum import Enum
from typing import Any

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


FUEL_GROUP_MEMBERS: dict[FuelGroup, list[FuelType]] = {
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


FUEL_GROUP_PRIMARY: dict[FuelGroup, FuelType] = {
    FuelGroup.diesel: FuelType.diesel_a_price,
    FuelGroup.gasoline_95: FuelType.gasoline_95_e5_price,
    FuelGroup.gasoline_98: FuelType.gasoline_98_e5_price,
    FuelGroup.biofuel: FuelType.biodiesel_price,
    FuelGroup.natural_gas: FuelType.compressed_natural_gas_price,
}

# Fuel types that don't belong to any group (shown as standalone in search)
FUEL_SINGLETONS: list[FuelType] = [
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
    distance_km: float | None = None
    score: float | None = None
    estimated_total_cost: float | None = None
    route_km: float | None = None
    detour_minutes: float | None = None
    pct_vs_avg: float | None = None
    variant_prices: dict[str, float] | None = None


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
    reasoning: str | None = None


class AlternativePlan(BaseModel):
    strategy_name: str
    strategy_description: str
    stops: list[TripStop]
    total_fuel_cost: float
    total_fuel_liters: float
    total_detour_minutes: float
    fuel_at_destination_pct: float = 0.0
    floor_unmet: bool = False

    @property
    def num_stops(self) -> int:
        return len(self.stops)


class TripPlan(BaseModel):
    stops: list[TripStop]
    total_fuel_cost: float
    total_distance_km: float
    duration_minutes: float
    total_fuel_liters: float
    savings_eur: float
    route_coordinates: list[list[float]]
    candidate_stations: list[StationResult]
    origin_coords: list[float]
    destination_coords: list[float]
    fuel_at_destination_pct: float = 0.0
    floor_unmet: bool = False
    alternative_plans: list[AlternativePlan] = []


class SearchLocation(BaseModel):
    latitude: float
    longitude: float


class StationListResponse(BaseModel):
    stations: list[StationResult]
    fuel_type: str
    query_type: str
    search_location: SearchLocation | None = None


class ZoneListResponse(BaseModel):
    zones: list[ZoneResult]
    province: str
    fuel_type: str


class TrendResponse(BaseModel):
    trend: list[TrendPoint]
    zip_code: str | None = None
    fuel_type: str
    period: str


class GroupTrendResponse(BaseModel):
    series: dict[str, list[TrendPoint]]
    zip_code: str | None = None
    fuel_group: str
    period: str


class HistoricalForecastResponse(BaseModel):
    geography_type: str
    geography_value: str
    source: str
    coverage_days: int = 0
    transition_observations: int = 0
    current_date: str | None = None
    current_avg_price: float | None = None
    current_regime: str | None = None
    next_day_probabilities: dict[str, float] = Field(default_factory=dict)
    cheaper_within_3d: float | None = None
    cheaper_within_7d: float | None = None
    expected_days_in_current_regime: float | None = None
    confidence: float = 0.0
    recommendation: str
    explanation: str
    insufficient_data: bool = False
    transition_matrix: dict[str, dict[str, float]] = Field(default_factory=dict)


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
    min_fuel_at_destination_pct: float = Field(
        default_factory=lambda: settings.default_min_fuel_at_destination_pct, ge=0.0, le=80.0
    )
    labels: list[str] | None = None


class NationalAvgResponse(BaseModel):
    fuel_type: str
    avg_price: float | None
    station_count: int


class LabelsResponse(BaseModel):
    labels: dict[str, str]


class ProvincesResponse(BaseModel):
    provinces: dict[str, str]


class MunicipalitiesResponse(BaseModel):
    province: str
    municipalities: list[str]


class FuelCatalogResponse(BaseModel):
    groups: dict[str, list[str]]
    primary: dict[str, str]
    singletons: list[str]


class DataFrameResponse(BaseModel):
    rows: list[dict[str, Any]]


class ProvinceMapResponse(BaseModel):
    items: list[ProvincePriceResult]
    fuel_type: str


class DistrictMapResponse(BaseModel):
    items: list[DistrictPriceResult]
    province: str
    fuel_type: str


class AddressSuggestion(BaseModel):
    display_name: str
    lat: float
    lon: float


class AddressSuggestionsResponse(BaseModel):
    suggestions: list[AddressSuggestion]


class GeocodeResponse(BaseModel):
    lat: float
    lon: float


class GeoJSONResponse(BaseModel):
    geojson: dict[str, Any]


class TripPlanResponse(BaseModel):
    plan: TripPlan


class BrandHistoricalResponse(BaseModel):
    ranking: list[dict[str, Any]]
    trend: list[dict[str, Any]]


class DataInventory(BaseModel):
    num_days: int
    num_months: int
    num_years: int
    total_size_bytes: int
    min_date: str | None
    max_date: str | None


class LatestDayStats(BaseModel):
    max_date: str | None
    unique_stations: int = 0
    unique_provinces: int = 0
    unique_communities: int = 0
    unique_localities: int = 0
    unique_fuel_types: int = 0


class RealtimeStatus(BaseModel):
    realtime_enabled: bool
    realtime_active: bool
    last_realtime_refresh: float | None


class QualityResponse(BaseModel):
    inventory: DataInventory
    latest_day: LatestDayStats
    missing_days: list[str]
    realtime: RealtimeStatus


class EnergyType(str, Enum):
    """Energy source a vehicle runs on.

    ``electric`` is declared but not yet priceable: the repo has no electricity price source, so the
    cost service has no resolver for it. Adding one resolver (plus catalog rows) is the whole EV
    extension — the cost/breakeven math is unit-agnostic and does not change.
    """

    gasoline = "gasoline"
    diesel = "diesel"
    lpg = "lpg"
    electric = "electric"


class BrandReportFuelType(str, Enum):
    gasoline_95_e5_price = "gasoline_95_e5_price"
    diesel_a_price = "diesel_a_price"


class Direction(str, Enum):
    cheapest = "cheapest"
    priciest = "priciest"


class BrandWinRateRow(BaseModel):
    brand: str
    win_rate_pct: float
    appearances: int
    confidence: str  # "high" | "medium" | "low" — banded by aggregate sample size (appearances)


class BrandPriceComparisonRow(BaseModel):
    brand: str
    price_delta_pct: float
    days_below_market_pct: float
    appearances: int
    confidence: str  # "high" | "medium" | "low" — banded by aggregate sample size (appearances)
    brand_avg_price: float  # appearance-weighted mean brand price (EUR/L)
    market_avg_price: float  # appearance-weighted mean market price (EUR/L)


class BrandCoverageRow(BaseModel):
    brand: str
    zip_codes: int
    localities: int
    municipalities: int
    total_observations: int


class BrandOptionsResponse(BaseModel):
    brands: list[str]  # selectable brands for the fuel type, ordered by coverage desc
    default: list[str]  # brands to pre-select in the picker


class RouteResponse(BaseModel):
    coordinates: list[list[float]]


# --- Fuel-type report: gasolina vs diésel running cost ----------------------------------


class SegmentOption(BaseModel):
    id: str
    label: str


class VehicleOption(BaseModel):
    vehicle_id: str
    label: str
    variant: str
    energy_type: str
    consumption: float  # units per 100 km
    consumption_unit: str  # "l/100km" | "kWh/100km"
    price_available: bool  # False when no price source exists for this energy type yet


class VehiclePairOption(BaseModel):
    pair_id: str
    model: str
    segment: str
    segment_label: str
    model_year: int
    # consumption_gasoline / consumption_diesel; None when the pair has no gasoline/diesel couple
    breakeven_ratio: float | None
    vehicles: list[VehicleOption]


class VehicleCatalogResponse(BaseModel):
    version: str
    source: str
    source_url: str
    notes: str
    segments: list[SegmentOption]
    pairs: list[VehiclePairOption]
    unpriceable_energy_types: list[str]


class VehicleCostRow(BaseModel):
    vehicle_id: str
    pair_id: str
    label: str
    model: str
    variant: str
    segment: str
    energy_type: str
    consumption: float
    consumption_unit: str
    price_per_unit: float | None  # None when the energy type has no price source or no local data
    price_date: str | None
    cost_per_100km: float | None


class BreakevenResponse(BaseModel):
    pair_id: str
    model: str
    segment: str
    province: str | None  # None = national
    gasoline: VehicleCostRow
    diesel: VehicleCostRow
    price_ratio: float  # diesel EUR/L over gasoline EUR/L
    breakeven_ratio: float  # the ratio at which both cost the same per 100 km
    breakeven_diesel_price: float  # EUR/L at which diesel stops being cheaper
    diesel_headroom_eur_l: float  # breakeven_diesel_price - current diesel price
    breakeven_diesel_consumption: float  # real-world l/100km at which the verdict flips
    margin_pct: float
    winner: str  # "diesel" | "gasoline" | "tie"
    cost_gasoline_per_100km: float
    cost_diesel_per_100km: float
    cost_gap_per_100km: float
    price_date: str  # the single day both prices come from


class ProvinceBreakevenRow(BaseModel):
    province: str
    gasoline_price: float
    diesel_price: float
    price_ratio: float
    breakeven_ratio: float
    breakeven_diesel_price: float
    diesel_headroom_eur_l: float
    breakeven_diesel_consumption: float
    margin_pct: float
    winner: str
    cost_gasoline_per_100km: float
    cost_diesel_per_100km: float
    cost_gap_per_100km: float
    station_count: int
    price_date: str


class ProvinceBreakevenResponse(BaseModel):
    pair_id: str
    model: str
    breakeven_ratio: float
    rows: list[ProvinceBreakevenRow]
    provinces_dropped: int  # missing one of the two fuels; never interpolated


class BreakevenHistoryPoint(BaseModel):
    date: str
    price_ratio: float
    margin_pct: float  # positive = diesel cheaper, same convention as the headline verdict
    cost_gasoline_per_100km: float
    cost_diesel_per_100km: float


class BreakevenCrossover(BaseModel):
    date: str
    winner: str


class BreakevenHistoryResponse(BaseModel):
    pair_id: str
    model: str
    province: str | None
    breakeven_ratio: float
    pct_days_diesel_wins: float
    pct_days_tie: float  # diesel + gasoline + tie = 100
    days: int
    flips: int
    crossovers: list[BreakevenCrossover]
    series: list[BreakevenHistoryPoint]
