from api.schemas import FuelType
from api.schemas import StationListResponse
from api.schemas import TrendPeriod
from api.schemas import TrendResponse
from api.schemas import ZoneListResponse
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from services.station_service import get_best_by_address
from services.station_service import get_cheapest_by_address
from services.station_service import get_cheapest_by_zip
from services.station_service import get_cheapest_zones
from services.station_service import get_nearest_by_address
from services.station_service import get_price_trends

router = APIRouter()


@router.get("/stations/cheapest-by-zip", response_model=StationListResponse)
def cheapest_by_zip(
    zip_code: str = Query(..., description="Zip code to search"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
    limit: int = Query(3, ge=1, le=20, description="Max results"),
):
    stations = get_cheapest_by_zip(zip_code, fuel_type, limit)
    return StationListResponse(stations=stations, fuel_type=fuel_type.value, query_type="cheapest_by_zip")


@router.get("/stations/nearest-by-address", response_model=StationListResponse)
def nearest_by_address(
    address: str = Query(..., description="Address to geocode"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
    limit: int = Query(3, ge=1, le=20, description="Max results"),
):
    stations = get_nearest_by_address(address, fuel_type, limit)
    if not stations:
        raise HTTPException(status_code=404, detail="Address could not be geocoded")
    return StationListResponse(stations=stations, fuel_type=fuel_type.value, query_type="nearest_by_address")


@router.get("/stations/cheapest-by-address", response_model=StationListResponse)
def cheapest_by_address(
    address: str = Query(..., description="Address to geocode"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
    radius_km: float = Query(5.0, ge=0.1, le=50.0, description="Search radius in km"),
):
    stations = get_cheapest_by_address(address, fuel_type, radius_km)
    if not stations:
        raise HTTPException(status_code=404, detail="No stations found or address could not be geocoded")
    return StationListResponse(stations=stations, fuel_type=fuel_type.value, query_type="cheapest_by_address")


@router.get("/stations/best-by-address", response_model=StationListResponse)
def best_by_address(
    address: str = Query(..., description="Address to geocode"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
    radius_km: float = Query(5.0, ge=0.1, le=50.0, description="Search radius in km"),
):
    stations = get_best_by_address(address, fuel_type, radius_km)
    if not stations:
        raise HTTPException(status_code=404, detail="No stations found or address could not be geocoded")
    return StationListResponse(stations=stations, fuel_type=fuel_type.value, query_type="best_by_address")


@router.get("/zones/cheapest", response_model=ZoneListResponse)
def cheapest_zones(
    province: str = Query(..., description="Province name"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
):
    zones = get_cheapest_zones(province, fuel_type)
    return ZoneListResponse(zones=zones, province=province, fuel_type=fuel_type.value)


@router.get("/trends/price", response_model=TrendResponse)
def price_trends(
    zip_code: str = Query(..., description="Zip code"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
    period: TrendPeriod = Query(TrendPeriod.month, description="Trend period"),
):
    trend = get_price_trends(zip_code, fuel_type, period)
    return TrendResponse(trend=trend, zip_code=zip_code, fuel_type=fuel_type.value, period=period.value)
