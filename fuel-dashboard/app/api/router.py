import json
from datetime import date
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import ui_test_support as ui_test
from api.schemas import BrandHistoricalResponse
from api.schemas import DataFrameResponse
from api.schemas import DataInventory
from api.schemas import DistrictMapResponse
from api.schemas import FUEL_GROUP_MEMBERS
from api.schemas import FUEL_GROUP_PRIMARY
from api.schemas import FUEL_SINGLETONS
from api.schemas import FuelCatalogResponse
from api.schemas import FuelGroup
from api.schemas import FuelType
from api.schemas import GeocodeResponse
from api.schemas import GeoJSONResponse
from api.schemas import GroupTrendResponse
from api.schemas import HISTORICAL_PERIOD_DAYS
from api.schemas import HistoricalForecastResponse
from api.schemas import HistoricalPeriod
from api.schemas import LabelsResponse
from api.schemas import LatestDayStats
from api.schemas import MunicipalitiesResponse
from api.schemas import NationalAvgResponse
from api.schemas import ProvinceMapResponse
from api.schemas import ProvincesResponse
from api.schemas import QualityResponse
from api.schemas import RealtimeStatus
from api.schemas import RouteResponse
from api.schemas import SearchLocation
from api.schemas import StationListResponse
from api.schemas import TrendPeriod
from api.schemas import TrendResponse
from api.schemas import TripPlanRequest
from api.schemas import TripPlanResponse
from api.schemas import ZoneListResponse
from config import settings
from fastapi import APIRouter
from fastapi import Body
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from services.data_quality_service import get_data_inventory
from services.data_quality_service import get_ingestion_stats
from services.data_quality_service import get_latest_day_stats
from services.data_quality_service import get_missing_days
from services.forecast_service import get_historical_forecast
from services.geocoding import geocode_address
from services.routing import get_full_route
from services.station_service import get_best_by_address
from services.station_service import get_brand_price_trend
from services.station_service import get_brand_ranking
from services.station_service import get_cheapest_by_address
from services.station_service import get_cheapest_by_zip
from services.station_service import get_cheapest_zones
from services.station_service import get_day_of_week_pattern
from services.station_service import get_district_price_geojson
from services.station_service import get_district_price_map
from services.station_service import get_group_price_trends
from services.station_service import get_municipalities
from services.station_service import get_national_avg_stats
from services.station_service import get_nearest_by_address
from services.station_service import get_price_trends
from services.station_service import get_province_price_geojson
from services.station_service import get_province_price_map_filtered
from services.station_service import get_province_ranking
from services.station_service import get_provinces
from services.station_service import get_station_labels
from services.station_service import get_zip_code_price_map_by_municipality
from services.station_service import get_zip_code_price_map_for_zips
from services.station_service import get_zip_codes_for_district
from services.station_service import get_zone_volatility_ranking
from services.trip_planner import plan_trip
from slowapi import Limiter

from data.cache import get_realtime_status
from data.geojson_loader import load_postal_code_boundary
from data.geojson_loader import load_postal_codes_for_zip_list


def get_real_client_ip(request: Request) -> str:
    return (
        request.headers.get("CF-Connecting-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.client.host
    )


limiter = Limiter(key_func=get_real_client_ip)
router = APIRouter()


def _rows(df) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso"))


@router.get("/stations/cheapest-by-zip", response_model=StationListResponse)
@limiter.limit(settings.rate_limit)
def cheapest_by_zip(
    request: Request,
    zip_code: str = Query(..., pattern=r"^\d{5}$", description="Zip code to search"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
    limit: int = Query(settings.default_limit, ge=1, le=20, description="Max results"),
    labels: Optional[List[str]] = Query(None, description="Filter by station brand labels"),
):
    if settings.ui_test_mode:
        return ui_test.station_list_response("cheapest_by_zip", location=zip_code, labels=labels)
    stations = get_cheapest_by_zip(zip_code, fuel_type, limit, labels=labels)
    return StationListResponse(stations=stations, fuel_type=fuel_type.value, query_type="cheapest_by_zip")


@router.get("/stations/nearest-by-address", response_model=StationListResponse)
@limiter.limit(settings.rate_limit)
def nearest_by_address(
    request: Request,
    address: str = Query(..., description="Address to geocode"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
    limit: int = Query(settings.default_limit, ge=1, le=20, description="Max results"),
    labels: Optional[List[str]] = Query(None, description="Filter by station brand labels"),
):
    if settings.ui_test_mode:
        return ui_test.station_list_response("nearest_by_address", location=address, labels=labels)
    coords = geocode_address(address)
    if coords is None:
        raise HTTPException(status_code=404, detail="Address could not be geocoded")
    lat, lon = coords
    stations = get_nearest_by_address(lat, lon, fuel_type, limit, labels=labels)
    if not stations:
        raise HTTPException(status_code=404, detail="No stations found near this address")
    return StationListResponse(
        stations=stations,
        fuel_type=fuel_type.value,
        query_type="nearest_by_address",
        search_location=SearchLocation(latitude=lat, longitude=lon),
    )


@router.get("/stations/cheapest-by-address", response_model=StationListResponse)
@limiter.limit(settings.rate_limit)
def cheapest_by_address(
    request: Request,
    address: str = Query(..., description="Address to geocode"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
    radius_km: float = Query(5.0, ge=0.1, le=50.0, description="Search radius in km"),
    limit: int = Query(settings.default_limit, ge=1, le=20, description="Max results"),
    labels: Optional[List[str]] = Query(None, description="Filter by station brand labels"),
):
    if settings.ui_test_mode:
        return ui_test.station_list_response("cheapest_by_address", location=address, labels=labels)
    coords = geocode_address(address)
    if coords is None:
        raise HTTPException(status_code=404, detail="Address could not be geocoded")
    lat, lon = coords
    stations = get_cheapest_by_address(lat, lon, fuel_type, radius_km, limit, labels=labels)
    if not stations:
        raise HTTPException(status_code=404, detail="No stations found within radius")
    return StationListResponse(
        stations=stations,
        fuel_type=fuel_type.value,
        query_type="cheapest_by_address",
        search_location=SearchLocation(latitude=lat, longitude=lon),
    )


@router.get("/stations/best-by-address", response_model=StationListResponse)
@limiter.limit(settings.rate_limit)
def best_by_address(
    request: Request,
    address: str = Query(..., description="Address to geocode"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
    radius_km: float = Query(settings.default_radius_km, ge=0.1, le=50.0, description="Search radius in km"),
    limit: int = Query(settings.default_limit, ge=1, le=20, description="Max results"),
    consumption_lper100km: float = Query(
        settings.default_consumption_lper100km, ge=1.0, le=30.0, description="Fuel consumption in l/100km"
    ),
    tank_liters: float = Query(settings.default_refill_liters, ge=5.0, le=120.0, description="Liters to refill"),
    labels: Optional[List[str]] = Query(None, description="Filter by station brand labels"),
):
    if settings.ui_test_mode:
        return ui_test.station_list_response("best_by_address", location=address, labels=labels)
    coords = geocode_address(address)
    if coords is None:
        raise HTTPException(status_code=404, detail="Address could not be geocoded")
    lat, lon = coords
    stations = get_best_by_address(
        lat, lon, fuel_type, radius_km, limit, consumption_lper100km, tank_liters, labels=labels
    )
    if not stations:
        raise HTTPException(status_code=404, detail="No stations found within radius")
    return StationListResponse(
        stations=stations,
        fuel_type=fuel_type.value,
        query_type="best_by_address",
        search_location=SearchLocation(latitude=lat, longitude=lon),
    )


@router.get("/zones/cheapest", response_model=ZoneListResponse)
@limiter.limit(settings.rate_limit)
def cheapest_zones(
    request: Request,
    province: str = Query(..., description="Province name"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
):
    zones = get_cheapest_zones(province, fuel_type)
    return ZoneListResponse(zones=zones, province=province, fuel_type=fuel_type.value)


@router.get("/trends/price", response_model=TrendResponse)
@limiter.limit(settings.rate_limit)
def price_trends(
    request: Request,
    zip_code: Optional[str] = Query(None, pattern=r"^\d{5}$", description="Zip code (omit for national average)"),
    fuel_type: FuelType = Query(..., description="Fuel type"),
    period: TrendPeriod = Query(TrendPeriod.month, description="Trend period"),
):
    if settings.ui_test_mode:
        return ui_test.trend_response(zip_code, fuel_type, period)
    trend = get_price_trends(zip_code, fuel_type, period)
    return TrendResponse(trend=trend, zip_code=zip_code, fuel_type=fuel_type.value, period=period.value)


@router.get("/trends/group", response_model=GroupTrendResponse)
@limiter.limit(settings.rate_limit)
def group_price_trends(
    request: Request,
    zip_code: Optional[str] = Query(None, pattern=r"^\d{5}$", description="Zip code (omit for national average)"),
    fuel_group: FuelGroup = Query(...),
    period: TrendPeriod = Query(TrendPeriod.month),
):
    if settings.ui_test_mode:
        return ui_test.group_trend_response(zip_code, fuel_group, period)
    series = get_group_price_trends(zip_code, fuel_group, period)
    return GroupTrendResponse(series=series, zip_code=zip_code, fuel_group=fuel_group.value, period=period.value)


@router.post("/trip/plan", response_model=TripPlanResponse)
@limiter.limit(settings.rate_limit)
def trip_plan(request: Request, body: TripPlanRequest = Body(...)):
    if settings.ui_test_mode:
        try:
            return ui_test.trip_plan_response(body)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    try:
        plan = plan_trip(
            origin_address=body.origin,
            destination_address=body.destination,
            fuel_type=body.fuel_type.value,
            consumption_lper100km=body.consumption_lper100km,
            tank_liters=body.tank_liters,
            fuel_level_pct=body.fuel_level_pct,
            max_detour_minutes=body.max_detour_minutes,
            labels=body.labels,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return TripPlanResponse(plan=plan)


@router.get("/geocode", response_model=GeocodeResponse)
@limiter.limit(settings.rate_limit)
def geocode(request: Request, address: str = Query(..., min_length=2, max_length=200)):
    if settings.ui_test_mode:
        coords = ui_test.geocode_response()
        return GeocodeResponse(lat=coords.latitude, lon=coords.longitude)
    coords = geocode_address(address)
    if coords is None:
        raise HTTPException(status_code=404, detail="Address could not be geocoded")
    lat, lon = coords
    return GeocodeResponse(lat=lat, lon=lon)


@router.get("/route", response_model=RouteResponse)
@limiter.limit(settings.rate_limit)
def route(
    request: Request,
    origin_lat: float = Query(..., ge=-90, le=90),
    origin_lon: float = Query(..., ge=-180, le=180),
    dest_lat: float = Query(..., ge=-90, le=90),
    dest_lon: float = Query(..., ge=-180, le=180),
):
    if settings.ui_test_mode:
        return ui_test.route_response()
    result = get_full_route((origin_lat, origin_lon), (dest_lat, dest_lon))
    if result is None:
        raise HTTPException(status_code=502, detail="Route unavailable")
    return RouteResponse(coordinates=result["coordinates"])


@router.get("/provinces", response_model=ProvincesResponse)
@limiter.limit(settings.rate_limit)
def provinces(request: Request):
    if settings.ui_test_mode:
        return ui_test.provinces_response()
    return ProvincesResponse(provinces=get_provinces())


@router.get("/labels", response_model=LabelsResponse)
@limiter.limit(settings.rate_limit)
def labels(request: Request, top_n: int = Query(25, ge=0, le=200)):
    if settings.ui_test_mode:
        return ui_test.labels_response(top_n=top_n)
    return LabelsResponse(labels=get_station_labels(top_n=top_n))


@router.get("/stats/national-avg", response_model=NationalAvgResponse)
@limiter.limit(settings.rate_limit)
def national_avg(request: Request, fuel_type: FuelType = Query(...)):
    avg, count = get_national_avg_stats(fuel_type.value)
    return NationalAvgResponse(fuel_type=fuel_type.value, avg_price=avg, station_count=count)


@router.get("/fuel/catalog", response_model=FuelCatalogResponse)
@limiter.limit(settings.rate_limit)
def fuel_catalog(request: Request):
    groups = {g.value: [f.value for f in members] for g, members in FUEL_GROUP_MEMBERS.items()}
    primary = {g.value: f.value for g, f in FUEL_GROUP_PRIMARY.items()}
    singletons = [f.value for f in FUEL_SINGLETONS]
    return FuelCatalogResponse(groups=groups, primary=primary, singletons=singletons)


def _period_days(period: HistoricalPeriod) -> int:
    return HISTORICAL_PERIOD_DAYS[period]


@router.get("/zones/provinces", response_model=DataFrameResponse)
@limiter.limit(settings.rate_limit)
def zones_provinces(
    request: Request,
    fuel_type: FuelType = Query(...),
    period: HistoricalPeriod = Query(HistoricalPeriod.quarter),
):
    if settings.ui_test_mode:
        return ui_test.zones_provinces_response()
    df = get_province_ranking(fuel_type, _period_days(period))
    return DataFrameResponse(rows=_rows(df))


@router.get("/zones/districts", response_model=DistrictMapResponse)
@limiter.limit(settings.rate_limit)
def zones_districts(
    request: Request,
    province: str = Query(..., min_length=1),
    fuel_type: FuelType = Query(...),
):
    if settings.ui_test_mode:
        return ui_test.zones_districts_response(province, fuel_type)
    items = get_district_price_map(province, fuel_type)
    return DistrictMapResponse(items=items, province=province, fuel_type=fuel_type.value)


@router.get("/zones/province-map", response_model=ProvinceMapResponse)
@limiter.limit(settings.rate_limit)
def zones_province_map(
    request: Request,
    fuel_type: FuelType = Query(...),
    mainland_only: bool = Query(False),
):
    if settings.ui_test_mode:
        return ui_test.zones_province_map_response(fuel_type)
    items = get_province_price_map_filtered(fuel_type, mainland_only)
    return ProvinceMapResponse(items=items, fuel_type=fuel_type.value)


@router.get("/zones/province-geojson", response_model=GeoJSONResponse)
@limiter.limit(settings.rate_limit)
def zones_province_geojson(
    request: Request,
    fuel_type: FuelType = Query(...),
    mainland_only: bool = Query(False),
):
    if settings.ui_test_mode:
        return ui_test.zones_province_geojson_response()
    geojson = get_province_price_geojson(fuel_type, mainland_only)
    return GeoJSONResponse(geojson=geojson)


@router.get("/zones/district-geojson", response_model=GeoJSONResponse)
@limiter.limit(settings.rate_limit)
def zones_district_geojson(
    request: Request,
    province: str = Query(..., min_length=1),
    fuel_type: FuelType = Query(...),
):
    if settings.ui_test_mode:
        return ui_test.zones_district_geojson_response()
    geojson = get_district_price_geojson(province, fuel_type)
    return GeoJSONResponse(geojson=geojson)


@router.get("/zones/municipalities", response_model=MunicipalitiesResponse)
@limiter.limit(settings.rate_limit)
def zones_municipalities(
    request: Request,
    province: str = Query(..., min_length=1),
):
    if settings.ui_test_mode:
        return ui_test.zones_municipalities_response(province)
    municipalities = get_municipalities(province)
    return MunicipalitiesResponse(province=province, municipalities=municipalities)


@router.get("/zones/municipality-zips", response_model=ZoneListResponse)
@limiter.limit(settings.rate_limit)
def zones_municipality_zips(
    request: Request,
    province: str = Query(..., min_length=1),
    municipality: str = Query(..., min_length=1),
    fuel_type: FuelType = Query(...),
):
    if settings.ui_test_mode:
        return ui_test.zones_municipality_zips_response(province, fuel_type, municipality)
    zones = get_zip_code_price_map_by_municipality(province, fuel_type, municipality)
    return ZoneListResponse(zones=zones, province=province, fuel_type=fuel_type.value)


@router.get("/zones/district-zips", response_model=ZoneListResponse)
@limiter.limit(settings.rate_limit)
def zones_district_zips(
    request: Request,
    province: str = Query(..., min_length=1),
    district: str = Query(..., min_length=1),
    fuel_type: FuelType = Query(...),
):
    if settings.ui_test_mode:
        return ui_test.zones_district_zips_response(province, fuel_type, district)
    zip_codes = get_zip_codes_for_district(province, fuel_type, district)
    zones = get_zip_code_price_map_for_zips(province, fuel_type, zip_codes)
    return ZoneListResponse(zones=zones, province=province, fuel_type=fuel_type.value)


@router.get("/zones/postal-geojson", response_model=GeoJSONResponse)
@limiter.limit(settings.rate_limit)
def zones_postal_geojson(request: Request, zip_codes: List[str] = Query(...)):
    if settings.ui_test_mode:
        return ui_test.postal_geojson_response(zip_codes)
    geo = load_postal_codes_for_zip_list(zip_codes)
    return GeoJSONResponse(geojson=geo or {"type": "FeatureCollection", "features": []})


@router.get("/zones/zip-boundary", response_model=GeoJSONResponse)
@limiter.limit(settings.rate_limit)
def zones_zip_boundary(request: Request, zip_code: str = Query(..., pattern=r"^\d{5}$")):
    if settings.ui_test_mode:
        return ui_test.zip_boundary_response(zip_code)
    geo = load_postal_code_boundary(zip_code)
    if geo is None:
        raise HTTPException(status_code=404, detail="Unknown zip code boundary")
    return GeoJSONResponse(geojson=geo)


@router.get("/historical/day-of-week", response_model=DataFrameResponse)
@limiter.limit(settings.rate_limit)
def historical_day_of_week(
    request: Request,
    fuel_type: FuelType = Query(...),
    province: Optional[str] = Query(None),
):
    if settings.ui_test_mode:
        return ui_test.historical_day_of_week_response()
    df = get_day_of_week_pattern(fuel_type, province=province)
    return DataFrameResponse(rows=_rows(df))


@router.get("/historical/forecast", response_model=HistoricalForecastResponse)
@limiter.limit(settings.rate_limit)
def historical_forecast(
    request: Request,
    fuel_type: FuelType = Query(...),
    zip_code: Optional[str] = Query(None, pattern=r"^\d{5}$"),
    province: Optional[str] = Query(None),
    window_days: int = Query(180, ge=60, le=365),
):
    if settings.ui_test_mode:
        return ui_test.historical_forecast_response(zip_code=zip_code, province=province)
    if not zip_code and not province:
        raise HTTPException(status_code=422, detail="zip_code or province is required")
    try:
        return get_historical_forecast(
            fuel_type,
            zip_code=zip_code,
            province=province,
            window_days=window_days,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/historical/brands", response_model=BrandHistoricalResponse)
@limiter.limit(settings.rate_limit)
def historical_brands(
    request: Request,
    fuel_type: FuelType = Query(...),
    period: HistoricalPeriod = Query(HistoricalPeriod.quarter),
    top_n: int = Query(15, ge=3, le=50),
):
    if settings.ui_test_mode:
        return ui_test.historical_brands_response()
    days = _period_days(period)
    ranking_df = get_brand_ranking(fuel_type, days, top_n)
    if ranking_df.empty:
        return BrandHistoricalResponse(ranking=[], trend=[])
    brands = ranking_df["brand"].tolist() if "brand" in ranking_df.columns else []
    trend_df = get_brand_price_trend(fuel_type, days, brands) if brands else None
    return BrandHistoricalResponse(ranking=_rows(ranking_df), trend=_rows(trend_df))


@router.get("/historical/volatility", response_model=DataFrameResponse)
@limiter.limit(settings.rate_limit)
def historical_volatility(
    request: Request,
    fuel_type: FuelType = Query(...),
    period: HistoricalPeriod = Query(HistoricalPeriod.quarter),
    mainland_only: bool = Query(True),
):
    if settings.ui_test_mode:
        return ui_test.historical_volatility_response()
    df = get_zone_volatility_ranking(fuel_type, _period_days(period), mainland_only)
    return DataFrameResponse(rows=_rows(df))


@router.get("/quality/inventory", response_model=QualityResponse)
@limiter.limit(settings.rate_limit)
def quality_inventory(request: Request):
    if settings.ui_test_mode:
        return ui_test.quality_response()
    stats = get_ingestion_stats()
    inventory = get_data_inventory(stats)
    max_date: Optional[date] = inventory.get("max_date")
    min_date: Optional[date] = inventory.get("min_date")
    latest = get_latest_day_stats(stats, max_date) if max_date else {}
    available: set[date] = inventory.get("available_dates") or set()
    missing = get_missing_days(available, min_date, max_date) if (min_date and max_date) else []
    return QualityResponse(
        inventory=DataInventory(
            num_days=inventory.get("num_days", 0),
            num_months=inventory.get("num_months", 0),
            num_years=inventory.get("num_years", 0),
            total_size_bytes=inventory.get("total_size_bytes", 0),
            min_date=min_date.isoformat() if min_date else None,
            max_date=max_date.isoformat() if max_date else None,
        ),
        latest_day=LatestDayStats(
            max_date=latest["max_date"].isoformat() if latest.get("max_date") else None,
            unique_stations=latest.get("unique_stations", 0),
            unique_provinces=latest.get("unique_provinces", 0),
            unique_communities=latest.get("unique_communities", 0),
            unique_localities=latest.get("unique_localities", 0),
            unique_fuel_types=latest.get("unique_fuel_types", 0),
        ),
        missing_days=missing,
        realtime=RealtimeStatus(**get_realtime_status()),
    )
