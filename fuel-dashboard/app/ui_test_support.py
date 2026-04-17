from __future__ import annotations

from contextvars import ContextVar
from typing import Any
from typing import Iterable
from typing import Optional

from api.schemas import AlternativePlan
from api.schemas import BrandHistoricalResponse
from api.schemas import DataFrameResponse
from api.schemas import DataInventory
from api.schemas import DistrictMapResponse
from api.schemas import DistrictPriceResult
from api.schemas import FuelGroup
from api.schemas import FuelType
from api.schemas import GeoJSONResponse
from api.schemas import GroupTrendResponse
from api.schemas import LabelsResponse
from api.schemas import LatestDayStats
from api.schemas import MunicipalitiesResponse
from api.schemas import ProvinceMapResponse
from api.schemas import ProvincePriceResult
from api.schemas import ProvincesResponse
from api.schemas import QualityResponse
from api.schemas import RealtimeStatus
from api.schemas import RouteResponse
from api.schemas import SearchLocation
from api.schemas import StationListResponse
from api.schemas import StationResult
from api.schemas import TrendPeriod
from api.schemas import TrendPoint
from api.schemas import TrendResponse
from api.schemas import TripPlan
from api.schemas import TripPlanRequest
from api.schemas import TripPlanResponse
from api.schemas import TripStop
from api.schemas import ZoneListResponse
from api.schemas import ZoneResult
from config import settings
from fastapi import HTTPException
from fastapi import Request


_CURRENT_FIXTURE_SET: ContextVar[str] = ContextVar("dashboard_ui_fixture_set", default=settings.ui_fixture_set)


def resolve_fixture_set(request: Request) -> str:
    header_name = request.headers.get("x-ui-fixture-set")
    cookie_name = request.cookies.get("ui_fixture_set")
    return (header_name or cookie_name or settings.ui_fixture_set).strip() or settings.ui_fixture_set


def push_fixture_set(name: str):
    return _CURRENT_FIXTURE_SET.set(name)


def pop_fixture_set(token) -> None:
    _CURRENT_FIXTURE_SET.reset(token)


def current_fixture_set() -> str:
    return _CURRENT_FIXTURE_SET.get()


def is_data_ready() -> bool:
    return current_fixture_set() != "loading"


def insights_flags() -> tuple[bool, bool]:
    fixture = current_fixture_set()
    zones = settings.insights_zones_enabled or fixture in {"zones_enabled", "insights_all"}
    historical = settings.insights_historical_enabled or fixture in {"historical_enabled", "insights_all"}
    return zones, historical


def health_data_response() -> tuple[int, dict[str, Any]]:
    fixture = current_fixture_set()
    if fixture == "loading":
        return 503, {"status": "error", "detail": "No parquet files found"}
    if fixture == "quality_stale":
        return (
            503,
            {
                "status": "stale",
                "source": "gcs",
                "latest_file": "prices_2026-04-14.parquet",
                "file_date": "2026-04-14",
                "data_datetime": "2026-04-14T05:45:00+00:00",
                "expected_date": "2026-04-17",
            },
        )
    return (
        200,
        {
            "status": "ok",
            "source": "realtime",
            "latest_file": "prices_2026-04-17.parquet",
            "file_date": "2026-04-17",
            "data_datetime": "2026-04-17T07:30:00+00:00",
            "realtime": {
                "realtime_enabled": True,
                "realtime_active": True,
                "last_realtime_refresh": 1713339000.0,
            },
        },
    )


def station_list_response(
    mode: str,
    *,
    location: Optional[str] = None,
    labels: Optional[list[str]] = None,
) -> StationListResponse:
    fixture = current_fixture_set()
    if fixture == "search_error":
        if mode == "cheapest_by_zip":
            return StationListResponse(stations=[], fuel_type=FuelType.diesel_a_price.value, query_type=mode)
        if mode == "nearest_by_address":
            raise HTTPException(status_code=404, detail="No stations found near this address")
        raise HTTPException(status_code=404, detail="No stations found within radius")

    if fixture == "search_empty":
        search_location = _search_location() if mode != "cheapest_by_zip" else None
        return StationListResponse(
            stations=[],
            fuel_type=FuelType.diesel_a_price.value,
            query_type=mode,
            search_location=search_location,
        )

    stations = _search_stations(mode, labels)
    search_location = _search_location() if mode != "cheapest_by_zip" else None
    return StationListResponse(
        stations=stations,
        fuel_type=FuelType.diesel_a_price.value,
        query_type=mode,
        search_location=search_location,
    )


def trip_plan_response(body: TripPlanRequest) -> TripPlanResponse:
    fixture = current_fixture_set()
    if fixture == "trip_error":
        raise ValueError("Route unavailable for selected itinerary")

    plan = _trip_plan(with_stops=fixture != "trip_no_stops", brand_filter=body.labels or [])
    return TripPlanResponse(plan=plan)


def route_response() -> RouteResponse:
    return RouteResponse(
        coordinates=[
            [-3.7038, 40.4168],
            [-3.6985, 40.4215],
            [-3.688, 40.427],
        ]
    )


def geocode_response() -> SearchLocation:
    return _search_location()


def labels_response(top_n: int = 25) -> LabelsResponse:
    labels = {
        "repsol": "Repsol",
        "cepsa": "Cepsa",
        "plenoil": "Plenoil",
        "bp": "BP",
    }
    pairs = list(labels.items())[:top_n]
    return LabelsResponse(labels=dict(pairs))


def provinces_response() -> ProvincesResponse:
    return ProvincesResponse(
        provinces={
            "madrid": "Madrid",
            "barcelona": "Barcelona",
            "valencia": "Valencia",
        }
    )


def trend_response(zip_code: Optional[str], fuel_type: FuelType, period: TrendPeriod) -> TrendResponse:
    price_offset = 0.02 if zip_code else 0.0
    points = [
        TrendPoint(date="2026-04-13", avg_price=1.479 + price_offset, min_price=1.439, max_price=1.519 + price_offset),
        TrendPoint(date="2026-04-14", avg_price=1.471 + price_offset, min_price=1.431, max_price=1.511 + price_offset),
        TrendPoint(date="2026-04-15", avg_price=1.466 + price_offset, min_price=1.426, max_price=1.506 + price_offset),
        TrendPoint(date="2026-04-16", avg_price=1.458 + price_offset, min_price=1.418, max_price=1.498 + price_offset),
        TrendPoint(date="2026-04-17", avg_price=1.452 + price_offset, min_price=1.412, max_price=1.492 + price_offset),
    ]
    return TrendResponse(trend=points, zip_code=zip_code, fuel_type=fuel_type.value, period=period.value)


def group_trend_response(zip_code: Optional[str], fuel_group: FuelGroup, period: TrendPeriod) -> GroupTrendResponse:
    diesel = [
        TrendPoint(date="2026-04-13", avg_price=1.479, min_price=1.439, max_price=1.519),
        TrendPoint(date="2026-04-14", avg_price=1.471, min_price=1.431, max_price=1.511),
        TrendPoint(date="2026-04-15", avg_price=1.466, min_price=1.426, max_price=1.506),
        TrendPoint(date="2026-04-16", avg_price=1.458, min_price=1.418, max_price=1.498),
        TrendPoint(date="2026-04-17", avg_price=1.452, min_price=1.412, max_price=1.492),
    ]
    diesel_b = [
        TrendPoint(date="2026-04-13", avg_price=1.453, min_price=1.423, max_price=1.488),
        TrendPoint(date="2026-04-14", avg_price=1.448, min_price=1.418, max_price=1.483),
        TrendPoint(date="2026-04-15", avg_price=1.444, min_price=1.414, max_price=1.479),
        TrendPoint(date="2026-04-16", avg_price=1.439, min_price=1.409, max_price=1.474),
        TrendPoint(date="2026-04-17", avg_price=1.435, min_price=1.405, max_price=1.47),
    ]
    diesel_premium = [
        TrendPoint(date="2026-04-13", avg_price=1.529, min_price=1.489, max_price=1.569),
        TrendPoint(date="2026-04-14", avg_price=1.521, min_price=1.481, max_price=1.561),
        TrendPoint(date="2026-04-15", avg_price=1.517, min_price=1.477, max_price=1.557),
        TrendPoint(date="2026-04-16", avg_price=1.509, min_price=1.469, max_price=1.549),
        TrendPoint(date="2026-04-17", avg_price=1.502, min_price=1.462, max_price=1.542),
    ]
    series = {
        FuelType.diesel_a_price.value: diesel,
        FuelType.diesel_b_price.value: diesel_b,
        FuelType.diesel_premium_price.value: diesel_premium,
    }
    return GroupTrendResponse(series=series, zip_code=zip_code, fuel_group=fuel_group.value, period=period.value)


def zones_provinces_response() -> DataFrameResponse:
    return DataFrameResponse(
        rows=[
            {"province": "Madrid", "avg_price": 1.452, "station_count": 138},
            {"province": "Valencia", "avg_price": 1.461, "station_count": 104},
            {"province": "Barcelona", "avg_price": 1.469, "station_count": 126},
        ]
    )


def zones_province_map_response(fuel_type: FuelType) -> ProvinceMapResponse:
    return ProvinceMapResponse(
        items=[
            ProvincePriceResult(province="Madrid", avg_price=1.452, station_count=138),
            ProvincePriceResult(province="Valencia", avg_price=1.461, station_count=104),
            ProvincePriceResult(province="Barcelona", avg_price=1.469, station_count=126),
        ],
        fuel_type=fuel_type.value,
    )


def zones_province_geojson_response() -> GeoJSONResponse:
    return GeoJSONResponse(
        geojson=_feature_collection(
            [
                _square_feature("Madrid", "province", -3.7, 40.4, avg_price=1.452, station_count=138),
                _square_feature("Valencia", "province", -0.38, 39.47, avg_price=1.461, station_count=104),
                _square_feature("Barcelona", "province", 2.17, 41.39, avg_price=1.469, station_count=126),
            ]
        )
    )


def zones_districts_response(province: str, fuel_type: FuelType) -> DistrictMapResponse:
    return DistrictMapResponse(
        items=[
            DistrictPriceResult(district="Centro", avg_price=1.441, station_count=19),
            DistrictPriceResult(district="Chamartin", avg_price=1.455, station_count=22),
            DistrictPriceResult(district="Tetuan", avg_price=1.462, station_count=17),
        ],
        province=province,
        fuel_type=fuel_type.value,
    )


def zones_district_geojson_response() -> GeoJSONResponse:
    return GeoJSONResponse(
        geojson=_feature_collection(
            [
                _square_feature("Centro", "district", -3.7035, 40.4175, avg_price=1.441, station_count=19),
                _square_feature("Chamartin", "district", -3.676, 40.467, avg_price=1.455, station_count=22),
                _square_feature("Tetuan", "district", -3.696, 40.459, avg_price=1.462, station_count=17),
            ]
        )
    )


def zones_municipalities_response(province: str) -> MunicipalitiesResponse:
    municipalities = ["Getafe", "Leganes", "Mostoles"] if province.lower() == "madrid" else ["Badalona", "Terrassa"]
    return MunicipalitiesResponse(province=province, municipalities=municipalities)


def zones_municipality_zips_response(province: str, fuel_type: FuelType, municipality: str) -> ZoneListResponse:
    zones = [
        ZoneResult(zip_code="28901", avg_price=1.447, min_price=1.429, station_count=7),
        ZoneResult(zip_code="28902", avg_price=1.453, min_price=1.435, station_count=5),
    ]
    return ZoneListResponse(zones=zones, province=province, fuel_type=fuel_type.value)


def zones_district_zips_response(province: str, fuel_type: FuelType, district: str) -> ZoneListResponse:
    zones = [
        ZoneResult(zip_code="28001", avg_price=1.441, min_price=1.423, station_count=8),
        ZoneResult(zip_code="28004", avg_price=1.446, min_price=1.428, station_count=6),
    ]
    return ZoneListResponse(zones=zones, province=province, fuel_type=fuel_type.value)


def postal_geojson_response(zip_codes: Iterable[str]) -> GeoJSONResponse:
    features = []
    for index, zip_code in enumerate(zip_codes):
        features.append(_square_feature(str(zip_code), "COD_POSTAL", -3.72 + index * 0.02, 40.41 + index * 0.02))
    return GeoJSONResponse(geojson=_feature_collection(features))


def zip_boundary_response(zip_code: str) -> GeoJSONResponse:
    if zip_code not in {"28001", "28004", "28901", "28902"}:
        raise HTTPException(status_code=404, detail="Unknown zip code boundary")
    return GeoJSONResponse(geojson=_feature_collection([_square_feature(zip_code, "COD_POSTAL", -3.7, 40.42)]))


def historical_day_of_week_response() -> DataFrameResponse:
    return DataFrameResponse(
        rows=[
            {"day_of_week": index, "avg_price": price}
            for index, price in enumerate([1.449, 1.447, 1.446, 1.444, 1.451, 1.463, 1.458])
        ]
    )


def historical_brands_response() -> BrandHistoricalResponse:
    ranking = [
        {"brand": "plenoil", "avg_price": 1.433},
        {"brand": "cepsa", "avg_price": 1.447},
        {"brand": "repsol", "avg_price": 1.458},
    ]
    trend = [
        {"date": "2026-04-13", "brand": "plenoil", "avg_price": 1.442},
        {"date": "2026-04-14", "brand": "plenoil", "avg_price": 1.439},
        {"date": "2026-04-17", "brand": "plenoil", "avg_price": 1.433},
        {"date": "2026-04-13", "brand": "cepsa", "avg_price": 1.454},
        {"date": "2026-04-14", "brand": "cepsa", "avg_price": 1.451},
        {"date": "2026-04-17", "brand": "cepsa", "avg_price": 1.447},
        {"date": "2026-04-13", "brand": "repsol", "avg_price": 1.466},
        {"date": "2026-04-14", "brand": "repsol", "avg_price": 1.462},
        {"date": "2026-04-17", "brand": "repsol", "avg_price": 1.458},
    ]
    return BrandHistoricalResponse(ranking=ranking, trend=trend)


def historical_volatility_response() -> DataFrameResponse:
    return DataFrameResponse(
        rows=[
            {"zip_code": "28001", "volatility_pct": 1.4, "coefficient_of_variation": 0.014},
            {"zip_code": "28004", "volatility_pct": 1.2, "coefficient_of_variation": 0.012},
            {"zip_code": "28901", "volatility_pct": 1.1, "coefficient_of_variation": 0.011},
        ]
    )


def quality_response() -> QualityResponse:
    fixture = current_fixture_set()
    missing_days = ["2026-04-12", "2026-04-13"] if fixture == "quality_stale" else []
    return QualityResponse(
        inventory=DataInventory(
            num_days=121,
            num_months=4,
            num_years=1,
            total_size_bytes=268_435_456,
            min_date="2026-01-01",
            max_date="2026-04-17",
        ),
        latest_day=LatestDayStats(
            max_date="2026-04-17",
            unique_stations=11_284,
            unique_provinces=50,
            unique_communities=17,
            unique_localities=4_120,
            unique_fuel_types=12,
        ),
        missing_days=missing_days,
        realtime=RealtimeStatus(
            realtime_enabled=True,
            realtime_active=fixture != "quality_stale",
            last_realtime_refresh=1713339000.0,
        ),
    )


def _search_location() -> SearchLocation:
    return SearchLocation(latitude=40.4168, longitude=-3.7038)


def _search_stations(mode: str, labels: Optional[list[str]]) -> list[StationResult]:
    selected = set(labels or [])
    rows = []
    for row in _base_station_rows():
        if selected and row["brand_slug"] not in selected:
            continue
        rows.append(dict(row))

    if mode == "nearest_by_address":
        rows.sort(key=lambda row: row["distance_km"])
        for row in rows:
            row.pop("estimated_total_cost", None)
            row.pop("pct_vs_avg", None)
    elif mode == "best_by_address":
        rows.sort(key=lambda row: row.get("estimated_total_cost") or row["price"])
    else:
        rows.sort(key=lambda row: row["price"])
        if mode == "cheapest_by_zip":
            for row in rows:
                row.pop("estimated_total_cost", None)
                row.pop("pct_vs_avg", None)

    return [_station(row) for row in rows[:5]]


def _trip_plan(*, with_stops: bool, brand_filter: list[str]) -> TripPlan:
    candidate_rows = [
        {
            "brand_slug": "plenoil",
            "label": "Plenoil A-4",
            "address": "A-4, km 32",
            "municipality": "Aranjuez",
            "province": "Madrid",
            "zip_code": "28300",
            "latitude": 40.03,
            "longitude": -3.6,
            "price": 1.429,
            "distance_km": 1.8,
            "route_km": 48.0,
            "detour_minutes": 4.0,
        },
        {
            "brand_slug": "cepsa",
            "label": "Cepsa Despenaperros",
            "address": "A-4, km 246",
            "municipality": "Santa Elena",
            "province": "Jaen",
            "zip_code": "23213",
            "latitude": 38.34,
            "longitude": -3.52,
            "price": 1.452,
            "distance_km": 3.2,
            "route_km": 246.0,
            "detour_minutes": 7.0,
        },
        {
            "brand_slug": "repsol",
            "label": "Repsol Sevilla Norte",
            "address": "SE-20, salida 3",
            "municipality": "Sevilla",
            "province": "Sevilla",
            "zip_code": "41015",
            "latitude": 37.43,
            "longitude": -5.96,
            "price": 1.474,
            "distance_km": 2.7,
            "route_km": 510.0,
            "detour_minutes": 6.0,
        },
    ]
    selected = set(brand_filter)
    if selected:
        candidate_rows = [row for row in candidate_rows if row["brand_slug"] in selected]
    candidate_stations = [_station(row) for row in candidate_rows]

    if with_stops and candidate_rows:
        stop_rows = candidate_rows[: min(2, len(candidate_rows))]
        stops = [
            TripStop(
                station=_station(row),
                route_km=float(row["route_km"]),
                detour_minutes=float(row["detour_minutes"]),
                fuel_at_arrival_pct=21.0 - index * 8.0,
                liters_to_fill=22.0 - index * 4.0,
                cost_eur=31.42 + index * 4.85,
                reasoning="Selected for price and minimal detour",
            )
            for index, row in enumerate(stop_rows)
        ]
    else:
        stops = []

    alternatives = (
        [
            AlternativePlan(
                strategy_name="Menos paradas",
                strategy_description="Prioriza una sola parada aunque el precio sea ligeramente superior.",
                stops=stops[:1],
                total_fuel_cost=66.2,
                total_fuel_liters=42.0,
                total_detour_minutes=7.0,
                fuel_at_destination_pct=16.0,
            ),
            AlternativePlan(
                strategy_name="Precio minimo",
                strategy_description="Acepta un pequeno desvio adicional para repostar mas barato.",
                stops=stops,
                total_fuel_cost=62.7,
                total_fuel_liters=41.5,
                total_detour_minutes=12.0,
                fuel_at_destination_pct=18.0,
            ),
        ]
        if stops
        else []
    )

    return TripPlan(
        stops=stops,
        total_fuel_cost=62.7 if stops else 43.1,
        total_distance_km=533.0,
        duration_minutes=312.0,
        total_fuel_liters=41.5 if stops else 28.8,
        savings_eur=5.4 if stops else 0.0,
        route_coordinates=[
            [-3.7038, 40.4168],
            [-3.51, 39.96],
            [-3.12, 39.22],
            [-4.77, 37.88],
            [-5.99, 37.39],
        ],
        candidate_stations=candidate_stations,
        origin_coords=[40.4168, -3.7038],
        destination_coords=[37.3891, -5.9845],
        fuel_at_destination_pct=18.0 if stops else 34.0,
        alternative_plans=alternatives,
    )


def _base_station_rows() -> list[dict[str, Any]]:
    return [
        {
            "brand_slug": "plenoil",
            "label": "Plenoil Atocha",
            "address": "Paseo de las Delicias 12",
            "municipality": "Madrid",
            "province": "Madrid",
            "zip_code": "28045",
            "latitude": 40.402,
            "longitude": -3.692,
            "price": 1.431,
            "distance_km": 1.2,
            "estimated_total_cost": 61.58,
            "pct_vs_avg": -2.4,
        },
        {
            "brand_slug": "cepsa",
            "label": "Cepsa Castellana",
            "address": "Paseo de la Castellana 220",
            "municipality": "Madrid",
            "province": "Madrid",
            "zip_code": "28046",
            "latitude": 40.463,
            "longitude": -3.689,
            "price": 1.447,
            "distance_km": 2.0,
            "estimated_total_cost": 62.31,
            "pct_vs_avg": -1.1,
        },
        {
            "brand_slug": "repsol",
            "label": "Repsol Chamberi",
            "address": "Calle de Bravo Murillo 18",
            "municipality": "Madrid",
            "province": "Madrid",
            "zip_code": "28015",
            "latitude": 40.437,
            "longitude": -3.705,
            "price": 1.463,
            "distance_km": 0.8,
            "estimated_total_cost": 63.02,
            "pct_vs_avg": 0.6,
        },
        {
            "brand_slug": "bp",
            "label": "BP O'Donnell",
            "address": "Calle de O'Donnell 48",
            "municipality": "Madrid",
            "province": "Madrid",
            "zip_code": "28009",
            "latitude": 40.423,
            "longitude": -3.674,
            "price": 1.474,
            "distance_km": 3.1,
            "estimated_total_cost": 64.8,
            "pct_vs_avg": 1.4,
        },
    ]


def _station(row: dict[str, Any]) -> StationResult:
    station_fields = {
        key: value
        for key, value in row.items()
        if key
        not in {
            "brand_slug",
        }
    }
    return StationResult(**station_fields)


def _feature_collection(features: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": features}


def _square_feature(
    value: str,
    key: str,
    lon: float,
    lat: float,
    *,
    avg_price: Optional[float] = None,
    station_count: int = 0,
) -> dict[str, Any]:
    return {
        "type": "Feature",
        "properties": {key: value, "avg_price": avg_price, "station_count": station_count},
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [lon - 0.05, lat - 0.03],
                    [lon + 0.05, lat - 0.03],
                    [lon + 0.05, lat + 0.03],
                    [lon - 0.05, lat + 0.03],
                    [lon - 0.05, lat - 0.03],
                ]
            ],
        },
    }
