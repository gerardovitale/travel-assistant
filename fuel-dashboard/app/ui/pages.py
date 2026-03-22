import json
import logging
from datetime import date
from datetime import timedelta
from typing import Any
from typing import Dict

from api.schemas import FuelType
from api.schemas import HISTORICAL_PERIOD_DAYS
from api.schemas import HistoricalPeriod
from api.schemas import TrendPeriod
from config import settings
from fastapi import FastAPI
from nicegui import run
from nicegui import ui
from services.geocoding import geocode_address
from services.station_service import get_best_by_address
from services.station_service import get_cheapest_by_address
from services.station_service import get_cheapest_by_zip
from services.station_service import get_cheapest_zones
from services.station_service import get_day_of_week_pattern
from services.station_service import get_district_price_map
from services.station_service import get_municipalities
from services.station_service import get_nearest_by_address
from services.station_service import get_postal_code_geojson
from services.station_service import get_price_trends
from services.station_service import get_province_price_map
from services.station_service import get_province_ranking
from services.station_service import get_provinces
from services.station_service import get_route_geometries_for_stations
from services.station_service import get_zip_code_boundary
from services.station_service import get_zip_code_price_map_by_municipality
from services.station_service import get_zip_code_price_map_for_zips
from services.station_service import get_zip_codes_for_district
from services.trip_planner import plan_trip
from ui.charts import build_day_of_week_chart
from ui.charts import build_district_choropleth
from ui.charts import build_province_choropleth
from ui.charts import build_station_map
from ui.charts import build_trend_chart
from ui.charts import build_trip_map
from ui.charts import build_zip_code_choropleth
from ui.components import empty_state
from ui.components import fuel_type_select
from ui.components import historical_period_select
from ui.components import kpi_row
from ui.components import loading_state
from ui.components import search_mode_select
from ui.components import station_results_table
from ui.components import status_banner
from ui.components import top_cheapest_table
from ui.components import trend_period_select
from ui.components import trip_stops_table
from ui.view_models import alternative_plan_cards
from ui.view_models import day_of_week_kpis
from ui.view_models import HISTORICAL_PERIOD_LABELS
from ui.view_models import province_ranking_kpis
from ui.view_models import SCORE_METHODOLOGY_LINES
from ui.view_models import search_mode_metadata
from ui.view_models import search_summary_cards
from ui.view_models import station_summary
from ui.view_models import trend_kpis
from ui.view_models import trend_summary_cards
from ui.view_models import trip_summary_cards
from ui.view_models import zone_kpis
from ui.view_models import zone_summary_cards

from data.cache import is_data_ready
from data.geojson_loader import normalize_data_province_name

logger = logging.getLogger(__name__)


def _make_set_status(container: ui.column):
    def set_status(status: str, message: str) -> None:
        container.clear()
        with container:
            if status == "loading":
                loading_state(message)
            else:
                status_banner(status, message)

    return set_status


def init_ui(app: FastAPI) -> None:
    @ui.page("/")
    def index():
        page_container = ui.column().classes("w-full max-w-7xl mx-auto gap-3 p-4")

        def _render_dashboard() -> None:
            page_container.clear()
            with page_container:
                ui.label("Panel de precios de combustible en Espana").classes("text-2xl font-bold")
                ui.label(
                    "Consulta estaciones, tendencias y zonas con una vista mas clara en movil y escritorio."
                ).classes("text-sm text-gray-600")

                with ui.tabs().classes("w-full") as tabs:
                    search_tab = ui.tab("Buscar estaciones")
                    trends_tab = ui.tab("Tendencias de precios")
                    zones_tab = ui.tab("Comparar zonas")
                    trip_tab = ui.tab("Planificar viaje")
                    historical_tab = ui.tab("Analisis historico")

                with ui.tab_panels(tabs, value=search_tab).classes("w-full"):
                    with ui.tab_panel(search_tab):
                        _build_search_panel()
                    with ui.tab_panel(trends_tab):
                        _build_trends_panel()
                    with ui.tab_panel(zones_tab):
                        _build_zones_panel()
                    with ui.tab_panel(trip_tab):
                        _build_trip_panel()
                    with ui.tab_panel(historical_tab):
                        _build_historical_panel()

        if is_data_ready():
            _render_dashboard()
        else:
            with page_container:
                with ui.column().classes("w-full items-center justify-center py-24 gap-4"):
                    ui.spinner(size="xl").classes("text-primary")
                    ui.label("Cargando datos iniciales...").classes("text-lg text-gray-600")

            def check_ready():
                if is_data_ready():
                    timer.deactivate()
                    _render_dashboard()

            timer = ui.timer(1.5, check_ready)

    ui.run_with(app, title="Panel de precios de combustible", favicon="⛽")


def _build_search_panel() -> None:
    state: Dict[str, Any] = {
        "query_input": None,
        "radius_input": None,
        "consumption_input": None,
        "tank_input": None,
        "dynamic_container": None,
    }
    mode = None
    search_button = None
    limit_input = None

    def on_mode_change(_: Any) -> None:
        container = state.get("dynamic_container")
        if container is None:
            return
        _render_query_inputs(mode, container, state)

    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("w-full p-4"):
            ui.label("Encuentra estaciones por codigo postal o direccion.").classes("text-sm text-gray-600")
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                mode = search_mode_select(on_change=on_mode_change)
                fuel = fuel_type_select()
            dynamic_container = ui.column().classes("w-full gap-2")
            state["dynamic_container"] = dynamic_container
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                limit_input = ui.number(label="Numero de estaciones", value=5, min=1, max=20).classes("w-48")
                search_button = ui.button("Buscar").props("unelevated color=primary")

        status_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        results_container = ui.column().classes("w-full")
        map_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def on_search() -> None:
        summary_container.clear()
        results_container.clear()
        map_container.clear()
        query_input = state.get("query_input")
        if query_input is None:
            return
        query_value = (query_input.value or "").strip()
        if not query_value:
            set_status("warning", "Introduce un codigo postal o una direccion para continuar.")
            return

        set_status("loading", "Buscando estaciones...")
        with results_container:
            with ui.column().classes("w-full items-center py-8"):
                ui.spinner(size="lg").classes("text-primary")
        search_button.disable()
        try:
            fuel_type = FuelType(fuel.value)
            current_mode = mode.value
            radius_input = state.get("radius_input")
            radius_km = radius_input.value if radius_input else None
            limit = int(limit_input.value)

            search_lat = None
            search_lon = None

            zip_boundary = None

            if current_mode == "cheapest_by_zip":
                coords = await run.io_bound(geocode_address, f"{query_value}, Spain")
                if coords:
                    search_lat, search_lon = coords
                stations = await run.io_bound(get_cheapest_by_zip, query_value, fuel_type, limit)
                zip_boundary = await run.io_bound(get_zip_code_boundary, query_value)
            elif current_mode in ("nearest_by_address", "cheapest_by_address", "best_by_address"):
                coords = await run.io_bound(geocode_address, query_value)
                if coords is None:
                    results_container.clear()
                    set_status("warning", "No se pudo geocodificar la direccion proporcionada.")
                    return
                search_lat, search_lon = coords
                if current_mode == "nearest_by_address":
                    stations = await run.io_bound(get_nearest_by_address, search_lat, search_lon, fuel_type, limit)
                elif current_mode == "cheapest_by_address":
                    stations = await run.io_bound(
                        get_cheapest_by_address, search_lat, search_lon, fuel_type, radius_km, limit
                    )
                elif current_mode == "best_by_address":
                    consumption_input = state.get("consumption_input")
                    consumption = consumption_input.value if consumption_input else None
                    tank_input = state.get("tank_input")
                    tank = tank_input.value if tank_input else None
                    stations = await run.io_bound(
                        get_best_by_address, search_lat, search_lon, fuel_type, radius_km, limit, consumption, tank
                    )
                else:
                    stations = []
            else:
                stations = []

            results_container.clear()
            if not stations:
                set_status("empty", "No se encontraron resultados para esta busqueda.")
                with results_container:
                    empty_state("Prueba con otro codigo postal, direccion o radio.")
                return

            summary = station_summary(stations)
            fetch_routes = current_mode != "cheapest_by_zip" and search_lat is not None and search_lon is not None
            if fetch_routes:
                set_status("success", f"{summary['count']} estaciones encontradas. Cargando rutas en el mapa...")
            else:
                set_status("success", f"{summary['count']} estaciones encontradas.")
            with summary_container:
                kpi_row(search_summary_cards(summary, current_mode))
            with map_container:
                fig, stations_trace_idx, highlight_trace_idx, route_trace_idx = build_station_map(
                    stations,
                    search_lat,
                    search_lon,
                    query_value,
                    zip_boundary=zip_boundary,
                )
                plotly_el = ui.plotly(fig).classes("w-full")
                plotly_id = f"c{plotly_el.id}"
            with results_container:
                table = station_results_table(
                    stations,
                    current_mode,
                    plotly_element_id=plotly_id,
                    stations_trace_idx=stations_trace_idx,
                    highlight_trace_idx=highlight_trace_idx,
                    route_trace_idx=route_trace_idx,
                )

            if fetch_routes:
                route_geometries = await get_route_geometries_for_stations(search_lat, search_lon, stations)
                set_status("success", f"{summary['count']} estaciones encontradas.")
                if route_geometries and table is not None:
                    lookup_key = f"__stationLookup_c{table.id}"
                    route_updates = {}
                    for label, geom in route_geometries.items():
                        if geom:
                            route_updates[label] = {
                                "route_lat": [c[1] for c in geom],
                                "route_lon": [c[0] for c in geom],
                            }
                    if route_updates:
                        updates_json = json.dumps(route_updates, ensure_ascii=True)
                        ui.run_javascript(
                            f"""
                            var lookup = window.{lookup_key};
                            if (lookup) {{
                                var updates = {updates_json};
                                for (var label in updates) {{
                                    if (lookup[label]) {{
                                        lookup[label].route_lat = updates[label].route_lat;
                                        lookup[label].route_lon = updates[label].route_lon;
                                    }}
                                }}
                            }}
                            """
                        )
        except ValueError as exc:
            logger.warning("Search validation error: %s", exc)
            set_status("warning", str(exc))
        except Exception:
            logger.exception("Search error")
            set_status("error", "No se pudo completar la busqueda. Revisa los datos e intentalo de nuevo.")
        finally:
            search_button.enable()

    search_button.on("click", lambda _: on_search())
    _render_query_inputs(mode, state["dynamic_container"], state)
    set_status("info", "Selecciona un modo y ejecuta una busqueda para ver resultados.")


def _render_query_inputs(mode: ui.select, container: ui.column, state: Dict[str, Any]) -> None:
    metadata = search_mode_metadata(mode.value)
    container.clear()
    state["radius_input"] = None
    state["consumption_input"] = None
    state["tank_input"] = None
    with container:
        query_input = ui.input(label=metadata.query_label, placeholder=metadata.query_placeholder).classes(
            "w-full max-w-lg"
        )
        ui.label(metadata.helper_text).classes("text-xs text-gray-500")
        state["query_input"] = query_input
        has_params = metadata.requires_radius or metadata.requires_consumption
        if has_params:
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                if metadata.requires_radius:
                    state["radius_input"] = ui.number(
                        label="Radio (km)", value=settings.default_radius_km, min=0.1, max=50.0
                    ).classes("w-36")
                if metadata.requires_consumption:
                    state["consumption_input"] = ui.number(
                        label="Consumo (l/100km)",
                        value=settings.default_consumption_lper100km,
                        min=1.0,
                        max=30.0,
                        step=0.5,
                    ).classes("w-44")
                    state["tank_input"] = ui.number(
                        label="Litros a repostar", value=settings.default_tank_liters, min=5.0, max=120.0, step=5.0
                    ).classes("w-40")
        if metadata.requires_consumption:
            with ui.expansion("Como se calcula la puntuacion?").classes("w-full text-sm").props("dense"):
                for line in SCORE_METHODOLOGY_LINES:
                    if line:
                        ui.label(line).classes("text-xs text-gray-600")
                    else:
                        ui.separator().classes("my-1")


def _build_trends_panel() -> None:
    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("w-full p-4"):
            ui.label("Analiza la evolucion de precios por codigo postal.").classes("text-sm text-gray-600")
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                zip_input = ui.input(label="Codigo postal", placeholder="Ejemplo: 28001").classes("w-56")
                fuel = fuel_type_select()
                period = trend_period_select()
                ui.label("Periodo: 7, 30 o 90 dias.").classes("text-xs text-gray-500")
                trend_button = ui.button("Cargar tendencia").props("unelevated color=primary")

        status_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        chart_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def on_load_trend() -> None:
        summary_container.clear()
        chart_container.clear()
        zip_code = (zip_input.value or "").strip()
        if not zip_code:
            set_status("warning", "Introduce un codigo postal para cargar la tendencia.")
            return

        set_status("loading", "Cargando tendencia de precios...")
        with chart_container:
            with ui.column().classes("w-full items-center py-8"):
                ui.spinner(size="lg").classes("text-primary")
        trend_button.disable()
        try:
            fuel_type = FuelType(fuel.value)
            trend_period = TrendPeriod(period.value)
            trend_data = await run.io_bound(get_price_trends, zip_code, fuel_type, trend_period)
            chart_container.clear()
            if not trend_data:
                set_status("empty", "No hay datos de tendencia para esta combinacion.")
                with chart_container:
                    empty_state("Prueba otro codigo postal, tipo de combustible o periodo.")
                return

            metrics = trend_kpis(trend_data)
            with summary_container:
                kpi_row(trend_summary_cards(metrics))

            with chart_container:
                fig = build_trend_chart(trend_data, fuel_type.value, zip_code)
                ui.plotly(fig).classes("w-full")
            set_status("success", f"Tendencia cargada para {zip_code}.")
        except ValueError as exc:
            logger.warning("Trend validation error: %s", exc)
            set_status("warning", str(exc))
        except Exception:
            logger.exception("Trend error")
            set_status("error", "No se pudo cargar la tendencia. Intentalo de nuevo.")
        finally:
            trend_button.enable()

    trend_button.on("click", lambda _: on_load_trend())
    set_status("info", "Carga una tendencia para comparar minimos, maximos y variacion promedio.")


def _build_zones_panel() -> None:
    zones_state: Dict[str, Any] = {
        "province": None,
        "fuel_type": None,
        "is_madrid": False,
        "detail_button": None,
    }

    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("w-full p-4"):
            ui.label("Compara zonas por precio promedio de combustible.").classes("text-sm text-gray-600")
            provinces = get_provinces()
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                province_input = ui.select(options=provinces, label="Provincia", with_input=True).classes("w-56")
                fuel = fuel_type_select()
                zones_button = ui.button("Cargar zonas").props("unelevated color=primary")

        status_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        preloaded_map_container = ui.column().classes("w-full")
        with ui.row().classes("w-full items-center"):
            mainland_only = ui.checkbox("Solo peninsula", value=True).classes("self-center")
        detail_map_container = ui.column().classes("w-full")
        subregion_container = ui.column().classes("w-full")
        postal_map_container = ui.column().classes("w-full")
        postal_kpi_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def _render_preloaded_map() -> None:
        preloaded_map_container.clear()
        try:
            fuel_type = FuelType(fuel.value)
            province_prices = await run.io_bound(get_province_price_map, fuel_type)
            if province_prices:
                with preloaded_map_container:
                    map_fig = build_province_choropleth(
                        province_prices,
                        "",
                        fuel_type.value,
                        mainland_only.value,
                    )
                    ui.plotly(map_fig).classes("w-full")
        except Exception:
            logger.exception("Preload map error")

    async def on_load_zones() -> None:
        summary_container.clear()
        detail_map_container.clear()
        preloaded_map_container.clear()
        subregion_container.clear()
        postal_map_container.clear()
        postal_kpi_container.clear()
        province = province_input.value
        if not province:
            set_status("warning", "Selecciona una provincia para cargar zonas.")
            return

        set_status("loading", "Cargando comparativa de zonas...")
        with detail_map_container:
            with ui.column().classes("w-full items-center py-8"):
                ui.spinner(size="lg").classes("text-primary")
        zones_button.disable()
        try:
            fuel_type = FuelType(fuel.value)
            zones_state["province"] = province
            zones_state["fuel_type"] = fuel_type

            zones = await run.io_bound(get_cheapest_zones, province, fuel_type)
            detail_map_container.clear()
            if not zones:
                set_status("empty", "No hay datos de zonas para esta provincia.")
                with detail_map_container:
                    empty_state("Prueba otra provincia o tipo de combustible.")
                return

            metrics = zone_kpis(zones)
            with summary_container:
                kpi_row(zone_summary_cards(metrics))

            is_madrid = province.strip().upper() == "MADRID"
            zones_state["is_madrid"] = is_madrid

            if is_madrid:
                district_prices = await run.io_bound(get_district_price_map, province, fuel_type)
                if district_prices:
                    with detail_map_container:
                        map_fig = build_district_choropleth(district_prices, fuel_type.value)
                        ui.plotly(map_fig).classes("w-full")
                    subregion_options = {dp.district: dp.district for dp in district_prices}
                    _render_subregion_card(subregion_options, "Distrito")
                else:
                    await _render_detail_province_map(province, fuel_type)
            else:
                await _render_detail_province_map(province, fuel_type)
                municipalities = await run.io_bound(get_municipalities, province)
                if municipalities:
                    subregion_options = {m: m.title() for m in municipalities}
                    _render_subregion_card(subregion_options, "Municipio")

            set_status("success", f"Comparativa cargada para {province}.")
        except ValueError as exc:
            logger.warning("Zone validation error: %s", exc)
            set_status("warning", str(exc))
        except Exception:
            logger.exception("Zone error")
            set_status("error", "No se pudo cargar la comparativa de zonas. Intentalo de nuevo.")
        finally:
            zones_button.enable()

    def _render_subregion_card(options: Dict[str, str], label: str) -> None:
        with subregion_container:
            with ui.card().classes("w-full p-4"):
                ui.label(f"Selecciona un {label.lower()} para ver detalle por codigo postal").classes(
                    "text-sm text-gray-600"
                )
                with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                    subregion_select = ui.select(options=options, label=label, with_input=True).classes("w-56")
                    detail_button = ui.button("Cargar detalle").props("unelevated color=primary")
                zones_state["detail_button"] = detail_button
                detail_button.on("click", lambda _: _on_load_subregion_detail(subregion_select))

    async def _on_load_subregion_detail(subregion_select: ui.select) -> None:
        postal_map_container.clear()
        postal_kpi_container.clear()
        subregion = subregion_select.value
        if not subregion:
            return

        province = zones_state["province"]
        fuel_type = zones_state["fuel_type"]
        is_madrid = zones_state["is_madrid"]
        detail_button = zones_state["detail_button"]

        with postal_map_container:
            with ui.column().classes("w-full items-center py-8"):
                ui.spinner(size="lg").classes("text-primary")
        detail_button.disable()

        try:
            if is_madrid:
                zip_codes = await run.io_bound(get_zip_codes_for_district, province, fuel_type, subregion)
                if not zip_codes:
                    postal_map_container.clear()
                    with postal_map_container:
                        empty_state("No se encontraron codigos postales para este distrito.")
                    return
                zip_prices = await run.io_bound(get_zip_code_price_map_for_zips, province, fuel_type, zip_codes)
                title = f"Precio por CP en {subregion} (Madrid)"
            else:
                zip_prices = await run.io_bound(get_zip_code_price_map_by_municipality, province, fuel_type, subregion)
                zip_codes = [zp.zip_code for zp in zip_prices]
                title = f"Precio por CP en {subregion.title()}"

            postal_map_container.clear()
            if not zip_prices:
                with postal_map_container:
                    empty_state("No hay datos de codigos postales para esta zona.")
                return

            geojson = await run.io_bound(get_postal_code_geojson, zip_codes)
            if not geojson.get("features"):
                with postal_map_container:
                    empty_state("No se encontraron limites geograficos para estos codigos postales.")
                return

            with postal_map_container:
                map_fig = build_zip_code_choropleth(zip_prices, geojson, title, fuel_type.value)
                ui.plotly(map_fig).classes("w-full")

            metrics = zone_kpis(zip_prices)
            with postal_kpi_container:
                kpi_row(zone_summary_cards(metrics))

        except Exception:
            logger.exception("Subregion detail error")
            postal_map_container.clear()
            with postal_map_container:
                empty_state("Error al cargar el detalle. Intentalo de nuevo.")
        finally:
            detail_button.enable()

    async def _render_detail_province_map(province: str, fuel_type: FuelType) -> None:
        province_prices = await run.io_bound(get_province_price_map, fuel_type)
        if province_prices:
            with detail_map_container:
                map_fig = build_province_choropleth(
                    province_prices,
                    province,
                    fuel_type.value,
                )
                ui.plotly(map_fig).classes("w-full")

    zones_button.on("click", lambda _: on_load_zones())
    mainland_only.on_value_change(lambda _: _render_preloaded_map())
    fuel.on_value_change(lambda _: _render_preloaded_map())
    set_status("info", "Carga una provincia para ver el ranking y detalle por distritos o municipios.")
    ui.timer(0, _render_preloaded_map, once=True)


def _build_trip_panel() -> None:
    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("w-full p-4"):
            ui.label("Planifica tu viaje y encuentra las paradas de repostaje mas baratas.").classes(
                "text-sm text-gray-600"
            )
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                origin_input = ui.input(label="Origen", placeholder="Ejemplo: Madrid").classes("w-56")
                dest_input = ui.input(label="Destino", placeholder="Ejemplo: Cadiz").classes("w-56")
                fuel = fuel_type_select()
                detour_input = ui.number(
                    label="Desviacion maxima (min)",
                    value=settings.default_max_detour_minutes,
                    min=1,
                    max=30,
                ).classes("w-48")

            with ui.expansion("Vehiculo").classes("w-full"):
                with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                    consumption_input = ui.number(
                        label="Consumo (l/100km)",
                        value=settings.default_consumption_lper100km,
                        min=1.0,
                        max=30.0,
                        step=0.5,
                    ).classes("w-44")
                    tank_input = ui.number(
                        label="Deposito (L)",
                        value=settings.default_tank_liters,
                        min=5.0,
                        max=120.0,
                        step=5.0,
                    ).classes("w-40")
                    fuel_level_slider = ui.slider(
                        min=5,
                        max=100,
                        value=settings.default_fuel_level_pct,
                        step=5,
                    ).classes("w-56")
                    fuel_level_label = ui.label(f"Nivel actual: {int(settings.default_fuel_level_pct)}%").classes(
                        "text-sm text-gray-600"
                    )
                    fuel_level_slider.on_value_change(
                        lambda e: fuel_level_label.set_text(f"Nivel actual: {int(e.value)}%")
                    )

            plan_button = ui.button("Planificar ruta").props("unelevated color=primary")

        status_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        map_container = ui.column().classes("w-full")
        table_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def on_plan() -> None:
        summary_container.clear()
        map_container.clear()
        table_container.clear()

        origin = (origin_input.value or "").strip()
        dest = (dest_input.value or "").strip()
        if not origin or not dest:
            set_status("warning", "Introduce una direccion de origen y destino.")
            return

        set_status("loading", "Planificando ruta...")
        plan_button.disable()
        try:
            fuel_type = FuelType(fuel.value)
            trip_result = await run.io_bound(
                plan_trip,
                origin,
                dest,
                fuel_type.value,
                consumption_input.value,
                tank_input.value,
                fuel_level_slider.value,
                detour_input.value,
            )

            if not trip_result.stops:
                set_status(
                    "success",
                    f"Ruta de {trip_result.total_distance_km:.0f} km. "
                    "No es necesario parar a repostar con el combustible actual.",
                )
            else:
                set_status(
                    "success",
                    f"Ruta planificada: {len(trip_result.stops)} parada(s) recomendada(s).",
                )

            with summary_container:
                kpi_row(trip_summary_cards(trip_result))

            with map_container:
                fig = build_trip_map(trip_result)
                ui.plotly(fig).classes("w-full")

            if trip_result.stops:
                with table_container:
                    trip_stops_table(trip_result.stops)

            if trip_result.candidate_stations:
                with table_container:
                    ui.label("Top 5 estaciones mas baratas en la ruta").classes("text-lg font-semibold mt-4")
                    top_cheapest_table(trip_result.candidate_stations)

            if trip_result.alternative_plans:
                with table_container:
                    with ui.expansion("Planes alternativos").classes("w-full mt-4"):
                        for alt_plan in trip_result.alternative_plans:
                            ui.label(alt_plan.strategy_name).classes("text-md font-semibold mt-2")
                            ui.label(alt_plan.strategy_description).classes("text-sm text-gray-600")
                            kpi_row(alternative_plan_cards(alt_plan))
                            trip_stops_table(alt_plan.stops)

        except ValueError as exc:
            logger.warning("Trip planning validation error: %s", exc)
            set_status("warning", str(exc))
        except Exception:
            logger.exception("Trip planning error")
            set_status("error", "No se pudo planificar la ruta. Revisa los datos e intentalo de nuevo.")
        finally:
            plan_button.enable()

    plan_button.on("click", lambda _: on_plan())
    set_status("info", "Introduce origen, destino y parametros del vehiculo para planificar tu ruta.")


def _build_historical_panel() -> None:
    with ui.column().classes("w-full gap-3"):
        ui.label("Analisis basado en datos historicos precomputados.").classes("text-sm text-gray-600")

        with ui.tabs().classes("w-full") as sub_tabs:
            ranking_tab = ui.tab("Ranking de provincias")
            dow_tab = ui.tab("Patron semanal")

        with ui.tab_panels(sub_tabs, value=ranking_tab).classes("w-full"):
            with ui.tab_panel(ranking_tab):
                _build_province_ranking_subtab()
            with ui.tab_panel(dow_tab):
                _build_day_of_week_subtab()


def _build_province_ranking_subtab() -> None:
    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("w-full p-4"):
            ui.label("Provincias ordenadas por precio medio de combustible.").classes("text-sm text-gray-600")
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                fuel = fuel_type_select()
                period = historical_period_select()
                mainland_only = ui.checkbox("Solo peninsula", value=True)
                ranking_button = ui.button("Cargar ranking").props("unelevated color=primary")

        status_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        table_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def on_load_ranking() -> None:
        summary_container.clear()
        table_container.clear()
        set_status("loading", "Cargando ranking de provincias...")
        ranking_button.disable()
        try:
            fuel_type = FuelType(fuel.value)
            hist_period = HistoricalPeriod(period.value)
            days_back = HISTORICAL_PERIOD_DAYS[hist_period]
            df = await run.io_bound(get_province_ranking, fuel_type, days_back)

            if df.empty:
                set_status("empty", "No hay datos de ranking disponibles. Ejecuta el backfill primero.")
                return

            if mainland_only.value:
                from data.geojson_loader import _NON_MAINLAND_DATA_NAMES

                df = df[~df["province"].isin(_NON_MAINLAND_DATA_NAMES)].reset_index(drop=True)

            with summary_container:
                kpi_row(province_ranking_kpis(df))
            with table_container:
                columns = [
                    {"name": "ranking", "label": "#", "field": "ranking", "align": "center"},
                    {"name": "province", "label": "Provincia", "field": "province", "align": "left"},
                    {
                        "name": "avg_price",
                        "label": "Precio medio (EUR/L)",
                        "field": "avg_price",
                        "align": "right",
                        "sortable": True,
                    },
                    {
                        "name": "diff",
                        "label": "Diff vs anterior",
                        "field": "diff",
                        "align": "right",
                        "sortable": True,
                    },
                    {
                        "name": "min_price",
                        "label": "Minimo (EUR/L)",
                        "field": "min_price",
                        "align": "right",
                        "sortable": True,
                    },
                    {
                        "name": "max_price",
                        "label": "Maximo (EUR/L)",
                        "field": "max_price",
                        "align": "right",
                        "sortable": True,
                    },
                    {
                        "name": "total_observations",
                        "label": "Observaciones",
                        "field": "total_observations",
                        "align": "right",
                        "sortable": True,
                    },
                ]
                rows = []
                prev_price = None
                for idx, row in df.iterrows():
                    avg = row["avg_price"]
                    diff = f"+{avg - prev_price:.4f}" if prev_price is not None and avg >= prev_price else ""
                    if prev_price is not None and avg < prev_price:
                        diff = f"{avg - prev_price:.4f}"
                    rows.append(
                        {
                            "ranking": idx + 1,
                            "province": str(row["province"]).title(),
                            "avg_price": f"{avg:.4f}",
                            "diff": diff,
                            "min_price": f"{row['min_price']:.4f}",
                            "max_price": f"{row['max_price']:.4f}",
                            "total_observations": int(row["total_observations"]),
                        }
                    )
                    prev_price = avg
                table = ui.table(columns=columns, rows=rows, row_key="ranking").classes("w-full")
                table.props("dense flat bordered separator=cell")
            date_to = date.today()
            date_from = date_to - timedelta(days=days_back)
            set_status(
                "success",
                f"Ranking cargado ({len(df)} provincias). "
                f"Periodo: {date_from.strftime('%d/%m/%Y')} — {date_to.strftime('%d/%m/%Y')} "
                f"({HISTORICAL_PERIOD_LABELS[hist_period]}).",
            )
        except Exception:
            logger.exception("Province ranking error")
            set_status("error", "No se pudo cargar el ranking. Intentalo de nuevo.")
        finally:
            ranking_button.enable()

    ranking_button.on("click", lambda _: on_load_ranking())
    set_status("info", "Selecciona tipo de combustible y periodo para ver el ranking de provincias.")


def _build_day_of_week_subtab() -> None:
    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("w-full p-4"):
            ui.label("Precio medio por dia de la semana.").classes("text-sm text-gray-600")
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                fuel = fuel_type_select()
                province_input = ui.input(label="Provincia (opcional)", placeholder="Toda Espana").classes("w-56")
                mainland_only = ui.checkbox("Solo peninsula", value=True)
                dow_button = ui.button("Cargar patron").props("unelevated color=primary")

        status_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        chart_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def on_load_dow() -> None:
        summary_container.clear()
        chart_container.clear()
        set_status("loading", "Cargando patron semanal...")
        dow_button.disable()
        try:
            fuel_type = FuelType(fuel.value)
            province = normalize_data_province_name(province_input.value)
            exclude = None
            if mainland_only.value and not province:
                from data.geojson_loader import _NON_MAINLAND_DATA_NAMES

                exclude = _NON_MAINLAND_DATA_NAMES
            df = await run.io_bound(get_day_of_week_pattern, fuel_type, province, exclude)

            if df.empty:
                set_status("empty", "No hay datos de patron semanal disponibles.")
                return

            with summary_container:
                kpi_row(day_of_week_kpis(df))
            with chart_container:
                fig = build_day_of_week_chart(df, fuel_type.value)
                ui.plotly(fig).classes("w-full")
            weeks = int(df["count_days"].iloc[0])
            set_status("success", f"Patron semanal cargado. Datos acumulados de ~{weeks} semanas.")
        except Exception:
            logger.exception("Day of week pattern error")
            set_status("error", "No se pudo cargar el patron semanal. Intentalo de nuevo.")
        finally:
            dow_button.enable()

    dow_button.on("click", lambda _: on_load_dow())
    set_status("info", "Descubre que dia de la semana es mas barato repostar.")
