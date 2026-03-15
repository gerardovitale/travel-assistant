import logging
from typing import Any
from typing import Dict

from api.schemas import FuelType
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
from services.station_service import get_district_price_map
from services.station_service import get_nearest_by_address
from services.station_service import get_price_trends
from services.station_service import get_province_price_map
from services.station_service import get_provinces
from services.station_service import get_zip_code_boundary
from ui.charts import build_district_choropleth
from ui.charts import build_province_choropleth
from ui.charts import build_station_map
from ui.charts import build_trend_chart
from ui.components import empty_state
from ui.components import fuel_type_select
from ui.components import kpi_row
from ui.components import loading_state
from ui.components import search_mode_select
from ui.components import station_results_table
from ui.components import status_banner
from ui.components import trend_period_select
from ui.view_models import search_mode_metadata
from ui.view_models import search_summary_cards
from ui.view_models import station_summary
from ui.view_models import trend_kpis
from ui.view_models import trend_summary_cards
from ui.view_models import zone_kpis
from ui.view_models import zone_summary_cards

from data.cache import is_data_ready

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

                with ui.tab_panels(tabs, value=search_tab).classes("w-full"):
                    with ui.tab_panel(search_tab):
                        _build_search_panel()
                    with ui.tab_panel(trends_tab):
                        _build_trends_panel()
                    with ui.tab_panel(zones_tab):
                        _build_zones_panel()

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
    state: Dict[str, Any] = {"query_input": None, "radius_input": None, "dynamic_container": None}
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
            with ui.row().classes("w-full items-start gap-4 flex-wrap"):
                mode = search_mode_select(on_change=on_mode_change)
                fuel = fuel_type_select()
                dynamic_container = ui.column().classes("gap-1")
                state["dynamic_container"] = dynamic_container
                limit_input = ui.number(label="Numero de estaciones", value=5, min=1, max=20).classes("w-48")
                search_button = ui.button("Buscar").props("unelevated color=primary").classes("self-end")

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
                    stations = await run.io_bound(
                        get_best_by_address, search_lat, search_lon, fuel_type, radius_km, limit
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
            set_status("success", f"{summary['count']} estaciones encontradas.")
            with summary_container:
                kpi_row(search_summary_cards(summary, current_mode))
            with map_container:
                fig, stations_trace_idx, highlight_trace_idx = build_station_map(
                    stations, search_lat, search_lon, query_value, zip_boundary=zip_boundary
                )
                plotly_el = ui.plotly(fig).classes("w-full")
                plotly_id = f"c{plotly_el.id}"
            with results_container:
                station_results_table(
                    stations,
                    current_mode,
                    plotly_element_id=plotly_id,
                    stations_trace_idx=stations_trace_idx,
                    highlight_trace_idx=highlight_trace_idx,
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
    with container:
        query_input = ui.input(label=metadata.query_label, placeholder=metadata.query_placeholder).classes(
            "w-80 max-w-full"
        )
        ui.label(metadata.helper_text).classes("text-xs text-gray-500")
        state["query_input"] = query_input
        state["radius_input"] = None
        if metadata.requires_radius:
            state["radius_input"] = ui.number(
                label="Radio (km)", value=settings.default_radius_km, min=0.1, max=50.0
            ).classes("w-36")


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
            mainland_only = ui.checkbox("Solo peninsula").classes("self-center")
        detail_map_container = ui.column().classes("w-full")

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

            if is_madrid:
                district_prices = await run.io_bound(get_district_price_map, province, fuel_type)
                if district_prices:
                    with detail_map_container:
                        map_fig = build_district_choropleth(district_prices, fuel_type.value)
                        ui.plotly(map_fig).classes("w-full")
                else:
                    await _render_detail_province_map(province, fuel_type)
            else:
                await _render_detail_province_map(province, fuel_type)
                with detail_map_container:
                    ui.label("Vista por distritos solo disponible para Madrid").classes("text-sm text-gray-500 italic")

            set_status("success", f"Comparativa cargada para {province}.")
        except ValueError as exc:
            logger.warning("Zone validation error: %s", exc)
            set_status("warning", str(exc))
        except Exception:
            logger.exception("Zone error")
            set_status("error", "No se pudo cargar la comparativa de zonas. Intentalo de nuevo.")
        finally:
            zones_button.enable()

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
    set_status("info", "Carga una provincia para ver el ranking y detalle por distritos.")
    _render_preloaded_map()
