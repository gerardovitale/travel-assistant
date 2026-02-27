import logging
from typing import Any
from typing import Dict

from api.schemas import FuelType
from api.schemas import TrendPeriod
from fastapi import FastAPI
from nicegui import ui
from services.station_service import get_best_by_address
from services.station_service import get_cheapest_by_address
from services.station_service import get_cheapest_by_zip
from services.station_service import get_cheapest_zones
from services.station_service import get_nearest_by_address
from services.station_service import get_price_trends
from services.station_service import get_provinces
from ui.charts import build_trend_chart
from ui.charts import build_zone_bar_chart
from ui.components import empty_state
from ui.components import fuel_type_select
from ui.components import kpi_row
from ui.components import loading_state
from ui.components import search_mode_select
from ui.components import station_results_table
from ui.components import status_banner
from ui.components import trend_period_select
from ui.components import zone_results_table
from ui.view_models import format_delta
from ui.view_models import format_price
from ui.view_models import search_mode_metadata
from ui.view_models import search_summary_cards
from ui.view_models import station_summary
from ui.view_models import trend_kpis
from ui.view_models import zone_kpis

from data.cache import is_data_ready

logger = logging.getLogger(__name__)


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
                search_button = ui.button("Buscar").props("unelevated color=primary").classes("self-end")

        status_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        results_container = ui.column().classes("w-full")

    def set_status(status: str, message: str) -> None:
        status_container.clear()
        with status_container:
            if status == "loading":
                loading_state(message)
            else:
                status_banner(status, message)

    def on_search() -> None:
        summary_container.clear()
        results_container.clear()
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

            if current_mode == "cheapest_by_zip":
                stations = get_cheapest_by_zip(query_value, fuel_type)
            elif current_mode == "nearest_by_address":
                stations = get_nearest_by_address(query_value, fuel_type)
            elif current_mode == "cheapest_by_address":
                stations = get_cheapest_by_address(query_value, fuel_type, radius_km)
            elif current_mode == "best_by_address":
                stations = get_best_by_address(query_value, fuel_type, radius_km)
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
            with results_container:
                station_results_table(stations, current_mode)
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
            state["radius_input"] = ui.number(label="Radio (km)", value=5.0, min=0.1, max=50.0).classes("w-36")


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

    def set_status(status: str, message: str) -> None:
        status_container.clear()
        with status_container:
            if status == "loading":
                loading_state(message)
            else:
                status_banner(status, message)

    def on_load_trend() -> None:
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
            trend_data = get_price_trends(zip_code, fuel_type, trend_period)
            chart_container.clear()
            if not trend_data:
                set_status("empty", "No hay datos de tendencia para esta combinacion.")
                with chart_container:
                    empty_state("Prueba otro codigo postal, tipo de combustible o periodo.")
                return

            metrics = trend_kpis(trend_data)
            with summary_container:
                kpi_row(
                    [
                        {"label": "Promedio actual", "value": format_price(metrics["current_avg_price"])},
                        {"label": "Minimo del periodo", "value": format_price(metrics["min_price"])},
                        {"label": "Maximo del periodo", "value": format_price(metrics["max_price"])},
                        {"label": "Variacion promedio", "value": format_delta(metrics["delta_avg_price"])},
                    ]
                )

            with chart_container:
                fig = build_trend_chart(trend_data, fuel_type.value, zip_code)
                ui.plotly(fig).classes("w-full")
            set_status("success", f"Tendencia cargada para {zip_code}.")
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
        chart_container = ui.column().classes("w-full")
        table_container = ui.column().classes("w-full")

    def set_status(status: str, message: str) -> None:
        status_container.clear()
        with status_container:
            if status == "loading":
                loading_state(message)
            else:
                status_banner(status, message)

    def on_load_zones() -> None:
        summary_container.clear()
        chart_container.clear()
        table_container.clear()
        province = province_input.value
        if not province:
            set_status("warning", "Selecciona una provincia para cargar zonas.")
            return

        set_status("loading", "Cargando comparativa de zonas...")
        with chart_container:
            with ui.column().classes("w-full items-center py-8"):
                ui.spinner(size="lg").classes("text-primary")
        zones_button.disable()
        try:
            fuel_type = FuelType(fuel.value)
            zones = get_cheapest_zones(province, fuel_type)
            chart_container.clear()
            if not zones:
                set_status("empty", "No hay datos de zonas para esta provincia.")
                with chart_container:
                    empty_state("Prueba otra provincia o tipo de combustible.")
                return

            metrics = zone_kpis(zones)
            with summary_container:
                kpi_row(
                    [
                        {"label": "Zonas analizadas", "value": str(metrics["zone_count"])},
                        {"label": "CP mas barato", "value": metrics["cheapest_zip"] or "-"},
                        {"label": "Mejor promedio", "value": format_price(metrics["cheapest_avg_price"])},
                        {"label": "Promedio provincial", "value": format_price(metrics["province_avg_price"])},
                    ]
                )

            with chart_container:
                fig = build_zone_bar_chart(zones, province, fuel_type.value)
                ui.plotly(fig).classes("w-full")
            with table_container:
                zone_results_table(zones)
            set_status("success", f"Comparativa cargada para {province}.")
        except Exception:
            logger.exception("Zone error")
            set_status("error", "No se pudo cargar la comparativa de zonas. Intentalo de nuevo.")
        finally:
            zones_button.enable()

    zones_button.on("click", lambda _: on_load_zones())
    set_status("info", "Carga una provincia para ver el ranking y detalle por codigo postal.")
