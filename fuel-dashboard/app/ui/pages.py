import json
import logging
from datetime import date
from datetime import timedelta
from typing import Any
from typing import Dict

from api.schemas import FUEL_GROUP_PRIMARY
from api.schemas import FuelGroup
from api.schemas import FuelType
from api.schemas import HISTORICAL_PERIOD_DAYS
from api.schemas import HistoricalPeriod
from api.schemas import TrendPeriod
from config import settings
from fastapi import FastAPI
from nicegui import run
from nicegui import ui
from nicegui.elements.toggle import Toggle
from services.data_quality_service import get_data_inventory
from services.data_quality_service import get_ingestion_stats
from services.data_quality_service import get_latest_day_stats
from services.data_quality_service import get_missing_days
from services.geocoding import geocode_address
from services.station_service import get_best_by_address
from services.station_service import get_best_by_address_group
from services.station_service import get_brand_price_trend
from services.station_service import get_brand_ranking
from services.station_service import get_cheapest_by_address
from services.station_service import get_cheapest_by_address_group
from services.station_service import get_cheapest_by_zip
from services.station_service import get_cheapest_by_zip_group
from services.station_service import get_cheapest_zones
from services.station_service import get_day_of_week_pattern
from services.station_service import get_district_price_map
from services.station_service import get_group_price_trends
from services.station_service import get_municipalities
from services.station_service import get_nearest_by_address
from services.station_service import get_nearest_by_address_group
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
from services.station_service import get_zone_volatility_ranking
from services.trip_planner import plan_trip
from ui.charts import build_brand_trend_chart
from ui.charts import build_day_of_week_chart
from ui.charts import build_district_choropleth
from ui.charts import build_group_trend_chart
from ui.charts import build_ingestion_stats_chart
from ui.charts import build_monthly_spread_chart
from ui.charts import build_province_choropleth
from ui.charts import build_spread_trend_chart
from ui.charts import build_station_map
from ui.charts import build_trend_chart
from ui.charts import build_trip_map
from ui.charts import build_zip_code_choropleth
from ui.components import advice_card
from ui.components import card_nav
from ui.components import comparison_period_select
from ui.components import empty_state
from ui.components import fuel_group_select
from ui.components import fuel_type_select
from ui.components import geolocation_button
from ui.components import historical_period_select
from ui.components import init_theme
from ui.components import kpi_row
from ui.components import loading_state
from ui.components import page_header
from ui.components import search_fuel_select
from ui.components import search_mode_select
from ui.components import section_intro
from ui.components import station_results_table
from ui.components import status_banner
from ui.components import summary_card
from ui.components import top_cheapest_table
from ui.components import trend_period_select
from ui.components import trip_stops_table
from ui.view_models import alternative_plan_cards
from ui.view_models import best_day_advice
from ui.view_models import BEST_OPTION_METHODOLOGY_LINES
from ui.view_models import brand_ranking_kpis
from ui.view_models import compute_daily_spread
from ui.view_models import data_inventory_kpis
from ui.view_models import day_of_week_kpis
from ui.view_models import format_percentage
from ui.view_models import group_trend_kpis
from ui.view_models import group_trend_summary_cards
from ui.view_models import HISTORICAL_PERIOD_LABELS
from ui.view_models import INSIGHT_SECTION_CARDS
from ui.view_models import latest_day_kpis
from ui.view_models import missing_days_kpis
from ui.view_models import monthly_spread_pattern
from ui.view_models import PRIMARY_NAV_ITEMS
from ui.view_models import province_ranking_kpis
from ui.view_models import search_mode_metadata
from ui.view_models import search_recommendation
from ui.view_models import search_summary_cards
from ui.view_models import spread_kpis
from ui.view_models import spread_summary_cards
from ui.view_models import station_summary
from ui.view_models import trend_kpis
from ui.view_models import trend_summary_cards
from ui.view_models import trip_recommendation
from ui.view_models import trip_summary_cards
from ui.view_models import volatility_kpis
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
    init_theme()

    @ui.page("/")
    def index():
        page_container = ui.column().classes("pe-page w-full max-w-7xl mx-auto p-4")

        def _render_dashboard() -> None:
            page_container.clear()
            with page_container:
                state: Dict[str, Any] = {"active": PRIMARY_NAV_ITEMS[0].key}

                page_header(
                    "Panel de precios de combustible en España",
                    "Consulta estaciones, prepara un viaje o explora tendencias e historicos de precios, "
                    "todo en un mismo lugar.",
                    eyebrow="Fuel Precision",
                    variant="hero",
                )
                nav_container = ui.row().classes("w-full gap-3 flex-wrap")
                content_container = ui.column().classes("w-full")

                def render_content() -> None:
                    content_container.clear()
                    with content_container:
                        if state["active"] == "search":
                            _build_search_panel()
                        elif state["active"] == "trip":
                            _build_trip_panel()
                        else:
                            _build_insights_panel()

                def set_active(section_key: str) -> None:
                    state["active"] = section_key
                    render_nav()
                    render_content()

                def render_nav() -> None:
                    nav_container.clear()
                    with nav_container:
                        card_nav(
                            [
                                {"key": item.key, "label": item.label, "description": item.description}
                                for item in PRIMARY_NAV_ITEMS
                            ],
                            state["active"],
                            set_active,
                        )

                render_nav()
                render_content()

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
        "advanced_container": None,
    }
    mode = None
    search_button = None
    limit_input = None

    def on_mode_change(_: Any) -> None:
        container = state.get("dynamic_container")
        advanced = state.get("advanced_container")
        if container is None or advanced is None:
            return
        _render_query_inputs(mode, container, advanced, state)
        search_button.set_text(search_mode_metadata(mode.value).action_label)

    with ui.column().classes("w-full gap-4"):
        page_header(
            "Busca la mejor estación",
            "Busca por direccion o codigo postal y compara por cercania, precio o coste total.",
            eyebrow="Buscar",
        )
        with ui.card().classes("pe-surface-panel w-full rounded-2xl p-5"):
            mode = search_mode_select(on_change=on_mode_change)
            with ui.row().classes("w-full items-start gap-4 flex-wrap"):
                dynamic_container = ui.column().classes("min-w-72 flex-1 gap-3")
                state["dynamic_container"] = dynamic_container
                fuel = search_fuel_select()

            with ui.expansion("Ajustes avanzados").classes("w-full").props("dense"):
                ui.label(
                    "Ajusta el numero de resultados y, si buscas la mejor opción, tus parametros de vehiculo."
                ).classes("text-xs text-gray-500")
                advanced_container = ui.column().classes("w-full gap-2")
                state["advanced_container"] = advanced_container
                limit_input = ui.number(label="Numero de estaciones", value=5, min=1, max=20).classes("w-48")

            with ui.row().classes("w-full justify-end"):
                search_button = ui.button("Buscar estaciones").props("unelevated color=primary")

        status_container = ui.column().classes("w-full")
        recommendation_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        advice_container = ui.column().classes("w-full")
        map_container = ui.column().classes("w-full")
        results_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def on_search() -> None:
        def clear_search_outputs() -> None:
            recommendation_container.clear()
            summary_container.clear()
            advice_container.clear()
            map_container.clear()
            results_container.clear()

        clear_search_outputs()
        query_input = state.get("query_input")
        if query_input is None:
            return
        query_value = (query_input.value or "").strip()
        if not query_value:
            set_status("warning", f"Introduce {search_mode_metadata(mode.value).query_label.lower()} para continuar.")
            return

        set_status("loading", "Buscando estaciones...")
        with map_container:
            with ui.column().classes("w-full items-center py-8"):
                ui.spinner(size="lg").classes("text-primary")
        search_button.disable()
        try:
            current_mode = mode.value
            radius_input = state.get("radius_input")
            radius_km = radius_input.value if radius_input else None
            limit = int(limit_input.value)

            # Parse fuel selection: "group:xxx" or "single:xxx"
            fuel_value = fuel.value
            is_group = fuel_value.startswith("group:")
            if is_group:
                fuel_group = FuelGroup(fuel_value.removeprefix("group:"))
                fuel_type = FUEL_GROUP_PRIMARY[fuel_group]
            else:
                fuel_type = FuelType(fuel_value.removeprefix("single:"))

            search_lat = None
            search_lon = None

            zip_boundary = None

            if current_mode == "cheapest_by_zip":
                coords = await run.io_bound(geocode_address, f"{query_value}, Spain")
                if coords:
                    search_lat, search_lon = coords
                if is_group:
                    stations = await run.io_bound(get_cheapest_by_zip_group, query_value, fuel_group, limit)
                else:
                    stations = await run.io_bound(get_cheapest_by_zip, query_value, fuel_type, limit)
                zip_boundary = await run.io_bound(get_zip_code_boundary, query_value)
            elif current_mode in ("nearest_by_address", "cheapest_by_address", "best_by_address"):
                coords = await run.io_bound(geocode_address, query_value)
                if coords is None:
                    clear_search_outputs()
                    set_status("warning", "No se pudo geocodificar la direccion proporcionada.")
                    return
                search_lat, search_lon = coords
                if current_mode == "nearest_by_address":
                    if is_group:
                        stations = await run.io_bound(
                            get_nearest_by_address_group, search_lat, search_lon, fuel_group, limit
                        )
                    else:
                        stations = await run.io_bound(get_nearest_by_address, search_lat, search_lon, fuel_type, limit)
                elif current_mode == "cheapest_by_address":
                    if is_group:
                        stations = await run.io_bound(
                            get_cheapest_by_address_group, search_lat, search_lon, fuel_group, radius_km, limit
                        )
                    else:
                        stations = await run.io_bound(
                            get_cheapest_by_address, search_lat, search_lon, fuel_type, radius_km, limit
                        )
                elif current_mode == "best_by_address":
                    consumption_input = state.get("consumption_input")
                    consumption = consumption_input.value if consumption_input else None
                    tank_input = state.get("tank_input")
                    tank = tank_input.value if tank_input else None
                    if is_group:
                        stations = await run.io_bound(
                            get_best_by_address_group,
                            search_lat,
                            search_lon,
                            fuel_group,
                            radius_km,
                            limit,
                            consumption,
                            tank,
                        )
                    else:
                        stations = await run.io_bound(
                            get_best_by_address, search_lat, search_lon, fuel_type, radius_km, limit, consumption, tank
                        )
                else:
                    stations = []
            else:
                stations = []

            results_container.clear()
            if not stations:
                map_container.clear()
                set_status("empty", "No se encontraron resultados para esta busqueda.")
                with results_container:
                    empty_state(search_mode_metadata(current_mode).empty_state_hint)
                return

            summary = station_summary(stations)
            fetch_routes = current_mode != "cheapest_by_zip" and search_lat is not None and search_lon is not None
            if fetch_routes:
                set_status("success", f"{summary['count']} estaciones encontradas. Cargando rutas en el mapa...")
            else:
                set_status("success", f"{summary['count']} estaciones encontradas.")
            map_container.clear()
            recommendation = search_recommendation(stations, current_mode)
            with recommendation_container:
                summary_card(
                    recommendation["title"],
                    recommendation["headline"],
                    recommendation["detail"],
                    recommendation["caption"],
                    tone="info" if current_mode == "nearest_by_address" else "primary",
                )
            with summary_container:
                kpi_row(search_summary_cards(summary, current_mode))
            if fetch_routes:
                with map_container:
                    ui.label("Mapa y accesos").classes("text-base font-semibold text-slate-900")
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
                    primary_fuel=fuel_type.value if is_group else None,
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

            # Best day to refuel advice
            try:
                province = stations[0].province if stations else None
                if province:
                    dow_df = await run.io_bound(get_day_of_week_pattern, fuel_type, province)
                    tip = best_day_advice(dow_df)
                    if tip:
                        with advice_container:
                            advice_card(tip)
            except Exception:
                logger.warning("Could not load day-of-week advice", exc_info=True)

        except ValueError as exc:
            map_container.clear()
            logger.warning("Search validation error: %s", exc)
            set_status("warning", str(exc))
        except Exception:
            map_container.clear()
            logger.exception("Search error")
            set_status("error", "No se pudo completar la busqueda. Revisa los datos e intentalo de nuevo.")
        finally:
            search_button.enable()

    search_button.on("click", lambda _: on_search())
    _render_query_inputs(mode, state["dynamic_container"], state["advanced_container"], state)
    search_button.set_text(search_mode_metadata(mode.value).action_label)
    set_status("info", "Completa tu ubicacion, el combustible y la estrategia para ver una recomendacion clara.")


def _render_query_inputs(
    mode: Toggle, container: ui.column, advanced_container: ui.column, state: Dict[str, Any]
) -> None:
    metadata = search_mode_metadata(mode.value)
    container.clear()
    advanced_container.clear()
    state["radius_input"] = None
    state["consumption_input"] = None
    state["tank_input"] = None
    with container:
        is_address_mode = mode.value != "cheapest_by_zip"
        if is_address_mode:
            with ui.row().classes("w-full items-end gap-2"):
                query_input = ui.input(label=metadata.query_label, placeholder=metadata.query_placeholder).classes(
                    "flex-grow max-w-xl"
                )
                geolocation_button(query_input)
        else:
            query_input = ui.input(label=metadata.query_label, placeholder=metadata.query_placeholder).classes(
                "w-full max-w-xl"
            )
        ui.label(metadata.helper_text).classes("text-xs text-gray-500")
        if metadata.requires_radius:
            state["radius_input"] = ui.number(
                label="Radio de busqueda (km)",
                value=settings.default_radius_km,
                min=0.1,
                max=50.0,
            ).classes("w-48")
        if metadata.requires_consumption:
            ui.label(
                "Usaremos tus ajustes del vehiculo para estimar si compensa desplazarte a una estacion mas barata."
            ).classes("text-xs text-emerald-700")
        state["query_input"] = query_input
    if metadata.requires_consumption:
        with advanced_container:
            section_intro(
                "Ajustes del vehiculo",
                "Solo se usan para la estrategia de mejor opción.",
            )
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                state["consumption_input"] = ui.number(
                    label="Consumo (l/100km)",
                    value=settings.default_consumption_lper100km,
                    min=1.0,
                    max=30.0,
                    step=0.5,
                ).classes("w-44")
                state["tank_input"] = ui.number(
                    label="Litros a repostar", value=settings.default_refill_liters, min=5.0, max=120.0, step=5.0
                ).classes("w-40")
            with ui.expansion("Como estimamos el coste total?").classes("w-full text-sm").props("dense"):
                for line in BEST_OPTION_METHODOLOGY_LINES:
                    if line:
                        ui.label(line).classes("text-xs text-gray-600")
                    else:
                        ui.separator().classes("my-1")


def _build_trends_panel() -> None:
    with ui.column().classes("w-full gap-3"):
        tabs = ui.tabs().classes("w-full")
        with tabs:
            ui.tab("individual", label="Tendencia individual")
            ui.tab("compare", label="Comparar variantes")

        with ui.tab_panels(tabs, value="individual").classes("w-full"):
            with ui.tab_panel("individual"):
                _build_individual_trend_tab()
            with ui.tab_panel("compare"):
                _build_group_comparison_tab()


def _build_individual_trend_tab() -> None:
    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("pe-surface-panel w-full rounded-2xl p-4"):
            ui.label("Consulta la evolucion de precios por codigo postal.").classes("text-sm text-gray-600")
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                zip_input = ui.input(label="Codigo postal", placeholder="Ejemplo: 28001").classes("w-56")
                fuel = fuel_type_select()
                trend_button = ui.button("Ver tendencia").props("unelevated color=primary")
            with ui.expansion("Busqueda personalizada").classes("w-full").props("dense"):
                with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                    period = trend_period_select()
                    ui.label("Periodo: 7, 30 o 90 dias.").classes("text-xs text-gray-500")

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


def _build_group_comparison_tab() -> None:
    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("pe-surface-panel w-full rounded-2xl p-4"):
            ui.label("Compara variantes de un mismo combustible (estandar vs premium).").classes(
                "text-sm text-gray-600"
            )
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                zip_input = ui.input(label="Codigo postal", placeholder="Ejemplo: 28001").classes("w-56")
                group = fuel_group_select()
                compare_button = ui.button("Comparar").props("unelevated color=primary")
            with ui.expansion("Busqueda personalizada").classes("w-full").props("dense"):
                with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                    period = comparison_period_select()
                    ui.label("Periodo: de 7 dias a 12 meses.").classes("text-xs text-gray-500")

        status_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        chart_container = ui.column().classes("w-full")
        spread_summary_container = ui.column().classes("w-full")
        spread_chart_container = ui.column().classes("w-full")
        monthly_chart_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def on_compare() -> None:
        summary_container.clear()
        chart_container.clear()
        spread_summary_container.clear()
        spread_chart_container.clear()
        monthly_chart_container.clear()
        zip_code = (zip_input.value or "").strip()
        if not zip_code:
            set_status("warning", "Introduce un codigo postal para comparar variantes.")
            return

        set_status("loading", "Cargando comparacion de variantes...")
        with chart_container:
            with ui.column().classes("w-full items-center py-8"):
                ui.spinner(size="lg").classes("text-primary")
        compare_button.disable()
        try:
            fuel_group = FuelGroup(group.value)
            trend_period = TrendPeriod(period.value)
            group_trends = await run.io_bound(get_group_price_trends, zip_code, fuel_group, trend_period)
            chart_container.clear()
            if not group_trends:
                set_status("empty", "No hay datos para esta combinacion.")
                with chart_container:
                    empty_state("Prueba otro codigo postal, familia de combustible o periodo.")
                return

            kpis = group_trend_kpis(group_trends)
            with summary_container:
                kpi_row(group_trend_summary_cards(kpis))

            with chart_container:
                fig = build_group_trend_chart(group_trends, fuel_group.value, zip_code)
                ui.plotly(fig).classes("w-full")

            daily_spreads = compute_daily_spread(group_trends)
            if len(daily_spreads) >= 2:
                s_kpis = spread_kpis(daily_spreads)
                with spread_summary_container:
                    ui.label("Analisis de diferencia premium").classes("text-lg font-semibold mt-4")
                    kpi_row(spread_summary_cards(s_kpis))
                with spread_chart_container:
                    fig = build_spread_trend_chart(daily_spreads, fuel_group.value, zip_code)
                    ui.plotly(fig).classes("w-full")

                monthly_df = monthly_spread_pattern(daily_spreads)
                if monthly_df is not None:
                    with monthly_chart_container:
                        ui.label("Patron mensual de la diferencia").classes("text-lg font-semibold mt-4")
                        fig = build_monthly_spread_chart(monthly_df, fuel_group.value, zip_code)
                        ui.plotly(fig).classes("w-full")

            set_status("success", f"Comparacion cargada para {zip_code}.")
        except ValueError as exc:
            logger.warning("Group trend validation error: %s", exc)
            set_status("warning", str(exc))
        except Exception:
            logger.exception("Group trend error")
            set_status("error", "No se pudo cargar la comparacion. Intentalo de nuevo.")
        finally:
            compare_button.enable()

    compare_button.on("click", lambda _: on_compare())
    set_status("info", "Selecciona una familia de combustible para comparar sus variantes.")


def _build_zones_panel() -> None:
    zones_state: Dict[str, Any] = {
        "province": None,
        "fuel_type": None,
        "is_madrid": False,
        "detail_button": None,
    }

    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("pe-surface-panel w-full rounded-2xl p-4"):
            ui.label("Compara zonas por precio promedio de combustible.").classes("text-sm text-gray-600")
            provinces = get_provinces()
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                province_input = ui.select(options=provinces, label="Provincia", with_input=True).classes("w-56")
                fuel = fuel_type_select()
                zones_button = ui.button("Comparar zonas").props("unelevated color=primary")

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
            with ui.card().classes("pe-surface-panel w-full rounded-2xl p-4"):
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
    with ui.column().classes("w-full gap-4"):
        page_header(
            "Planifica tu viaje",
            "Mantiene origen y destino siempre visibles y resume primero si necesitas parar, donde compensa hacerlo "
            "y cuanto desvio supone.",
            eyebrow="Viaje",
        )
        with ui.card().classes("pe-surface-panel w-full rounded-2xl p-5"):
            section_intro("1. Ruta", "Indica el trayecto y el combustible que quieres usar en la planificacion.")
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                with ui.row().classes("items-end gap-1"):
                    origin_input = ui.input(label="Origen", placeholder="Ejemplo: Madrid").classes("w-64")
                    geolocation_button(origin_input)
                with ui.row().classes("items-end gap-1"):
                    dest_input = ui.input(label="Destino", placeholder="Ejemplo: Cadiz").classes("w-64")
                    geolocation_button(dest_input)
                fuel = fuel_type_select()

        with ui.card().classes("pe-surface-panel w-full rounded-2xl p-5"):
            section_intro(
                "2. Ajustes del vehiculo",
                "Solo ajusta lo necesario: autonomia, nivel actual y el desvio maximo que estas dispuesto a asumir.",
            )
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                detour_input = ui.number(
                    label="Desviacion maxima (min)",
                    value=settings.default_max_detour_minutes,
                    min=1,
                    max=30,
                ).classes("w-48")
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
            with ui.row().classes("w-full items-center gap-4 flex-wrap"):
                fuel_level_slider = ui.slider(
                    min=5,
                    max=100,
                    value=settings.default_fuel_level_pct,
                    step=5,
                ).classes("w-72")
                fuel_level_label = ui.label(f"Nivel actual: {int(settings.default_fuel_level_pct)}%").classes(
                    "text-sm text-gray-600"
                )
                fuel_level_slider.on_value_change(lambda e: fuel_level_label.set_text(f"Nivel actual: {int(e.value)}%"))
            with ui.row().classes("w-full justify-end"):
                plan_button = ui.button("Planificar viaje").props("unelevated color=primary")

        status_container = ui.column().classes("w-full")
        recommendation_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        map_container = ui.column().classes("w-full")
        table_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def on_plan() -> None:
        recommendation_container.clear()
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

            recommendation = trip_recommendation(trip_result)
            with recommendation_container:
                summary_card(
                    recommendation["title"],
                    recommendation["headline"],
                    recommendation["detail"],
                    recommendation["caption"],
                    tone="info" if not trip_result.stops else "primary",
                )
            with summary_container:
                kpi_row(trip_summary_cards(trip_result))

            with map_container:
                fig = build_trip_map(trip_result)
                ui.plotly(fig).classes("w-full")

            if trip_result.stops:
                with table_container:
                    ui.label("Paradas recomendadas").classes("text-lg font-semibold")
                    trip_stops_table(trip_result.stops)

            if trip_result.alternative_plans:
                with table_container:
                    ui.label("Planes alternativos").classes("text-lg font-semibold mt-4")
                    for alt_plan in trip_result.alternative_plans:
                        with ui.card().classes("pe-surface-card pe-ghost-outline w-full rounded-2xl p-4"):
                            ui.label(alt_plan.strategy_name).classes("text-md font-semibold")
                            ui.label(alt_plan.strategy_description).classes("text-sm text-gray-600")
                            kpi_row(alternative_plan_cards(alt_plan))
                            with ui.expansion("Ver detalle de paradas").classes("w-full").props("dense"):
                                trip_stops_table(alt_plan.stops)

            if trip_result.candidate_stations:
                with table_container:
                    ui.label("Estaciones baratas en la ruta").classes("text-lg font-semibold mt-4")
                    top_cheapest_table(trip_result.candidate_stations)

        except ValueError as exc:
            logger.warning("Trip planning validation error: %s", exc)
            set_status("warning", str(exc))
        except Exception:
            logger.exception("Trip planning error")
            set_status("error", "No se pudo planificar la ruta. Revisa los datos e intentalo de nuevo.")
        finally:
            plan_button.enable()

    plan_button.on("click", lambda _: on_plan())
    set_status("info", "Introduce origen, destino y los ajustes del vehiculo para obtener una recomendacion de ruta.")


def _build_insights_panel() -> None:
    with ui.column().classes("w-full gap-4"):
        page_header(
            "Insights y contexto",
            "Explora tendencias, mapas y calidad de datos.",
            eyebrow="Insights",
        )
        state: Dict[str, Any] = {"active": INSIGHT_SECTION_CARDS[0].key}
        nav_container = ui.column().classes("w-full")
        content_container = ui.column().classes("w-full")

        def render_content() -> None:
            content_container.clear()
            with content_container:
                if state["active"] == "trends":
                    _build_trends_panel()
                elif state["active"] == "zones":
                    _build_zones_panel()
                elif state["active"] == "historical":
                    _build_historical_panel()
                else:
                    _build_data_quality_panel()

        def set_active(section_key: str) -> None:
            state["active"] = section_key
            render_nav()
            render_content()

        def render_nav() -> None:
            nav_container.clear()
            with nav_container:
                card_nav(
                    [
                        {
                            "key": item.key,
                            "label": item.label,
                            "description": "" if item.key == "quality" else item.description,
                        }
                        for item in INSIGHT_SECTION_CARDS
                    ],
                    state["active"],
                    set_active,
                    tone="secondary",
                )

        render_nav()
        render_content()


def _build_historical_panel() -> None:
    with ui.column().classes("w-full gap-3"):
        ui.label("Analisis basado en datos historicos precomputados.").classes("text-sm text-gray-600")

        with ui.tabs().classes("w-full") as sub_tabs:
            ranking_tab = ui.tab("Ranking de provincias")
            dow_tab = ui.tab("Patron semanal")
            brand_tab = ui.tab("Comparacion por marca")
            volatility_tab = ui.tab("Volatilidad de precios")

        with ui.tab_panels(sub_tabs, value=ranking_tab).classes("w-full"):
            with ui.tab_panel(ranking_tab):
                _build_province_ranking_subtab()
            with ui.tab_panel(dow_tab):
                _build_day_of_week_subtab()
            with ui.tab_panel(brand_tab):
                _build_brand_comparison_subtab()
            with ui.tab_panel(volatility_tab):
                _build_zone_volatility_subtab()


def _build_province_ranking_subtab() -> None:
    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("pe-surface-panel w-full rounded-2xl p-4"):
            ui.label("Provincias ordenadas por precio medio de combustible.").classes("text-sm text-gray-600")
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                fuel = fuel_type_select()
                ranking_button = ui.button("Ver ranking").props("unelevated color=primary")
            with ui.expansion("Busqueda personalizada").classes("w-full").props("dense"):
                with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                    period = historical_period_select()
                    mainland_only = ui.checkbox("Solo peninsula", value=True)

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
                table = ui.table(columns=columns, rows=rows, row_key="ranking").classes("pe-table w-full")
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
        with ui.card().classes("pe-surface-panel w-full rounded-2xl p-4"):
            ui.label("Precio medio por dia de la semana.").classes("text-sm text-gray-600")
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                fuel = fuel_type_select()
                dow_button = ui.button("Ver patron").props("unelevated color=primary")
            with ui.expansion("Busqueda personalizada").classes("w-full").props("dense"):
                with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                    province_input = ui.input(label="Provincia (opcional)", placeholder="Toda Espana").classes("w-56")
                    mainland_only = ui.checkbox("Solo peninsula", value=True)

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


def _build_brand_comparison_subtab() -> None:
    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("pe-surface-panel w-full rounded-2xl p-4"):
            ui.label("Ranking de marcas/operadores por precio medio de combustible.").classes("text-sm text-gray-600")
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                fuel = fuel_type_select()
                brand_button = ui.button("Ver ranking").props("unelevated color=primary")
            with ui.expansion("Busqueda personalizada").classes("w-full").props("dense"):
                period = historical_period_select()

        status_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        table_container = ui.column().classes("w-full")
        chart_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def on_load_brand_comparison() -> None:
        summary_container.clear()
        table_container.clear()
        chart_container.clear()
        set_status("loading", "Cargando ranking de marcas...")
        brand_button.disable()
        try:
            fuel_type = FuelType(fuel.value)
            hist_period = HistoricalPeriod(period.value)
            days_back = HISTORICAL_PERIOD_DAYS[hist_period]
            df = await run.io_bound(get_brand_ranking, fuel_type, days_back)

            if df.empty:
                set_status("empty", "No hay datos de marcas disponibles. Ejecuta la agregacion primero.")
                return

            with summary_container:
                kpi_row(brand_ranking_kpis(df))

            with table_container:
                columns = [
                    {"name": "ranking", "label": "#", "field": "ranking", "align": "center"},
                    {"name": "brand", "label": "Marca", "field": "brand", "align": "left"},
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
                        "label": "Estaciones",
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
                            "brand": str(row["brand"]).title(),
                            "avg_price": f"{avg:.4f}",
                            "diff": diff,
                            "min_price": f"{row['min_price']:.4f}",
                            "max_price": f"{row['max_price']:.4f}",
                            "total_observations": int(row["total_observations"]),
                        }
                    )
                    prev_price = avg
                table = ui.table(columns=columns, rows=rows, row_key="ranking").classes("pe-table w-full")
                table.props("dense flat bordered separator=cell")

            # Load trend chart for the top brands
            top_brands = df["brand"].tolist()[:10]
            trend_df = await run.io_bound(get_brand_price_trend, fuel_type, days_back, top_brands)
            if not trend_df.empty:
                with chart_container:
                    fig = build_brand_trend_chart(trend_df, fuel_type.value)
                    ui.plotly(fig).classes("w-full")

            date_to = date.today()
            date_from = date_to - timedelta(days=days_back)
            set_status(
                "success",
                f"Ranking cargado ({len(df)} marcas). "
                f"Periodo: {date_from.strftime('%d/%m/%Y')} — {date_to.strftime('%d/%m/%Y')} "
                f"({HISTORICAL_PERIOD_LABELS[hist_period]}).",
            )
        except Exception:
            logger.exception("Brand comparison error")
            set_status("error", "No se pudo cargar el ranking de marcas. Intentalo de nuevo.")
        finally:
            brand_button.enable()

    brand_button.on("click", lambda _: on_load_brand_comparison())
    set_status("info", "Selecciona tipo de combustible y periodo para comparar marcas.")


def _build_zone_volatility_subtab() -> None:
    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("pe-surface-panel w-full rounded-2xl p-4"):
            ui.label("Ranking de codigos postales por estabilidad de precios.").classes("text-sm text-gray-600")
            with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                fuel = fuel_type_select()
                volatility_button = ui.button("Ver volatilidad").props("unelevated color=primary")
            with ui.expansion("Busqueda personalizada").classes("w-full").props("dense"):
                with ui.row().classes("w-full items-end gap-4 flex-wrap"):
                    period = historical_period_select()
                    mainland_only = ui.checkbox("Solo peninsula", value=True)

        status_container = ui.column().classes("w-full")
        summary_container = ui.column().classes("w-full")
        table_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def on_load_volatility() -> None:
        summary_container.clear()
        table_container.clear()
        set_status("loading", "Cargando ranking de volatilidad...")
        volatility_button.disable()
        try:
            fuel_type = FuelType(fuel.value)
            hist_period = HistoricalPeriod(period.value)
            days_back = HISTORICAL_PERIOD_DAYS[hist_period]
            df = await run.io_bound(get_zone_volatility_ranking, fuel_type, days_back, mainland_only.value)

            if df.empty:
                set_status(
                    "empty",
                    "No hay datos de volatilidad disponibles.",
                )
                return

            with summary_container:
                kpi_row(volatility_kpis(df))

            with table_container:
                columns = [
                    {"name": "ranking", "label": "#", "field": "ranking", "align": "center"},
                    {"name": "zip_code", "label": "CP", "field": "zip_code", "align": "left", "sortable": True},
                    {"name": "province", "label": "Provincia", "field": "province", "align": "left", "sortable": True},
                    {
                        "name": "coefficient_of_variation",
                        "label": "CV",
                        "field": "coefficient_of_variation",
                        "align": "right",
                        "sortable": True,
                    },
                    {
                        "name": "std_dev_price",
                        "label": "Desv. est. (EUR/L)",
                        "field": "std_dev_price",
                        "align": "right",
                        "sortable": True,
                    },
                    {
                        "name": "avg_price",
                        "label": "Precio medio (EUR/L)",
                        "field": "avg_price",
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
                        "name": "price_range",
                        "label": "Rango (EUR/L)",
                        "field": "price_range",
                        "align": "right",
                        "sortable": True,
                    },
                    {
                        "name": "observation_days",
                        "label": "Dias observados",
                        "field": "observation_days",
                        "align": "right",
                        "sortable": True,
                    },
                ]
                rows = []
                for idx, row in df.iterrows():
                    rows.append(
                        {
                            "ranking": idx + 1,
                            "zip_code": str(row["zip_code"]),
                            "province": str(row["province"]).title(),
                            "coefficient_of_variation": format_percentage(row["coefficient_of_variation"]),
                            "std_dev_price": f"{row['std_dev_price']:.4f}",
                            "avg_price": f"{row['avg_price']:.4f}",
                            "min_price": f"{row['min_price']:.4f}",
                            "max_price": f"{row['max_price']:.4f}",
                            "price_range": f"{row['price_range']:.4f}",
                            "observation_days": int(row["observation_days"]),
                        }
                    )
                table = ui.table(columns=columns, rows=rows, row_key="ranking").classes("pe-table w-full")
                table.props("dense flat bordered separator=cell")

            date_to = date.today()
            date_from = date_to - timedelta(days=days_back)
            set_status(
                "success",
                f"Ranking cargado ({len(df)} zonas). "
                f"Periodo: {date_from.strftime('%d/%m/%Y')} — {date_to.strftime('%d/%m/%Y')} "
                f"({HISTORICAL_PERIOD_LABELS[hist_period]}).",
            )
        except Exception:
            logger.exception("Zone volatility error")
            set_status("error", "No se pudo cargar el ranking de volatilidad. Intentalo de nuevo.")
        finally:
            volatility_button.enable()

    volatility_button.on("click", lambda _: on_load_volatility())
    set_status("info", "Descubre que codigos postales han sido mas estables en el tiempo.")


def _build_data_quality_panel() -> None:
    with ui.column().classes("w-full gap-3"):
        with ui.card().classes("pe-surface-panel w-full rounded-2xl p-4"):
            ui.label(
                "Transparencia de datos: revisa la calidad y completitud de los datos utilizados en este panel."
            ).classes("text-sm text-gray-600")
            quality_button = ui.button("Ver calidad de datos").props("unelevated color=primary")

        status_container = ui.column().classes("w-full")
        inventory_container = ui.column().classes("w-full")
        latest_day_container = ui.column().classes("w-full")
        chart_container = ui.column().classes("w-full")
        missing_container = ui.column().classes("w-full")

    set_status = _make_set_status(status_container)

    async def on_load_quality() -> None:
        inventory_container.clear()
        latest_day_container.clear()
        chart_container.clear()
        missing_container.clear()
        set_status("loading", "Cargando metricas de calidad de datos...")
        quality_button.disable()
        try:
            stats_df = await run.io_bound(get_ingestion_stats)
            inventory = await run.io_bound(get_data_inventory, stats_df)

            if inventory["num_days"] == 0:
                set_status("empty", "No se encontraron datos validos en el almacenamiento.")
                return

            with inventory_container:
                kpi_row(data_inventory_kpis(inventory))

            latest_stats = get_latest_day_stats(stats_df, inventory["max_date"])
            with latest_day_container:
                kpi_row(latest_day_kpis(latest_stats))

            missing = get_missing_days(inventory["available_dates"], inventory["min_date"], inventory["max_date"])
            with missing_container:
                kpi_row(missing_days_kpis(missing))
                if missing:
                    with ui.expansion("Ver dias sin datos").classes("w-full"):
                        columns = [
                            {"name": "date", "label": "Fecha", "field": "date", "align": "left", "sortable": True},
                        ]
                        rows = [{"date": d} for d in missing]
                        ui.table(columns=columns, rows=rows, row_key="date").classes("pe-table w-full").props(
                            "dense flat bordered separator=cell"
                        )

            with chart_container:
                fig = build_ingestion_stats_chart(stats_df)
                ui.plotly(fig).classes("w-full")

            set_status("success", f"Metricas cargadas. {inventory['num_days']} dias de datos disponibles.")
        except Exception:
            logger.exception("Data quality panel error")
            set_status("error", "No se pudieron cargar las metricas de calidad. Intentalo de nuevo.")
        finally:
            quality_button.enable()

    quality_button.on("click", lambda _: on_load_quality())
    set_status("info", "Consulta las metricas de calidad y completitud de los datos.")
