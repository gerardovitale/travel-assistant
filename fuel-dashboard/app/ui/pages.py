import logging

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
from ui.charts import build_trend_chart
from ui.charts import build_zone_bar_chart
from ui.components import fuel_type_select
from ui.components import search_mode_select
from ui.components import station_results_table
from ui.components import trend_period_select

logger = logging.getLogger(__name__)


def init_ui(app: FastAPI) -> None:
    @ui.page("/")
    def index():
        ui.label("Spain Fuel Prices Dashboard").classes("text-2xl font-bold mb-4")

        with ui.tabs().classes("w-full") as tabs:
            search_tab = ui.tab("Search Stations")
            trends_tab = ui.tab("Price Trends")
            zones_tab = ui.tab("Zone Comparison")

        with ui.tab_panels(tabs, value=search_tab).classes("w-full"):
            with ui.tab_panel(search_tab):
                _build_search_panel()
            with ui.tab_panel(trends_tab):
                _build_trends_panel()
            with ui.tab_panel(zones_tab):
                _build_zones_panel()

    ui.run_with(app, title="Spain Fuel Prices", favicon="⛽")


def _build_search_panel():
    results_container = ui.column().classes("w-full mt-4")

    with ui.row().classes("items-end gap-4"):
        mode = search_mode_select()
        fuel = fuel_type_select()
        query_input = ui.input(label="Zip Code / Address").classes("w-64")
        radius = ui.number(label="Radius (km)", value=5.0, min=0.1, max=50.0).classes("w-32")

        def on_search():
            results_container.clear()
            fuel_type = FuelType(fuel.value)
            query = query_input.value
            if not query:
                with results_container:
                    ui.label("Please enter a zip code or address.").classes("text-orange-500")
                return
            try:
                if mode.value == "cheapest_by_zip":
                    stations = get_cheapest_by_zip(query, fuel_type)
                elif mode.value == "nearest_by_address":
                    stations = get_nearest_by_address(query, fuel_type)
                elif mode.value == "cheapest_by_address":
                    stations = get_cheapest_by_address(query, fuel_type, radius.value)
                elif mode.value == "best_by_address":
                    stations = get_best_by_address(query, fuel_type, radius.value)
                else:
                    stations = []
                with results_container:
                    station_results_table(stations)
            except Exception as e:
                logger.error(f"Search error: {e}")
                with results_container:
                    ui.label(f"Error: {e}").classes("text-red-500")

        ui.button("Search", on_click=on_search)


def _build_trends_panel():
    chart_container = ui.column().classes("w-full mt-4")

    with ui.row().classes("items-end gap-4"):
        zip_input = ui.input(label="Zip Code").classes("w-48")
        fuel = fuel_type_select()
        period = trend_period_select()

        def on_load_trend():
            chart_container.clear()
            zip_code = zip_input.value
            if not zip_code:
                with chart_container:
                    ui.label("Please enter a zip code.").classes("text-orange-500")
                return
            try:
                fuel_type = FuelType(fuel.value)
                trend_period = TrendPeriod(period.value)
                trend_data = get_price_trends(zip_code, fuel_type, trend_period)
                if not trend_data:
                    with chart_container:
                        ui.label("No trend data found.").classes("text-gray-500 italic")
                    return
                fig = build_trend_chart(trend_data, fuel_type.value, zip_code)
                with chart_container:
                    ui.plotly(fig).classes("w-full")
            except Exception as e:
                logger.error(f"Trend error: {e}")
                with chart_container:
                    ui.label(f"Error: {e}").classes("text-red-500")

        ui.button("Load Trend", on_click=on_load_trend)


def _build_zones_panel():
    chart_container = ui.column().classes("w-full mt-4")

    with ui.row().classes("items-end gap-4"):
        province_input = ui.input(label="Province").classes("w-48")
        fuel = fuel_type_select()

        def on_load_zones():
            chart_container.clear()
            province = province_input.value
            if not province:
                with chart_container:
                    ui.label("Please enter a province.").classes("text-orange-500")
                return
            try:
                fuel_type = FuelType(fuel.value)
                zones = get_cheapest_zones(province, fuel_type)
                if not zones:
                    with chart_container:
                        ui.label("No zone data found.").classes("text-gray-500 italic")
                    return
                fig = build_zone_bar_chart(zones, province, fuel_type.value)
                with chart_container:
                    ui.plotly(fig).classes("w-full")
            except Exception as e:
                logger.error(f"Zone error: {e}")
                with chart_container:
                    ui.label(f"Error: {e}").classes("text-red-500")

        ui.button("Load Zones", on_click=on_load_zones)
