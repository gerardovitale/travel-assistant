from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from api.schemas import FuelType
from api.schemas import StationResult
from api.schemas import TrendPeriod
from api.schemas import ZoneResult
from nicegui import ui
from ui.view_models import FUEL_DISPLAY_NAMES
from ui.view_models import SEARCH_MODE_OPTIONS
from ui.view_models import TREND_PERIOD_LABELS


def fuel_type_select(label: str = "Tipo de combustible", on_change: Optional[Callable] = None) -> ui.select:
    options = {ft.value: FUEL_DISPLAY_NAMES.get(ft.value, ft.value.replace("_", " ").title()) for ft in FuelType}
    return ui.select(options, value=FuelType.diesel_a_price.value, label=label, on_change=on_change).classes("w-72")


def trend_period_select(label: str = "Periodo", on_change: Optional[Callable] = None) -> ui.select:
    options = {tp.value: TREND_PERIOD_LABELS[tp.value] for tp in TrendPeriod}
    return ui.select(options, value=TrendPeriod.month.value, label=label, on_change=on_change).classes("w-40")


def search_mode_select(label: str = "Modo de busqueda", on_change: Optional[Callable] = None) -> ui.select:
    return ui.select(SEARCH_MODE_OPTIONS, value="cheapest_by_zip", label=label, on_change=on_change).classes("w-72")


def status_banner(status: str, message: str) -> None:
    styles: Dict[str, str] = {
        "info": "bg-slate-50 text-slate-700 border border-slate-200",
        "loading": "bg-blue-50 text-blue-700 border border-blue-200",
        "success": "bg-emerald-50 text-emerald-700 border border-emerald-200",
        "warning": "bg-amber-50 text-amber-700 border border-amber-200",
        "empty": "bg-gray-50 text-gray-700 border border-gray-200",
        "error": "bg-red-50 text-red-700 border border-red-200",
    }
    css = styles.get(status, styles["info"])
    with ui.row().classes(f"w-full items-center gap-2 px-3 py-2 rounded-md {css}"):
        if status == "loading":
            ui.spinner(size="sm")
        ui.label(message).classes("text-sm")


def loading_state(message: str = "Cargando resultados...") -> None:
    status_banner("loading", message)


def empty_state(message: str = "No hay datos para mostrar.") -> None:
    status_banner("empty", message)


def kpi_row(kpis: List[Dict[str, str]]) -> None:
    with ui.row().classes("w-full gap-3 flex-wrap"):
        for kpi in kpis:
            with ui.card().classes("p-3 min-w-44 flex-1"):
                ui.label(kpi["label"]).classes("text-xs text-gray-500 uppercase")
                ui.label(kpi["value"]).classes("text-lg font-semibold")


def station_results_table(stations: List[StationResult], mode: str) -> None:
    if not stations:
        empty_state("No se encontraron estaciones para esta busqueda.")
        return

    has_distance = any(s.distance_km is not None for s in stations)
    has_score = any(s.score is not None for s in stations)
    columns = [
        {"name": "recommended", "label": "Top", "field": "recommended", "align": "center"},
        {"name": "label", "label": "Estacion", "field": "label", "align": "left"},
        {"name": "address", "label": "Direccion", "field": "address", "align": "left"},
        {"name": "municipality", "label": "Municipio", "field": "municipality", "align": "left"},
        {"name": "price", "label": "Precio (EUR/L)", "field": "price", "align": "right", "sortable": True},
    ]
    if has_distance:
        columns.append(
            {
                "name": "distance_km",
                "label": "Distancia (km)",
                "field": "distance_km",
                "align": "right",
                "sortable": True,
            }
        )
    if has_score:
        columns.append({"name": "score", "label": "Puntuacion", "field": "score", "align": "right", "sortable": True})

    rows = []
    for idx, station in enumerate(stations):
        row = {
            "recommended": "Si" if idx == 0 else "",
            "label": station.label,
            "address": station.address,
            "municipality": station.municipality,
            "price": round(station.price, 3),
        }
        if station.distance_km is not None:
            row["distance_km"] = round(station.distance_km, 2)
        if station.score is not None:
            row["score"] = round(station.score, 2)
        rows.append(row)

    table = ui.table(columns=columns, rows=rows, row_key="label").classes("w-full")
    table.props("dense flat bordered separator=cell wrap-cells")
    if mode in ("nearest_by_address", "best_by_address"):
        table.props("rows-per-page-options=[3,5,10]")


def zone_results_table(zones: List[ZoneResult]) -> None:
    if not zones:
        empty_state("No hay zonas para mostrar.")
        return

    columns = [
        {"name": "zip_code", "label": "Codigo postal", "field": "zip_code", "align": "left", "sortable": True},
        {"name": "avg_price", "label": "Promedio (EUR/L)", "field": "avg_price", "align": "right", "sortable": True},
        {"name": "min_price", "label": "Minimo (EUR/L)", "field": "min_price", "align": "right", "sortable": True},
        {"name": "station_count", "label": "Estaciones", "field": "station_count", "align": "right", "sortable": True},
    ]
    rows = [
        {
            "zip_code": zone.zip_code,
            "avg_price": round(zone.avg_price, 3),
            "min_price": round(zone.min_price, 3),
            "station_count": int(zone.station_count),
        }
        for zone in zones
    ]
    table = ui.table(columns=columns, rows=rows, row_key="zip_code").classes("w-full")
    table.props("dense flat bordered separator=cell")
