from typing import List

from api.schemas import FuelType
from api.schemas import StationResult
from api.schemas import TrendPeriod
from nicegui import ui


def fuel_type_select(label: str = "Fuel Type") -> ui.select:
    options = {ft.value: ft.value.replace("_", " ").title() for ft in FuelType}
    return ui.select(options, value=FuelType.diesel_a_price.value, label=label).classes("w-64")


def trend_period_select(label: str = "Period") -> ui.select:
    options = {tp.value: tp.value.title() for tp in TrendPeriod}
    return ui.select(options, value=TrendPeriod.month.value, label=label).classes("w-48")


def search_mode_select() -> ui.select:
    options = {
        "cheapest_by_zip": "Cheapest by Zip Code",
        "nearest_by_address": "Nearest by Address",
        "cheapest_by_address": "Cheapest near Address",
        "best_by_address": "Best (Price + Distance)",
    }
    return ui.select(options, value="cheapest_by_zip", label="Search Mode").classes("w-64")


def station_results_table(stations: List[StationResult]) -> None:
    if not stations:
        ui.label("No results found.").classes("text-gray-500 italic")
        return
    columns = [
        {"name": "label", "label": "Station", "field": "label", "align": "left"},
        {"name": "address", "label": "Address", "field": "address", "align": "left"},
        {"name": "municipality", "label": "Municipality", "field": "municipality", "align": "left"},
        {"name": "price", "label": "Price (EUR/L)", "field": "price", "align": "right"},
    ]
    if stations[0].distance_km is not None:
        columns.append({"name": "distance_km", "label": "Distance (km)", "field": "distance_km", "align": "right"})
    if stations[0].score is not None:
        columns.append({"name": "score", "label": "Score", "field": "score", "align": "right"})
    rows = []
    for s in stations:
        row = {
            "label": s.label,
            "address": s.address,
            "municipality": s.municipality,
            "price": f"{s.price:.3f}",
        }
        if s.distance_km is not None:
            row["distance_km"] = f"{s.distance_km:.2f}"
        if s.score is not None:
            row["score"] = f"{s.score:.2f}"
        rows.append(row)
    ui.table(columns=columns, rows=rows, row_key="label").classes("w-full")
