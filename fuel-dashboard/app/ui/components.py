import json
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
    return ui.select(options, value=FuelType.gasoline_95_e5_price.value, label=label, on_change=on_change).classes(
        "w-72"
    )


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
            with ui.card().classes("p-3 min-w-44 flex-1").style("flex-basis:0; min-height:5.5rem"):
                ui.label(kpi["label"]).classes("text-xs text-gray-500 uppercase")
                value_color = kpi.get("color", "")
                ui.label(kpi["value"]).classes(f"text-lg font-semibold {value_color}")
                delta = kpi.get("delta")
                if delta:
                    delta_color = kpi.get("delta_color", "text-gray-500")
                    delta_icon = kpi.get("delta_icon")
                    with ui.row().classes("items-center gap-1"):
                        if delta_icon:
                            ui.icon(delta_icon).classes(f"text-sm {delta_color}")
                        ui.label(delta).classes(f"text-xs {delta_color}")
                description = kpi.get("description")
                if description:
                    ui.label(description).classes("text-xs text-gray-400")


def station_results_table(
    stations: List[StationResult],
    mode: str,
    plotly_element_id: Optional[str] = None,
    stations_trace_idx: Optional[int] = None,
    highlight_trace_idx: Optional[int] = None,
) -> Optional[ui.table]:
    if not stations:
        empty_state("No se encontraron estaciones para esta busqueda.")
        return None

    has_distance = any(s.distance_km is not None for s in stations)
    has_score = any(s.score is not None for s in stations)
    has_cost = any(s.estimated_total_cost is not None for s in stations)
    columns = [
        {"name": "ranking", "label": "#", "field": "ranking", "align": "center"},
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
    if has_cost:
        columns.append(
            {
                "name": "estimated_total_cost",
                "label": "Coste total (EUR)",
                "field": "estimated_total_cost",
                "align": "right",
                "sortable": True,
            }
        )
    if has_score:
        columns.append({"name": "score", "label": "Puntuacion", "field": "score", "align": "right", "sortable": True})

    rows = []
    for idx, station in enumerate(stations):
        row = {
            "ranking": idx + 1,
            "label": station.label,
            "address": station.address,
            "municipality": station.municipality,
            "price": round(station.price, 3),
        }
        if station.distance_km is not None:
            row["distance_km"] = round(station.distance_km, 2)
        if station.estimated_total_cost is not None:
            row["estimated_total_cost"] = round(station.estimated_total_cost, 2)
        if station.score is not None:
            row["score"] = round(station.score, 1)
        rows.append(row)

    table = ui.table(columns=columns, rows=rows, row_key="label").classes("w-full")
    table.props("dense flat bordered separator=cell wrap-cells")
    if mode in ("nearest_by_address", "best_by_address"):
        table.props("rows-per-page-options=[3,5,10]")

    if plotly_element_id is not None and stations_trace_idx is not None and highlight_trace_idx is not None:
        station_lookup = {}
        for s in stations:
            station_lookup[s.label] = {
                "lat": s.latitude,
                "lon": s.longitude,
                "text": f"{s.label}<br>{s.price:.3f} EUR/L<br>{s.address}",
            }
        lookup_json = json.dumps(station_lookup, ensure_ascii=True)

        col_slots = " ".join(
            f'<q-td key="{c["name"]}" :props="props">{{{{ props.row.{c["field"]} }}}}</q-td>' for c in columns
        )

        # Use slot only to add data-label attribute to rows.
        # JS event handlers go via run_javascript (global scope) because
        # Vue 3 template expressions block access to globals like Plotly/window.
        table.add_slot(
            "body",
            f"""
            <q-tr :props="props" :data-label="props.row.label">
                {col_slots}
            </q-tr>
            """,
        )

        table_dom_id = f"c{table.id}"
        # Use requestAnimationFrame to ensure the table DOM element is rendered
        # before attaching event listeners (NiceGUI batches UI updates over WebSocket).
        ui.run_javascript(
            f"""
            requestAnimationFrame(function() {{
                var lookup = {lookup_json};
                var tableEl = document.getElementById('{table_dom_id}');
                if (!tableEl) return;
                var lastTr = null;
                tableEl.addEventListener('mouseover', function(e) {{
                    var tr = e.target.closest('tr[data-label]');
                    if (!tr || tr === lastTr) return;
                    lastTr = tr;
                    var info = lookup[tr.getAttribute('data-label')];
                    if (!info || typeof Plotly === 'undefined') return;
                    Plotly.restyle('{plotly_element_id}',
                        {{lat: [[info.lat]], lon: [[info.lon]], text: [[info.text]]}},
                        [{highlight_trace_idx}]);
                    Plotly.restyle('{plotly_element_id}',
                        {{'marker.opacity': 0.4}}, [{stations_trace_idx}]);
                }});
                tableEl.addEventListener('mouseout', function(e) {{
                    var related = e.relatedTarget;
                    if (related && related.closest && related.closest('tr[data-label]') === lastTr) return;
                    lastTr = null;
                    if (typeof Plotly === 'undefined') return;
                    Plotly.restyle('{plotly_element_id}',
                        {{lat: [[null]], lon: [[null]], text: [[null]]}},
                        [{highlight_trace_idx}]);
                    Plotly.restyle('{plotly_element_id}',
                        {{'marker.opacity': 1}}, [{stations_trace_idx}]);
                }});
            }});
            """
        )

    return table


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
