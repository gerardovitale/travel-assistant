import json
from pathlib import Path
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from api.schemas import FUEL_GROUP_MEMBERS
from api.schemas import FuelGroup
from api.schemas import FuelType
from api.schemas import HistoricalPeriod
from api.schemas import StationResult
from api.schemas import TrendPeriod
from api.schemas import TripStop
from api.schemas import ZoneResult
from nicegui import ui
from nicegui.elements.toggle import Toggle
from ui.view_models import FUEL_DISPLAY_NAMES
from ui.view_models import FUEL_GROUP_DISPLAY_NAMES
from ui.view_models import FUEL_VARIANT_SHORT_NAMES
from ui.view_models import HISTORICAL_PERIOD_LABELS
from ui.view_models import SEARCH_FUEL_OPTIONS
from ui.view_models import SEARCH_MODE_OPTIONS
from ui.view_models import TREND_PERIOD_LABELS

_THEME_CSS = (Path(__file__).parent / "theme.css").read_text()


def init_theme() -> None:
    # Keep ui.colors() in sync with the --pe-* tokens in theme.css
    ui.colors(
        primary="#003d9b",
        secondary="#0c56d0",
        positive="#004e33",
        negative="#ba1a1a",
        warning="#c27c00",
        info="#0c56d0",
    )
    ui.add_head_html(
        '<link rel="preconnect" href="https://fonts.googleapis.com">'
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
        '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700'
        '&family=Manrope:wght@600;700;800&display=swap" rel="stylesheet">'
        f"<style>{_THEME_CSS}</style>"
    )


def advice_card(message: str) -> None:
    with ui.row().classes("pe-status w-full items-center gap-2 rounded-2xl bg-emerald-50 px-3 py-2 text-emerald-900"):
        ui.icon("lightbulb").classes("text-lg")
        ui.label(message).classes("text-sm")


def page_header(
    title: str,
    description: Optional[str] = None,
    eyebrow: Optional[str] = None,
    variant: str = "section",
) -> None:
    wrapper_classes = "pe-heading w-full gap-2 rounded-2xl p-5"
    title_classes = "font-bold"
    if variant == "hero":
        wrapper_classes += " pe-hero"
        title_classes += " text-4xl"
        eyebrow_classes = "text-xs font-semibold uppercase tracking-[0.2em] text-slate-300"
        description_classes = "max-w-3xl text-base font-semibold text-slate-200"
    else:
        wrapper_classes += " pe-surface-panel pe-ghost-outline"
        title_classes += " text-3xl text-slate-900"
        eyebrow_classes = "text-xs font-semibold uppercase tracking-[0.2em] text-slate-500"
        description_classes = "max-w-3xl text-sm text-slate-600"
    with ui.column().classes(wrapper_classes):
        if eyebrow:
            ui.label(eyebrow).classes(eyebrow_classes)
        ui.label(title).classes(title_classes)
        if description:
            ui.label(description).classes(description_classes)


def section_intro(title: str, description: str) -> None:
    with ui.column().classes("w-full gap-1"):
        ui.label(title).classes("pe-heading text-base font-semibold text-slate-900")
        ui.label(description).classes("text-sm text-slate-600")


def summary_card(title: str, headline: str, detail: str, caption: str = "", tone: str = "primary") -> None:
    styles = {
        "primary": "pe-summary-card--primary",
        "info": "pe-summary-card--info",
        "neutral": "pe-summary-card--neutral",
    }
    css = styles.get(tone, styles["primary"])
    with ui.card().classes(f"pe-summary-card pe-heading w-full rounded-2xl p-4 {css}"):
        ui.label(title).classes("text-xs font-semibold uppercase tracking-[0.15em] opacity-70")
        ui.label(headline).classes("text-xl font-semibold")
        ui.label(detail).classes("text-sm opacity-90")
        if caption:
            ui.label(caption).classes("text-xs opacity-70")


def insight_card(title: str, description: str, icon: str, muted: bool = False) -> None:
    card_classes = "pe-surface-card w-full rounded-2xl p-4"
    if muted:
        card_classes += " pe-ghost-outline bg-slate-50"
    else:
        card_classes += " pe-ghost-outline"
    with ui.card().classes(card_classes):
        with ui.row().classes("items-start gap-3"):
            ui.icon(icon).classes("mt-1 text-xl text-slate-500")
            with ui.column().classes("gap-1"):
                ui.label(title).classes("pe-heading text-base font-semibold text-slate-900")
                ui.label(description).classes("text-sm text-slate-600")


_NAV_ACTIVE_STYLE = {
    "primary": (
        "background: linear-gradient(135deg, rgba(0,61,155,0.96), rgba(12,86,208,0.90)) !important;"
        "color: white !important;"
        "box-shadow: 0 24px 48px rgba(11,28,48,0.10) !important;"
    ),
    "secondary": (
        "background: linear-gradient(135deg, rgba(11,28,48,0.94), rgba(0,61,155,0.88)) !important;"
        "color: white !important;"
        "box-shadow: 0 24px 48px rgba(11,28,48,0.08) !important;"
    ),
}


def card_nav(
    items: List[Dict[str, str]],
    active_key: str,
    on_select: Callable[[str], None],
    tone: str = "primary",
) -> None:
    with ui.row().classes("w-full gap-3 flex-wrap"):
        for item in items:
            is_active = item["key"] == active_key
            card_classes = "pe-nav-card pe-heading min-w-44 flex-1 rounded-2xl p-4 cursor-pointer"
            if tone == "secondary":
                card_classes += " pe-nav-card--secondary"
            label_classes = "text-base font-semibold"
            description_classes = "text-xs"
            card = ui.card().classes(card_classes).on("click", lambda _, key=item["key"]: on_select(key))
            if is_active:
                card.style(_NAV_ACTIVE_STYLE[tone])
                label_classes += " text-white"
                description_classes += " text-slate-200"
            with card:
                ui.label(item["label"]).classes(label_classes)
                if item.get("description"):
                    ui.label(item["description"]).classes(description_classes)


def geolocation_button(address_input: ui.input) -> None:
    """Render a button that fills the address input with the user's current GPS coordinates."""

    async def _on_click() -> None:
        result = await ui.run_javascript(
            """
            return await new Promise((resolve) => {
                if (!navigator.geolocation) {
                    resolve({error: 'Geolocalizacion no soportada por el navegador.'});
                    return;
                }
                navigator.geolocation.getCurrentPosition(
                    (pos) => resolve({lat: pos.coords.latitude, lon: pos.coords.longitude}),
                    (err) => resolve({error: 'No se pudo obtener la ubicacion: ' + err.message}),
                    {enableHighAccuracy: true, timeout: 10000}
                );
            });
            """,
            timeout=15.0,
        )
        if isinstance(result, dict) and "lat" in result:
            address_input.set_value(f"{result['lat']:.6f}, {result['lon']:.6f}")
        elif isinstance(result, dict) and "error" in result:
            ui.notify(result["error"], type="warning")

    ui.button(icon="my_location", on_click=_on_click).props("flat dense round").classes("pe-secondary-btn").tooltip(
        "Usar mi ubicacion"
    )


def fuel_type_select(label: str = "Tipo de combustible", on_change: Optional[Callable] = None) -> ui.select:
    options = {ft.value: FUEL_DISPLAY_NAMES.get(ft.value, ft.value.replace("_", " ").title()) for ft in FuelType}
    return (
        ui.select(options, value=FuelType.gasoline_95_e5_price.value, label=label, on_change=on_change)
        .props("outlined")
        .classes("pe-input w-72")
    )


def fuel_group_select(label: str = "Familia de combustible", on_change: Optional[Callable] = None) -> ui.select:
    options = {fg.value: FUEL_GROUP_DISPLAY_NAMES.get(fg.value, fg.value.replace("_", " ").title()) for fg in FuelGroup}
    return (
        ui.select(options, value=FuelGroup.diesel.value, label=label, on_change=on_change)
        .props("outlined")
        .classes("pe-input w-72")
    )


def search_fuel_select(label: str = "Tipo de combustible", on_change: Optional[Callable] = None) -> ui.select:
    return (
        ui.select(SEARCH_FUEL_OPTIONS, value="group:diesel", label=label, on_change=on_change)
        .props("outlined")
        .classes("pe-input w-72")
    )


_SHORT_PERIODS = {TrendPeriod.week, TrendPeriod.month, TrendPeriod.quarter}


def trend_period_select(label: str = "Periodo", on_change: Optional[Callable] = None) -> ui.select:
    options = {tp.value: TREND_PERIOD_LABELS[tp.value] for tp in TrendPeriod if tp in _SHORT_PERIODS}
    return (
        ui.select(options, value=TrendPeriod.month.value, label=label, on_change=on_change)
        .props("outlined")
        .classes("pe-input w-40")
    )


def comparison_period_select(label: str = "Periodo", on_change: Optional[Callable] = None) -> ui.select:
    options = {tp.value: TREND_PERIOD_LABELS[tp.value] for tp in TrendPeriod}
    return (
        ui.select(options, value=TrendPeriod.quarter.value, label=label, on_change=on_change)
        .props("outlined")
        .classes("pe-input w-40")
    )


def historical_period_select(label: str = "Periodo", on_change: Optional[Callable] = None) -> ui.select:
    options = {hp.value: HISTORICAL_PERIOD_LABELS[hp] for hp in HistoricalPeriod}
    return (
        ui.select(options, value=HistoricalPeriod.quarter.value, label=label, on_change=on_change)
        .props("outlined")
        .classes("pe-input w-40")
    )


def search_mode_select(label: str = "Como quieres comparar", on_change: Optional[Callable] = None) -> Toggle:
    return (
        ui.toggle(SEARCH_MODE_OPTIONS, value="best_by_address", on_change=on_change)
        .props("spread unelevated no-caps color=grey-2 text-color=grey-8 toggle-color=primary")
        .classes("pe-toggle w-full rounded-2xl overflow-hidden")
    )


def status_banner(status: str, message: str) -> None:
    styles: Dict[str, str] = {
        "info": "bg-slate-50 text-slate-700",
        "loading": "bg-blue-50 text-blue-700",
        "success": "bg-emerald-50 text-emerald-900",
        "warning": "bg-amber-50 text-amber-900",
        "empty": "bg-slate-100 text-slate-700",
        "error": "bg-red-50 text-red-800",
    }
    css = styles.get(status, styles["info"])
    with ui.row().classes(f"pe-status w-full items-center gap-2 rounded-2xl px-3 py-2 {css}"):
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
            with ui.card().classes("pe-kpi pe-heading min-w-44 flex-1 rounded-2xl p-3"):
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
    primary_fuel: Optional[str] = None,
    plotly_element_id: Optional[str] = None,
    stations_trace_idx: Optional[int] = None,
    highlight_trace_idx: Optional[int] = None,
    route_trace_idx: Optional[int] = None,
) -> Optional[ui.table]:
    if not stations:
        empty_state("No se encontraron estaciones para esta busqueda.")
        return None

    has_distance = any(s.distance_km is not None for s in stations)
    has_cost = any(s.estimated_total_cost is not None for s in stations)
    has_pct = any(s.pct_vs_avg is not None for s in stations)
    has_variants = any(s.variant_prices for s in stations)
    variant_keys: List[str] = []
    if has_variants:
        seen: Dict[str, None] = {}
        for s in stations:
            if s.variant_prices:
                for k in s.variant_prices:
                    seen.setdefault(k, None)
        if primary_fuel:
            canonical = [ft.value for group in FUEL_GROUP_MEMBERS.values() for ft in group]
            order = {k: i for i, k in enumerate(canonical)}
            variant_keys = sorted(seen, key=lambda k: order.get(k, len(canonical)))
        else:
            variant_keys = list(seen)

    columns = [
        {"name": "ranking", "label": "#", "field": "ranking", "align": "center"},
        {"name": "label", "label": "Estacion", "field": "label", "align": "left"},
        {"name": "address", "label": "Direccion", "field": "address", "align": "left"},
    ]
    if has_variants:
        for vk in variant_keys:
            if primary_fuel:
                col_label = FUEL_VARIANT_SHORT_NAMES.get(vk, FUEL_DISPLAY_NAMES.get(vk, vk))
            else:
                col_label = FUEL_DISPLAY_NAMES.get(vk, vk.replace("_price", "").replace("_", " ").title())
            if vk == primary_fuel:
                col_label += " (EUR/L)"
            elif primary_fuel:
                col_label += " (vs base)"
            columns.append({"name": vk, "label": col_label, "field": vk, "align": "right", "sortable": True})
    else:
        columns.append(
            {"name": "price", "label": "Precio (EUR/L)", "field": "price", "align": "right", "sortable": True}
        )
    if has_pct:
        columns.append(
            {
                "name": "pct_vs_avg",
                "label": "vs media",
                "field": "pct_vs_avg",
                "align": "right",
                "sortable": True,
            }
        )
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
    rows = []
    for idx, station in enumerate(stations):
        row = {
            "ranking": idx + 1,
            "label": station.label,
            "address": f"{station.address}, {station.municipality}, {station.province}, {station.zip_code}",
        }
        if has_variants and station.variant_prices:
            base_price = station.variant_prices.get(primary_fuel) if primary_fuel else None
            for vk in variant_keys:
                val = station.variant_prices.get(vk)
                if val is None:
                    row[vk] = "-"
                elif base_price is not None and vk != primary_fuel and base_price > 0:
                    delta = val - base_price
                    row[vk] = f"{delta:+.3f}"
                else:
                    row[vk] = round(val, 3)
        else:
            row["price"] = round(station.price, 3)
        if station.pct_vs_avg is not None:
            row["pct_vs_avg"] = f"{station.pct_vs_avg:+.1f}%"
        if station.distance_km is not None:
            row["distance_km"] = round(station.distance_km, 2)
        if station.estimated_total_cost is not None:
            if has_variants and primary_fuel and station.variant_prices and station.price > 0:
                primary_price = station.variant_prices.get(primary_fuel)
                multiplier = station.estimated_total_cost / station.price
                if primary_price is not None and primary_price > 0:
                    primary_cost = round(primary_price * multiplier, 2)
                    max_variant_price = max(station.variant_prices.values())
                    if max_variant_price > primary_price:
                        premium_extra = round((max_variant_price - primary_price) * multiplier, 2)
                        row["estimated_total_cost"] = f"{primary_cost} (+{premium_extra})"
                    else:
                        row["estimated_total_cost"] = primary_cost
                else:
                    row["estimated_total_cost"] = round(station.estimated_total_cost, 2)
            else:
                row["estimated_total_cost"] = round(station.estimated_total_cost, 2)
        rows.append(row)

    table = ui.table(columns=columns, rows=rows, row_key="label").classes("pe-table w-full")
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
                window.__stationLookup_{table_dom_id} = {lookup_json};
                var lookup = window.__stationLookup_{table_dom_id};
                var tableEl = document.getElementById('{table_dom_id}');
                if (!tableEl) return;
                var lastTr = null;
                var routeIdx = {route_trace_idx if route_trace_idx is not None else 'null'};
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
                    if (routeIdx !== null && info.route_lat) {{
                        Plotly.restyle('{plotly_element_id}',
                            {{lat: [info.route_lat], lon: [info.route_lon]}},
                            [routeIdx]);
                    }}
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
                    if (routeIdx !== null) {{
                        Plotly.restyle('{plotly_element_id}',
                            {{lat: [[null]], lon: [[null]]}},
                            [routeIdx]);
                    }}
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
    table = ui.table(columns=columns, rows=rows, row_key="zip_code").classes("pe-table w-full")
    table.props("dense flat bordered separator=cell")


def trip_stops_table(stops: List[TripStop]) -> None:
    if not stops:
        empty_state("No se necesitan paradas en esta ruta.")
        return

    columns = [
        {"name": "ranking", "label": "#", "field": "ranking", "align": "center"},
        {"name": "label", "label": "Estacion", "field": "label", "align": "left"},
        {"name": "address", "label": "Direccion", "field": "address", "align": "left"},
        {"name": "route_km", "label": "Km", "field": "route_km", "align": "right"},
        {"name": "detour", "label": "Desvio (min)", "field": "detour", "align": "right"},
        {"name": "price", "label": "Precio (EUR/L)", "field": "price", "align": "right"},
        {"name": "fuel_arrival", "label": "Combustible llegada", "field": "fuel_arrival", "align": "right"},
        {"name": "liters", "label": "Litros", "field": "liters", "align": "right"},
        {"name": "cost", "label": "Coste (EUR)", "field": "cost", "align": "right"},
        {"name": "reasoning", "label": "Razon", "field": "reasoning", "align": "left"},
    ]
    rows = [
        {
            "ranking": i + 1,
            "label": stop.station.label,
            "address": (
                f"{stop.station.address}, {stop.station.municipality}, {stop.station.province}, {stop.station.zip_code}"
            ),
            "route_km": round(stop.route_km, 0),
            "detour": round(stop.detour_minutes, 1),
            "price": round(stop.station.price, 3),
            "fuel_arrival": f"{stop.fuel_at_arrival_pct:.0f}%",
            "liters": round(stop.liters_to_fill, 1),
            "cost": round(stop.cost_eur, 2),
            "reasoning": stop.reasoning or "",
        }
        for i, stop in enumerate(stops)
    ]
    table = ui.table(columns=columns, rows=rows, row_key="ranking").classes("pe-table w-full")
    table.props("dense flat bordered separator=cell")


def top_cheapest_table(candidates: List[StationResult], top_n: int = 5) -> None:
    if not candidates:
        empty_state("No hay estaciones candidatas.")
        return

    sorted_candidates = sorted(candidates, key=lambda c: c.price)[:top_n]
    columns = [
        {"name": "ranking", "label": "#", "field": "ranking", "align": "center"},
        {"name": "label", "label": "Estacion", "field": "label", "align": "left"},
        {"name": "address", "label": "Direccion", "field": "address", "align": "left"},
        {"name": "price", "label": "Precio (EUR/L)", "field": "price", "align": "right"},
        {"name": "route_km", "label": "Km en ruta", "field": "route_km", "align": "right"},
        {"name": "detour", "label": "Desvio (min)", "field": "detour", "align": "right"},
    ]
    rows = [
        {
            "ranking": i + 1,
            "label": c.label,
            "address": f"{c.address}, {c.municipality}, {c.province}, {c.zip_code}",
            "price": round(c.price, 3),
            "route_km": round(c.route_km, 0) if c.route_km is not None else "-",
            "detour": round(c.detour_minutes, 1) if c.detour_minutes is not None else "-",
        }
        for i, c in enumerate(sorted_candidates)
    ]
    table = ui.table(columns=columns, rows=rows, row_key="ranking").classes("pe-table w-full")
    table.props("dense flat bordered separator=cell")
