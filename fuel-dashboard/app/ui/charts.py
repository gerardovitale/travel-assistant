import logging
import math
from typing import List
from typing import Optional
from typing import Tuple

import pandas as pd
import plotly.graph_objects as go
from api.schemas import DistrictPriceResult
from api.schemas import ProvincePriceResult
from api.schemas import StationResult
from api.schemas import TrendPoint
from api.schemas import TripPlan
from api.schemas import ZoneResult
from ui.view_models import fuel_label

from data.geojson_loader import _NON_MAINLAND_PROVINCES
from data.geojson_loader import get_geojson_province_name
from data.geojson_loader import load_madrid_districts
from data.geojson_loader import load_provinces_geojson
from data.geojson_loader import ZIP_CODE_PROPERTY

logger = logging.getLogger(__name__)


def build_district_choropleth(
    district_prices: List[DistrictPriceResult],
    fuel_type: str,
) -> go.Figure:
    geojson = load_madrid_districts()
    fuel_name = fuel_label(fuel_type)

    districts = [dp.district for dp in district_prices]
    avg_prices = [round(dp.avg_price, 4) for dp in district_prices]
    station_counts = [dp.station_count for dp in district_prices]

    fig = go.Figure(
        go.Choroplethmapbox(
            geojson=geojson,
            locations=districts,
            z=avg_prices,
            featureidkey="properties.nombre",
            colorscale=[[0, "#2166ac"], [0.5, "#f7f7f7"], [1, "#b2182b"]],
            colorbar=dict(title="EUR/L", tickformat=".3f"),
            marker_opacity=0.7,
            marker_line_width=1,
            customdata=station_counts,
            hovertemplate=("%{location}<br>" "Promedio: %{z:.3f} EUR/L<br>" "Estaciones: %{customdata}<extra></extra>"),
        )
    )

    fig.update_layout(
        title=f"Precio promedio por distrito (Madrid): {fuel_name}",
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=40.42, lon=-3.70),
            zoom=10,
        ),
        height=550,
        margin=dict(l=0, r=0, t=50, b=0),
    )
    return fig


def build_zip_code_choropleth(
    zip_code_prices: List[ZoneResult],
    geojson: dict,
    title: str,
    fuel_type: str,
) -> go.Figure:
    fuel_name = fuel_label(fuel_type)

    zip_codes = [zp.zip_code for zp in zip_code_prices]
    avg_prices = [round(zp.avg_price, 4) for zp in zip_code_prices]
    station_counts = [zp.station_count for zp in zip_code_prices]

    fig = go.Figure(
        go.Choroplethmapbox(
            geojson=geojson,
            locations=zip_codes,
            z=avg_prices,
            featureidkey=f"properties.{ZIP_CODE_PROPERTY}",
            colorscale=[[0, "#2166ac"], [0.5, "#f7f7f7"], [1, "#b2182b"]],
            colorbar=dict(title="EUR/L", tickformat=".3f"),
            marker_opacity=0.7,
            marker_line_width=1,
            customdata=station_counts,
            hovertemplate=(
                "CP %{location}<br>" "Promedio: %{z:.3f} EUR/L<br>" "Estaciones: %{customdata}<extra></extra>"
            ),
        )
    )

    center = dict(lat=40.0, lon=-3.7)
    zoom = 11
    all_coords = _flatten_coordinates([f["geometry"]["coordinates"] for f in geojson.get("features", [])])
    if all_coords:
        lats = [c[1] for c in all_coords]
        lons = [c[0] for c in all_coords]
        center = dict(
            lat=(min(lats) + max(lats)) / 2,
            lon=(min(lons) + max(lons)) / 2,
        )
        lat_span = (max(lats) - min(lats)) or 0.01
        lon_span = (max(lons) - min(lons)) or 0.01
        zoom_lat = math.log2(180 / lat_span)
        zoom_lon = math.log2(360 / lon_span)
        zoom = min(zoom_lat, zoom_lon)
        zoom = max(8, min(zoom, 15))

    fig.update_layout(
        title=f"{title}: {fuel_name}",
        mapbox=dict(
            style="open-street-map",
            center=center,
            zoom=zoom,
        ),
        height=550,
        margin=dict(l=0, r=0, t=50, b=0),
    )
    return fig


def build_trend_chart(trend_data: List[TrendPoint], fuel_type: str, zip_code: str) -> go.Figure:
    dates = [p.date for p in trend_data]
    avg_prices = [p.avg_price for p in trend_data]
    min_prices = [p.min_price for p in trend_data]
    max_prices = [p.max_price for p in trend_data]

    fig = go.Figure()
    fuel_name = fuel_label(fuel_type)

    fig.add_trace(
        go.Scatter(
            x=dates,
            y=max_prices,
            mode="lines",
            name="Maximo",
            line=dict(color="rgba(239,68,68,0.5)", width=1.5),
            hovertemplate="Fecha: %{x}<br>Maximo: %{y:.3f} EUR/L<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=min_prices,
            mode="lines",
            name="Minimo",
            line=dict(color="rgba(34,197,94,0.5)", width=1.5),
            fill="tonexty",
            fillcolor="rgba(148,163,184,0.18)",
            hovertemplate="Fecha: %{x}<br>Minimo: %{y:.3f} EUR/L<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=avg_prices,
            mode="lines+markers",
            name="Promedio",
            line=dict(color="#2563eb", width=3),
            marker=dict(size=6, color="#1d4ed8"),
            hovertemplate="Fecha: %{x}<br>Promedio: %{y:.3f} EUR/L<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"Evolucion de precios: {fuel_name} - CP {zip_code}",
        xaxis_title="Fecha",
        yaxis_title="Precio (EUR/L)",
        template="plotly_white",
        height=420,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.0),
        margin=dict(l=50, r=30, t=70, b=50),
    )
    fig.update_yaxes(tickformat=".3f")
    return fig


def build_province_choropleth(
    province_prices: List[ProvincePriceResult],
    selected_province: str,
    fuel_type: str,
    mainland_only: bool = False,
) -> go.Figure:
    geojson = load_provinces_geojson()
    fuel_name = fuel_label(fuel_type)

    locations = []
    avg_prices = []
    station_counts = []
    for pp in province_prices:
        geojson_name = get_geojson_province_name(pp.province)
        if geojson_name is None:
            logger.debug(f"Province '{pp.province}' not found in GeoJSON, skipping")
            continue
        if mainland_only and geojson_name in _NON_MAINLAND_PROVINCES:
            continue
        locations.append(geojson_name)
        avg_prices.append(round(pp.avg_price, 4))
        station_counts.append(pp.station_count)

    selected_geojson_name = get_geojson_province_name(selected_province) or selected_province

    fig = go.Figure(
        go.Choroplethmapbox(
            geojson=geojson,
            locations=locations,
            z=avg_prices,
            featureidkey="properties.name",
            colorscale=[[0, "#2166ac"], [0.5, "#f7f7f7"], [1, "#b2182b"]],
            colorbar=dict(title="EUR/L", tickformat=".3f"),
            marker_opacity=0.7,
            marker_line_width=1,
            customdata=station_counts,
            hovertemplate=("%{location}<br>" "Promedio: %{z:.3f} EUR/L<br>" "Estaciones: %{customdata}<extra></extra>"),
        )
    )

    center = dict(lat=40.0, lon=-3.7)
    zoom = 5
    for feature in geojson["features"]:
        if feature["properties"]["name"] == selected_geojson_name:
            coords = _flatten_coordinates(feature["geometry"]["coordinates"])
            if coords:
                lats = [c[1] for c in coords]
                lons = [c[0] for c in coords]
                center = dict(
                    lat=(min(lats) + max(lats)) / 2,
                    lon=(min(lons) + max(lons)) / 2,
                )
                lat_span = max(lats) - min(lats)
                lon_span = max(lons) - min(lons)
                span = max(lat_span, lon_span)
                if span > 3:
                    zoom = 6
                elif span > 1.5:
                    zoom = 7
                else:
                    zoom = 8
            break

    fig.update_layout(
        title=f"Precio promedio por provincia: {fuel_name}",
        mapbox=dict(
            style="open-street-map",
            center=center,
            zoom=zoom,
        ),
        height=550,
        margin=dict(l=0, r=0, t=50, b=0),
    )
    return fig


def _extract_boundary_coords(geometry: dict) -> list:
    """Extract coordinate rings from a Polygon or MultiPolygon geometry."""
    geom_type = geometry["type"]
    coords = geometry["coordinates"]
    if geom_type == "Polygon":
        return [coords[0]]
    elif geom_type == "MultiPolygon":
        return [polygon[0] for polygon in coords]
    return []


def build_station_map(
    stations: List[StationResult],
    search_lat: Optional[float],
    search_lon: Optional[float],
    search_label: str,
    zip_boundary: Optional[dict] = None,
) -> Tuple[go.Figure, int, int, int]:
    fig = go.Figure()

    all_lats: list = []
    all_lons: list = []

    if zip_boundary is not None:
        rings = _extract_boundary_coords(zip_boundary["geometry"])
        for ring in rings:
            boundary_lons = [c[0] for c in ring]
            boundary_lats = [c[1] for c in ring]
            fig.add_trace(
                go.Scattermapbox(
                    lat=boundary_lats,
                    lon=boundary_lons,
                    mode="lines",
                    fill="toself",
                    fillcolor="rgba(220, 38, 38, 0.1)",
                    line=dict(width=2, color="#dc2626"),
                    name="Zona CP",
                    hoverinfo="skip",
                    showlegend=(ring is rings[0]),
                )
            )
            all_lats.extend(boundary_lats)
            all_lons.extend(boundary_lons)

    st_lats = [s.latitude for s in stations]
    st_lons = [s.longitude for s in stations]
    st_texts = [f"{s.label}<br>{s.price:.3f} EUR/L<br>{s.address}" for s in stations]

    route_trace_idx = len(fig.data)
    fig.add_trace(
        go.Scattermapbox(
            lat=[None],
            lon=[None],
            mode="lines",
            line=dict(width=4, color="#6366f1"),
            hoverinfo="skip",
            name="Ruta",
            showlegend=False,
        )
    )

    stations_trace_idx = len(fig.data)
    fig.add_trace(
        go.Scattermapbox(
            lat=st_lats,
            lon=st_lons,
            mode="markers",
            marker=dict(size=12, color="#2563eb"),
            text=st_texts,
            hoverinfo="text",
            name="Estaciones",
        )
    )

    highlight_trace_idx = len(fig.data)
    fig.add_trace(
        go.Scattermapbox(
            lat=[None],
            lon=[None],
            mode="markers",
            marker=dict(size=20, color="#f59e0b"),
            text=[None],
            hoverinfo="text",
            name="Seleccion",
            showlegend=False,
        )
    )

    all_lats.extend(st_lats)
    all_lons.extend(st_lons)

    if search_lat is not None and search_lon is not None:
        fig.add_trace(
            go.Scattermapbox(
                lat=[search_lat],
                lon=[search_lon],
                mode="markers",
                marker=dict(size=18, color="#dc2626"),
                text=[search_label],
                hoverinfo="text",
                name="Ubicacion buscada",
            )
        )
        all_lats.append(search_lat)
        all_lons.append(search_lon)

    if all_lats:
        min_lat, max_lat = min(all_lats), max(all_lats)
        min_lon, max_lon = min(all_lons), max(all_lons)
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2
        # Add 20% padding so edge markers aren't clipped
        lat_span = (max_lat - min_lat) * 1.2 or 0.005
        lon_span = (max_lon - min_lon) * 1.2 or 0.005
        # Mapbox zoom: world ≈ 360° at zoom 0, each zoom level halves the span
        zoom_lat = math.log2(180 / lat_span)
        zoom_lon = math.log2(360 / lon_span)
        zoom = min(zoom_lat, zoom_lon)
        zoom = max(2, min(zoom, 15))
    else:
        center_lat, center_lon, zoom = 40.0, -3.7, 6

    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom,
        ),
        height=450,
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0.0),
    )
    return fig, stations_trace_idx, highlight_trace_idx, route_trace_idx


def build_trip_map(trip_plan: TripPlan) -> go.Figure:
    """Build a map showing the trip route, recommended stops, candidate stations, and origin/destination."""
    fig = go.Figure()

    route_coords = trip_plan.route_coordinates
    all_lats = []
    all_lons = []

    # Route polyline
    if route_coords:
        route_lats = [c[1] for c in route_coords]
        route_lons = [c[0] for c in route_coords]
        fig.add_trace(
            go.Scattermapbox(
                lat=route_lats,
                lon=route_lons,
                mode="lines",
                line=dict(width=4, color="#6366f1"),
                hoverinfo="skip",
                name="Ruta",
            )
        )
        all_lats.extend(route_lats)
        all_lons.extend(route_lons)

    # Candidate stations (blue, small, semi-transparent)
    candidates = trip_plan.candidate_stations
    if candidates:
        fig.add_trace(
            go.Scattermapbox(
                lat=[c.latitude for c in candidates],
                lon=[c.longitude for c in candidates],
                mode="markers",
                marker=dict(size=8, color="#3b82f6", opacity=0.4),
                text=[f"{c.label}<br>{c.price:.3f} EUR/L" for c in candidates],
                hoverinfo="text",
                name="Candidatas",
            )
        )

    # Top 5 cheapest stations (orange/gold)
    if candidates:
        top5 = sorted(candidates, key=lambda c: c.price)[:5]
        fig.add_trace(
            go.Scattermapbox(
                lat=[c.latitude for c in top5],
                lon=[c.longitude for c in top5],
                mode="markers",
                marker=dict(size=12, color="#f59e0b", opacity=0.9),
                text=[
                    f"Top {i + 1}: {c.label}<br>{c.price:.3f} EUR/L"
                    + (f"<br>Km {c.route_km:.0f}" if c.route_km is not None else "")
                    + (f" | Desvio {c.detour_minutes:.0f} min" if c.detour_minutes is not None else "")
                    for i, c in enumerate(top5)
                ],
                hoverinfo="text",
                name="Top 5 mas baratas",
            )
        )

    # Recommended stops (green, large)
    stops = trip_plan.stops
    if stops:
        fig.add_trace(
            go.Scattermapbox(
                lat=[s.station.latitude for s in stops],
                lon=[s.station.longitude for s in stops],
                mode="markers",
                marker=dict(size=16, color="#22c55e"),
                text=[
                    f"Parada {i + 1}: {s.station.label}<br>"
                    f"{s.station.price:.3f} EUR/L<br>"
                    f"Km {s.route_km:.0f} | Desvio {s.detour_minutes:.0f} min<br>"
                    f"Repostar {s.liters_to_fill:.1f} L ({s.cost_eur:.2f} EUR)"
                    for i, s in enumerate(stops)
                ],
                hoverinfo="text",
                name="Paradas recomendadas",
            )
        )

    # Origin + Destination (red, large)
    origin = trip_plan.origin_coords
    dest = trip_plan.destination_coords
    fig.add_trace(
        go.Scattermapbox(
            lat=[origin[0], dest[0]],
            lon=[origin[1], dest[1]],
            mode="markers",
            marker=dict(size=18, color="#dc2626"),
            text=["Origen", "Destino"],
            hoverinfo="text",
            name="Origen / Destino",
        )
    )
    all_lats.extend([origin[0], dest[0]])
    all_lons.extend([origin[1], dest[1]])

    # Auto-zoom
    if all_lats:
        min_lat, max_lat = min(all_lats), max(all_lats)
        min_lon, max_lon = min(all_lons), max(all_lons)
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2
        lat_span = (max_lat - min_lat) * 1.2 or 0.01
        lon_span = (max_lon - min_lon) * 1.2 or 0.01
        zoom_lat = math.log2(180 / lat_span)
        zoom_lon = math.log2(360 / lon_span)
        zoom = min(zoom_lat, zoom_lon)
        zoom = max(2, min(zoom, 15))
    else:
        center_lat, center_lon, zoom = 40.0, -3.7, 6

    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom,
        ),
        height=500,
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0.0),
    )
    return fig


def _flatten_coordinates(coords):
    result = []
    if not coords:
        return result
    if isinstance(coords[0], (int, float)):
        return [coords]
    for item in coords:
        result.extend(_flatten_coordinates(item))
    return result


def build_day_of_week_chart(df, fuel_type: str) -> go.Figure:
    """Vertical bar chart showing average price by day of week."""
    from ui.view_models import SPANISH_DAY_NAMES

    fuel_name = fuel_label(fuel_type)
    day_names = [SPANISH_DAY_NAMES.get(int(d), "?") for d in df["day_of_week"]]
    avg_prices = df["avg_price"].tolist()

    # Highlight cheapest day in green, rest in blue
    cheapest_idx = avg_prices.index(min(avg_prices)) if avg_prices else -1
    colors = ["#22c55e" if i == cheapest_idx else "#2563eb" for i in range(len(avg_prices))]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=day_names,
            y=avg_prices,
            marker=dict(color=colors),
            hovertemplate="Dia: %{x}<br>Precio medio: %{y:.4f} EUR/L<extra></extra>",
        )
    )
    # Zoom y-axis to data range so small differences are visible
    if avg_prices:
        price_min, price_max = min(avg_prices), max(avg_prices)
        padding = max((price_max - price_min) * 0.3, 0.002)
        y_range = [price_min - padding, price_max + padding]
    else:
        y_range = None

    fig.update_layout(
        title=f"Patron semanal: {fuel_name}",
        xaxis_title="Dia de la semana",
        yaxis_title="Precio medio (EUR/L)",
        yaxis_range=y_range,
        template="plotly_white",
        height=420,
        margin=dict(l=50, r=30, t=50, b=50),
        showlegend=False,
    )
    fig.update_yaxes(tickformat=".4f")
    return fig


def build_ingestion_stats_chart(df: pd.DataFrame) -> go.Figure:
    """Build a multi-line chart showing daily ingestion metrics."""
    fig = go.Figure()

    metrics = [
        ("record_count", "Registros", "#2563eb"),
        ("unique_stations", "Estaciones", "#16a34a"),
        ("unique_provinces", "Provincias", "#dc2626"),
        ("unique_municipalities", "Municipios", "#9333ea"),
        ("unique_localities", "Localidades", "#ea580c"),
    ]

    dates = pd.to_datetime(df["date"])
    for col, name, color in metrics:
        if col not in df.columns:
            continue
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=df[col],
                mode="lines+markers",
                name=name,
                line=dict(color=color, width=2),
                marker=dict(size=4),
                hovertemplate=f"Fecha: %{{x}}<br>{name}: %{{y:,}}<extra></extra>",
            )
        )

    fig.update_layout(
        title="Metricas de ingestion diaria",
        xaxis_title="Fecha",
        yaxis_title="Cantidad",
        template="plotly_white",
        height=450,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.0),
        margin=dict(l=50, r=30, t=70, b=50),
    )
    return fig


BRAND_COLORS = [
    "#2563eb",
    "#dc2626",
    "#16a34a",
    "#f59e0b",
    "#9333ea",
    "#ea580c",
    "#0891b2",
    "#be185d",
    "#4f46e5",
    "#65a30d",
    "#c026d3",
    "#0d9488",
    "#b91c1c",
    "#1d4ed8",
    "#ca8a04",
]


def build_brand_trend_chart(df: pd.DataFrame, fuel_type: str) -> go.Figure:
    """Build a multi-line chart showing price evolution for top brands over time."""
    fig = go.Figure()

    if df.empty:
        fig.update_layout(title="Sin datos disponibles", template="plotly_white", height=420)
        return fig

    brands = df["brand"].unique()
    for i, brand in enumerate(brands):
        brand_data = df[df["brand"] == brand].sort_values("date")
        color = BRAND_COLORS[i % len(BRAND_COLORS)]
        fig.add_trace(
            go.Scatter(
                x=pd.to_datetime(brand_data["date"]),
                y=brand_data["avg_price"],
                mode="lines+markers",
                name=brand.title(),
                line=dict(color=color, width=2),
                marker=dict(size=4),
                hovertemplate=f"{brand.title()}: %{{y:.3f}} EUR/L<extra></extra>",
            )
        )

    fig.update_layout(
        title=f"Evolucion de precios por marca: {fuel_label(fuel_type)}",
        xaxis_title="Fecha",
        yaxis_title="Precio medio (EUR/L)",
        yaxis_tickformat=".3f",
        template="plotly_white",
        height=420,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.0),
        margin=dict(l=50, r=30, t=70, b=50),
    )
    return fig
