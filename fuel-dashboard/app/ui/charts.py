import logging
import math
from typing import List
from typing import Optional
from typing import Tuple

import plotly.graph_objects as go
from api.schemas import DistrictPriceResult
from api.schemas import ProvincePriceResult
from api.schemas import StationResult
from api.schemas import TrendPoint
from ui.view_models import fuel_label

from data.geojson_loader import _NON_MAINLAND_PROVINCES
from data.geojson_loader import get_geojson_province_name
from data.geojson_loader import load_madrid_districts
from data.geojson_loader import load_provinces_geojson

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


def _flatten_coordinates(coords):
    result = []
    if not coords:
        return result
    if isinstance(coords[0], (int, float)):
        return [coords]
    for item in coords:
        result.extend(_flatten_coordinates(item))
    return result
