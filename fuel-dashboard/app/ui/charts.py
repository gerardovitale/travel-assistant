import logging
from typing import List

import plotly.graph_objects as go
from api.schemas import DistrictPriceResult
from api.schemas import ProvincePriceResult
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


def _flatten_coordinates(coords):
    result = []
    if not coords:
        return result
    if isinstance(coords[0], (int, float)):
        return [coords]
    for item in coords:
        result.extend(_flatten_coordinates(item))
    return result
