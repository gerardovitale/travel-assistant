from typing import List

import plotly.graph_objects as go
from api.schemas import TrendPoint
from api.schemas import ZoneResult
from ui.view_models import fuel_label


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


def build_zone_bar_chart(zones: List[ZoneResult], province: str, fuel_type: str) -> go.Figure:
    zip_codes = [z.zip_code for z in zones]
    avg_prices = [z.avg_price for z in zones]
    min_prices = [z.min_price for z in zones]
    station_counts = [z.station_count for z in zones]
    fuel_name = fuel_label(fuel_type)

    if avg_prices:
        min_index = avg_prices.index(min(avg_prices))
    else:
        min_index = 0
    colors = ["#0f766e" if idx == min_index else "#4f46e5" for idx in range(len(zip_codes))]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=avg_prices,
            y=zip_codes,
            orientation="h",
            marker_color=colors,
            text=[f"{value:.3f}" for value in avg_prices],
            textposition="outside",
            customdata=list(zip(min_prices, station_counts)),
            hovertemplate=(
                "CP %{y}<br>Promedio: %{x:.3f} EUR/L<br>"
                "Minimo: %{customdata[0]:.3f} EUR/L<br>"
                "Estaciones: %{customdata[1]}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        title=f"Promedio por codigo postal: {fuel_name} - {province}",
        xaxis_title="Precio promedio (EUR/L)",
        yaxis_title="Codigo postal",
        template="plotly_white",
        height=max(320, len(zip_codes) * 30),
        yaxis=dict(autorange="reversed"),
        margin=dict(l=70, r=40, t=70, b=50),
    )
    fig.update_xaxes(tickformat=".3f")
    return fig
