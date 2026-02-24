from typing import List

import plotly.graph_objects as go
from api.schemas import TrendPoint
from api.schemas import ZoneResult


def build_trend_chart(trend_data: List[TrendPoint], fuel_type: str, zip_code: str) -> go.Figure:
    dates = [p.date for p in trend_data]
    avg_prices = [p.avg_price for p in trend_data]
    min_prices = [p.min_price for p in trend_data]
    max_prices = [p.max_price for p in trend_data]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates, y=max_prices, mode="lines", name="Max", line=dict(color="rgba(255,0,0,0.3)")))
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=min_prices,
            mode="lines",
            name="Min",
            line=dict(color="rgba(0,128,0,0.3)"),
            fill="tonexty",
            fillcolor="rgba(200,200,200,0.2)",
        )
    )
    fig.add_trace(go.Scatter(x=dates, y=avg_prices, mode="lines+markers", name="Average", line=dict(color="blue")))
    fig.update_layout(
        title=f"Price Trend — {fuel_type} in {zip_code}",
        xaxis_title="Date",
        yaxis_title="Price (EUR/L)",
        template="plotly_white",
        height=400,
    )
    return fig


def build_zone_bar_chart(zones: List[ZoneResult], province: str, fuel_type: str) -> go.Figure:
    zip_codes = [z.zip_code for z in zones]
    avg_prices = [z.avg_price for z in zones]

    fig = go.Figure()
    fig.add_trace(go.Bar(x=avg_prices, y=zip_codes, orientation="h", marker_color="steelblue"))
    fig.update_layout(
        title=f"Avg Price by Zip Code — {fuel_type} in {province}",
        xaxis_title="Avg Price (EUR/L)",
        yaxis_title="Zip Code",
        template="plotly_white",
        height=max(300, len(zip_codes) * 25),
        yaxis=dict(autorange="reversed"),
    )
    return fig
