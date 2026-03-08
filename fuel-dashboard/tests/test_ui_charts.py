import importlib.util

import pytest
from api.schemas import TrendPoint

PLOTLY_AVAILABLE = importlib.util.find_spec("plotly") is not None
pytestmark = pytest.mark.skipif(not PLOTLY_AVAILABLE, reason="plotly not installed")


def test_build_trend_chart_uses_spanish_labels():
    from ui.charts import build_trend_chart

    trend = [
        TrendPoint(date="2025-01-01", avg_price=1.60, min_price=1.55, max_price=1.65),
        TrendPoint(date="2025-01-02", avg_price=1.56, min_price=1.50, max_price=1.61),
    ]
    fig = build_trend_chart(trend, "diesel_a_price", "28001")
    assert "Evolucion de precios" in fig.layout.title.text
    assert fig.layout.xaxis.title.text == "Fecha"
    assert fig.layout.yaxis.title.text == "Precio (EUR/L)"
    assert len(fig.data) == 3
    assert fig.data[2].name == "Promedio"
