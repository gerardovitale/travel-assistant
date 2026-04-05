import importlib.util

import pytest
from api.schemas import StationResult
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


def _make_stations(n=2):
    return [
        StationResult(
            label=f"Station {i}",
            address=f"Calle {i}",
            municipality="Madrid",
            province="Madrid",
            zip_code="28001",
            latitude=40.42 + i * 0.01,
            longitude=-3.70 + i * 0.01,
            price=1.50 + i * 0.05,
        )
        for i in range(n)
    ]


def test_build_station_map_without_boundary():
    from ui.charts import build_station_map

    stations = _make_stations()
    fig, stations_idx, highlight_idx, route_idx = build_station_map(stations, 40.42, -3.70, "28001")
    # route trace + stations trace + highlight trace + search marker trace
    assert len(fig.data) == 4
    assert fig.data[0].name == "Ruta"
    assert fig.data[1].name == "Estaciones"
    assert fig.data[2].name == "Seleccion"
    assert fig.data[3].name == "Ubicacion buscada"
    assert route_idx == 0
    assert stations_idx == 1
    assert highlight_idx == 2


def test_build_station_map_with_polygon_boundary():
    from ui.charts import build_station_map

    stations = _make_stations()
    boundary = {
        "type": "Feature",
        "properties": {"COD_POSTAL": "28001"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-3.72, 40.40], [-3.68, 40.40], [-3.68, 40.44], [-3.72, 40.44], [-3.72, 40.40]]],
        },
    }
    fig, stations_idx, highlight_idx, route_idx = build_station_map(
        stations, 40.42, -3.70, "28001", zip_boundary=boundary
    )
    # boundary trace + route trace + stations trace + highlight trace + search marker trace
    assert len(fig.data) == 5
    assert fig.data[0].name == "Zona CP"
    assert fig.data[0].fill == "toself"
    assert route_idx == 1
    assert stations_idx == 2
    assert highlight_idx == 3


def test_build_station_map_with_multipolygon_boundary():
    from ui.charts import build_station_map

    stations = _make_stations()
    boundary = {
        "type": "Feature",
        "properties": {"COD_POSTAL": "08001"},
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[2.16, 41.38], [2.17, 41.38], [2.17, 41.39], [2.16, 41.39], [2.16, 41.38]]],
                [[[2.18, 41.38], [2.19, 41.38], [2.19, 41.39], [2.18, 41.39], [2.18, 41.38]]],
            ],
        },
    }
    fig, stations_idx, highlight_idx, route_idx = build_station_map(
        stations, 41.38, 2.17, "08001", zip_boundary=boundary
    )
    # 2 boundary traces + route trace + stations trace + highlight trace + search marker trace
    assert len(fig.data) == 6
    assert fig.data[0].name == "Zona CP"
    assert fig.data[1].name == "Zona CP"
    assert route_idx == 2
    assert stations_idx == 3
    assert highlight_idx == 4


def test_build_ingestion_stats_chart():
    import pandas as pd

    from ui.charts import build_ingestion_stats_chart

    df = pd.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02"],
            "record_count": [10000, 10100],
            "unique_stations": [8000, 8050],
            "unique_provinces": [52, 52],
            "unique_municipalities": [3000, 3010],
            "unique_localities": [4000, 4020],
        }
    )
    fig = build_ingestion_stats_chart(df)
    assert len(fig.data) == 5
    assert fig.layout.title.text == "Metricas de ingestion diaria"
    assert fig.data[0].name == "Registros"
    assert fig.data[4].name == "Localidades"


def test_build_brand_trend_chart_traces_per_brand():
    import pandas as pd

    from ui.charts import build_brand_trend_chart

    df = pd.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02", "2026-01-01", "2026-01-02"],
            "brand": ["repsol", "repsol", "shell", "shell"],
            "avg_price": [1.42, 1.43, 1.48, 1.47],
        }
    )
    fig = build_brand_trend_chart(df, "gasoline_95_e5_price")
    assert len(fig.data) == 2
    assert "Evolucion de precios por marca" in fig.layout.title.text
    brand_names = {trace.name for trace in fig.data}
    assert "Repsol" in brand_names
    assert "Shell" in brand_names


def test_build_brand_trend_chart_empty_df():
    import pandas as pd

    from ui.charts import build_brand_trend_chart

    fig = build_brand_trend_chart(pd.DataFrame(columns=["date", "brand", "avg_price"]), "diesel_a_price")
    assert len(fig.data) == 0
    assert "Sin datos" in fig.layout.title.text


def test_build_station_map_route_trace_is_lines():
    from ui.charts import build_station_map

    stations = _make_stations()
    fig, _, _, route_idx = build_station_map(stations, 40.42, -3.70, "28001")
    route_trace = fig.data[route_idx]
    assert route_trace.mode == "lines"
    assert route_trace.line.color == "#6366f1"
    assert route_trace.line.width == 4


def test_build_spread_trend_chart():
    from ui.view_models import DailySpread

    from ui.charts import build_spread_trend_chart

    spreads = [
        DailySpread(date="2025-01-01", spread=0.10, max_variant="diesel_premium_price", min_variant="diesel_a_price"),
        DailySpread(date="2025-01-02", spread=0.12, max_variant="diesel_premium_price", min_variant="diesel_a_price"),
    ]
    fig = build_spread_trend_chart(spreads, "diesel", "28001")
    assert "Diferencia premium" in fig.layout.title.text
    assert len(fig.data) == 1
    assert fig.layout.shapes is not None or hasattr(fig.layout, "_props")


def test_build_monthly_spread_chart():
    import pandas as pd

    from ui.charts import build_monthly_spread_chart

    monthly_df = pd.DataFrame(
        {
            "month": ["2025-01", "2025-02", "2025-03"],
            "avg_spread": [0.10, 0.15, 0.12],
            "min_spread": [0.08, 0.10, 0.09],
            "max_spread": [0.12, 0.18, 0.14],
        }
    )
    fig = build_monthly_spread_chart(monthly_df, "diesel", "28001")
    assert "Patron mensual" in fig.layout.title.text
    assert len(fig.data) == 1
    assert len(fig.data[0].x) == 3
    colors = list(fig.data[0].marker.color)
    assert colors[1] == "#dc2626"
    assert colors[0] == "#2563eb"
