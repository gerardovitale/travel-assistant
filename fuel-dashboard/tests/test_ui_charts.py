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
    fig, stations_idx, highlight_idx = build_station_map(stations, 40.42, -3.70, "28001")
    # stations trace + highlight trace + search marker trace
    assert len(fig.data) == 3
    assert fig.data[0].name == "Estaciones"
    assert fig.data[1].name == "Seleccion"
    assert fig.data[2].name == "Ubicacion buscada"
    assert stations_idx == 0
    assert highlight_idx == 1


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
    fig, stations_idx, highlight_idx = build_station_map(stations, 40.42, -3.70, "28001", zip_boundary=boundary)
    # boundary trace + stations trace + highlight trace + search marker trace
    assert len(fig.data) == 4
    assert fig.data[0].name == "Zona CP"
    assert fig.data[0].fill == "toself"
    assert stations_idx == 1
    assert highlight_idx == 2


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
    fig, stations_idx, highlight_idx = build_station_map(stations, 41.38, 2.17, "08001", zip_boundary=boundary)
    # 2 boundary traces + stations trace + highlight trace + search marker trace
    assert len(fig.data) == 5
    assert fig.data[0].name == "Zona CP"
    assert fig.data[1].name == "Zona CP"
    assert stations_idx == 2
    assert highlight_idx == 3
