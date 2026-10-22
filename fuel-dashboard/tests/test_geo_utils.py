import numpy as np
import services.geo_utils as geo_utils
from services.geo_utils import assign_districts
from services.geo_utils import locate_features
from services.geo_utils import point_in_multipolygon
from services.geo_utils import point_in_polygon
from services.geo_utils import points_in_multipolygon
from services.geo_utils import points_in_polygon

from data.geojson_loader import load_madrid_districts


def test_point_inside_polygon():
    # Simple square polygon around (0, 0)
    polygon = [[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]
    assert point_in_polygon(0.0, 0.0, polygon) is True


def test_point_outside_polygon():
    polygon = [[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]
    assert point_in_polygon(5.0, 5.0, polygon) is False


def test_points_in_polygon_matches_scalar_on_square():
    polygon = [[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]
    lats = np.array([0.0, 5.0, 0.5, -0.99, 1.0, -1.0])
    lons = np.array([0.0, 5.0, -0.5, 0.99, 0.0, 0.0])
    expected = [point_in_polygon(lat, lon, polygon) for lat, lon in zip(lats, lons)]
    assert points_in_polygon(lats, lons, polygon).tolist() == expected


def test_points_in_polygon_empty_inputs():
    polygon = [[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]
    assert points_in_polygon(np.array([]), np.array([]), polygon).tolist() == []
    assert points_in_polygon(np.array([0.0]), np.array([0.0]), []).tolist() == [False]


def test_points_in_multipolygon_matches_scalar_on_madrid_districts():
    features = load_madrid_districts()["features"]
    rng = np.random.default_rng(0)
    lats = rng.uniform(40.30, 40.55, 300)
    lons = rng.uniform(-3.85, -3.55, 300)
    for feature in features:
        expected = [point_in_multipolygon(lat, lon, feature["geometry"]) for lat, lon in zip(lats, lons)]
        assert points_in_multipolygon(lats, lons, feature["geometry"]).tolist() == expected


def _assign_districts_scalar(lats, lons, prices, features):
    result = {}
    for lat, lon, price in zip(lats, lons, prices):
        for feature in features:
            name = feature["properties"]["nombre"]
            if point_in_multipolygon(lat, lon, feature["geometry"]):
                if name not in result:
                    result[name] = {"total_price": 0.0, "count": 0}
                result[name]["total_price"] += price
                result[name]["count"] += 1
                break
    return result


def test_assign_districts_matches_scalar_reference():
    features = load_madrid_districts()["features"]
    rng = np.random.default_rng(1)
    lats = rng.uniform(40.30, 40.55, 300).tolist()
    lons = rng.uniform(-3.85, -3.55, 300).tolist()
    prices = rng.uniform(1.3, 1.9, 300).tolist()
    result = assign_districts(lats, lons, prices, features)
    expected = _assign_districts_scalar(lats, lons, prices, features)
    assert list(result.items()) == list(expected.items())


def test_locate_features_uses_first_matching_feature():
    square = {"type": "Polygon", "coordinates": [[[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]]}
    features = [
        {"properties": {"nombre": "first"}, "geometry": square},
        {"properties": {"nombre": "second"}, "geometry": square},
    ]
    assert locate_features([0.0, 5.0], [0.0, 5.0], features) == ["first", None]


def test_points_in_polygon_accepts_mixed_2d_and_3d_positions():
    polygon = [[-1, -1], [1, -1, 650.0], [1, 1], [-1, 1, 640.0], [-1, -1]]
    assert points_in_polygon(np.array([0.0, 5.0]), np.array([0.0, 5.0]), polygon).tolist() == [True, False]


def test_points_in_polygon_chunked_matches_unchunked(monkeypatch):
    features = load_madrid_districts()["features"]
    rng = np.random.default_rng(2)
    lats = rng.uniform(40.30, 40.55, 300)
    lons = rng.uniform(-3.85, -3.55, 300)
    ring = max((feature["geometry"] for feature in features), key=lambda g: len(str(g)))
    expected = points_in_multipolygon(lats, lons, ring)
    monkeypatch.setattr(geo_utils, "_MAX_CELLS", 5000)
    assert points_in_multipolygon(lats, lons, ring).tolist() == expected.tolist()


def test_locate_features_pairs_coordinates_like_zip():
    square = {"type": "Polygon", "coordinates": [[[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]]}
    features = [{"properties": {"nombre": "only"}, "geometry": square}]
    assert locate_features([0.0, 5.0, 0.0], [0.0, 5.0], features) == ["only", None]
