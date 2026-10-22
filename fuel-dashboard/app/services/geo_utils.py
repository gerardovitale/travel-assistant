from typing import Sequence

import numpy as np

# Padding (degrees) for the bounding-box prefilter. Far above float64 rounding error on the edge
# intersection, far below any real distance, so the prefilter never changes a ray-casting result.
_BBOX_EPS = 1e-9
# Caps the (points x edges) matrix of one ray-casting pass (~1M cells, ~8 MB of float64 per temporary)
# so large districts stay cheap on the Raspberry Pi.
_MAX_CELLS = 1_000_000


def point_in_polygon(lat: float, lon: float, polygon: Sequence[Sequence[float]]) -> bool:
    """Ray-casting algorithm. *polygon* is a list of [lon, lat] pairs (GeoJSON order)."""
    n = len(polygon)
    inside = False
    px, py = lon, lat
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i][0], polygon[i][1]
        xj, yj = polygon[j][0], polygon[j][1]
        if ((yi > py) != (yj > py)) and (px < (xj - xi) * (py - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def point_in_multipolygon(lat: float, lon: float, geometry: dict) -> bool:
    """Check if a point is inside a GeoJSON Polygon or MultiPolygon geometry."""
    geom_type = geometry["type"]
    coords = geometry["coordinates"]
    if geom_type == "Polygon":
        return point_in_polygon(lat, lon, coords[0])
    elif geom_type == "MultiPolygon":
        for polygon in coords:
            if point_in_polygon(lat, lon, polygon[0]):
                return True
    return False


def _outer_rings(geometry: dict) -> list[Sequence[Sequence[float]]]:
    if geometry["type"] == "Polygon":
        return [geometry["coordinates"][0]]
    if geometry["type"] == "MultiPolygon":
        return [polygon[0] for polygon in geometry["coordinates"]]
    return []


def points_in_polygon(lats: np.ndarray, lons: np.ndarray, polygon: Sequence[Sequence[float]]) -> np.ndarray:
    """Vectorized :func:`point_in_polygon`: boolean mask of which points fall inside *polygon*.

    Points outside the ring's (padded) bounding box are rejected without ray casting; the rest are
    tested against every edge at once with the same arithmetic as the scalar version.
    """
    lats = np.asarray(lats, dtype=float)
    lons = np.asarray(lons, dtype=float)
    result = np.zeros(lats.shape, dtype=bool)
    if len(polygon) == 0 or len(lats) == 0:
        return result
    # Keep only [lon, lat]; GeoJSON positions may carry an optional altitude.
    ring = np.array([(vertex[0], vertex[1]) for vertex in polygon], dtype=float)
    xs, ys = ring[:, 0], ring[:, 1]
    in_bbox = (
        (lons >= xs.min() - _BBOX_EPS)
        & (lons <= xs.max() + _BBOX_EPS)
        & (lats >= ys.min() - _BBOX_EPS)
        & (lats <= ys.max() + _BBOX_EPS)
    )
    idx = np.flatnonzero(in_bbox)
    if idx.size == 0:
        return result
    # Edge i joins vertex i to vertex i-1 (vertex n-1 for i=0), matching the scalar loop.
    xi, yi = xs[np.newaxis, :], ys[np.newaxis, :]
    xj, yj = np.roll(xs, 1)[np.newaxis, :], np.roll(ys, 1)[np.newaxis, :]
    chunk = max(1, _MAX_CELLS // len(xs))
    for start in range(0, idx.size, chunk):
        stop = start + chunk
        part = idx[start:stop]
        px, py = lons[part, np.newaxis], lats[part, np.newaxis]
        with np.errstate(divide="ignore", invalid="ignore"):
            crosses = ((yi > py) != (yj > py)) & (px < (xj - xi) * (py - yi) / (yj - yi) + xi)
        result[part] = (np.count_nonzero(crosses, axis=1) % 2) == 1
    return result


def points_in_multipolygon(lats: np.ndarray, lons: np.ndarray, geometry: dict) -> np.ndarray:
    """Vectorized :func:`point_in_multipolygon`."""
    lats = np.asarray(lats, dtype=float)
    lons = np.asarray(lons, dtype=float)
    result = np.zeros(lats.shape, dtype=bool)
    for ring in _outer_rings(geometry):
        pending = np.flatnonzero(~result)
        if pending.size == 0:
            break
        result[pending] = points_in_polygon(lats[pending], lons[pending], ring)
    return result


def locate_features(
    latitudes: Sequence[float],
    longitudes: Sequence[float],
    geojson_features: list[dict],
) -> list[str | None]:
    """Return the ``nombre`` of the first feature containing each point (``None`` if no feature does)."""
    # Pair coordinates like zip(): extra trailing values in the longer sequence are ignored.
    n = min(len(latitudes), len(longitudes))
    lats = np.asarray(latitudes, dtype=float)[:n]
    lons = np.asarray(longitudes, dtype=float)[:n]
    names: list[str | None] = [None] * len(lats)
    unassigned = np.ones(len(lats), dtype=bool)
    for feature in geojson_features:
        pending = np.flatnonzero(unassigned)
        if pending.size == 0:
            break
        hits = pending[points_in_multipolygon(lats[pending], lons[pending], feature["geometry"])]
        name = feature["properties"]["nombre"]
        for i in hits:
            names[i] = name
        unassigned[hits] = False
    return names


def assign_districts(
    latitudes: Sequence[float],
    longitudes: Sequence[float],
    prices: Sequence[float],
    geojson_features: list[dict],
) -> dict[str, dict[str, float]]:
    """Assign stations to districts via point-in-polygon and aggregate prices.

    Returns ``{district_name: {"total_price": float, "count": int}}``.
    """
    result: dict[str, dict[str, float]] = {}
    for name, price in zip(locate_features(latitudes, longitudes, geojson_features), prices):
        if name is None:
            continue
        if name not in result:
            result[name] = {"total_price": 0.0, "count": 0}
        result[name]["total_price"] += price
        result[name]["count"] += 1
    return result
