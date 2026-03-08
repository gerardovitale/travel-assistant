import math
from typing import Dict
from typing import List
from typing import Sequence


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance in km between two points on Earth."""
    R = 6371.0
    lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
    lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)

    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return R * c


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


def assign_districts(
    latitudes: Sequence[float],
    longitudes: Sequence[float],
    prices: Sequence[float],
    geojson_features: List[dict],
) -> Dict[str, Dict[str, float]]:
    """Assign stations to districts via point-in-polygon and aggregate prices.

    Returns ``{district_name: {"total_price": float, "count": int}}``.
    """
    result: Dict[str, Dict[str, float]] = {}
    for lat, lon, price in zip(latitudes, longitudes, prices):
        for feature in geojson_features:
            name = feature["properties"]["nombre"]
            if point_in_multipolygon(lat, lon, feature["geometry"]):
                if name not in result:
                    result[name] = {"total_price": 0.0, "count": 0}
                result[name]["total_price"] += price
                result[name]["count"] += 1
                break
    return result
