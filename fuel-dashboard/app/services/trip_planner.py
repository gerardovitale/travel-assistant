import logging
import math
from typing import List
from typing import Tuple

from api.schemas import StationResult
from api.schemas import TripPlan
from api.schemas import TripStop
from services.geocoding import geocode_address
from services.routing import get_full_route

from data.duckdb_engine import query_stations_along_corridor

logger = logging.getLogger(__name__)


def sample_route_waypoints(
    route_coords: List[List[float]],
    interval_km: float = 10.0,
) -> List[Tuple[float, float, float]]:
    """Sample waypoints along a route at regular intervals.

    Args:
        route_coords: list of [lon, lat] pairs from OSRM.
        interval_km: distance between sampled waypoints.

    Returns:
        list of (lat, lon, cumulative_km) tuples.
    """
    if not route_coords:
        return []

    waypoints = [(route_coords[0][1], route_coords[0][0], 0.0)]
    cumulative = 0.0

    for i in range(1, len(route_coords)):
        lat1 = math.radians(route_coords[i - 1][1])
        lon1 = math.radians(route_coords[i - 1][0])
        lat2 = math.radians(route_coords[i][1])
        lon2 = math.radians(route_coords[i][0])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        segment_km = 2 * 6371 * math.asin(math.sqrt(a))
        cumulative += segment_km

        if cumulative - waypoints[-1][2] >= interval_km:
            waypoints.append((route_coords[i][1], route_coords[i][0], round(cumulative, 2)))

    # Always include the last point
    last = route_coords[-1]
    if waypoints[-1] != (last[1], last[0], round(cumulative, 2)):
        waypoints.append((last[1], last[0], round(cumulative, 2)))

    return waypoints


def project_stations_onto_route(
    stations_df,
    waypoints: List[Tuple[float, float, float]],
) -> None:
    """Add route_km column to stations_df based on closest waypoint's cumulative km.

    Modifies stations_df in place.
    """
    if stations_df.empty or not waypoints:
        return
    stations_df["route_km"] = stations_df["closest_waypoint_idx"].apply(
        lambda idx: waypoints[min(idx, len(waypoints) - 1)][2]
    )


def find_optimal_stops(
    stations: List[dict],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    safety: float = 0.15,
) -> List[dict]:
    """Greedy cheapest-in-reachable-window stop selection.

    Args:
        stations: list of dicts with 'route_km', 'price', 'label', etc.
        total_km: total route distance in km.
        tank_liters: full tank capacity in liters.
        consumption_lper100km: fuel consumption in L/100km.
        fuel_pct: initial fuel level as percentage (0-100).
        safety: safety margin fraction (default 15%).

    Returns:
        list of stop dicts with added 'fuel_at_arrival_pct', 'liters_to_fill', 'cost_eur'.
    """
    if not stations:
        return []

    max_range_km = (tank_liters / consumption_lper100km) * 100
    current_range_km = max_range_km * (fuel_pct / 100)
    usable_range_km = max_range_km * (1 - safety)
    current_km = 0.0
    stops = []
    used_labels = set()

    # Sort stations by route_km
    sorted_stations = sorted(stations, key=lambda s: s["route_km"])

    while current_km + current_range_km < total_km:
        # Find candidates within reachable range (minus safety margin)
        effective_range = current_range_km - max_range_km * safety
        if effective_range <= 0:
            # Can't even reach the safety margin — pick the nearest station
            effective_range = current_range_km * 0.9

        candidates = [
            s
            for s in sorted_stations
            if current_km < s["route_km"] <= current_km + effective_range and s["label"] not in used_labels
        ]

        if not candidates:
            # No candidate in range — trip may not be feasible, break to avoid infinite loop
            logger.warning(
                "No fuel station reachable at km %.1f with range %.1f km",
                current_km,
                effective_range,
            )
            break

        # Pick cheapest
        best = min(candidates, key=lambda s: s["price"])

        # Calculate fuel state
        km_driven = best["route_km"] - current_km
        fuel_used_liters = km_driven * consumption_lper100km / 100
        fuel_remaining_liters = (current_range_km / max_range_km) * tank_liters - fuel_used_liters
        fuel_at_arrival_pct = max(0, (fuel_remaining_liters / tank_liters) * 100)
        liters_to_fill = tank_liters - fuel_remaining_liters
        cost_eur = liters_to_fill * best["price"]

        stop = dict(best)
        stop["fuel_at_arrival_pct"] = round(fuel_at_arrival_pct, 1)
        stop["liters_to_fill"] = round(max(0, liters_to_fill), 1)
        stop["cost_eur"] = round(cost_eur, 2)
        stops.append(stop)
        used_labels.add(best["label"])

        # After filling up: full tank
        current_km = best["route_km"]
        current_range_km = usable_range_km

    return stops


def plan_trip(
    origin_address: str,
    destination_address: str,
    fuel_type: str,
    consumption_lper100km: float,
    tank_liters: float,
    fuel_level_pct: float,
    max_detour_minutes: float,
) -> TripPlan:
    """Main trip planning orchestrator.

    Raises ValueError for invalid inputs or geocoding failures.
    """
    # 1. Geocode
    origin_coords = geocode_address(origin_address)
    if origin_coords is None:
        raise ValueError(f"No se pudo geocodificar el origen: {origin_address}")

    dest_coords = geocode_address(destination_address)
    if dest_coords is None:
        raise ValueError(f"No se pudo geocodificar el destino: {destination_address}")

    # 2. Get full route
    route = get_full_route(origin_coords, dest_coords)
    if route is None:
        raise ValueError("No se pudo obtener la ruta entre origen y destino.")

    route_coords = route["coordinates"]
    total_km = route["distance_km"]
    duration_min = route["duration_minutes"]

    # 3. Sample waypoints
    waypoints = sample_route_waypoints(route_coords, interval_km=10)

    # 4. Query stations along corridor
    corridor_km = max_detour_minutes * 1.5
    stations_df = query_stations_along_corridor(waypoints, fuel_type, corridor_km)

    if stations_df.empty:
        # No stations found — return plan with no stops
        return TripPlan(
            stops=[],
            total_fuel_cost=0,
            total_distance_km=total_km,
            duration_minutes=duration_min,
            total_fuel_liters=0,
            savings_eur=0,
            route_coordinates=route_coords,
            candidate_stations=[],
            origin_coords=list(origin_coords),
            destination_coords=list(dest_coords),
        )

    # 5. Project stations onto route
    project_stations_onto_route(stations_df, waypoints)

    # 6. Estimate detour time from Haversine distance to route
    # Round-trip off-route at ~60 km/h average urban speed → min per km ≈ 2 min/km
    avg_detour_min_per_km = 2.0
    stations_df["detour_minutes"] = (stations_df["min_distance_km"] * avg_detour_min_per_km).round(1)

    # Filter by max detour
    filtered = stations_df[stations_df["detour_minutes"] <= max_detour_minutes].copy()

    # Build candidate list and station dicts in a single pass
    candidate_stations = []
    station_dicts = []
    for _, row in filtered.iterrows():
        candidate_stations.append(
            StationResult(
                label=row["label"],
                address=row["address"],
                municipality=row["municipality"],
                province=row["province"],
                zip_code=str(row["zip_code"]),
                latitude=row["latitude"],
                longitude=row["longitude"],
                price=row[fuel_type],
                distance_km=row.get("min_distance_km"),
            )
        )
        station_dicts.append(
            {
                "label": row["label"],
                "address": row["address"],
                "municipality": row["municipality"],
                "province": row["province"],
                "zip_code": str(row["zip_code"]),
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "price": row[fuel_type],
                "route_km": row["route_km"],
                "detour_minutes": row["detour_minutes"],
                "min_distance_km": row.get("min_distance_km", 0),
            }
        )

    # 7. Greedy stop selection

    optimal_stops = find_optimal_stops(
        station_dicts,
        total_km,
        tank_liters,
        consumption_lper100km,
        fuel_level_pct,
    )

    # 8. Build TripStop list
    trip_stops = []
    for stop in optimal_stops:
        trip_stops.append(
            TripStop(
                station=StationResult(
                    label=stop["label"],
                    address=stop["address"],
                    municipality=stop["municipality"],
                    province=stop["province"],
                    zip_code=stop["zip_code"],
                    latitude=stop["latitude"],
                    longitude=stop["longitude"],
                    price=stop["price"],
                    distance_km=stop.get("min_distance_km"),
                ),
                route_km=stop["route_km"],
                detour_minutes=stop.get("detour_minutes", 0),
                fuel_at_arrival_pct=stop["fuel_at_arrival_pct"],
                liters_to_fill=stop["liters_to_fill"],
                cost_eur=stop["cost_eur"],
            )
        )

    total_fuel_cost = sum(s.cost_eur for s in trip_stops)
    total_fuel_liters = sum(s.liters_to_fill for s in trip_stops)

    # Baseline savings estimate: compare with median price
    if candidate_stations:
        prices = sorted(c.price for c in candidate_stations)
        median_price = prices[len(prices) // 2]
        baseline_cost = total_fuel_liters * median_price
        savings = max(0, round(baseline_cost - total_fuel_cost, 2))
    else:
        savings = 0

    return TripPlan(
        stops=trip_stops,
        total_fuel_cost=round(total_fuel_cost, 2),
        total_distance_km=total_km,
        duration_minutes=duration_min,
        total_fuel_liters=round(total_fuel_liters, 1),
        savings_eur=savings,
        route_coordinates=route_coords,
        candidate_stations=candidate_stations,
        origin_coords=list(origin_coords),
        destination_coords=list(dest_coords),
    )
