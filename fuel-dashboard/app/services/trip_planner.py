import logging
import math
from typing import Callable
from typing import List
from typing import Optional
from typing import Tuple

from api.schemas import AlternativePlan
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


def _find_stops_with_strategy(
    stations: List[dict],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    pick_fn: Callable[[List[dict], float, float], dict],
    safety: float = 0.15,
    reason_template: str = "Mas barata de {n} candidatas en ventana km {a:.0f}-{b:.0f}",
) -> List[dict]:
    """Greedy stop selection with pluggable candidate picker.

    Args:
        stations: list of dicts with 'route_km', 'price', 'label', etc.
        total_km: total route distance in km.
        tank_liters: full tank capacity in liters.
        consumption_lper100km: fuel consumption in L/100km.
        fuel_pct: initial fuel level as percentage (0-100).
        pick_fn: callable(candidates, window_start_km, window_end_km) -> best candidate dict.
        safety: safety margin fraction (default 15%).

    Returns:
        list of stop dicts with added 'fuel_at_arrival_pct', 'liters_to_fill', 'cost_eur', 'reasoning'.
    """
    if not stations:
        return []

    max_range_km = (tank_liters / consumption_lper100km) * 100
    current_range_km = max_range_km * (fuel_pct / 100)
    usable_range_km = max_range_km * (1 - safety)
    current_km = 0.0
    stops = []
    used_labels = set()

    sorted_stations = sorted(stations, key=lambda s: s["route_km"])

    while current_km + current_range_km < total_km:
        effective_range = current_range_km - max_range_km * safety
        if effective_range <= 0:
            effective_range = current_range_km * 0.9

        window_start = current_km
        window_end = current_km + effective_range

        candidates = [
            s for s in sorted_stations if window_start < s["route_km"] <= window_end and s["label"] not in used_labels
        ]

        if not candidates:
            logger.warning(
                "No fuel station reachable at km %.1f with range %.1f km",
                current_km,
                effective_range,
            )
            break

        best = pick_fn(candidates, window_start, window_end)

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
        stop["reasoning"] = reason_template.format(n=len(candidates), a=window_start, b=window_end)
        stops.append(stop)
        used_labels.add(best["label"])

        current_km = best["route_km"]
        current_range_km = usable_range_km

    return stops


def _pick_cheapest(candidates: List[dict], window_start: float, window_end: float) -> dict:
    return min(candidates, key=lambda s: s["price"])


def _pick_min_stops(candidates: List[dict], window_start: float, window_end: float) -> dict:
    """Pick cheapest in the last third of the window to push stops farther apart."""
    far_threshold = window_start + (window_end - window_start) * 2 / 3
    far_candidates = [s for s in candidates if s["route_km"] >= far_threshold]
    pool = far_candidates if far_candidates else candidates
    return min(pool, key=lambda s: s["price"])


def _pick_min_detour(candidates: List[dict], window_start: float, window_end: float) -> dict:
    """Pick by minimum detour first, then by price as tiebreaker."""
    return min(candidates, key=lambda s: (s.get("detour_minutes", 0), s["price"]))


def find_optimal_stops(
    stations: List[dict],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    safety: float = 0.15,
) -> List[dict]:
    """Greedy cheapest-in-reachable-window stop selection."""
    return _find_stops_with_strategy(
        stations,
        total_km,
        tank_liters,
        consumption_lper100km,
        fuel_pct,
        _pick_cheapest,
        safety,
        reason_template="Mas barata de {n} candidatas en ventana km {a:.0f}-{b:.0f}",
    )


def find_min_stops(
    stations: List[dict],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    safety: float = 0.15,
) -> List[dict]:
    """Fewer stops by picking stations in the far zone of each window."""
    return _find_stops_with_strategy(
        stations,
        total_km,
        tank_liters,
        consumption_lper100km,
        fuel_pct,
        _pick_min_stops,
        safety,
        reason_template="Mas lejana y barata de {n} candidatas en ventana km {a:.0f}-{b:.0f}",
    )


def find_min_detour(
    stations: List[dict],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    safety: float = 0.15,
) -> List[dict]:
    """Prefer on-route stations with minimal detour."""
    return _find_stops_with_strategy(
        stations,
        total_km,
        tank_liters,
        consumption_lper100km,
        fuel_pct,
        _pick_min_detour,
        safety,
        reason_template="Menor desvio de {n} candidatas en ventana km {a:.0f}-{b:.0f}",
    )


def _fuel_at_destination_pct(
    stops: List[TripStop],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_level_pct: float,
) -> float:
    """Estimate the fuel level (%) when arriving at the destination."""
    if stops:
        last_km = stops[-1].route_km
        remaining_km = total_km - last_km
        fuel_used = remaining_km * consumption_lper100km / 100
        fuel_remaining = tank_liters - fuel_used
    else:
        fuel_used = total_km * consumption_lper100km / 100
        fuel_remaining = tank_liters * (fuel_level_pct / 100) - fuel_used
    return round(max(0, fuel_remaining / tank_liters * 100), 1)


def _build_trip_stop(stop_dict: dict) -> TripStop:
    """Build a TripStop from a stop dict returned by the strategy functions."""
    return TripStop(
        station=StationResult(
            label=stop_dict["label"],
            address=stop_dict["address"],
            municipality=stop_dict["municipality"],
            province=stop_dict["province"],
            zip_code=stop_dict["zip_code"],
            latitude=stop_dict["latitude"],
            longitude=stop_dict["longitude"],
            price=stop_dict["price"],
            distance_km=stop_dict.get("min_distance_km"),
        ),
        route_km=stop_dict["route_km"],
        detour_minutes=stop_dict.get("detour_minutes", 0),
        fuel_at_arrival_pct=stop_dict["fuel_at_arrival_pct"],
        liters_to_fill=stop_dict["liters_to_fill"],
        cost_eur=stop_dict["cost_eur"],
        reasoning=stop_dict.get("reasoning"),
    )


def plan_trip(
    origin_address: str,
    destination_address: str,
    fuel_type: str,
    consumption_lper100km: float,
    tank_liters: float,
    fuel_level_pct: float,
    max_detour_minutes: float,
    labels: Optional[List[str]] = None,
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
    stations_df = query_stations_along_corridor(waypoints, fuel_type, corridor_km, labels=labels)

    if stations_df.empty:
        # No stations found — return plan with no stops
        dest_fuel = _fuel_at_destination_pct([], total_km, tank_liters, consumption_lper100km, fuel_level_pct)
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
            fuel_at_destination_pct=dest_fuel,
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
                route_km=row["route_km"],
                detour_minutes=row["detour_minutes"],
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
    trip_stops = [_build_trip_stop(stop) for stop in optimal_stops]

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

    # 9. Generate alternative plans
    recommended_labels = {s.station.label for s in trip_stops}
    stop_args = (station_dicts, total_km, tank_liters, consumption_lper100km, fuel_level_pct)
    alternative_strategies = [
        ("Menos paradas", "Prioriza estaciones lejanas para reducir el numero de paradas", find_min_stops),
        ("Menor desvio", "Prioriza estaciones cercanas a la ruta para minimizar desvios", find_min_detour),
    ]

    alternative_plans = []
    for name, description, strategy_fn in alternative_strategies:
        alt_stops_raw = strategy_fn(*stop_args)
        alt_trip_stops = [_build_trip_stop(s) for s in alt_stops_raw]
        alt_labels = {s.station.label for s in alt_trip_stops}
        if alt_labels == recommended_labels:
            continue
        alt_cost = sum(s.cost_eur for s in alt_trip_stops)
        alt_liters = sum(s.liters_to_fill for s in alt_trip_stops)
        alt_detour = sum(s.detour_minutes for s in alt_trip_stops)
        alt_dest_fuel = _fuel_at_destination_pct(
            alt_trip_stops, total_km, tank_liters, consumption_lper100km, fuel_level_pct
        )
        alternative_plans.append(
            AlternativePlan(
                strategy_name=name,
                strategy_description=description,
                stops=alt_trip_stops,
                total_fuel_cost=round(alt_cost, 2),
                total_fuel_liters=round(alt_liters, 1),
                total_detour_minutes=round(alt_detour, 1),
                fuel_at_destination_pct=alt_dest_fuel,
            )
        )

    dest_fuel = _fuel_at_destination_pct(trip_stops, total_km, tank_liters, consumption_lper100km, fuel_level_pct)

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
        fuel_at_destination_pct=dest_fuel,
        alternative_plans=alternative_plans,
    )
