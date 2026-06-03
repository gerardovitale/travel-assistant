import logging
import math
from typing import Any
from typing import Callable

from api.schemas import AlternativePlan
from api.schemas import StationResult
from api.schemas import TripPlan
from api.schemas import TripStop
from services.geocoding import geocode_address
from services.routing import get_full_route

from data.duckdb_engine import query_stations_along_corridor

logger = logging.getLogger(__name__)


def sample_route_waypoints(
    route_coords: list[list[float]],
    interval_km: float = 10.0,
) -> list[tuple[float, float, float]]:
    """Sample route_coords (OSRM [lon,lat] pairs) at interval_km; return list of (lat, lon, cumulative_km)."""
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

    last = route_coords[-1]
    if waypoints[-1] != (last[1], last[0], round(cumulative, 2)):
        waypoints.append((last[1], last[0], round(cumulative, 2)))

    return waypoints


def project_stations_onto_route(
    stations_df,
    waypoints: list[tuple[float, float, float]],
) -> None:
    """Add route_km column to stations_df in place based on closest waypoint cumulative km."""
    if stations_df.empty or not waypoints:
        return
    stations_df["route_km"] = stations_df["closest_waypoint_idx"].apply(
        lambda idx: waypoints[min(idx, len(waypoints) - 1)][2]
    )


def _compute_stop_metrics(
    price: float,
    prev_km: float,
    prev_range_km: float,
    route_km: float,
    max_range_km: float,
    tank_liters: float,
    consumption_lper100km: float,
) -> tuple[float, float, float]:
    """Return (fuel_at_arrival_pct, liters_to_fill, cost_eur) for a stop given the prior leg."""
    km_driven = route_km - prev_km
    fuel_used = km_driven * consumption_lper100km / 100
    fuel_remaining = (prev_range_km / max_range_km) * tank_liters - fuel_used
    fuel_at_arrival_pct = round(max(0.0, fuel_remaining / tank_liters * 100), 1)
    liters_to_fill = round(max(0.0, tank_liters - fuel_remaining), 1)
    cost_eur = round(liters_to_fill * price, 2)
    return fuel_at_arrival_pct, liters_to_fill, cost_eur


def _predict_arrival_fuel_pct(
    stops: list[dict[str, Any]],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
) -> float:
    """Predicted fuel level (%) at destination assuming full refill at every stop."""
    if stops:
        last_km = stops[-1]["route_km"]
        remaining_km = max(0.0, total_km - last_km)
        fuel_used = remaining_km * consumption_lper100km / 100
        fuel_remaining = tank_liters - fuel_used
    else:
        fuel_used = total_km * consumption_lper100km / 100
        fuel_remaining = tank_liters * (fuel_pct / 100) - fuel_used
    return round(max(0.0, fuel_remaining / tank_liters * 100), 1)


def _segments_feasible(
    stops: list[dict[str, Any]],
    total_km: float,
    fuel_pct: float,
    max_range_km: float,
    safety_margin_km: float,
) -> bool:
    """Every leg must fit within (range - safety_margin). First leg uses initial fuel."""
    initial_range_km = max_range_km * (fuel_pct / 100)
    usable_after_refill = max_range_km - safety_margin_km
    if not stops:
        return total_km <= initial_range_km - safety_margin_km
    if stops[0]["route_km"] > initial_range_km - safety_margin_km:
        return False
    for prev, nxt in zip(stops, stops[1:]):
        if nxt["route_km"] - prev["route_km"] > usable_after_refill:
            return False
    if total_km - stops[-1]["route_km"] > usable_after_refill:
        return False
    return True


def _recompute_stop_metrics(
    stops: list[dict[str, Any]],
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    max_range_km: float,
) -> None:
    """Refresh fuel_at_arrival_pct / liters_to_fill / cost_eur after the stop set changes.

    Mutates stops in place — each dict's fuel_at_arrival_pct, liters_to_fill,
    and cost_eur are overwritten based on the (possibly shorter) leg from the
    new previous stop.
    """
    prev_km = 0.0
    prev_range_km = max_range_km * (fuel_pct / 100)
    for s in stops:
        arrival, fill, cost = _compute_stop_metrics(
            s["price"], prev_km, prev_range_km, s["route_km"], max_range_km, tank_liters, consumption_lper100km
        )
        s["fuel_at_arrival_pct"] = arrival
        s["liters_to_fill"] = fill
        s["cost_eur"] = cost
        prev_km = s["route_km"]
        prev_range_km = max_range_km


def _prune_redundant_stops(
    stops: list[dict[str, Any]],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    max_range_km: float,
    safety_margin_km: float,
    min_fuel_at_destination_pct: float,
) -> list[dict[str, Any]]:
    """Drop any stop whose removal still leaves a feasible plan meeting the arrival floor."""
    pruned = list(stops)
    for i in range(len(pruned) - 1, -1, -1):
        trial = [s for j, s in enumerate(pruned) if j != i]
        if not _segments_feasible(trial, total_km, fuel_pct, max_range_km, safety_margin_km):
            continue
        if (
            _predict_arrival_fuel_pct(trial, total_km, tank_liters, consumption_lper100km, fuel_pct)
            < min_fuel_at_destination_pct
        ):
            continue
        pruned = trial
    if len(pruned) != len(stops):
        _recompute_stop_metrics(pruned, tank_liters, consumption_lper100km, fuel_pct, max_range_km)
    return pruned


def _append_stop(
    stops: list[dict[str, Any]],
    used_labels: set[str],
    best: dict[str, Any],
    prev_km: float,
    prev_range_km: float,
    max_range_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    reasoning: str,
) -> None:
    """Compute metrics, attach reasoning, and append the stop in place."""
    arrival, fill, cost = _compute_stop_metrics(
        best["price"], prev_km, prev_range_km, best["route_km"], max_range_km, tank_liters, consumption_lper100km
    )
    stop = dict(best)
    stop["fuel_at_arrival_pct"] = arrival
    stop["liters_to_fill"] = fill
    stop["cost_eur"] = cost
    stop["reasoning"] = reasoning
    stops.append(stop)
    used_labels.add(best["label"])


def _insert_floor_safeguard_stop(
    stops: list[dict[str, Any]],
    used_labels: set[str],
    sorted_stations: list[dict[str, Any]],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    max_range_km: float,
    safety_margin_km: float,
    min_fuel_at_destination_pct: float,
) -> None:
    """If predicted arrival is below the floor, insert one late stop close enough
    to the destination that a full refill keeps arrival >= the floor.

    Mutates `stops` and `used_labels` in place. No-op if the floor is already met
    or if no station satisfies both the reachability and reserve-window constraints.
    """
    arrival_pct = _predict_arrival_fuel_pct(stops, total_km, tank_liters, consumption_lper100km, fuel_pct)
    if arrival_pct >= min_fuel_at_destination_pct:
        return

    anchor_km = stops[-1]["route_km"] if stops else 0.0
    anchor_range_km = max_range_km if stops else max_range_km * (fuel_pct / 100)
    reach_upper = anchor_km + (anchor_range_km - safety_margin_km)
    # Station qualifies only if a full tank from there still arrives at >= floor:
    # `total_km - route_km` is the post-refill burn; cap it at the floor's km equivalent.
    max_remaining_km = max_range_km * (1 - min_fuel_at_destination_pct / 100)
    fix_candidates = [
        s
        for s in sorted_stations
        if anchor_km < s["route_km"] <= min(reach_upper, total_km)
        and total_km - s["route_km"] <= max_remaining_km
        and s["label"] not in used_labels
    ]
    if not fix_candidates:
        return

    best = max(fix_candidates, key=lambda s: (s["route_km"], -s["price"]))
    window_end_km = min(reach_upper, total_km)
    _append_stop(
        stops,
        used_labels,
        best,
        anchor_km,
        anchor_range_km,
        max_range_km,
        tank_liters,
        consumption_lper100km,
        (
            f"Reserva de llegada >= {min_fuel_at_destination_pct:.0f}% "
            f"en ventana km {anchor_km:.0f}-{window_end_km:.0f}"
        ),
    )


def _find_stops_with_strategy(
    stations: list[dict[str, Any]],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    pick_fn: Callable[[list[dict[str, Any]], float, float], dict[str, Any]],
    safety: float = 0.15,
    reason_template: str = "Mas barata de {n} candidatas en ventana km {a:.0f}-{b:.0f}",
    min_fuel_at_destination_pct: float = 0.0,
) -> list[dict[str, Any]]:
    """Greedy stop selection with pluggable picker, soft arrival-floor, and prune pass."""
    if not stations:
        return []

    max_range_km = (tank_liters / consumption_lper100km) * 100
    safety_margin_km = max_range_km * safety
    current_range_km = max_range_km * (fuel_pct / 100)
    current_km = 0.0
    stops: list[dict[str, Any]] = []
    used_labels: set[str] = set()

    sorted_stations = sorted(stations, key=lambda s: s["route_km"])

    # Greedy: refill whenever the car cannot reach destination on the current fuel.
    # Real refill = full tank (current_range_km = max_range_km after each stop).
    while current_km + current_range_km < total_km:
        effective_range = current_range_km - safety_margin_km
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
        _append_stop(
            stops,
            used_labels,
            best,
            current_km,
            current_range_km,
            max_range_km,
            tank_liters,
            consumption_lper100km,
            reason_template.format(n=len(candidates), a=window_start, b=window_end),
        )
        current_km = best["route_km"]
        current_range_km = max_range_km

    _insert_floor_safeguard_stop(
        stops,
        used_labels,
        sorted_stations,
        total_km,
        tank_liters,
        consumption_lper100km,
        fuel_pct,
        max_range_km,
        safety_margin_km,
        min_fuel_at_destination_pct,
    )

    stops = _prune_redundant_stops(
        stops,
        total_km,
        tank_liters,
        consumption_lper100km,
        fuel_pct,
        max_range_km,
        safety_margin_km,
        min_fuel_at_destination_pct,
    )

    return stops


def _pick_cheapest(candidates: list[dict], window_start: float, window_end: float) -> dict:
    return min(candidates, key=lambda s: s["price"])


def _pick_min_stops(candidates: list[dict], window_start: float, window_end: float) -> dict:
    """Pick cheapest in the last third of the window to push stops farther apart."""
    far_threshold = window_start + (window_end - window_start) * 2 / 3
    far_candidates = [s for s in candidates if s["route_km"] >= far_threshold]
    pool = far_candidates if far_candidates else candidates
    return min(pool, key=lambda s: s["price"])


def _pick_min_detour(candidates: list[dict], window_start: float, window_end: float) -> dict:
    """Pick by minimum detour first, then by price as tiebreaker."""
    return min(candidates, key=lambda s: (s.get("detour_minutes", 0), s["price"]))


def find_optimal_stops(
    stations: list[dict],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    safety: float = 0.15,
    min_fuel_at_destination_pct: float = 0.0,
) -> list[dict]:
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
        min_fuel_at_destination_pct=min_fuel_at_destination_pct,
    )


def find_min_stops(
    stations: list[dict],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    safety: float = 0.15,
    min_fuel_at_destination_pct: float = 0.0,
) -> list[dict]:
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
        min_fuel_at_destination_pct=min_fuel_at_destination_pct,
    )


def find_min_detour(
    stations: list[dict],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_pct: float,
    safety: float = 0.15,
    min_fuel_at_destination_pct: float = 0.0,
) -> list[dict]:
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
        min_fuel_at_destination_pct=min_fuel_at_destination_pct,
    )


def _fuel_at_destination_pct(
    stops: list[TripStop],
    total_km: float,
    tank_liters: float,
    consumption_lper100km: float,
    fuel_level_pct: float,
) -> float:
    """Thin adapter over _predict_arrival_fuel_pct for TripStop input."""
    stop_dicts: list[dict[str, Any]] = [{"route_km": s.route_km} for s in stops]
    return _predict_arrival_fuel_pct(stop_dicts, total_km, tank_liters, consumption_lper100km, fuel_level_pct)


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
    min_fuel_at_destination_pct: float = 50.0,
    labels: list[str] | None = None,
) -> TripPlan:
    """Main trip planning orchestrator.

    Raises ValueError for invalid inputs or geocoding failures.
    """
    origin_coords = geocode_address(origin_address)
    if origin_coords is None:
        raise ValueError(f"No se pudo geocodificar el origen: {origin_address}")

    dest_coords = geocode_address(destination_address)
    if dest_coords is None:
        raise ValueError(f"No se pudo geocodificar el destino: {destination_address}")

    route = get_full_route(origin_coords, dest_coords)
    if route is None:
        raise ValueError("No se pudo obtener la ruta entre origen y destino.")

    route_coords = route["coordinates"]
    total_km = route["distance_km"]
    duration_min = route["duration_minutes"]

    waypoints = sample_route_waypoints(route_coords, interval_km=10)

    corridor_km = max_detour_minutes * 1.5
    stations_df = query_stations_along_corridor(waypoints, fuel_type, corridor_km, labels=labels)

    if stations_df.empty:
        # No stations in corridor — min_fuel_at_destination_pct cannot be enforced;
        # return plan with computed arrival fuel so the KPI reflects reality.
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

    project_stations_onto_route(stations_df, waypoints)

    # Round-trip off-route at ~60 km/h average urban speed → min per km ≈ 2 min/km
    avg_detour_min_per_km = 2.0
    stations_df["detour_minutes"] = (stations_df["min_distance_km"] * avg_detour_min_per_km).round(1)

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

    optimal_stops = find_optimal_stops(
        station_dicts,
        total_km,
        tank_liters,
        consumption_lper100km,
        fuel_level_pct,
        min_fuel_at_destination_pct=min_fuel_at_destination_pct,
    )

    trip_stops = [_build_trip_stop(stop) for stop in optimal_stops]

    total_fuel_cost = sum(s.cost_eur for s in trip_stops)
    total_fuel_liters = sum(s.liters_to_fill for s in trip_stops)

    # Baseline savings: compare with median price across candidates
    if candidate_stations:
        prices = sorted(c.price for c in candidate_stations)
        median_price = prices[len(prices) // 2]
        baseline_cost = total_fuel_liters * median_price
        savings = max(0, round(baseline_cost - total_fuel_cost, 2))
    else:
        savings = 0

    recommended_labels = {s.station.label for s in trip_stops}
    stop_args = (station_dicts, total_km, tank_liters, consumption_lper100km, fuel_level_pct)
    alternative_strategies = [
        ("Menos paradas", "Prioriza estaciones lejanas para reducir el numero de paradas", find_min_stops),
        ("Menor desvio", "Prioriza estaciones cercanas a la ruta para minimizar desvios", find_min_detour),
    ]

    alternative_plans = []
    for name, description, strategy_fn in alternative_strategies:
        alt_stops_raw = strategy_fn(*stop_args, min_fuel_at_destination_pct=min_fuel_at_destination_pct)
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
