from unittest.mock import patch

import pandas as pd
import pytest
from services.trip_planner import _build_trip_stop
from services.trip_planner import _find_stops_with_strategy
from services.trip_planner import _floor_unmet
from services.trip_planner import _fuel_at_destination_pct
from services.trip_planner import find_min_detour
from services.trip_planner import find_min_stops
from services.trip_planner import find_optimal_stops
from services.trip_planner import plan_trip
from services.trip_planner import project_stations_onto_route
from services.trip_planner import sample_route_waypoints


# --- sample_route_waypoints ---


def test_sample_waypoints_straight_line():
    # ~111 km between these two points (1 degree of latitude)
    coords = [[0.0, 40.0], [0.0, 41.0]]
    waypoints = sample_route_waypoints(coords, interval_km=50)
    assert len(waypoints) >= 2
    assert waypoints[0][2] == 0.0  # first waypoint at km 0
    assert waypoints[-1][2] > 0  # last waypoint has cumulative km


def test_sample_waypoints_short_route():
    # Very short route, less than interval
    coords = [[0.0, 40.0], [0.001, 40.001]]
    waypoints = sample_route_waypoints(coords, interval_km=10)
    assert len(waypoints) == 2  # start + end


def test_sample_waypoints_empty():
    assert sample_route_waypoints([], interval_km=10) == []


def test_sample_waypoints_single_point():
    coords = [[0.0, 40.0]]
    waypoints = sample_route_waypoints(coords, interval_km=10)
    assert len(waypoints) >= 1


# --- project_stations_onto_route ---


def test_project_stations_basic():
    waypoints = [(40.0, -3.0, 0.0), (40.5, -3.0, 55.0), (41.0, -3.0, 111.0)]
    df = pd.DataFrame(
        {
            "label": ["A", "B"],
            "closest_waypoint_idx": [0, 2],
        }
    )
    project_stations_onto_route(df, waypoints)
    assert df.loc[0, "route_km"] == 0.0
    assert df.loc[1, "route_km"] == 111.0


def test_project_stations_empty_df():
    waypoints = [(40.0, -3.0, 0.0)]
    df = pd.DataFrame()
    project_stations_onto_route(df, waypoints)
    assert df.empty


def test_project_stations_beyond_end():
    waypoints = [(40.0, -3.0, 0.0), (41.0, -3.0, 111.0)]
    df = pd.DataFrame({"label": ["A"], "closest_waypoint_idx": [5]})
    project_stations_onto_route(df, waypoints)
    # Should clamp to last waypoint
    assert df.loc[0, "route_km"] == 111.0


# --- find_optimal_stops ---


def test_find_stops_within_range():
    """Trip is within tank range, no stops needed."""
    stations = [{"label": "S1", "route_km": 50, "price": 1.5}]
    stops = find_optimal_stops(
        stations,
        total_km=100,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=100,
    )
    # max_range = (40/7)*100 = 571 km, well over 100 km
    assert len(stops) == 0


def test_find_stops_multi_segment():
    """Long trip requiring multiple stops."""
    stations = [
        {"label": "S1", "route_km": 200, "price": 1.50},
        {"label": "S2", "route_km": 400, "price": 1.45},
        {"label": "S3", "route_km": 250, "price": 1.60},
    ]
    # max_range = (50/7)*100 = 714 km, usable (85%) = 607 km
    # With 50% fuel: initial range = 357 km
    stops = find_optimal_stops(
        stations,
        total_km=800,
        tank_liters=50,
        consumption_lper100km=7.0,
        fuel_pct=50,
    )
    assert len(stops) >= 1
    # Should pick cheapest in each window
    for stop in stops:
        assert "fuel_at_arrival_pct" in stop
        assert "liters_to_fill" in stop
        assert "cost_eur" in stop


def test_find_stops_no_candidates():
    """No stations available."""
    stops = find_optimal_stops(
        [],
        total_km=500,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=50,
    )
    assert stops == []


def test_find_stops_same_prices():
    """All stations same price — should still pick one."""
    stations = [{"label": f"S{i}", "route_km": 50 + i * 50, "price": 1.50} for i in range(8)]
    stops = find_optimal_stops(
        stations,
        total_km=600,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=30,
    )
    assert len(stops) >= 1


def test_find_stops_low_fuel():
    """Low initial fuel should trigger early stop."""
    stations = [
        {"label": "Near", "route_km": 30, "price": 1.60},
        {"label": "Far", "route_km": 200, "price": 1.40},
    ]
    # max_range = (40/7)*100 = 571 km
    # With 10% fuel: initial range = 57 km
    stops = find_optimal_stops(
        stations,
        total_km=500,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=10,
    )
    assert len(stops) >= 1
    assert stops[0]["label"] == "Near"


def test_min_fuel_at_destination_soft_check_inserts_stop():
    """Car can reach destination unaided but would arrive with < 50% fuel.
    Soft check must add one stop close enough to destination that the post-refill
    burn keeps arrival >= the floor."""
    station = {
        "label": "Mid",
        "route_km": 200,
        "price": 1.50,
        "address": "Calle Test 1",
        "municipality": "Madrid",
        "province": "Madrid",
        "zip_code": "28001",
        "latitude": 40.4,
        "longitude": -3.7,
        "detour_minutes": 0,
        "min_distance_km": 0,
    }
    stops_with = find_optimal_stops(
        [station],
        total_km=300,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=80,
        min_fuel_at_destination_pct=50,
    )
    assert len(stops_with) >= 1, "soft check should add a stop to meet the 50% arrival floor"
    trip_stops = [_build_trip_stop(s) for s in stops_with]
    dest_fuel = _fuel_at_destination_pct(trip_stops, 300, 40, 7.0, 80)
    assert dest_fuel >= 50.0


def test_min_fuel_at_destination_zero_matches_baseline():
    """min_fuel_at_destination_pct=0 must reproduce the same behaviour as the default."""
    stations = [{"label": "S1", "route_km": 200, "price": 1.50}]
    stops_baseline = find_optimal_stops(stations, total_km=300, tank_liters=40, consumption_lper100km=7.0, fuel_pct=80)
    stops_zero = find_optimal_stops(
        stations,
        total_km=300,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=80,
        min_fuel_at_destination_pct=0,
    )
    assert [s["label"] for s in stops_baseline] == [s["label"] for s in stops_zero]


# --- plan_trip (integration with mocks) ---


@patch("services.trip_planner.query_stations_along_corridor")
@patch("services.trip_planner.get_full_route")
@patch("services.trip_planner.geocode_address")
def test_plan_trip_basic(mock_geocode, mock_route, mock_corridor):
    mock_geocode.side_effect = [(40.4, -3.7), (36.5, -6.3)]
    mock_route.return_value = {
        "coordinates": [[-3.7, 40.4], [-5.0, 38.5], [-6.3, 36.5]],
        "distance_km": 650,
        "duration_minutes": 360,
    }

    corridor_df = pd.DataFrame(
        {
            "label": ["S1", "S2"],
            "address": ["addr1", "addr2"],
            "municipality": ["m1", "m2"],
            "province": ["p1", "p2"],
            "zip_code": ["28001", "11001"],
            "latitude": [39.0, 37.5],
            "longitude": [-4.5, -5.5],
            "diesel_a_price": [1.45, 1.50],
            "min_distance_km": [2.0, 3.0],
            "closest_waypoint_idx": [0, 1],
        }
    )
    mock_corridor.return_value = corridor_df

    result = plan_trip("Madrid", "Cadiz", "diesel_a_price", 7.0, 40, 50, 5.0)
    assert result.total_distance_km == 650
    assert result.duration_minutes == 360
    assert isinstance(result.route_coordinates, list)
    assert result.origin_coords == [40.4, -3.7]
    assert result.destination_coords == [36.5, -6.3]


@patch("services.trip_planner.geocode_address")
def test_plan_trip_bad_origin(mock_geocode):
    mock_geocode.return_value = None
    with pytest.raises(ValueError, match="origen"):
        plan_trip("Invalid", "Cadiz", "diesel_a_price", 7.0, 40, 50, 5.0)


@patch("services.trip_planner.query_stations_along_corridor")
@patch("services.trip_planner.get_full_route")
@patch("services.trip_planner.geocode_address")
def test_plan_trip_passes_labels(mock_geocode, mock_route, mock_corridor):
    mock_geocode.side_effect = [(40.4, -3.7), (36.5, -6.3)]
    mock_route.return_value = {
        "coordinates": [[-3.7, 40.4], [-5.0, 38.5], [-6.3, 36.5]],
        "distance_km": 650,
        "duration_minutes": 360,
    }

    corridor_df = pd.DataFrame(
        {
            "label": ["S1"],
            "address": ["addr1"],
            "municipality": ["m1"],
            "province": ["p1"],
            "zip_code": ["28001"],
            "latitude": [39.0],
            "longitude": [-4.5],
            "diesel_a_price": [1.45],
            "min_distance_km": [2.0],
            "closest_waypoint_idx": [0],
        }
    )
    mock_corridor.return_value = corridor_df

    plan_trip("Madrid", "Cadiz", "diesel_a_price", 7.0, 40, 50, 5.0, labels=["S1"])
    _, kwargs = mock_corridor.call_args
    assert kwargs.get("labels") == ["S1"]


@patch("services.trip_planner.query_stations_along_corridor")
@patch("services.trip_planner.get_full_route")
@patch("services.trip_planner.geocode_address")
def test_plan_trip_without_labels_defaults_to_none(mock_geocode, mock_route, mock_corridor):
    mock_geocode.side_effect = [(40.4, -3.7), (36.5, -6.3)]
    mock_route.return_value = {
        "coordinates": [[-3.7, 40.4], [-5.0, 38.5], [-6.3, 36.5]],
        "distance_km": 650,
        "duration_minutes": 360,
    }
    mock_corridor.return_value = pd.DataFrame()

    plan_trip("Madrid", "Cadiz", "diesel_a_price", 7.0, 40, 50, 5.0)
    _, kwargs = mock_corridor.call_args
    assert kwargs.get("labels") is None


# --- floor_unmet flag ---


def test_floor_unmet_helper_boundaries():
    # Tolerance absorbs 1-decimal rounding: 49.96 counts as meeting a 50% floor.
    assert _floor_unmet(49.96, 50.0) is False
    assert _floor_unmet(49.9, 50.0) is True
    # Floor of 0 is never unmet (baseline parity).
    assert _floor_unmet(0.0, 0.0) is False


@patch("services.trip_planner.query_stations_along_corridor")
@patch("services.trip_planner.get_full_route")
@patch("services.trip_planner.geocode_address")
def test_plan_trip_empty_corridor_sets_floor_unmet(mock_geocode, mock_route, mock_corridor):
    mock_geocode.side_effect = [(40.4, -3.7), (36.5, -6.3)]
    mock_route.return_value = {
        "coordinates": [[-3.7, 40.4], [-5.0, 38.5], [-6.3, 36.5]],
        "distance_km": 650,
        "duration_minutes": 360,
    }
    mock_corridor.return_value = pd.DataFrame()

    # 650 km on 40 L tank at 7 L/100km from 50% fuel arrives empty -> floor of 50% cannot be met.
    result = plan_trip("Madrid", "Cadiz", "diesel_a_price", 7.0, 40, 50, 5.0, 50)
    assert result.stops == []
    assert result.floor_unmet is True
    assert result.fuel_at_destination_pct < 50


@patch("services.trip_planner.query_stations_along_corridor")
@patch("services.trip_planner.get_full_route")
@patch("services.trip_planner.geocode_address")
def test_plan_trip_floor_met_sets_flag_false(mock_geocode, mock_route, mock_corridor):
    mock_geocode.side_effect = [(40.4, -3.7), (40.5, -3.6)]
    mock_route.return_value = {
        "coordinates": [[-3.7, 40.4], [-3.6, 40.5]],
        "distance_km": 50,
        "duration_minutes": 40,
    }
    corridor_df = pd.DataFrame(
        {
            "label": ["S1"],
            "address": ["addr1"],
            "municipality": ["m1"],
            "province": ["p1"],
            "zip_code": ["28001"],
            "latitude": [40.45],
            "longitude": [-3.65],
            "diesel_a_price": [1.45],
            "min_distance_km": [2.0],
            "closest_waypoint_idx": [0],
        }
    )
    mock_corridor.return_value = corridor_df

    # 50 km on a full 40 L tank arrives well above the 50% floor; no stop needed.
    result = plan_trip("Madrid", "Pinto", "diesel_a_price", 7.0, 40, 100, 5.0, 50)
    assert result.stops == []
    assert result.floor_unmet is False
    assert result.fuel_at_destination_pct >= 50


@patch("services.trip_planner.query_stations_along_corridor")
@patch("services.trip_planner.get_full_route")
@patch("services.trip_planner.geocode_address")
def test_plan_trip_floor_zero_never_unmet(mock_geocode, mock_route, mock_corridor):
    mock_geocode.side_effect = [(40.4, -3.7), (36.5, -6.3)]
    mock_route.return_value = {
        "coordinates": [[-3.7, 40.4], [-5.0, 38.5], [-6.3, 36.5]],
        "distance_km": 650,
        "duration_minutes": 360,
    }
    mock_corridor.return_value = pd.DataFrame()

    # Same arrives-empty trip, but a 0% floor can never be unmet.
    result = plan_trip("Madrid", "Cadiz", "diesel_a_price", 7.0, 40, 50, 5.0, 0)
    assert result.floor_unmet is False


@patch("services.trip_planner.get_full_route")
@patch("services.trip_planner.geocode_address")
def test_plan_trip_no_route(mock_geocode, mock_route):
    mock_geocode.side_effect = [(40.4, -3.7), (36.5, -6.3)]
    mock_route.return_value = None
    with pytest.raises(ValueError, match="ruta"):
        plan_trip("Madrid", "Cadiz", "diesel_a_price", 7.0, 40, 50, 5.0)


# --- _find_stops_with_strategy ---


def test_find_stops_with_custom_pick_fn():
    """Custom pick_fn that always picks the most expensive station."""
    stations = [
        {"label": "Cheap", "route_km": 50, "price": 1.20},
        {"label": "Expensive", "route_km": 70, "price": 1.80},
    ]

    def pick_most_expensive(candidates, ws, we):
        return max(candidates, key=lambda s: s["price"])

    stops = _find_stops_with_strategy(
        stations,
        total_km=500,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=30,
        pick_fn=pick_most_expensive,
    )
    assert len(stops) >= 1
    assert stops[0]["label"] == "Expensive"


# --- find_min_stops ---


def test_find_min_stops_prefers_far_window():
    """find_min_stops should prefer stations in the far third of the window."""
    # max_range = (50/7)*100 = 714 km, initial_range (50%) = 357 km
    # effective_range = 357 - 714*0.15 = 250 km
    # Far third starts at ~167 km
    stations = [
        {"label": "Near", "route_km": 50, "price": 1.40, "detour_minutes": 2},
        {"label": "Far", "route_km": 200, "price": 1.45, "detour_minutes": 3},
    ]
    stops = find_min_stops(
        stations,
        total_km=800,
        tank_liters=50,
        consumption_lper100km=7.0,
        fuel_pct=50,
    )
    assert len(stops) >= 1
    # Far station should be preferred because it's in the last third of the window
    assert stops[0]["label"] == "Far"


# --- find_min_detour ---


def test_find_min_detour_prefers_low_detour():
    """find_min_detour should prefer stations with lower detour."""
    stations = [
        {"label": "OnRoute", "route_km": 50, "price": 1.50, "detour_minutes": 0.5},
        {"label": "OffRoute", "route_km": 60, "price": 1.40, "detour_minutes": 8.0},
    ]
    stops = find_min_detour(
        stations,
        total_km=500,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=30,
    )
    assert len(stops) >= 1
    assert stops[0]["label"] == "OnRoute"


# --- reasoning ---


def test_reasoning_string_populated():
    """Stops should have a reasoning string."""
    stations = [
        {"label": "S1", "route_km": 50, "price": 1.40},
    ]
    stops = find_optimal_stops(
        stations,
        total_km=500,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=30,
    )
    assert len(stops) >= 1
    assert "reasoning" in stops[0]
    assert "candidatas" in stops[0]["reasoning"]
    assert "ventana" in stops[0]["reasoning"]


def test_reasoning_per_strategy():
    """Each strategy should produce its own reasoning text."""
    stations = [
        {"label": "S1", "route_km": 50, "price": 1.40, "detour_minutes": 0.5},
    ]
    stops = find_min_detour(
        stations,
        total_km=500,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=30,
    )
    assert len(stops) >= 1
    assert "Menor desvio" in stops[0]["reasoning"]

    stops_min = find_min_stops(
        stations,
        total_km=500,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=30,
    )
    assert len(stops_min) >= 1
    assert "Mas lejana" in stops_min[0]["reasoning"]


# --- _build_trip_stop ---


def test_build_trip_stop():
    """_build_trip_stop should convert a dict to a TripStop."""
    stop_dict = {
        "label": "TestStation",
        "address": "Calle Test 1",
        "municipality": "TestCity",
        "province": "TestProv",
        "zip_code": "28001",
        "latitude": 40.4,
        "longitude": -3.7,
        "price": 1.45,
        "route_km": 150.0,
        "detour_minutes": 3.5,
        "min_distance_km": 2.0,
        "fuel_at_arrival_pct": 25.0,
        "liters_to_fill": 30.0,
        "cost_eur": 43.50,
        "reasoning": "Test reasoning",
    }
    trip_stop = _build_trip_stop(stop_dict)
    assert trip_stop.station.label == "TestStation"
    assert trip_stop.route_km == 150.0
    assert trip_stop.detour_minutes == 3.5
    assert trip_stop.fuel_at_arrival_pct == 25.0
    assert trip_stop.liters_to_fill == 30.0
    assert trip_stop.cost_eur == 43.50
    assert trip_stop.reasoning == "Test reasoning"
    assert trip_stop.station.distance_km == 2.0


# --- alternative plans in plan_trip ---


@patch("services.trip_planner.query_stations_along_corridor")
@patch("services.trip_planner.get_full_route")
@patch("services.trip_planner.geocode_address")
def test_plan_trip_generates_alternative_plans(mock_geocode, mock_route, mock_corridor):
    mock_geocode.side_effect = [(40.4, -3.7), (36.5, -6.3)]
    mock_route.return_value = {
        "coordinates": [[-3.7, 40.4], [-5.0, 38.5], [-6.3, 36.5]],
        "distance_km": 650,
        "duration_minutes": 360,
    }

    # Create enough stations to allow different strategies to pick differently
    corridor_df = pd.DataFrame(
        {
            "label": ["S1", "S2", "S3", "S4"],
            "address": ["addr1", "addr2", "addr3", "addr4"],
            "municipality": ["m1", "m2", "m3", "m4"],
            "province": ["p1", "p2", "p3", "p4"],
            "zip_code": ["28001", "28002", "28003", "11001"],
            "latitude": [39.5, 39.0, 38.5, 37.5],
            "longitude": [-4.0, -4.5, -5.0, -5.5],
            "diesel_a_price": [1.45, 1.50, 1.42, 1.55],
            "min_distance_km": [0.5, 5.0, 1.0, 3.0],
            "closest_waypoint_idx": [0, 0, 1, 1],
        }
    )
    mock_corridor.return_value = corridor_df

    result = plan_trip("Madrid", "Cadiz", "diesel_a_price", 7.0, 40, 50, 5.0)
    assert hasattr(result, "alternative_plans")
    assert isinstance(result.alternative_plans, list)
    # Alternative plans should have proper structure
    for alt in result.alternative_plans:
        assert alt.strategy_name
        assert alt.strategy_description
        assert isinstance(alt.stops, list)
        assert alt.total_fuel_cost >= 0
        assert alt.num_stops >= 0
        # Each alternative carries its own floor flag derived from its own arrival fuel
        # against the requested floor (default 50%).
        assert alt.floor_unmet == _floor_unmet(alt.fuel_at_destination_pct, 50.0)


# --- stop-pruning and soft-check behaviour ---


def _make_station(label: str, route_km: float, price: float) -> dict:
    return {
        "label": label,
        "route_km": route_km,
        "price": price,
        "address": f"addr {label}",
        "municipality": "x",
        "province": "y",
        "zip_code": "00000",
        "latitude": 40.0,
        "longitude": -3.0,
        "detour_minutes": 0,
        "min_distance_km": 0,
    }


def test_full_tank_realistic_road_trip_produces_one_stop():
    """Madrid→Chiclana stand-in: 470 km, 40 L, 7 L/100 km, full tank, floor 40%."""
    stations = [_make_station(f"S{i}", k, 1.40 + i * 0.01) for i, k in enumerate([80, 180, 280, 380])]
    stops = find_optimal_stops(
        stations,
        total_km=470,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=100,
        min_fuel_at_destination_pct=40,
    )
    assert len(stops) == 1
    trip_stops = [_build_trip_stop(s) for s in stops]
    assert _fuel_at_destination_pct(trip_stops, 470, 40, 7.0, 100) >= 40


def test_prune_removes_redundant_first_stop():
    """Tank covers the whole trip — greedy must not insert an early stop."""
    stations = [
        _make_station("Early", 100, 1.30),
        _make_station("Mid", 400, 1.40),
    ]
    stops = find_optimal_stops(
        stations,
        total_km=500,
        tank_liters=60,
        consumption_lper100km=6.0,
        fuel_pct=100,
        min_fuel_at_destination_pct=0,
    )
    assert stops == []


def test_prune_removes_redundant_last_stop():
    """Trip within range with floor=0 ⇒ no stops at all."""
    stations = [
        _make_station("S1", 200, 1.40),
        _make_station("S2", 480, 1.50),
    ]
    stops = find_optimal_stops(
        stations,
        total_km=500,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=100,
        min_fuel_at_destination_pct=0,
    )
    assert stops == []


def test_soft_check_inserts_only_when_below_threshold():
    """Same trip: floor=40 forces one stop; floor=0 forces none."""
    stations = [_make_station("Late", 300, 1.45)]
    with_floor = find_optimal_stops(
        stations,
        total_km=470,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=100,
        min_fuel_at_destination_pct=40,
    )
    no_floor = find_optimal_stops(
        stations,
        total_km=470,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=100,
        min_fuel_at_destination_pct=0,
    )
    assert len(with_floor) == 1
    assert with_floor[0]["label"] == "Late"
    assert no_floor == []


def test_post_refill_range_is_full_tank():
    """Two stations 480 km apart on a 900 km trip. Under the old 85%-refill bug
    the second station was unreachable; under the fix it must be selected."""
    stations = [
        _make_station("S1", 200, 1.40),
        _make_station("S2", 680, 1.40),
    ]
    stops = find_optimal_stops(
        stations,
        total_km=900,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=100,
        min_fuel_at_destination_pct=0,
    )
    labels = [s["label"] for s in stops]
    assert "S2" in labels


def test_prune_single_pass_is_sufficient():
    """Three candidate stations on a 500 km trip with full tank ⇒ all pruned."""
    stations = [
        _make_station("A", 150, 1.30),
        _make_station("B", 300, 1.30),
        _make_station("C", 450, 1.30),
    ]
    stops = find_optimal_stops(
        stations,
        total_km=500,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=100,
        min_fuel_at_destination_pct=0,
    )
    assert stops == []


def test_soft_check_skips_when_no_station_near_destination():
    """Only station is close to origin → soft check cannot meet the floor.
    Stops list stays empty and predicted arrival reflects the shortfall."""
    stations = [_make_station("Early", 50, 1.40)]
    stops = find_optimal_stops(
        stations,
        total_km=470,
        tank_liters=40,
        consumption_lper100km=7.0,
        fuel_pct=100,
        min_fuel_at_destination_pct=40,
    )
    # max_range = 571 km. max_remaining_km for floor=40 = 571 * 0.6 = 342.6 km.
    # Station at 50 km is 420 km from destination, well outside the reserve window.
    assert stops == []
    trip_stops = [_build_trip_stop(s) for s in stops]
    arrival = _fuel_at_destination_pct(trip_stops, 470, 40, 7.0, 100)
    assert arrival < 40, "with no insertable station, arrival must fall below the floor"


def test_prune_recomputes_metrics_for_surviving_stops():
    """Drop a redundant middle stop; surviving stops' metrics must reflect new prior leg.

    Setup: tank=50, consumption=10 → max_range=500, safety_margin=75, usable_after_refill=425.
    Stations A(150), B(400, cheapest), C(550). total_km=950, fuel_pct=50 (initial range 250).
    Greedy: A → B (cheapest in window) → C. Three stops.
    Prune: dropping B leaves A→C gap=400 ≤ 425 and C→dest=400 ≤ 425 → feasible.
    Result [A, C]; C's metrics recomputed because its prior leg is now A→C (400 km), not B→C (150 km).
    """
    stations = [
        _make_station("A", 150, 1.40),
        _make_station("B", 400, 1.30),
        _make_station("C", 550, 1.50),
    ]
    stops = find_optimal_stops(
        stations,
        total_km=950,
        tank_liters=50,
        consumption_lper100km=10.0,
        fuel_pct=50,
        min_fuel_at_destination_pct=0,
    )
    labels = [s["label"] for s in stops]
    assert labels == ["A", "C"], f"prune should drop redundant B, got {labels}"

    # C now refuels after a full 400 km leg from A (full tank → 40 L used → 10 L remaining = 20%).
    c_stop = stops[1]
    assert c_stop["fuel_at_arrival_pct"] == 20.0
    assert c_stop["liters_to_fill"] == 40.0
    assert c_stop["cost_eur"] == 60.0  # 40 L * 1.50

    # A's metrics use the initial leg (150 km on 50% tank → 25 L start - 15 L used = 10 L = 20%).
    a_stop = stops[0]
    assert a_stop["fuel_at_arrival_pct"] == 20.0
    assert a_stop["liters_to_fill"] == 40.0
    assert a_stop["cost_eur"] == 56.0  # 40 L * 1.40
