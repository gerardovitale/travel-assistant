from unittest.mock import patch

import pandas as pd
import pytest
from services.trip_planner import _build_trip_stop
from services.trip_planner import _find_stops_with_strategy
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
