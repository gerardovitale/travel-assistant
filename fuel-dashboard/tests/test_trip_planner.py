from unittest.mock import patch

import pandas as pd
import pytest
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


@patch("services.trip_planner.get_full_route")
@patch("services.trip_planner.geocode_address")
def test_plan_trip_no_route(mock_geocode, mock_route):
    mock_geocode.side_effect = [(40.4, -3.7), (36.5, -6.3)]
    mock_route.return_value = None
    with pytest.raises(ValueError, match="ruta"):
        plan_trip("Madrid", "Cadiz", "diesel_a_price", 7.0, 40, 50, 5.0)
