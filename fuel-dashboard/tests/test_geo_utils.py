from services.geo_utils import haversine_distance


def test_haversine_same_point():
    assert haversine_distance(40.0, -3.0, 40.0, -3.0) == 0.0


def test_haversine_known_distance():
    # Madrid to Barcelona ~ 504 km
    distance = haversine_distance(40.4168, -3.7038, 41.3851, 2.1734)
    assert 490 < distance < 520


def test_haversine_short_distance():
    # Two points ~1 km apart
    distance = haversine_distance(40.4168, -3.7038, 40.4258, -3.7038)
    assert 0.5 < distance < 1.5


def test_haversine_symmetry():
    d1 = haversine_distance(40.0, -3.0, 41.0, -2.0)
    d2 = haversine_distance(41.0, -2.0, 40.0, -3.0)
    assert abs(d1 - d2) < 1e-10
