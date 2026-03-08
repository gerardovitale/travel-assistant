from services.geo_utils import point_in_polygon


def test_point_inside_polygon():
    # Simple square polygon around (0, 0)
    polygon = [[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]
    assert point_in_polygon(0.0, 0.0, polygon) is True


def test_point_outside_polygon():
    polygon = [[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]
    assert point_in_polygon(5.0, 5.0, polygon) is False
