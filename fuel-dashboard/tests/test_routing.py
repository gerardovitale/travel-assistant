from unittest.mock import MagicMock
from unittest.mock import patch

from services.routing import get_road_distances


@patch("services.routing.httpx.Client")
def test_successful_response(mock_client_cls):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"code": "Ok", "distances": [[1500.0, 3200.5]]}
    mock_client_cls.return_value.__enter__ = lambda self: self
    mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value.get.return_value = mock_response

    result = get_road_distances((40.0, -3.0), [(40.1, -3.1), (40.2, -3.2)])
    assert result == [1.5, 3.2]


@patch("services.routing.httpx.Client")
def test_osrm_request_failure(mock_client_cls):
    mock_client_cls.return_value.__enter__ = lambda self: self
    mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value.get.side_effect = Exception("connection error")

    result = get_road_distances((40.0, -3.0), [(40.1, -3.1)])
    assert result is None


@patch("services.routing.httpx.Client")
def test_non_ok_response_code(mock_client_cls):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"code": "InvalidQuery"}
    mock_client_cls.return_value.__enter__ = lambda self: self
    mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value.get.return_value = mock_response

    result = get_road_distances((40.0, -3.0), [(40.1, -3.1)])
    assert result is None


def test_empty_destinations():
    result = get_road_distances((40.0, -3.0), [])
    assert result == []


@patch("services.routing.httpx.Client")
def test_unreachable_destination(mock_client_cls):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"code": "Ok", "distances": [[1500.0, None]]}
    mock_client_cls.return_value.__enter__ = lambda self: self
    mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value.get.return_value = mock_response

    result = get_road_distances((40.0, -3.0), [(40.1, -3.1), (40.2, -3.2)])
    assert result == [1.5, None]


@patch("services.routing.httpx.Client")
def test_http_error_status(mock_client_cls):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_client_cls.return_value.__enter__ = lambda self: self
    mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
    mock_client_cls.return_value.get.return_value = mock_response

    result = get_road_distances((40.0, -3.0), [(40.1, -3.1)])
    assert result is None
