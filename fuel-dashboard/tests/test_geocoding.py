from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from services.geocoding import geocode_address
from services.geocoding import parse_coordinates


@patch("services.geocoding._get_geocoder")
def test_geocode_address_success(mock_get_geocoder):
    geocode_address.cache_clear()
    mock_geocoder = MagicMock()
    mock_location = MagicMock()
    mock_location.latitude = 40.4168
    mock_location.longitude = -3.7038
    mock_geocoder.geocode.return_value = mock_location
    mock_get_geocoder.return_value = mock_geocoder

    result = geocode_address("Madrid, Spain")
    assert result == (40.4168, -3.7038)
    mock_geocoder.geocode.assert_called_once_with("Madrid, Spain", country_codes=["es"])


@patch("services.geocoding._get_geocoder")
def test_geocode_address_not_found(mock_get_geocoder):
    geocode_address.cache_clear()
    mock_geocoder = MagicMock()
    mock_geocoder.geocode.return_value = None
    mock_get_geocoder.return_value = mock_geocoder

    result = geocode_address("nonexistent place xyz")
    assert result is None


@pytest.mark.parametrize(
    "text, expected",
    [
        ("40.416775, -3.703790", (40.416775, -3.70379)),
        ("-33.8688, 151.2093", (-33.8688, 151.2093)),
        ("  40.416775 , -3.703790  ", (40.416775, -3.70379)),
        ("0.0, 0.0", (0.0, 0.0)),
        ("90, 180", (90.0, 180.0)),
        ("-90, -180", (-90.0, -180.0)),
    ],
)
def test_parse_coordinates_valid(text, expected):
    assert parse_coordinates(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "Madrid, Spain",
        "91.0, 0.0",
        "0.0, 181.0",
        "-91.0, 0.0",
        "0.0, -181.0",
        "",
        "abc, def",
        "40.416775",
        "40.416775, -3.703790, 100",
    ],
)
def test_parse_coordinates_invalid(text):
    assert parse_coordinates(text) is None


def test_geocode_address_with_coordinates():
    geocode_address.cache_clear()
    result = geocode_address("40.416775, -3.703790")
    assert result == (40.416775, -3.70379)
