from unittest.mock import MagicMock
from unittest.mock import patch

from services.geocoding import geocode_address


@patch("services.geocoding._get_geocoder")
def test_geocode_address_success(mock_get_geocoder):
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
    mock_geocoder = MagicMock()
    mock_geocoder.geocode.return_value = None
    mock_get_geocoder.return_value = mock_geocoder

    result = geocode_address("nonexistent place xyz")
    assert result is None
