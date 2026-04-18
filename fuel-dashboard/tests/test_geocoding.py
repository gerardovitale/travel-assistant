from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from services.geocoding import _fetch_address_suggestions
from services.geocoding import _short_display_name
from services.geocoding import geocode_address
from services.geocoding import get_address_suggestions
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


@pytest.mark.parametrize(
    "result, expected",
    [
        # road + house_number + city
        (
            {
                "address": {
                    "road": "Calle Alcalá",
                    "house_number": "1",
                    "city": "Madrid",
                    "state": "Comunidad de Madrid",
                }
            },
            "Calle Alcalá 1, Madrid",
        ),
        # road only (no city)
        (
            {"address": {"road": "Calle Mayor", "state": "Comunidad de Madrid"}},
            "Calle Mayor",
        ),
        # city + state
        (
            {"address": {"city": "Madrid", "state": "Comunidad de Madrid"}},
            "Madrid, Comunidad de Madrid",
        ),
        # city == state guard
        (
            {"address": {"city": "Murcia", "state": "Murcia"}},
            "Murcia",
        ),
        # no address fields — fallback to display_name
        (
            {"address": {}, "display_name": "Some Place, España"},
            "Some Place, España",
        ),
        # pedestrian road variant
        (
            {"address": {"pedestrian": "Paseo del Prado", "city": "Madrid", "state": "Comunidad de Madrid"}},
            "Paseo del Prado, Madrid",
        ),
    ],
)
def test_short_display_name(result, expected):
    assert _short_display_name(result) == expected


def test_short_display_name_empty_filtered_out():
    result = {"address": {}}
    assert _short_display_name(result) == ""


_NOMINATIM_RESPONSE = [
    {
        "display_name": "Madrid, Comunidad de Madrid, España",
        "lat": "40.4168",
        "lon": "-3.7038",
        "address": {"city": "Madrid", "state": "Comunidad de Madrid", "country": "España"},
    },
    {
        "display_name": "Valencia, Comunitat Valenciana, España",
        "lat": "39.4699",
        "lon": "-0.3763",
        "address": {"city": "Valencia", "state": "Comunitat Valenciana", "country": "España"},
    },
]

_NOMINATIM_STREET_RESPONSE = [
    {
        "display_name": "Calle Puente la Reina, Las Tablas, ..., Madrid, Comunidad de Madrid, 28050, España",
        "lat": "40.5000",
        "lon": "-3.6000",
        "address": {
            "road": "Calle Puente la Reina",
            "house_number": "27",
            "city": "Madrid",
            "state": "Comunidad de Madrid",
        },
    }
]


@patch("services.geocoding.httpx.get")
def test_get_address_suggestions_success(mock_get):
    _fetch_address_suggestions.cache_clear()
    mock_resp = MagicMock()
    mock_resp.json.return_value = _NOMINATIM_RESPONSE
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    results = get_address_suggestions("Madr")
    assert len(results) == 2
    assert results[0]["display_name"] == "Madrid, Comunidad de Madrid"
    assert results[0]["lat"] == 40.4168
    assert results[0]["lon"] == -3.7038


@patch("services.geocoding.httpx.get")
def test_get_address_suggestions_street_with_house_number(mock_get):
    _fetch_address_suggestions.cache_clear()
    mock_resp = MagicMock()
    mock_resp.json.return_value = _NOMINATIM_STREET_RESPONSE
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    results = get_address_suggestions("calle puente la reina 27")
    assert len(results) == 1
    assert results[0]["display_name"] == "Calle Puente la Reina 27, Madrid"


@patch("services.geocoding.httpx.get")
def test_get_address_suggestions_empty_response(mock_get):
    _fetch_address_suggestions.cache_clear()
    mock_resp = MagicMock()
    mock_resp.json.return_value = []
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    results = get_address_suggestions("xyz")
    assert results == []


@patch("services.geocoding.httpx.get")
def test_get_address_suggestions_network_error(mock_get):
    _fetch_address_suggestions.cache_clear()
    mock_get.side_effect = Exception("network error")

    results = get_address_suggestions("Madrid")
    assert results == []


@patch("services.geocoding.httpx.get")
def test_get_address_suggestions_error_not_cached(mock_get):
    _fetch_address_suggestions.cache_clear()
    mock_get.side_effect = Exception("transient error")
    get_address_suggestions("Valencia")

    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {
            "display_name": "Valencia, España",
            "lat": "39.4699",
            "lon": "-0.3763",
            "address": {"city": "Valencia", "state": "Comunitat Valenciana"},
        }
    ]
    mock_resp.raise_for_status.return_value = None
    mock_get.side_effect = None
    mock_get.return_value = mock_resp

    results = get_address_suggestions("Valencia")
    assert len(results) == 1
    assert results[0]["display_name"] == "Valencia, Comunitat Valenciana"
