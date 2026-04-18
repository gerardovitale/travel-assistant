import functools
import logging
import re
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import httpx
from config import settings
from geopy.geocoders import Nominatim

logger = logging.getLogger(__name__)

_COORD_RE = re.compile(r"^\s*(-?\d{1,3}(?:\.\d+)?)\s*,\s*(-?\d{1,3}(?:\.\d+)?)\s*$")

_geocoder: Optional[Nominatim] = None


def _get_geocoder() -> Nominatim:
    global _geocoder
    if _geocoder is None:
        _geocoder = Nominatim(user_agent=settings.geocoding_user_agent)
    return _geocoder


def parse_coordinates(text: str) -> Optional[Tuple[float, float]]:
    """Return (lat, lon) if *text* looks like a coordinate pair, else None."""
    match = _COORD_RE.match(text)
    if match is None:
        return None
    lat, lon = float(match.group(1)), float(match.group(2))
    if -90 <= lat <= 90 and -180 <= lon <= 180:
        return (lat, lon)
    return None


_NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"


def _short_display_name(result: Dict[str, Any]) -> str:
    addr = result.get("address", {})
    road = addr.get("road") or addr.get("pedestrian") or addr.get("footway")
    city = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("municipality")
    state = addr.get("state")
    if road:
        house = addr.get("house_number")
        primary = f"{road} {house}" if house else road
        return f"{primary}, {city}" if city else primary
    if city:
        return f"{city}, {state}" if state and state != city else city
    return result.get("display_name", "")


@functools.lru_cache(maxsize=512)
def _fetch_address_suggestions(query: str) -> List[Dict[str, Any]]:
    resp = httpx.get(
        _NOMINATIM_SEARCH_URL,
        params={
            "q": query,
            "countrycodes": "es",
            "format": "jsonv2",
            "addressdetails": 1,
            "limit": 5,
            "accept-language": "es",
        },
        headers={"User-Agent": settings.geocoding_user_agent},
        timeout=5.0,
    )
    resp.raise_for_status()
    data = resp.json()
    return [
        {"display_name": name, "lat": float(r["lat"]), "lon": float(r["lon"])}
        for r in data
        if (name := _short_display_name(r))
    ]


def get_address_suggestions(query: str) -> List[Dict[str, Any]]:
    """Return up to 5 address suggestions for *query* via Nominatim search."""
    try:
        return _fetch_address_suggestions(query)
    except Exception:
        logger.warning("Nominatim autocomplete failed for query: %s", query, exc_info=True)
        return []


@functools.lru_cache(maxsize=256)
def geocode_address(address: str) -> Optional[Tuple[float, float]]:
    """Convert an address string to (latitude, longitude) coordinates."""
    coords = parse_coordinates(address)
    if coords is not None:
        logger.info("Parsed coordinates directly from input: %s", coords)
        return coords
    geocoder = _get_geocoder()
    location = geocoder.geocode(address, country_codes=["es"])
    if location is None:
        logger.warning(f"Could not geocode address: {address}")
        return None
    logger.info(f"Geocoded '{address}' to ({location.latitude}, {location.longitude})")
    return (location.latitude, location.longitude)
