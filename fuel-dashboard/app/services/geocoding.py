import functools
import logging
import re
from typing import Optional
from typing import Tuple

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
