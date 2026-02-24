import logging
from typing import Optional
from typing import Tuple

from config import settings
from geopy.geocoders import Nominatim

logger = logging.getLogger(__name__)

_geocoder: Optional[Nominatim] = None


def _get_geocoder() -> Nominatim:
    global _geocoder
    if _geocoder is None:
        _geocoder = Nominatim(user_agent=settings.geocoding_user_agent)
    return _geocoder


def geocode_address(address: str) -> Optional[Tuple[float, float]]:
    """Convert an address string to (latitude, longitude) coordinates."""
    geocoder = _get_geocoder()
    location = geocoder.geocode(address, country_codes=["es"])
    if location is None:
        logger.warning(f"Could not geocode address: {address}")
        return None
    logger.info(f"Geocoded '{address}' to ({location.latitude}, {location.longitude})")
    return (location.latitude, location.longitude)
