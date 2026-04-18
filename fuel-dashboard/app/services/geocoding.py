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


_PHOTON_URL = "https://photon.komoot.io/api/"
_PHOTON_BBOX = "-9.3,35.9,4.3,43.8"


def _build_display_name(props: Dict[str, Any]) -> str:
    parts = [props.get("name"), props.get("street"), props.get("city"), props.get("state")]
    seen: set = set()
    unique: List[str] = []
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            unique.append(p)
    return ", ".join(unique)


@functools.lru_cache(maxsize=512)
def get_address_suggestions(query: str) -> List[Dict[str, Any]]:
    """Return up to 5 address suggestions for *query* via the Photon API."""
    try:
        resp = httpx.get(
            _PHOTON_URL,
            params={"q": query, "limit": 5, "bbox": _PHOTON_BBOX},
            timeout=5.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        logger.warning("Photon autocomplete failed for query: %s", query)
        return []
    suggestions: List[Dict[str, Any]] = []
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        if props.get("countrycode") != "ES":
            continue
        coords = feature.get("geometry", {}).get("coordinates", [])
        if len(coords) < 2:
            continue
        display_name = _build_display_name(props)
        if not display_name:
            continue
        suggestions.append({"display_name": display_name, "lat": coords[1], "lon": coords[0]})
    return suggestions


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
