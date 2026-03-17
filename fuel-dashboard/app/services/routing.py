import asyncio
import logging
import time
from typing import List
from typing import Optional
from typing import Tuple

import httpx
from config import settings

logger = logging.getLogger(__name__)

_OSRM_RETRIES = 3
_OSRM_RETRY_DELAY = 1.0


def _osrm_get(url: str, timeout: float = 30.0) -> httpx.Response:
    """GET with retries for transient connection errors.

    Raises httpx.ConnectError after all retries are exhausted.
    """
    last_err: Optional[Exception] = None
    for attempt in range(_OSRM_RETRIES):
        try:
            with httpx.Client(timeout=timeout) as client:
                return client.get(url)

        except httpx.ConnectError as exc:
            last_err = exc
            if attempt < _OSRM_RETRIES - 1:
                logger.info("OSRM connect failed (attempt %d/%d), retrying...", attempt + 1, _OSRM_RETRIES)
                time.sleep(_OSRM_RETRY_DELAY)

    raise last_err  # type: ignore[misc]


def get_full_route(
    origin: Tuple[float, float],
    destination: Tuple[float, float],
) -> Optional[dict]:
    """Fetch full route from OSRM with coordinates, distance, and duration.

    Returns dict with 'coordinates' (list of [lon, lat]), 'distance_km', 'duration_minutes',
    or None on failure.
    """
    origin_str = f"{origin[1]},{origin[0]}"
    dest_str = f"{destination[1]},{destination[0]}"
    url = f"{settings.osrm_base_url}/route/v1/driving/{origin_str};{dest_str}?overview=full&geometries=geojson"
    try:
        response = _osrm_get(url)
        if response.status_code != 200:
            logger.warning("OSRM full route returned status %d", response.status_code)
            return None

        data = response.json()
        if data.get("code") != "Ok":
            logger.warning("OSRM full route response code: %s", data.get("code"))
            return None

        route = data["routes"][0]
        return {
            "coordinates": route["geometry"]["coordinates"],
            "distance_km": round(route["distance"] / 1000, 2),
            "duration_minutes": round(route["duration"] / 60, 1),
        }

    except Exception:
        logger.warning("OSRM full route request failed", exc_info=True)
        return None


async def _fetch_single_route(
    client: httpx.AsyncClient,
    origin_str: str,
    lat: float,
    lon: float,
) -> Optional[List[List[float]]]:
    dest_str = f"{lon},{lat}"
    url = f"{settings.osrm_base_url}/route/v1/driving/{origin_str};{dest_str}" "?overview=full&geometries=geojson"
    try:
        response = await client.get(url)
        if response.status_code != 200:
            logger.warning("OSRM route returned status %d", response.status_code)
            return None
        data = response.json()
        if data.get("code") != "Ok":
            logger.warning("OSRM route response code: %s", data.get("code"))
            return None
        return data["routes"][0]["geometry"]["coordinates"]
    except Exception:
        logger.warning("OSRM route request failed", exc_info=True)
        return None


async def get_route_geometries(
    origin: Tuple[float, float],
    destinations: List[Tuple[float, float]],
) -> List[Optional[List[List[float]]]]:
    """Fetch route geometries from OSRM Route API in parallel.

    Args:
        origin: (lat, lon) of the starting point.
        destinations: list of (lat, lon) for each station.

    Returns:
        List of coordinate lists (each a list of [lon, lat] pairs),
        or None per destination on failure.
    """
    if not destinations:
        return []

    origin_str = f"{origin[1]},{origin[0]}"

    async with httpx.AsyncClient(timeout=settings.osrm_timeout) as client:
        tasks = [_fetch_single_route(client, origin_str, lat, lon) for lat, lon in destinations]
        return list(await asyncio.gather(*tasks))


def get_road_distances(
    origin: Tuple[float, float],
    destinations: List[Tuple[float, float]],
) -> Optional[List[Optional[float]]]:
    """Fetch road distances from OSRM Table API.

    Args:
        origin: (lat, lon) of the starting point.
        destinations: list of (lat, lon) for each station.

    Returns:
        List of distances in km (None for unreachable destinations),
        or None if the request fails entirely.
    """
    if not destinations:
        return []

    # OSRM uses lon,lat order
    coords_parts = [f"{origin[1]},{origin[0]}"]
    for lat, lon in destinations:
        coords_parts.append(f"{lon},{lat}")
    coords_str = ";".join(coords_parts)

    dest_indices = ";".join(str(i) for i in range(1, len(destinations) + 1))
    url = (
        f"{settings.osrm_base_url}/table/v1/driving/{coords_str}"
        "?sources=0"
        f"&destinations={dest_indices}"
        "&annotations=distance"
    )
    try:
        with httpx.Client(timeout=settings.osrm_timeout) as client:
            response = client.get(url)
        if response.status_code != 200:
            logger.warning("OSRM returned status %d", response.status_code)
            return None
        data = response.json()
        if data.get("code") != "Ok":
            logger.warning("OSRM response code: %s", data.get("code"))
            return None
        row = data["distances"][0]
        return [round(d / 1000, 2) if d is not None else None for d in row]
    except Exception:
        logger.warning("OSRM request failed", exc_info=True)
        return None
