import asyncio
import functools
import logging
import time

import httpx
from config import settings

logger = logging.getLogger(__name__)

_OSRM_RETRIES = 3
_OSRM_RETRY_DELAY = 1.0

_sync_client: httpx.Client | None = None


def _get_sync_client() -> httpx.Client:
    global _sync_client
    if _sync_client is None or _sync_client.is_closed:
        _sync_client = httpx.Client(timeout=30.0, limits=httpx.Limits(max_connections=10, max_keepalive_connections=5))
    return _sync_client


def _osrm_get(url: str, timeout: float = 30.0) -> httpx.Response:
    """GET with retries; raises httpx.ConnectError after _OSRM_RETRIES exhausted."""
    client = _get_sync_client()
    last_err: Exception | None = None
    for attempt in range(_OSRM_RETRIES):
        try:
            return client.get(url)

        except httpx.ConnectError as exc:
            last_err = exc
            if attempt < _OSRM_RETRIES - 1:
                logger.info("OSRM connect failed (attempt %d/%d), retrying...", attempt + 1, _OSRM_RETRIES)
                time.sleep(_OSRM_RETRY_DELAY)

    raise last_err  # type: ignore[misc]


@functools.lru_cache(maxsize=128)
def get_full_route(
    origin: tuple[float, float],
    destination: tuple[float, float],
) -> dict | None:
    """Fetch full OSRM route; return dict with 'coordinates', 'distance_km', 'duration_minutes', or None."""
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
) -> list[list[float]] | None:
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
    origin: tuple[float, float],
    destinations: list[tuple[float, float]],
) -> list[list[list[float]] | None]:
    """Fetch OSRM route geometries in parallel for each destination; None per failed destination."""
    if not destinations:
        return []

    origin_str = f"{origin[1]},{origin[0]}"

    async with httpx.AsyncClient(timeout=settings.osrm_timeout) as client:
        tasks = [_fetch_single_route(client, origin_str, lat, lon) for lat, lon in destinations]
        return list(await asyncio.gather(*tasks))


def get_road_distances(
    origin: tuple[float, float],
    destinations: list[tuple[float, float]],
) -> list[float | None] | None:
    """Fetch road distances via OSRM Table API; return list of km (None per unreachable), or None on failure."""
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
        client = _get_sync_client()
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
