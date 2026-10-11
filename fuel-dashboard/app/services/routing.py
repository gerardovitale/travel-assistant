import asyncio
import functools
import logging
import threading
import time

import httpx
from config import settings

logger = logging.getLogger(__name__)

_OSRM_RETRIES = 3
_OSRM_RETRY_DELAY = 1.0

_sync_client: httpx.Client | None = None
_sync_client_lock = threading.Lock()


def _get_sync_client() -> httpx.Client:
    """Shared client; the lock stops concurrent first calls (e.g. round-trip routes) creating one client each."""
    global _sync_client
    with _sync_client_lock:
        if _sync_client is None or _sync_client.is_closed:
            _sync_client = httpx.Client(
                timeout=30.0, limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)
            )
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


class _OSRMUnavailable(Exception):
    """Raised inside the cached fetch so lru_cache never stores a failed lookup."""


# Small cache: each entry can hold up to 3 full-resolution geometries (~1 MB each on long routes).
@functools.lru_cache(maxsize=32)
def _cached_route_options(
    origin: tuple[float, float],
    destination: tuple[float, float],
    max_alternatives: int,
) -> tuple[dict, ...]:
    origin_str = f"{origin[1]},{origin[0]}"
    dest_str = f"{destination[1]},{destination[0]}"
    url = f"{settings.osrm_base_url}/route/v1/driving/{origin_str};{dest_str}?overview=full&geometries=geojson"
    if max_alternatives > 0:
        # steps=true is required for OSRM to fill legs[].summary (road names), only shown when comparing routes
        url += f"&alternatives={max_alternatives}&steps=true"
    try:
        response = _osrm_get(url)
    except Exception as exc:
        raise _OSRMUnavailable("request failed") from exc
    if response.status_code != 200:
        raise _OSRMUnavailable(f"status {response.status_code}")

    data = response.json()
    if data.get("code") != "Ok":
        raise _OSRMUnavailable(f"response code {data.get('code')}")

    return tuple(
        {
            "coordinates": route["geometry"]["coordinates"],
            "distance_km": round(route["distance"] / 1000, 2),
            "duration_minutes": round(route["duration"] / 60, 1),
            "summary": ", ".join(leg["summary"] for leg in route.get("legs", []) if leg.get("summary")),
        }
        for route in data["routes"][: max_alternatives + 1]
    )


def get_route_options(
    origin: tuple[float, float],
    destination: tuple[float, float],
    max_alternatives: int = 0,
) -> tuple[dict, ...]:
    """Fetch OSRM route plus up to max_alternatives alternatives; empty tuple on failure (failures are not cached).

    Each route dict has 'coordinates', 'distance_km', 'duration_minutes' and 'summary' (main road names; only
    filled when max_alternatives > 0). Successful results are cached and shared: treat them as read-only.
    """
    try:
        return _cached_route_options(origin, destination, max(max_alternatives, 0))
    except Exception:
        logger.warning("OSRM full route request failed", exc_info=True)
        return ()


def get_full_route(
    origin: tuple[float, float],
    destination: tuple[float, float],
) -> dict | None:
    """Fetch the main OSRM route; return dict with 'coordinates', 'distance_km', 'duration_minutes', or None."""
    routes = get_route_options(origin, destination)
    return routes[0] if routes else None


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
