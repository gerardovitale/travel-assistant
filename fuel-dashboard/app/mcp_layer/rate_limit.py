import re
import threading
import time

from config import settings
from fastapi import Request
from fastapi.responses import JSONResponse
from net_utils import get_real_client_ip

_PERIOD_SECONDS = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}
_RATE_PATTERN = re.compile(r"^(\d+)\s*/\s*(second|minute|hour|day)s?$", re.IGNORECASE)


def _parse_rate(rate: str) -> tuple[int, int]:
    """Parse a slowapi-style "N/period" string (e.g. "30/minute") into (max_requests, window_seconds)."""
    match = _RATE_PATTERN.match(rate.strip())
    if not match:
        raise ValueError(f"Invalid rate limit string: {rate!r}")

    count, period = match.groups()
    return int(count), _PERIOD_SECONDS[period.lower()]


class _FixedWindowLimiter:
    """Coarse per-process, per-key fixed-window limiter.

    Not shared across replicas — fine for fuel-dashboard's single-container-on-a-Pi topology,
    unlike slowapi's REST limiter this deliberately doesn't try to be more than that. Exists
    only because slowapi's `@limiter.limit()` decorator pattern can't reach routes generated
    inside the mounted MCP ASGI sub-app (see the MCP gap-analysis doc).
    """

    # How often to sweep stale entries out of `_buckets`, in windows. Bounds memory to roughly
    # "distinct keys seen in the last ~2 windows" instead of every key ever seen for the life of
    # the process — relevant on a long-running, resource-constrained (Pi) deployment sitting
    # behind a public Cloudflare Tunnel, where scanners/bots each mint a new bucket entry.
    _PRUNE_EVERY_N_WINDOWS = 2

    def __init__(self, max_requests: int, window_seconds: int):
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._lock = threading.Lock()
        self._buckets: dict[str, tuple[float, int]] = {}
        self._prune_interval = window_seconds * self._PRUNE_EVERY_N_WINDOWS
        self._last_prune = time.monotonic()

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        with self._lock:
            self._prune_stale(now)
            window_start, count = self._buckets.get(key, (now, 0))
            if now - window_start >= self._window_seconds:
                window_start, count = now, 0
            count += 1
            self._buckets[key] = (window_start, count)
            return count <= self._max_requests

    def _prune_stale(self, now: float) -> None:
        """Evict buckets whose window has already lapsed. Called with `_lock` held."""
        if now - self._last_prune < self._prune_interval:
            return
        cutoff = now - self._window_seconds
        stale_keys = [key for key, (window_start, _) in self._buckets.items() if window_start < cutoff]
        for key in stale_keys:
            del self._buckets[key]
        self._last_prune = now


_limiter: _FixedWindowLimiter | None = None


def _get_limiter() -> _FixedWindowLimiter:
    global _limiter
    if _limiter is None:
        max_requests, window_seconds = _parse_rate(settings.mcp_rate_limit)
        _limiter = _FixedWindowLimiter(max_requests, window_seconds)
    return _limiter


async def mcp_rate_limit_middleware(request: Request, call_next):
    """Scoped in-memory rate limit for `/mcp` only; `/api/v1/...` etc. are unaffected."""
    if not request.url.path.startswith("/mcp"):
        return await call_next(request)

    key = get_real_client_ip(request)
    if not _get_limiter().allow(key):
        return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded for /mcp"})
    return await call_next(request)
