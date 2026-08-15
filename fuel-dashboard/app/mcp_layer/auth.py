import hmac
import logging

from config import settings
from fastapi import Request
from fastapi.responses import JSONResponse
from net_utils import get_real_client_ip

logger = logging.getLogger(__name__)


async def mcp_auth_middleware(request: Request, call_next):
    """Gate `/mcp` behind a shared-secret header; every other path passes through untouched.

    Fails closed: `main.py` only mounts `/mcp` when `settings.mcp_api_key` is set, so this
    middleware never runs unauthenticated in practice — but it re-checks the key on every
    request anyway rather than trusting the mount-time check alone.
    """
    if not request.url.path.startswith("/mcp"):
        return await call_next(request)

    configured_key = settings.mcp_api_key or ""
    provided_key = request.headers.get("X-MCP-API-Key", "")
    if not configured_key or not hmac.compare_digest(provided_key, configured_key):
        logger.warning("Rejected unauthenticated /mcp request from %s", get_real_client_ip(request))
        return JSONResponse(status_code=401, content={"detail": "Missing or invalid X-MCP-API-Key"})

    return await call_next(request)
