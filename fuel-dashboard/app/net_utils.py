from fastapi import Request


def get_real_client_ip(request: Request) -> str:
    """Best-effort client IP, trusting Cloudflare Tunnel's header since that's the only ingress path."""
    # request.client is None when the ASGI scope carries no client address. This feeds the rate
    # limiter's key function, so letting that raise would fail the request rather than the lookup.
    return (
        request.headers.get("CF-Connecting-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or (request.client.host if request.client else "")
        or "unknown"
    )
