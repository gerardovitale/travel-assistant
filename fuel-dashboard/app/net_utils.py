from fastapi import Request


def get_real_client_ip(request: Request) -> str:
    """Best-effort client IP, trusting Cloudflare Tunnel's header since that's the only ingress path."""
    return (
        request.headers.get("CF-Connecting-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.client.host
    )
