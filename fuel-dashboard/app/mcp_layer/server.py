from typing import Any

from api.schemas import BrandPriceComparisonRow
from api.schemas import BrandReportFuelType
from api.schemas import BrandWinRateRow
from api.schemas import Direction
from api.schemas import FuelType
from api.schemas import TrendPeriod
from config import settings
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from services.data_quality_service import get_quality_report
from services.forecast_service import get_historical_forecast
from services.geocoding import geocode_address
from services.station_service import get_brand_price_comparison_report
from services.station_service import get_brand_win_rate_report
from services.station_service import get_cheapest_by_address
from services.station_service import get_cheapest_by_zip
from services.station_service import get_price_trends
from services.station_service import get_report_available_brands

# Mounted at /mcp by main.py; streamable_http_path="/" so the mount prefix alone is the
# full MCP endpoint path (no /mcp/mcp double-nesting).
mcp = FastMCP(
    "fuel-precision",
    instructions=(
        "Spain fuel-station price data: current cheapest stations, historical price trends, a "
        "Markov-chain price-regime forecast, and brand win-rate/price-comparison reports. All "
        "prices are EUR per liter. Call list_report_brands before the brand report tools to learn "
        "valid brand codes for a fuel type."
    ),
    streamable_http_path="/",
    # FastMCP's DNS-rebinding protection defaults to allowing only localhost Host/Origin headers
    # (it's meant to stop malicious webpages reaching a loopback-bound dev server). We're mounted
    # behind Cloudflare Tunnel under the site's real hostname and gated by the X-MCP-API-Key
    # shared secret in mcp_layer/auth.py instead — that's the actual access control for this
    # server-to-server (non-browser) MCP client use case, so the Host-header check is disabled
    # rather than hardcoded to a hostname that would need updating on every domain change.
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)


@mcp.tool()
def find_cheapest_by_zip(
    zip_code: str, fuel_type: FuelType, limit: int = settings.default_limit, labels: list[str] | None = None
) -> list[dict]:
    """Find the cheapest fuel stations in a Spanish 5-digit postal code.

    Args:
        zip_code: 5-digit Spanish postal code, e.g. "28001".
        fuel_type: fuel type to compare.
        limit: max stations to return (1-20).
        labels: optional brand labels to filter by, e.g. ["repsol", "cepsa"].

    Returns stations sorted cheapest first. Prices are EUR/liter.
    """
    stations = get_cheapest_by_zip(zip_code, fuel_type, limit, labels=labels)
    return [s.model_dump(mode="json") for s in stations]


@mcp.tool()
def find_cheapest_by_address(
    address: str,
    fuel_type: FuelType,
    radius_km: float = settings.default_radius_km,
    limit: int = settings.default_limit,
    labels: list[str] | None = None,
) -> list[dict]:
    """Find the cheapest fuel stations within a radius of a free-text address.

    Geocodes the address first (Spain-focused). Returns an empty list if the address
    can't be geocoded or no stations fall within the radius.

    Args:
        address: free-text address/town, or a "lat,lon" string.
        fuel_type: fuel type to compare.
        radius_km: search radius in km (0.1-50).
        limit: max stations to return (1-20).
        labels: optional brand labels to filter by.
    """
    coords = geocode_address(address)
    if coords is None:
        return []
    lat, lon = coords
    stations = get_cheapest_by_address(lat, lon, fuel_type, radius_km, limit, labels=labels)
    return [s.model_dump(mode="json") for s in stations]


@mcp.tool()
def get_price_trend(
    fuel_type: FuelType,
    zip_code: str | None = None,
    province: str | None = None,
    period: TrendPeriod = TrendPeriod.month,
) -> list[dict]:
    """Get a daily average/min/max price time series.

    Args:
        fuel_type: fuel type.
        zip_code: 5-digit postal code; omit for the province/national aggregate.
        province: province name, used only when zip_code is omitted.
        period: "week", "month", "quarter", "half_year", or "year".

    Returns points ordered oldest to newest. Prices are EUR/liter.
    """
    trend = get_price_trends(zip_code, fuel_type, period, province=province)
    return [p.model_dump(mode="json") for p in trend]


@mcp.tool()
def forecast_price_regime(
    fuel_type: FuelType,
    zip_code: str | None = None,
    province: str | None = None,
) -> dict[str, Any]:
    """Forecast whether fuel prices in an area are cheap/normal/expensive right now, and the
    probability of a cheaper regime in the next 3-7 days, from a Markov chain fit on historical
    daily averages. Includes a Spanish-language recommendation ("Reposta hoy" = fill up now,
    "Puedes esperar" = you can wait) and explanation text.

    Args:
        fuel_type: fuel type.
        zip_code: 5-digit postal code (falls back to province if zip-level history is too thin).
        province: province name; required if zip_code is omitted.

    Either zip_code or province must be given. If neither has enough history, the response's
    `insufficient_data` field is true.
    """
    if not zip_code and not province:
        raise ValueError("zip_code or province is required")
    response = get_historical_forecast(fuel_type, zip_code=zip_code, province=province)
    return response.model_dump(mode="json")


@mcp.tool()
def list_report_brands(fuel_type: BrandReportFuelType) -> list[str]:
    """List brand codes with enough national coverage to appear in the win-rate and
    price-comparison reports for a fuel type. Call this before get_brand_win_rate or
    get_brand_price_comparison to learn valid brand codes (e.g. "repsol", "cepsa",
    "ballenoil") instead of guessing.

    Args:
        fuel_type: "gasoline_95_e5_price" or "diesel_a_price" (the only two report-backed types).
    """
    brands = get_report_available_brands(fuel_type.value)
    return brands or []


@mcp.tool()
def get_brand_win_rate(
    fuel_type: BrandReportFuelType, direction: Direction, brands: list[str] | None = None
) -> list[dict]:
    """Get how often each brand is the cheapest ("cheapest") or priciest ("priciest") station in
    its area, aggregated nationally. Call list_report_brands first for valid brand codes.

    Args:
        fuel_type: "gasoline_95_e5_price" or "diesel_a_price".
        direction: "cheapest" or "priciest".
        brands: up to 4 brand codes; omit to use the site's configured default brands.

    Returns rows with brand, win_rate_pct, appearances, and a confidence band
    (low/medium/high, based on sample size).
    """
    rows = get_brand_win_rate_report(fuel_type.value, direction.value, brands)
    return [BrandWinRateRow(**row).model_dump(mode="json") for row in (rows or [])]


@mcp.tool()
def get_brand_price_comparison(fuel_type: BrandReportFuelType, brands: list[str] | None = None) -> list[dict]:
    """Compare each brand's average price against the national market average. Call
    list_report_brands first for valid brand codes.

    Args:
        fuel_type: "gasoline_95_e5_price" or "diesel_a_price".
        brands: up to 4 brand codes; omit to use the site's configured default brands.

    Returns rows with brand, brand_avg_price, market_avg_price, price_delta_pct,
    days_below_market_pct, and a confidence band (low/medium/high). Prices EUR/liter.
    """
    rows = get_brand_price_comparison_report(fuel_type.value, brands)
    return [BrandPriceComparisonRow(**row).model_dump(mode="json") for row in (rows or [])]


@mcp.tool()
def get_data_quality_inventory() -> dict[str, Any]:
    """Get a data-quality summary: how many days/months/years of history are available, the
    most recent day's station/province/community/locality counts, any missing ingestion days,
    and realtime-feed status. Use this to gauge how much to trust the other tools' results.
    """
    return get_quality_report().model_dump(mode="json")


def build_mcp_route(path: str = "/mcp"):
    """A Starlette Route serving the raw MCP ASGI handler at the exact path `main.py` registers.

    Deliberately NOT `app.mount(path, mcp.streamable_http_app())`: FastMCP's own Starlette app only
    has a route at "/" (see `streamable_http_path` above), so a `Mount` strips the "/mcp" prefix
    before dispatching, the inner app sees an empty path, and Starlette's default redirect_slashes
    301/307s it back to "/mcp/" — a redirect every real client would transparently follow, but one
    that also makes each logical request cost 2 requests against `mcp_layer/rate_limit.py`'s
    scoped-by-path-prefix limiter. Building the raw ASGI handler and routing it directly at the
    exact "/mcp" path (the same technique FastMCP uses internally, and the one its own
    `session_manager` property docstring calls out for "mounting ... in a single FastAPI
    application") avoids the redirect entirely.

    `mcp.session_manager.run()` (used in main.py's lifespan) requires `streamable_http_app()` to
    have been called at least once first — this function does that as a side effect.
    """
    from mcp.server.fastmcp.server import StreamableHTTPASGIApp
    from starlette.routing import Route

    mcp.streamable_http_app()  # lazily creates mcp._session_manager
    return Route(path, endpoint=StreamableHTTPASGIApp(mcp.session_manager))
