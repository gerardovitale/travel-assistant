import logging
from contextlib import asynccontextmanager
from datetime import datetime
from datetime import timezone
from pathlib import Path

from api.router import limiter
from api.router import router
from config import settings
from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from ui_test_support import health_data_response as ui_test_health_data_response
from ui_test_support import insights_flags as ui_test_insights_flags
from ui_test_support import is_data_ready as ui_test_is_data_ready
from ui_test_support import pop_fixture_set
from ui_test_support import push_fixture_set
from ui_test_support import resolve_fixture_set

from data.cache import get_realtime_status
from data.cache import is_data_ready
from data.cache import start_cache_refresh
from data.duckdb_engine import get_latest_data_timestamp
from data.duckdb_engine import refresh_zip_code_trend_snapshot
from data.gcs_client import get_latest_parquet_file
from data.gcs_client import PARQUET_PATTERN
from data.geojson_loader import load_provinces_geojson

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI):
    if settings.ui_test_mode:
        logger.info("UI test mode enabled; skipping cache warmup and external preloads")
        yield
        return

    logger.info("Preloading zip-code trend cache")
    trend_preloaded = False
    try:
        trend_preloaded = refresh_zip_code_trend_snapshot()
    except Exception:
        logger.exception("Failed to preload zip-code trend cache")
    logger.info("Starting cache refresh background task")
    start_cache_refresh(skip_initial_trend_refresh=trend_preloaded)
    logger.info("Preloading GeoJSON data")
    load_provinces_geojson()
    yield


app = FastAPI(title="Spain Fuel Prices Dashboard", version="1.0.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.include_router(router, prefix="/api/v1")


@app.middleware("http")
async def add_security_headers(request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache, must-revalidate"
    return response


@app.middleware("http")
async def bind_ui_test_fixture_set(request: Request, call_next):
    if not settings.ui_test_mode:
        return await call_next(request)

    token = push_fixture_set(resolve_fixture_set(request))
    try:
        return await call_next(request)
    finally:
        pop_fixture_set(token)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/health/data")
def health_data():
    if settings.ui_test_mode:
        status_code, body = ui_test_health_data_response()
        if status_code == 200:
            return body
        return JSONResponse(status_code=status_code, content=body)

    realtime = get_realtime_status()
    latest = get_latest_parquet_file()
    if latest is None and not realtime["realtime_active"]:
        return JSONResponse(status_code=503, content={"status": "error", "detail": "No parquet files found"})

    match = PARQUET_PATTERN.search(latest) if latest else None
    file_date_str = match.group(1) if match else "unknown"
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    source = "realtime" if realtime["realtime_active"] else "gcs"
    data_datetime = get_latest_data_timestamp() or file_date_str

    result = {
        "status": "ok",
        "source": source,
        "latest_file": latest,
        "file_date": file_date_str,
        "data_datetime": data_datetime,
        "realtime": realtime,
    }

    if source == "realtime":
        return result
    if file_date_str == today_str:
        return result
    return JSONResponse(
        status_code=503,
        content={
            "status": "stale",
            "source": source,
            "latest_file": latest,
            "file_date": file_date_str,
            "data_datetime": data_datetime,
            "expected_date": today_str,
        },
    )


_WEB_DIR = Path(__file__).parent / "web"
app.mount("/static", StaticFiles(directory=str(_WEB_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(_WEB_DIR / "templates"))


def _render_page(request: Request, template_name: str, current_page: str):
    data_ready = ui_test_is_data_ready() if settings.ui_test_mode else is_data_ready()
    if not data_ready:
        return templates.TemplateResponse(
            request,
            "loading.html",
            {
                "current_page": current_page,
                "disable_external_assets": settings.disable_external_assets or settings.ui_test_mode,
                "app_config": {
                    "ui_test_mode": settings.ui_test_mode,
                    "disable_geolocation_lookup": settings.ui_test_mode,
                },
            },
            status_code=503,
            headers={"Retry-After": "5"},
        )
    return templates.TemplateResponse(
        request,
        template_name,
        {
            "current_page": current_page,
            "disable_external_assets": settings.disable_external_assets or settings.ui_test_mode,
            "app_config": {
                "ui_test_mode": settings.ui_test_mode,
                "disable_geolocation_lookup": settings.ui_test_mode,
            },
        },
    )


@app.get("/")
def page_search(request: Request):
    return _render_page(request, "search.html", "search")


@app.get("/trip")
def page_trip(request: Request):
    return _render_page(request, "trip.html", "trip")


@app.get("/insights")
def page_insights(request: Request):
    data_ready = ui_test_is_data_ready() if settings.ui_test_mode else is_data_ready()
    if not data_ready:
        return templates.TemplateResponse(
            request,
            "loading.html",
            {
                "current_page": "insights",
                "disable_external_assets": settings.disable_external_assets or settings.ui_test_mode,
                "app_config": {
                    "ui_test_mode": settings.ui_test_mode,
                    "disable_geolocation_lookup": settings.ui_test_mode,
                },
            },
            status_code=503,
            headers={"Retry-After": "5"},
        )
    insights_zones_enabled, insights_historical_enabled = (
        ui_test_insights_flags()
        if settings.ui_test_mode
        else (settings.insights_zones_enabled, settings.insights_historical_enabled)
    )
    return templates.TemplateResponse(
        request,
        "insights.html",
        {
            "current_page": "insights",
            "disable_external_assets": settings.disable_external_assets or settings.ui_test_mode,
            "app_config": {
                "ui_test_mode": settings.ui_test_mode,
                "disable_geolocation_lookup": settings.ui_test_mode,
            },
            "insights_zones_enabled": insights_zones_enabled,
            "insights_historical_enabled": insights_historical_enabled,
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=settings.host, port=settings.port)
