import logging
from contextlib import asynccontextmanager
from datetime import datetime
from datetime import timezone

from api.router import limiter
from api.router import router
from config import settings
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from ui.pages import init_ui

from data.cache import start_cache_refresh
from data.gcs_client import get_latest_parquet_file
from data.gcs_client import PARQUET_PATTERN
from data.geojson_loader import load_provinces_geojson

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.info("Starting cache refresh background task")
    start_cache_refresh()
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
    return response


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/health/data")
def health_data():
    latest = get_latest_parquet_file()
    if latest is None:
        return JSONResponse(status_code=503, content={"status": "error", "detail": "No parquet files found"})

    match = PARQUET_PATTERN.search(latest)
    file_date_str = match.group(1) if match else "unknown"
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if file_date_str == today_str:
        return {"status": "ok", "latest_file": latest, "file_date": file_date_str}

    return JSONResponse(
        status_code=503,
        content={"status": "stale", "latest_file": latest, "file_date": file_date_str, "expected_date": today_str},
    )


init_ui(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=settings.host, port=settings.port)
