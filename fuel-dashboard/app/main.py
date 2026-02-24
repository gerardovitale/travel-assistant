import logging
from contextlib import asynccontextmanager

from api.router import router
from config import settings
from fastapi import FastAPI
from ui.pages import init_ui

from data.cache import start_cache_refresh

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.info("Starting cache refresh background task")
    start_cache_refresh()
    yield


app = FastAPI(title="Spain Fuel Prices Dashboard", version="1.0.0", lifespan=lifespan)
app.include_router(router, prefix="/api/v1")

init_ui(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=settings.host, port=settings.port)
