import io
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import List
from typing import Optional

import pandas as pd
from config import settings
from google.cloud import storage

logger = logging.getLogger(__name__)

PARQUET_PATTERN = re.compile(r"spain_fuel_prices_(\d{4}-\d{2}-\d{2})T")

_client: Optional[storage.Client] = None
_bucket = None


def _get_bucket():
    global _client, _bucket
    if _bucket is None:
        _client = storage.Client(project=settings.gcp_project_id)
        _bucket = _client.bucket(settings.gcs_bucket_name)
    return _bucket


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_cache_dir() -> Path:
    cache_dir = Path(settings.parquet_cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _aggregate_cached_path(name: str) -> Path:
    blob_name = f"aggregates/{name}"
    safe_name = blob_name.replace("/", "_")
    return _get_cache_dir() / safe_name


def _is_today_file(blob_name: str) -> bool:
    match = PARQUET_PATTERN.search(blob_name)
    if not match:
        return False
    file_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
    return file_date == utcnow().date()


def _cached_download(blob_name: str) -> pd.DataFrame:
    """Download a parquet file, using local disk cache for historical (immutable) files."""
    cache_dir = _get_cache_dir()
    safe_name = blob_name.replace("/", "_")
    cached_path = cache_dir / safe_name

    if cached_path.exists():
        if _is_today_file(blob_name):
            age_hours = (time.time() - cached_path.stat().st_mtime) / 3600
            if age_hours < settings.parquet_cache_max_age_hours:
                logger.debug(f"Cache hit (today, fresh): {blob_name}")
                return pd.read_parquet(cached_path)
            logger.info(f"Cache stale (today, {age_hours:.1f}h old): {blob_name}")
        else:
            logger.debug(f"Cache hit (historical): {blob_name}")
            return pd.read_parquet(cached_path)

    bucket = _get_bucket()
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    df = pd.read_parquet(io.BytesIO(data))

    try:
        df.to_parquet(cached_path)
    except Exception:
        logger.warning(f"Failed to write cache file: {cached_path}", exc_info=True)

    return df


def list_parquet_files_with_metadata(
    days_back: Optional[int] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> List[dict]:
    """List parquet files with name, date, and size metadata."""
    bucket = _get_bucket()
    blobs = bucket.list_blobs(prefix="spain_fuel_prices_")

    if start_date is None and days_back is not None:
        start_date = utcnow() - timedelta(days=days_back)
    if end_date is None:
        end_date = utcnow()
    if start_date is not None:
        start_date = _to_utc(start_date)
    end_date = _to_utc(end_date)
    start_day = start_date.date() if start_date is not None else None
    end_day = end_date.date()

    files = []
    for blob in blobs:
        if not blob.name.endswith(".parquet"):
            continue
        match = PARQUET_PATTERN.search(blob.name)
        if not match:
            continue
        file_date_str = match.group(1)
        file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
        if start_day and not (start_day <= file_date <= end_day):
            continue
        files.append({"name": blob.name, "date": file_date_str, "size_bytes": blob.size or 0})

    logger.info(f"Found {len(files)} parquet files in GCS bucket")
    return sorted(files, key=lambda f: f["date"])


def list_parquet_files(
    days_back: Optional[int] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> List[str]:
    """List parquet file names, optionally filtered by date range."""
    return [f["name"] for f in list_parquet_files_with_metadata(days_back, start_date, end_date)]


def get_latest_parquet_file() -> Optional[str]:
    """Get the latest parquet file, checking today and yesterday first."""
    try:
        bucket = _get_bucket()
        now = utcnow()
        for days_ago in range(3):
            date_str = (now - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            prefix = f"spain_fuel_prices_{date_str}"
            blobs = list(bucket.list_blobs(prefix=prefix))
            parquets = sorted([b.name for b in blobs if b.name.endswith(".parquet")])
            if parquets:
                return parquets[-1]

        logger.warning("No recent parquet files found, falling back to full listing")
        files = list_parquet_files()
        return files[-1] if files else None
    except Exception:
        logger.warning("Failed to list parquet files from GCS (network error)", exc_info=True)
        return None


def download_parquet_as_df(blob_name: str) -> pd.DataFrame:
    return _cached_download(blob_name)


def download_parquets_as_df(blob_names: List[str]) -> pd.DataFrame:
    if not blob_names:
        return pd.DataFrame()

    max_workers = min(4, len(blob_names))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        dfs = list(executor.map(_cached_download, blob_names))

    return pd.concat(dfs, ignore_index=True)


def download_aggregate(name: str) -> Optional[pd.DataFrame]:
    """Download a pre-computed aggregate parquet file from GCS.

    Cached locally with a TTL of parquet_cache_max_age_hours (same as today's files).
    """
    blob_name = f"aggregates/{name}"
    cached_path = _aggregate_cached_path(name)

    if cached_path.exists():
        age_hours = (time.time() - cached_path.stat().st_mtime) / 3600
        if age_hours < settings.parquet_cache_max_age_hours:
            logger.debug(f"Aggregate cache hit (fresh): {blob_name}")
            return pd.read_parquet(cached_path)
        logger.info(f"Aggregate cache stale ({age_hours:.1f}h old): {blob_name}")

    try:
        bucket = _get_bucket()
        blob = bucket.blob(blob_name)
        if not blob.exists():
            logger.warning(f"Aggregate file not found: {blob_name}")
            return None
        data = blob.download_as_bytes()
    except Exception:
        if cached_path.exists():
            logger.warning(
                f"Failed to download aggregate {blob_name} (network error); serving stale cache", exc_info=True
            )
            return pd.read_parquet(cached_path)
        logger.warning(f"Failed to download aggregate {blob_name} (network error)", exc_info=True)
        return None

    df = pd.read_parquet(io.BytesIO(data))

    try:
        df.to_parquet(cached_path)
    except Exception:
        logger.warning(f"Failed to write aggregate cache: {cached_path}", exc_info=True)

    logger.info(f"Downloaded aggregate {blob_name} ({len(df)} rows)")
    return df


def get_aggregate_cache_age_seconds(name: str) -> Optional[float]:
    cached_path = _aggregate_cached_path(name)
    if not cached_path.exists():
        return None
    return max(0.0, time.time() - cached_path.stat().st_mtime)
