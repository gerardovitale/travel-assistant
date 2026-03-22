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


def list_parquet_files(
    days_back: Optional[int] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> List[str]:
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
        if match and start_day:
            file_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
            if start_day <= file_date <= end_day:
                files.append(blob.name)
        elif start_day is None:
            files.append(blob.name)

    logger.info(f"Found {len(files)} parquet files in GCS bucket")
    return sorted(files)


def get_latest_parquet_file() -> Optional[str]:
    """Get the latest parquet file, checking today and yesterday first."""
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


def download_parquet_as_df(blob_name: str) -> pd.DataFrame:
    return _cached_download(blob_name)


def download_parquets_as_df(blob_names: List[str]) -> pd.DataFrame:
    if not blob_names:
        return pd.DataFrame()

    max_workers = min(4, len(blob_names))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        dfs = list(executor.map(_cached_download, blob_names))

    return pd.concat(dfs, ignore_index=True)
