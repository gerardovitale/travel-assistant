import io
import logging
import re
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import List
from typing import Optional

import pandas as pd
from config import settings
from google.cloud import storage

logger = logging.getLogger(__name__)

PARQUET_PATTERN = re.compile(r"spain_fuel_prices_(\d{4}-\d{2}-\d{2})T")


def _get_bucket():
    client = storage.Client(project=settings.gcp_project_id)
    return client.bucket(settings.gcs_bucket_name)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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
    files = list_parquet_files()
    return files[-1] if files else None


def download_parquet_as_df(blob_name: str) -> pd.DataFrame:
    bucket = _get_bucket()
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    return pd.read_parquet(io.BytesIO(data))


def download_parquets_as_df(blob_names: List[str]) -> pd.DataFrame:
    dfs = []
    for name in blob_names:
        logger.info(f"Downloading {name}")
        dfs.append(download_parquet_as_df(name))
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)
