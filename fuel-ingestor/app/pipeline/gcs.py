from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class GCSParquetSource:
    bucket: Any
    blob_name: str
    columns: Optional[List[str]] = None

    def read(self) -> pd.DataFrame:
        blob = self.bucket.blob(self.blob_name)
        if not blob.exists():
            logger.warning(f"gcs_blob_missing blob={self.blob_name!r}")
            return pd.DataFrame()
        data = blob.download_as_bytes()
        df = pd.read_parquet(io.BytesIO(data), columns=self.columns)
        logger.info(f"gcs_read blob={self.blob_name!r} rows={len(df)}")
        return df


@dataclass
class GCSParquetSink:
    bucket: Any
    blob_name: str

    def write(self, df: pd.DataFrame) -> None:
        blob = self.bucket.blob(self.blob_name)
        blob.upload_from_string(df.to_parquet(index=False, compression="snappy"), "application/octet-stream")
        logger.info(f"gcs_write blob={self.blob_name!r} rows={len(df)}")


@dataclass
class DataFrameSource:
    df: pd.DataFrame

    def read(self) -> pd.DataFrame:
        return self.df


@dataclass
class CallableSource:
    """Adapts any zero-argument callable that returns a DataFrame to the Source protocol."""

    fn: Callable[[], pd.DataFrame]

    def read(self) -> pd.DataFrame:
        return self.fn()


@dataclass
class CallableSink:
    """Adapts any single-argument callable to the Sink protocol."""

    fn: Callable[[pd.DataFrame], None]

    def write(self, df: pd.DataFrame) -> None:
        self.fn(df)


@dataclass
class IncrementalGCSParquetSink:
    """GCS parquet sink that merges new rows with existing data.

    Deduplicates on `date_col` (removes existing rows for today before appending)
    and optionally prunes rows older than `retention_days`.
    """

    bucket: Any
    blob_name: str
    date_col: str = "date"
    retention_days: Optional[int] = None

    def write(self, df: pd.DataFrame) -> None:
        if df.empty:
            logger.warning(f"incremental_sink_skipped_empty blob={self.blob_name!r}")
            return
        existing = GCSParquetSource(self.bucket, self.blob_name).read()
        if not existing.empty:
            # Assumes all rows in df share the same date — callers pass one day's slice at a time.
            date_val = pd.Timestamp(df[self.date_col].iloc[0])
            existing = existing[pd.to_datetime(existing[self.date_col]) != date_val]
            if self.retention_days is not None:
                cutoff = date_val - pd.Timedelta(days=self.retention_days - 1)
                existing = existing[pd.to_datetime(existing[self.date_col]) >= cutoff]
            df = pd.concat([existing, df], ignore_index=True)
        GCSParquetSink(self.bucket, self.blob_name).write(df)
