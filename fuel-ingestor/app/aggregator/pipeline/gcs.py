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


def _dedup_keys(frame: pd.DataFrame, date_col: str, extra_key_cols: tuple[str, ...]) -> pd.Series:
    dates = pd.to_datetime(frame[date_col])
    if not extra_key_cols:
        return dates
    return pd.Series(list(zip(dates, *(frame[col] for col in extra_key_cols))), index=frame.index)


@dataclass
class IncrementalGCSParquetSink:
    """GCS parquet sink that merges new rows with existing data.

    Deduplicates on `date_col` plus `extra_key_cols` (removes existing rows whose full key
    matches an incoming row before appending) and optionally prunes rows older than
    `retention_days`. `extra_key_cols` defaults to empty for callers whose blob holds one
    row per date (the daily-stats aggregates); pass e.g. `("series_id",)` for a blob that
    holds multiple independent series per date (commodity_prices.parquet), so a re-fetch of
    one series doesn't drop another series' rows on the same dates.
    """

    bucket: Any
    blob_name: str
    date_col: str = "date"
    extra_key_cols: tuple[str, ...] = ()
    retention_days: Optional[int] = None

    def write(self, df: pd.DataFrame) -> None:
        if df.empty:
            logger.warning(f"incremental_sink_skipped_empty blob={self.blob_name!r}")
            return
        existing = GCSParquetSource(self.bucket, self.blob_name).read()
        if not existing.empty:
            # Drop existing rows whose key is present in the incoming df, not just its first
            # row's — supports both a single-day slice (most callers) and a multi-day window
            # (e.g. a rolling commodity-price re-fetch).
            incoming_dates = pd.to_datetime(df[self.date_col])
            incoming_keys = _dedup_keys(df, self.date_col, self.extra_key_cols)
            existing_keys = _dedup_keys(existing, self.date_col, self.extra_key_cols)
            existing = existing[~existing_keys.isin(incoming_keys)]
            if self.retention_days is not None:
                cutoff = incoming_dates.max() - pd.Timedelta(days=self.retention_days - 1)
                existing = existing[pd.to_datetime(existing[self.date_col]) >= cutoff]
            df = pd.concat([existing, df], ignore_index=True)
        GCSParquetSink(self.bucket, self.blob_name).write(df)
