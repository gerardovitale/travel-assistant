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
