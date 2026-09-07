# Shared constants and GCS helpers for the IDAE vehicle-consumption ingest pipeline.
#
# Three stages, each its own script, meant to be run in order and independently re-runnable:
#
#     idae_ingest_raw.py                    -> vehicles/raw/idae_vehicle_consumption.parquet
#     idae_transform_vehicle_consumption.py -> vehicles/transformed/idae_vehicle_consumption.parquet
#     idae_build_vehicle_catalog.py         -> fuel-dashboard/app/data/vehicle_catalog.json
#
# See idae-consumption-ingest-task.md for why this replaces the hand-written catalog, and the plan
# this was built from for why the pipeline is shaped this way.
from __future__ import annotations

from io import BytesIO

import pandas as pd
from google.cloud import storage

# Same bucket the rest of the project already uses for fuel-price data (see fuel-dashboard's
# `gcs_bucket_name` setting and scripts/download_fuel_data.py). Vehicle-consumption data gets its
# own top-level prefix rather than a new bucket -- it's a different dataset, not different infra.
BUCKET_NAME = "travel-assistant-spain-fuel-prices"

RAW_BLOB = "vehicles/raw/idae_vehicle_consumption.parquet"
RAW_META_BLOB = "vehicles/raw/idae_vehicle_consumption.meta.json"
TRANSFORMED_BLOB = "vehicles/transformed/idae_vehicle_consumption.parquet"

# The CSV's own URL changes every time IDAE publishes a new semester (the filename embeds it, e.g.
# idae-historico-202606.csv); this page is where the current one is always linked from.
IDAE_GUIDE_PAGE_URL = "https://coches.idae.es/guia-emisiones-consumos"


def get_bucket() -> storage.Bucket:
    return storage.Client().bucket(BUCKET_NAME)


def upload_parquet(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame) -> None:
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)
    bucket.blob(blob_name).upload_from_file(buffer, content_type="application/octet-stream")


def download_parquet(bucket: storage.Bucket, blob_name: str) -> pd.DataFrame:
    data = bucket.blob(blob_name).download_as_bytes()
    return pd.read_parquet(BytesIO(data))
