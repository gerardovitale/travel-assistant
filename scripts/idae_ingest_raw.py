# Stage 1 of the IDAE vehicle-consumption ingest pipeline: capture the published CSV as-is.
#
# Fetches IDAE's public "Base de Datos de Consumo de Carburante y Emisiones" CSV, converts it to
# parquet with no filtering or transformation (only lower-casing column names, which are already
# underscore-separated in the source), and uploads it to GCS under `vehicles/raw/`. A metadata
# sidecar records exactly which file was fetched, so a re-run first checks whether IDAE has published
# something newer before re-uploading -- this dataset changes semi-annually, not daily, so most runs
# should be a cheap no-op.
#
# Stages 2 (idae_transform_vehicle_consumption.py) and 3 (idae_build_vehicle_catalog.py) are never
# triggered automatically from here: re-pairing model variants after new data lands needs a human
# look, so a "new data available" print is the extent of the automation.
#
# Usage:
#     cd fuel-dashboard && uv run python ../scripts/idae_ingest_raw.py [--force] [--check-only]
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from datetime import timezone
from io import BytesIO

import httpx
import pandas as pd
from idae_common import BUCKET_NAME
from idae_common import get_bucket
from idae_common import IDAE_GUIDE_PAGE_URL
from idae_common import RAW_BLOB
from idae_common import RAW_META_BLOB
from idae_common import upload_parquet

CSV_FILENAME_PATTERN = re.compile(r"idae-historico-(\d{6})\.csv")


def resolve_current_csv_url() -> str:
    """Scrape the guide page for whichever CSV IDAE currently links -- the filename embeds the
    semester (e.g. idae-historico-202606.csv) and changes each time IDAE republishes.

    The page has only ever linked one such file at a time (checked at the time this was written),
    but if IDAE ever lists more than one (e.g. a "previous semester" link alongside the current
    one), picking the first match in page-source order would silently lock onto the wrong one.
    Guard against that by taking the newest YYYYMM found instead of assuming there's only one.
    """
    response = httpx.get(IDAE_GUIDE_PAGE_URL, follow_redirects=True, timeout=30)
    response.raise_for_status()
    matches = CSV_FILENAME_PATTERN.findall(response.text)
    if not matches:
        raise RuntimeError(f"Could not find an idae-historico-*.csv link on {IDAE_GUIDE_PAGE_URL}")
    newest_yyyymm = max(matches)
    return f"https://coches.idae.es/storage/csv/idae-historico-{newest_yyyymm}.csv"


def load_stored_metadata(bucket) -> dict | None:
    blob = bucket.blob(RAW_META_BLOB)
    if not blob.exists():
        return None
    return json.loads(blob.download_as_text())


def probe(csv_url: str) -> tuple[str, str]:
    """HEAD the CSV to get its Last-Modified/ETag without downloading the ~17MB body."""
    response = httpx.head(csv_url, follow_redirects=True, timeout=30)
    response.raise_for_status()
    return response.headers.get("last-modified", ""), response.headers.get("etag", "")


def is_up_to_date(stored: dict | None, csv_url: str, last_modified: str) -> bool:
    return stored is not None and stored.get("csv_url") == csv_url and stored.get("last_modified") == last_modified


def check_status(bucket) -> tuple[bool, str, str, str, dict | None]:
    """Resolve the current CSV, probe its headers, and compare against stored metadata -- shared
    by `ingest()` and `check_only()` so there's one place that defines "is there new data"."""
    csv_url = resolve_current_csv_url()
    stored = load_stored_metadata(bucket)
    last_modified, etag = probe(csv_url)
    return is_up_to_date(stored, csv_url, last_modified), csv_url, last_modified, etag, stored


def ingest(force: bool = False) -> None:
    bucket = get_bucket()
    up_to_date, csv_url, last_modified, etag, stored = check_status(bucket)

    if up_to_date and not force:
        print(f"Up to date: {csv_url} (last-modified {last_modified}). Nothing to do.")
        return

    print(f"Fetching {csv_url} ...")
    response = httpx.get(csv_url, follow_redirects=True, timeout=120)
    response.raise_for_status()
    df = pd.read_csv(BytesIO(response.content))
    df.columns = [c.lower() for c in df.columns]

    print(f"Fetched {len(df)} rows, {len(df.columns)} columns. Uploading raw parquet ...")
    upload_parquet(bucket, RAW_BLOB, df)

    metadata = {
        "csv_url": csv_url,
        "last_modified": last_modified,
        "etag": etag,
        "row_count": len(df),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    bucket.blob(RAW_META_BLOB).upload_from_string(json.dumps(metadata, indent=2), content_type="application/json")

    print(f"Uploaded gs://{BUCKET_NAME}/{RAW_BLOB}")
    if stored is not None:
        print(
            "New IDAE data detected -- re-run idae_transform_vehicle_consumption.py and "
            "idae_build_vehicle_catalog.py, then spot-check the result before shipping."
        )


def check_only() -> None:
    bucket = get_bucket()
    up_to_date, csv_url, last_modified, _etag, _stored = check_status(bucket)

    if up_to_date:
        print(f"Up to date: {csv_url} (last-modified {last_modified}).")
    else:
        print(f"New data available: {csv_url} (last-modified {last_modified}). Run without --check-only to fetch it.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch IDAE's raw vehicle-consumption CSV and upload it to GCS as-is.")
    parser.add_argument("--force", action="store_true", help="Re-fetch and re-upload even if nothing changed.")
    parser.add_argument("--check-only", action="store_true", help="Only report whether new data is available.")
    args = parser.parse_args()

    if args.check_only:
        check_only()
    else:
        ingest(force=args.force)
