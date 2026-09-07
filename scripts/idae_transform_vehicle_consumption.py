# Stage 2 of the IDAE vehicle-consumption ingest pipeline: clean the raw snapshot for reuse.
#
# Downloads the raw parquet stage 1 (idae_ingest_raw.py) uploaded to `vehicles/raw/`, and produces a
# filtered, normalized version at `vehicles/transformed/`:
#
# - keeps only passenger cars (``categoria == "M1"``) -- the raw file also carries vans, trucks,
#   motorbikes and buses under other IDAE ``categoria`` codes.
# - drops rows with no usable WLTP figure: either the column is genuinely empty (an old NEDC-only
#   row) or it is a liquid-fuel row reported as exactly 0, which IDAE's own data shows is a
#   data-entry gap rather than a real "burns nothing" car (electric rows legitimately report 0 and
#   are left alone). Either way this means a NEDC or bad-data figure can never be picked up
#   downstream and silently compared against a real WLTP one.
# - normalizes ``marca``: IDAE lists some brands under more than one name -- a region-specific
#   homologation/tax entry for the Canary Islands (e.g. "Volkswagen Canarias"), and for Volkswagen
#   specifically a passenger-car/commercial-vehicle split ("Volkswagen Turismos" vs "Volkswagen
#   Vehículos Comerciales", each also with its own "... Canarias" variant). The passenger-car alias
#   is collapsed onto the plain brand name; the two commercial-vehicle spellings are collapsed onto
#   each other (not onto "Volkswagen") so the commercial line stays one consistent string, distinct
#   from the passenger one, instead of silently splitting into two different marca values depending
#   on whether the Canarias suffix was present.
#
# This is still a generic, reusable cleaned dataset -- not paired or curated for the fuel-type report.
# That happens in stage 3 (idae_build_vehicle_catalog.py).
#
# Usage:
#     cd fuel-dashboard && uv run python ../scripts/idae_transform_vehicle_consumption.py
from __future__ import annotations

import pandas as pd
from idae_common import BUCKET_NAME
from idae_common import download_parquet
from idae_common import get_bucket
from idae_common import RAW_BLOB
from idae_common import TRANSFORMED_BLOB
from idae_common import upload_parquet

LIQUID_FUEL_MOTORIZACIONES = {"Gasolina", "Gasóleo"}

# Regional/line-of-business suffix IDAE appends to a brand name, and the aliases applied after
# stripping it. "Turismos" (Volkswagen's passenger-car line) collapses onto the bare brand name;
# the two commercial-vehicle spellings collapse onto one canonical string instead of the passenger
# brand, so the commercial line stays distinct but consistent. See module docstring.
REGIONAL_SUFFIX = " Canarias"
MARCA_ALIASES = {
    "Volkswagen Turismos": "Volkswagen",
    "Volkswagen Comerciales": "Volkswagen Vehículos Comerciales",
}


def normalize_marca(marca: str) -> str:
    if marca.endswith(REGIONAL_SUFFIX):
        marca = marca[: -len(REGIONAL_SUFFIX)]
    return MARCA_ALIASES.get(marca, marca)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df[df["categoria"] == "M1"].copy()
    print(f"Filtered to categoria=M1: {before} -> {len(df)} rows")

    is_liquid = df["motorizacion"].isin(LIQUID_FUEL_MOTORIZACIONES)
    missing_wltp = df["consumo_mixto_wltp"].isna()
    bad_zero_wltp = is_liquid & (df["consumo_mixto_wltp"] == 0)
    before = len(df)
    df = df[~(missing_wltp | bad_zero_wltp)].copy()
    print(
        f"Dropped rows with no usable WLTP figure (NEDC-only or zero-reported liquid fuel): {before} -> {len(df)} rows"
    )

    df["marca"] = df["marca"].map(normalize_marca)

    # A brand name that still contains a space after normalization is either a legitimate two-word
    # brand (Alfa Romeo, Land Rover -- fine) or a suffix this script doesn't know about yet (not
    # fine). Print them so a human can spot a new one on a future raw snapshot.
    multi_word_marcas = sorted({m for m in df["marca"].unique() if " " in m})
    print(f"Multi-word brand names after normalization (expected: real two-word brands only): {multi_word_marcas}")

    return df.reset_index(drop=True)


def main() -> None:
    bucket = get_bucket()

    print(f"Downloading gs://{BUCKET_NAME}/{RAW_BLOB} ...")
    raw_df = download_parquet(bucket, RAW_BLOB)
    print(f"Loaded {len(raw_df)} raw rows.")

    transformed_df = transform(raw_df)

    upload_parquet(bucket, TRANSFORMED_BLOB, transformed_df)
    print(f"Uploaded {len(transformed_df)} rows to gs://{BUCKET_NAME}/{TRANSFORMED_BLOB}")


if __name__ == "__main__":
    main()
