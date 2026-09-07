# Stage 3 of the IDAE vehicle-consumption ingest pipeline: curate the fuel-type report catalog.
#
# Downloads the transformed parquet stage 2 (idae_transform_vehicle_consumption.py) uploaded to
# `vehicles/transformed/`, resolves each entry in the hand-curated `PAIRS` list
# (idae_vehicle_pairs.py) against it, and writes the resulting `Vehicle` records to the committed
# `fuel-dashboard/app/data/vehicle_catalog.json` -- the one artifact in this pipeline that lives in
# git, not GCS, because it's small and reviewed like any other source change.
#
# Resolution rule, deliberately dumb and auditable: every string in a fuel variant's ``include`` list
# must appear in the row's ``vehiculo`` text (case-insensitive substring), none of ``exclude`` may.
# That should narrow the (already M1-only, WLTP-only) transformed dataset to rows sharing one
# ``vehiculo`` string. IDAE's data sometimes repeats the exact same ``vehiculo`` text with a different
# ``consumo_mixto_wltp`` value (seemingly a duplicate submission, not two different cars) -- so the
# real uniqueness check is on the *(vehiculo, consumo_mixto_wltp)* pair, not the text alone. Zero
# matches, or more than one distinct pair, is treated as an unresolved match and raises -- the script
# never guesses which one is right; `idae_vehicle_pairs.py` gets tightened by hand instead.
#
# Two more things are checked before a catalog is allowed to ship: the built dict is validated through
# the real `VehicleCatalog` Pydantic model (so consumption bounds, unit/energy consistency etc. fail
# the *build*, not a later `pytest` run), and every pair's two model years are checked against
# `MAX_MODEL_YEAR_GAP` (so a pair that resolves cleanly but compares a 2025 trim against a 2019 one --
# not "same generation" by any reasonable reading -- gets rejected rather than silently shipped).
#
# Usage:
#     cd fuel-dashboard && uv run python ../scripts/idae_build_vehicle_catalog.py
from __future__ import annotations

import json
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from idae_common import BUCKET_NAME
from idae_common import download_parquet
from idae_common import get_bucket
from idae_common import IDAE_GUIDE_PAGE_URL
from idae_common import TRANSFORMED_BLOB
from idae_ingest_raw import load_stored_metadata
from idae_vehicle_pairs import PAIRS

_REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = _REPO_ROOT / "fuel-dashboard" / "app" / "data" / "vehicle_catalog.json"

# So `from services.vehicle_catalog import VehicleCatalog` below resolves regardless of how this
# script is invoked (its own directory is on sys.path automatically; fuel-dashboard/app is not).
sys.path.insert(0, str(_REPO_ROOT / "fuel-dashboard" / "app"))
from services.vehicle_catalog import VehicleCatalog  # noqa: E402  (needs the sys.path insert above)

FUEL_MOTORIZACION = {"gasoline": "Gasolina", "diesel": "Gasóleo"}
CONSUMPTION_UNIT = "l/100km"

# A pair spanning more than this many model years isn't "same model, same generation, different
# engine" by any reasonable reading -- WLTP methodology itself has picked up correction refinements
# over a multi-year span, so "both WLTP" doesn't mean "measured the same way". Two years covers a
# mid-cycle facelift where the engine is genuinely unchanged; more than that and the pair should be
# dropped from idae_vehicle_pairs.py, not force-shipped.
MAX_MODEL_YEAR_GAP = 2


class UnresolvedPairError(RuntimeError):
    pass


class GenerationMismatchError(RuntimeError):
    pass


def _resolve_row(df: pd.DataFrame, marca_match: str, modelo_match: str, fuel: str, spec: dict) -> pd.Series:
    """``marca`` is matched exactly (idae_transform_vehicle_consumption.py normalizes it to one
    canonical string per brand, so exact equality is safe and precise). ``modelo`` is matched by
    substring, deliberately: IDAE's `modelo` column carries generation/trim suffixes inline (e.g.
    "Golf 8", "GOLF MY18", "Duster JD1", "Nuevo Duster (fase 2) JD1" are all distinct `modelo`
    values for what a human calls "the Golf"/"the Duster") -- there is no single exact string that
    matches all of them. The substring risk this creates (e.g. "308" matching "3008", a different
    Peugeot model) is mitigated by matching within the already marca-filtered subset first, and is
    covered by a regression test in test_idae_ingest_pipeline.py.
    """
    sub = df[
        (df["marca"] == marca_match)
        & (df["modelo"].str.contains(modelo_match, case=False, na=False, regex=False))
        & (df["motorizacion"] == FUEL_MOTORIZACION[fuel])
    ]
    for token in spec["include"]:
        sub = sub[sub["vehiculo"].str.contains(token, case=False, na=False, regex=False)]
    for token in spec["exclude"]:
        sub = sub[~sub["vehiculo"].str.contains(token, case=False, na=False, regex=False)]

    distinct = sub[["vehiculo", "consumo_mixto_wltp"]].drop_duplicates()
    if len(distinct) == 0:
        raise UnresolvedPairError(
            f"{marca_match} {modelo_match} [{fuel}]: no row matched include={spec['include']} exclude={spec['exclude']}"
        )
    if len(distinct) > 1:
        raise UnresolvedPairError(
            f"{marca_match} {modelo_match} [{fuel}]: ambiguous match for include={spec['include']} "
            f"exclude={spec['exclude']} -- {len(distinct)} distinct (vehiculo, consumo) candidates:\n"
            + distinct.to_string(index=False)
        )

    # Several IDAE rows can share the exact same (vehiculo, consumo_mixto_wltp) -- historical
    # re-submissions of the same trim. Take the most recently updated one -- fecha_actualizacion,
    # not fecha_alta (that's when the row was first added, used separately for model_year below).
    matches = sub[sub["vehiculo"] == distinct.iloc[0]["vehiculo"]].copy()
    matches["fecha_actualizacion_dt"] = pd.to_datetime(
        matches["fecha_actualizacion"], format="%d/%m/%Y", errors="coerce"
    )
    return matches.sort_values("fecha_actualizacion_dt", ascending=False).iloc[0]


def _clean_variant(vehiculo: str) -> str:
    # Manufacturer-internal option codes in parentheses aren't meaningful to a reader comparing
    # fuel cost -- strip them, keep everything else from IDAE's own description untouched.
    return re.sub(r"\s*\(semi[^)]*\)", "", vehiculo).strip()


def _model_year(row: pd.Series) -> int:
    """Prefer fecha_comercializacion (the actual on-sale date) over fecha_alta (when IDAE's own
    database row was added, which can lag or lead the real on-sale date). fecha_comercializacion is
    only populated on ~7.5% of rows though, so fecha_alta is the necessary fallback for the rest."""
    commercial = pd.to_datetime(row.get("fecha_comercializacion"), format="%d/%m/%Y", errors="coerce")
    if pd.notna(commercial):
        return int(commercial.year)
    return int(pd.to_datetime(row["fecha_alta"], format="%d/%m/%Y").year)


def build_vehicle(pair: dict, fuel: str, row: pd.Series, csv_url: str, reviewed_on: date) -> dict:
    spec = pair[fuel]
    return {
        "id": f"{pair['pair_id']}-{fuel}",
        "pair_id": pair["pair_id"],
        "model": pair["model"],
        "variant": _clean_variant(str(row["vehiculo"])),
        "model_year": _model_year(row),
        "segment": pair["segment"],
        "energy_type": fuel,
        "consumption": float(row["consumo_mixto_wltp"]),
        "consumption_unit": CONSUMPTION_UNIT,
        "source_url": csv_url,
        "cycle": "WLTP",
        "reviewed_on": reviewed_on.isoformat(),
        "market_status": spec["market_status"],
    }


def _check_generation_gap(pair: dict, vehicles: list[dict]) -> None:
    years = {v["model_year"] for v in vehicles}
    gap = max(years) - min(years)
    if gap > MAX_MODEL_YEAR_GAP:
        raise GenerationMismatchError(
            f"{pair['pair_id']}: model years {sorted(years)} span {gap} years (max allowed "
            f"{MAX_MODEL_YEAR_GAP}) -- not the same generation, drop this pair from "
            "idae_vehicle_pairs.py instead of shipping it."
        )


def build_catalog(df: pd.DataFrame, csv_url: str, reviewed_on: date, data_fetched_on: str | None) -> dict:
    vehicles = []
    for pair in PAIRS:
        pair_vehicles = [
            build_vehicle(
                pair,
                fuel,
                _resolve_row(df, pair["marca_match"], pair["modelo_match"], fuel, pair[fuel]),
                csv_url,
                reviewed_on,
            )
            for fuel in ("gasoline", "diesel")
        ]
        _check_generation_gap(pair, pair_vehicles)
        vehicles.extend(pair_vehicles)

    # Two dates matter and are not the same thing: `reviewed_on` (today, when this build ran) and
    # when IDAE actually published the data it's built from. Surfacing only the former would read
    # as "these numbers are as fresh as today" even when the raw snapshot is from months ago and
    # nobody has re-run idae_ingest_raw.py since.
    freshness = f"datos IDAE de {data_fetched_on}" if data_fetched_on else "fecha de los datos IDAE desconocida"
    version = f"{reviewed_on.isoformat()} (IDAE, {freshness})"

    return {
        "version": version,
        "source": "IDAE — Base de Datos de Consumo de Carburante y Emisiones",
        "source_url": csv_url,
        "notes": (
            "Consumos WLTP mixtos tal y como los publica IDAE. Selección: pares gasolina/diésel de "
            "modelos populares en España donde IDAE ofrece una fila comparable (mismo acabado, "
            "potencia aproximada y año de modelo) para ambos motores; generado por "
            "scripts/idae_build_vehicle_catalog.py a partir de scripts/idae_vehicle_pairs.py."
        ),
        "vehicles": vehicles,
    }


def main() -> None:
    bucket = get_bucket()
    print(f"Downloading gs://{BUCKET_NAME}/{TRANSFORMED_BLOB} ...")
    df = download_parquet(bucket, TRANSFORMED_BLOB)
    print(f"Loaded {len(df)} transformed rows.")

    # The transformed dataset doesn't carry a single "source" URL for the whole snapshot -- reuse
    # the raw metadata sidecar's csv_url so the catalog cites exactly what was fetched, not a
    # freshly re-resolved (and possibly now-different) IDAE link. Same sidecar's fetched_at is the
    # data-freshness date surfaced in `version` (see build_catalog's docstring note above).
    raw_metadata = load_stored_metadata(bucket)
    csv_url = raw_metadata["csv_url"] if raw_metadata else IDAE_GUIDE_PAGE_URL
    data_fetched_on = None
    if raw_metadata and raw_metadata.get("fetched_at"):
        data_fetched_on = pd.to_datetime(raw_metadata["fetched_at"]).date().isoformat()

    reviewed_on = date.today()
    catalog = build_catalog(df, csv_url, reviewed_on, data_fetched_on)

    # Validate through the real model before writing anything: a structural problem (a pair failing
    # the consumption bounds, a duplicate id, a unit/energy mismatch) fails this run, not a pytest
    # run someone might not think to trigger after hand-editing idae_vehicle_pairs.py.
    VehicleCatalog(**catalog)

    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=4, ensure_ascii=False)
        f.write("\n")

    print(f"Wrote {len(catalog['vehicles'])} vehicles ({len(PAIRS)} pairs) to {CATALOG_PATH}")
    print()
    print(f"{'pair_id':<18}{'gasoline':>10}{'diesel':>10}{'ratio':>8}")
    by_pair: dict[str, dict[str, float]] = {}
    for v in catalog["vehicles"]:
        by_pair.setdefault(v["pair_id"], {})[v["energy_type"]] = v["consumption"]
    for pair_id, consumptions in by_pair.items():
        gasoline = consumptions.get("gasoline")
        diesel = consumptions.get("diesel")
        ratio = gasoline / diesel if gasoline and diesel else float("nan")
        print(f"{pair_id:<18}{gasoline:>10.2f}{diesel:>10.2f}{ratio:>8.2f}")
    print()
    print("Spot-check a few of these against coches.idae.es before shipping.")


if __name__ == "__main__":
    main()
