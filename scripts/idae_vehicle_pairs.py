# Hand-curated gasoline/diesel model pairs for the fuel-type report catalog.
#
# This is the human-judgment artifact in the ingest pipeline. IDAE's transformed dataset
# (``vehicles/transformed/idae_vehicle_consumption.parquet``) has ~24k passenger-car rows with
# free-text trim descriptions (``vehiculo``) that vary in wording between fuel variants of "the same"
# model -- different power outputs, gearboxes, trim badges, model years. Matching a gasoline row to
# its diesel sibling from that text is not safe to fully automate, so each pair here is defined by the
# substrings a human confirmed actually pick out one specific, comparable row per fuel type.
#
# ``idae_build_vehicle_catalog.py`` resolves each entry against the transformed parquet: every string
# in ``include`` must appear in ``vehiculo`` (case-insensitive) and none of ``exclude`` may; if that
# narrows to more than one distinct ``(vehiculo, consumo_mixto_wltp)`` combination, the build fails
# loudly instead of guessing -- see that script for the resolution rule. Tighten the tokens here to
# fix it.
#
# Selection so far favors models where IDAE actually carries a clean, single-row-per-fuel match with
# comparable trim/power AND comparable model year between the gasoline and diesel variant. Candidates
# that did not resolve cleanly were dropped rather than forced, per the ingest task's own instruction
# to drop what doesn't resolve cleanly:
#
# - SEAT León, Opel Corsa -- IDAE carries duplicate rows with the same trim text but different
#   consumption figures (ambiguous per `_resolve_row`'s own uniqueness check).
# - VW Tiguan -- no current-generation row pairs a plain-ICE gasoline trim against a diesel at
#   matching power.
# - Skoda Octavia -- only a performance RS gasoline trim and thin old data.
# - SEAT Ibiza -- resolved cleanly (one row per fuel), but the only diesel row is from 2019 against a
#   2025/MY26 gasoline row: a 6 model-year gap, which `build_catalog`'s generation-gap guard (see
#   `MAX_MODEL_YEAR_GAP` in idae_build_vehicle_catalog.py) would now refuse to ship anyway. Dropped
#   instead of force-accepted, matching the others above.
#
# ``market_status`` is hand-set here, not inferred from the data (IDAE's "still on sale" convention
# isn't something this script can safely guess): "vigente" means still sold new in Spain as of the
# last time this file was reviewed, "descontinuado" means it is not. Unlike the consumption figures,
# this is **not** sourced from IDAE or any citation -- it's the curator's general knowledge as of the
# date in each entry's comment below, and it can go stale silently (a model can be discontinued, or
# un-discontinued, without this file changing). Re-verify against a manufacturer/press source before
# relying on it for anything more than a UI hint, and update the comment's date when you do.
from __future__ import annotations

PAIRS: list[dict] = [
    {
        "pair_id": "vw-golf",
        "model": "Volkswagen Golf",
        "segment": "compacto",
        "marca_match": "Volkswagen",
        "modelo_match": "Golf",
        "gasoline": {
            "include": ["MY26", "1.5 TSI", "85 kW (115 CV)", "6 vel."],
            "exclude": ["Variant", "Life", "Style", "Ready2Go", "Aniversario", "eTSI", "Más"],
            "market_status": "vigente",
        },
        "diesel": {
            "include": ["MY26", "2.0 TDI", "85 kW (115 CV)", "6 vel."],
            "exclude": ["Variant", "R-Line", "Style", "Automatico", "Automático", "Más"],
            "market_status": "vigente",
        },
    },
    {
        "pair_id": "ford-focus",
        "model": "Ford Focus",
        "segment": "compacto",
        "marca_match": "Ford",
        "modelo_match": "Focus",
        "gasoline": {
            "include": ["Berlina 1.0 EcoBoost 125CV Man. Nuevo Trend+/ST-Line/ST-Line X/Active/ActiveX"],
            "exclude": [],
            # Unverified, checked 2026-08-20: Ford ended Focus production in 2025; no longer sold
            # new in Spain.
            "market_status": "descontinuado",
        },
        "diesel": {
            "include": ["Berlina 1.5 EcoBlue 120CV Man. Nuevo Trend+/ST-Line/ST-Line X/Active/Active X/Titanium"],
            "exclude": [],
            "market_status": "descontinuado",
        },
    },
    {
        "pair_id": "peugeot-308",
        "model": "Peugeot 308",
        "segment": "compacto",
        "marca_match": "Peugeot",
        "modelo_match": "308",
        "gasoline": {"include": ["5P Active Pack Puretech 130 S&S MAN"], "exclude": [], "market_status": "vigente"},
        "diesel": {"include": ["5P Active Pack BlueHDi 130 S&S MAN"], "exclude": [], "market_status": "vigente"},
    },
    {
        "pair_id": "renault-megane",
        "model": "Renault Megane",
        "segment": "compacto",
        "marca_match": "Renault",
        "modelo_match": "Megane",
        "gasoline": {
            "include": ["TCe 103 kW (140 cv) EDC GPF (semi: 2EA3 NBA6USA)"],
            "exclude": [],
            # Unverified, checked 2026-08-20: the ICE Mégane hatchback was discontinued in Spain
            # in favour of the Mégane E-Tech Electric.
            "market_status": "descontinuado",
        },
        "diesel": {
            "include": ["Blue dCi 85 kW (115CV) EDC (semi: 2EA2 A6A6USA)"],
            "exclude": [],
            "market_status": "descontinuado",
        },
    },
    {
        "pair_id": "nissan-qashqai",
        "model": "Nissan Qashqai",
        "segment": "suv_compacto",
        "marca_match": "Nissan",
        "modelo_match": "Qashqai",
        "gasoline": {
            "include": ["1.3DIG-T 140CV 2WD MT N-TEC"],
            "exclude": [],
            # Unverified, checked 2026-08-20: both rows are the previous (2019-2021) generation;
            # the current Qashqai in Spain is mild-hybrid/e-Power only, no plain-ICE diesel or
            # gasoline offering.
            "market_status": "descontinuado",
        },
        "diesel": {"include": ["1.5dCi 115CV 2WD N-TEC"], "exclude": [], "market_status": "descontinuado"},
    },
    {
        "pair_id": "hyundai-tucson",
        "model": "Hyundai Tucson",
        "segment": "suv_compacto",
        "marca_match": "Hyundai",
        "modelo_match": "Tucson",
        "gasoline": {"include": ["FL 1.6T 160CV KLASS"], "exclude": [], "market_status": "vigente"},
        "diesel": {"include": ["FL 1.6D 115CV KLASS"], "exclude": [], "market_status": "vigente"},
    },
    {
        "pair_id": "kia-sportage",
        "model": "Kia Sportage",
        "segment": "suv_compacto",
        "marca_match": "Kia",
        "modelo_match": "Sportage",
        "gasoline": {"include": ["1.6 T-GDi Concept 4X2 150CV"], "exclude": [], "market_status": "vigente"},
        "diesel": {"include": ["1.6 CRDi Concept 4X2 115CV"], "exclude": [], "market_status": "vigente"},
    },
    {
        "pair_id": "dacia-duster",
        "model": "Dacia Duster",
        "segment": "suv_compacto",
        "marca_match": "Dacia",
        "modelo_match": "Duster",
        "gasoline": {
            "include": ["TCe 110kW (150CV) 4X2 EDC (semi: D3 2 M3A 6US)"],
            "exclude": [],
            "market_status": "vigente",
        },
        "diesel": {
            "include": ["Blue dCi 85kW (115CV) 4X2 (semi: D2 2 ADM 6US)"],
            "exclude": [],
            "market_status": "vigente",
        },
    },
]
