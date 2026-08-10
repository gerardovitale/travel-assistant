import re
from typing import Optional

import duckdb

NON_BRAND_PATTERN = re.compile(
    r"^("
    r"n\.?[ºo°.]?\s*\d"  # "Nº 10.935", "No 123", "N.º 456", etc.
    r"|\d{3,}"  # purely numeric IDs with 3+ digits
    r"|estacion\s+n"  # "estacion n..."
    r"|e\.?\s*s\.?\s*\d"  # "E.S. 123", "ES 456"
    r")",
    re.IGNORECASE,
)

BRAND_ALIASES = {
    "cepsa estaciones de servicio": "cepsa",
    "repsol autogas": "repsol",
    "bp oil": "bp",
    "bp oil españa": "bp",
    "shell recharge": "shell",
    "galp energia": "galp",
    "avia operador": "avia",
    "costco wholesale": "costco",
}

MIN_STATION_COUNT = 10

# Brand families whose stations reach the feed under several names, so an exact BRAND_ALIASES
# lookup cannot collapse them. Matched as a whole word anywhere in the label, which also catches
# the dealer labels that wrap the brand in a site name ("moeve tahiche i", "sutullena-cepsa").
#
# cepsa: Cepsa is rebranding to Moeve and the feed carries both names mid-migration (Oct 2024:
# 1323 cepsa / 0 moeve; May 2026: 608 / 562), which otherwise splits one operator across two
# brands in every report. "cepsa" stays the canonical key because the whole historical series is
# labelled that way and it is the key in DASHBOARD_REPORT_BRANDS and in shared ?brands= URLs;
# the dashboard renders it as "Moeve (Cepsa)". Flip the key here once the migration completes.
BRAND_FAMILIES = [
    (re.compile(r"\b(?:cepsa|moeve)\b"), "cepsa"),
]


def normalize_brand(label: str) -> Optional[str]:
    """Return normalized brand name, or None if not a real brand."""
    if not label or not isinstance(label, str):
        return None
    cleaned = label.strip().lower()
    if not cleaned:
        return None
    # Before the non-brand check: a label naming a brand is a brand even when it also carries a
    # station-ID shape ("es el caleyu nº 22905 cepsa").
    for pattern, canonical in BRAND_FAMILIES:
        if pattern.search(cleaned):
            return canonical
    if NON_BRAND_PATTERN.match(cleaned):
        return None
    return BRAND_ALIASES.get(cleaned, cleaned)


def register_normalize_brand(con) -> None:
    """Register normalize_brand as a DuckDB scalar UDF. Idempotent across shared connections."""
    try:
        # null_handling="special": normalize_brand returns None for non-brand labels ("Nº 10.935",
        # "E.S. 123", numeric IDs). Under the DEFAULT policy DuckDB rejects a NULL return and the
        # whole report task fails. "special" also passes NULL inputs through, which the helper handles.
        con.create_function("normalize_brand", normalize_brand, ["VARCHAR"], "VARCHAR", null_handling="special")
    except (duckdb.NotImplementedException, duckdb.CatalogException):
        # Already registered on this connection (reports share one con) — safe to ignore.
        # Narrower than a bare except so a genuine signature/type error still surfaces.
        pass
