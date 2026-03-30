import re
from typing import Optional

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
}

MIN_STATION_COUNT = 10


def normalize_brand(label: str) -> Optional[str]:
    """Return normalized brand name, or None if not a real brand."""
    if not label or not isinstance(label, str):
        return None
    cleaned = label.strip().lower()
    if not cleaned:
        return None
    if NON_BRAND_PATTERN.match(cleaned):
        return None
    return BRAND_ALIASES.get(cleaned, cleaned)
