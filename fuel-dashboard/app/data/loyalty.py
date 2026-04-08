from typing import NamedTuple
from typing import Optional


class LoyaltyProgram(NamedTuple):
    program_name: str
    discount_eur_per_liter: float


LOYALTY_DISCOUNTS: dict[str, LoyaltyProgram] = {
    "repsol": LoyaltyProgram("Waylet", 0.03),
    "moeve": LoyaltyProgram("Club Moeve GOW", 0.05),
    "cepsa": LoyaltyProgram("Club Moeve GOW", 0.05),
    "galp": LoyaltyProgram("MundoGalp", 0.10),
    "bp": LoyaltyProgram("Mi BP", 0.03),
    "shell": LoyaltyProgram("Shell", 0.05),
}

LOYALTY_LABEL_ALIASES: dict[str, str] = {
    "cepsa estaciones de servicio": "cepsa",
    "repsol autogas": "repsol",
    "bp oil": "bp",
    "bp oil españa": "bp",
    "shell recharge": "shell",
    "galp energia": "galp",
}


def normalize_loyalty_label(label: str) -> Optional[str]:
    """Normalize raw API labels to the supported loyalty brand keys."""
    if not label:
        return None
    cleaned = label.lower().strip()
    if not cleaned:
        return None
    return LOYALTY_LABEL_ALIASES.get(cleaned, cleaned)


def get_loyalty_program(label: str) -> Optional[LoyaltyProgram]:
    """Return the loyalty program for a brand label, or None."""
    normalized_label = normalize_loyalty_label(label)
    return LOYALTY_DISCOUNTS.get(normalized_label) if normalized_label else None


def get_loyalty_discount(label: str) -> Optional[float]:
    """Return the loyalty discount in EUR/L for a brand, or None."""
    program = get_loyalty_program(label)
    return program.discount_eur_per_liter if program else None


def get_loyalty_price(label: str, price: float) -> Optional[float]:
    """Return the loyalty-adjusted price (price - discount), or None if no program exists."""
    discount = get_loyalty_discount(label)
    if discount is None:
        return None
    return round(price - discount, 3)


def format_loyalty_cell(label: str, price: float) -> str:
    """Format a loyalty price table cell: '1.420 (Waylet)' or '-'."""
    prog = get_loyalty_program(label)
    if prog is None:
        return "-"
    adjusted = round(price - prog.discount_eur_per_liter, 3)
    return f"{adjusted:.3f} ({prog.program_name})"
