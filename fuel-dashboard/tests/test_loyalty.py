from data.loyalty import format_loyalty_cell
from data.loyalty import get_loyalty_discount
from data.loyalty import get_loyalty_price
from data.loyalty import get_loyalty_program
from data.loyalty import LOYALTY_DISCOUNTS
from data.loyalty import normalize_loyalty_label


def test_get_loyalty_program_known_brand():
    prog = get_loyalty_program("repsol")
    assert prog is not None
    assert prog.program_name == "Waylet"
    assert prog.discount_eur_per_liter == 0.03


def test_get_loyalty_program_unknown_brand():
    assert get_loyalty_program("ballenoil") is None


def test_get_loyalty_program_empty_and_none():
    assert get_loyalty_program("") is None
    assert get_loyalty_program(None) is None


def test_normalize_loyalty_label_maps_known_raw_aliases():
    assert normalize_loyalty_label(" CEPSA ESTACIONES DE SERVICIO ") == "cepsa"
    assert normalize_loyalty_label("Repsol Autogas") == "repsol"


def test_get_loyalty_program_normalizes_raw_brand_labels():
    prog = get_loyalty_program("Repsol Autogas")
    assert prog is not None
    assert prog.program_name == "Waylet"


def test_get_loyalty_discount_known_brands():
    assert get_loyalty_discount("repsol") == 0.03
    assert get_loyalty_discount("moeve") == 0.05
    assert get_loyalty_discount("cepsa") == 0.05
    assert get_loyalty_discount("galp") == 0.10
    assert get_loyalty_discount("bp") == 0.03
    assert get_loyalty_discount("shell") == 0.05


def test_get_loyalty_discount_unknown_brand():
    assert get_loyalty_discount("petroprix") is None


def test_get_loyalty_price_with_discount():
    price = get_loyalty_price("repsol", 1.450)
    assert price == 1.420


def test_get_loyalty_price_without_discount():
    assert get_loyalty_price("ballenoil", 1.450) is None


def test_get_loyalty_price_rounds_to_3_decimals():
    price = get_loyalty_price("galp", 1.555)
    assert price == 1.455


def test_moeve_and_cepsa_share_same_program():
    moeve = LOYALTY_DISCOUNTS["moeve"]
    cepsa = LOYALTY_DISCOUNTS["cepsa"]
    assert moeve.program_name == cepsa.program_name
    assert moeve.discount_eur_per_liter == cepsa.discount_eur_per_liter


def test_format_loyalty_cell_known_brand():
    assert format_loyalty_cell("repsol", 1.450) == "1.420 (Waylet)"


def test_format_loyalty_cell_normalizes_known_alias():
    assert format_loyalty_cell("CEPSA ESTACIONES DE SERVICIO", 1.450) == "1.400 (Club Moeve GOW)"


def test_format_loyalty_cell_unknown_brand():
    assert format_loyalty_cell("ballenoil", 1.450) == "-"
