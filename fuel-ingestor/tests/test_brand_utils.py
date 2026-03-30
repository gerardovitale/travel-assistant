from brand_utils import normalize_brand


class TestNormalizeBrand:

    def test_lowercase_and_strip(self):
        assert normalize_brand("  REPSOL  ") == "repsol"
        assert normalize_brand("Shell") == "shell"

    def test_numbered_station_ids_filtered(self):
        assert normalize_brand("Nº 10.935") is None
        assert normalize_brand("No 123") is None
        assert normalize_brand("N.º 456") is None
        assert normalize_brand("12345") is None
        assert normalize_brand("Estacion N1") is None
        assert normalize_brand("E.S. 789") is None
        assert normalize_brand("ES 456") is None

    def test_known_aliases_normalized(self):
        assert normalize_brand("CEPSA ESTACIONES DE SERVICIO") == "cepsa"
        assert normalize_brand("Repsol Autogas") == "repsol"
        assert normalize_brand("BP Oil") == "bp"
        assert normalize_brand("BP Oil España") == "bp"
        assert normalize_brand("Galp Energia") == "galp"

    def test_real_brand_names_pass_through(self):
        assert normalize_brand("repsol") == "repsol"
        assert normalize_brand("shell") == "shell"
        assert normalize_brand("bp") == "bp"
        assert normalize_brand("cepsa") == "cepsa"
        assert normalize_brand("naturgy") == "naturgy"

    def test_empty_or_none_returns_none(self):
        assert normalize_brand("") is None
        assert normalize_brand("   ") is None
        assert normalize_brand(None) is None

    def test_non_string_returns_none(self):
        assert normalize_brand(123) is None
