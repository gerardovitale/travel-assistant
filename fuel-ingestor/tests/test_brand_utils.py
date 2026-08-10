import duckdb
from aggregator.brand_utils import normalize_brand
from aggregator.brand_utils import register_normalize_brand


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


class TestBrandFamilies:

    def test_moeve_collapses_onto_cepsa(self):
        # Cepsa is mid-rebrand to Moeve; both names must resolve to one key or the operator is
        # split across two brands in every report.
        assert normalize_brand("moeve") == "cepsa"
        assert normalize_brand("Moeve") == "cepsa"
        assert normalize_brand("cepsa") == "cepsa"
        assert normalize_brand("moeve-cepsa") == "cepsa"

    def test_dealer_labels_with_a_site_name_resolve_to_the_brand(self):
        for label in (
            "MOEVE TAHICHE I",
            "Moeve - Fierroil",
            "MOEVE COMMERCIAL, S.A.U.",
            "CEPSA LA MARINA",
            "cepsa a4 pinto 365",
            "gasolinera cepsa la caridad",
            "sutullena-cepsa",
            'inlocor s.l. "cepsa"',
            "grupo cacho - moeve",
        ):
            assert normalize_brand(label) == "cepsa", label

    def test_brand_name_wins_over_the_station_id_shape(self):
        # NON_BRAND_PATTERN would otherwise discard these, but they name a real brand.
        assert normalize_brand("es el caleyu nº 22905 cepsa") == "cepsa"
        assert normalize_brand("E.S. CEPSA CHIO") == "cepsa"

    def test_family_match_requires_a_whole_word(self):
        # substring matches must not pull unrelated brands into the family
        assert normalize_brand("cepsamania") == "cepsamania"
        assert normalize_brand("moeveland") == "moeveland"

    def test_other_brands_are_unaffected(self):
        assert normalize_brand("repsol") == "repsol"
        assert normalize_brand("galp") == "galp"
        assert normalize_brand("Nº 10.935") is None


class TestRegisterNormalizeBrand:

    def test_udf_returns_null_for_non_brand_labels(self):
        # Requires null_handling="special": under the DEFAULT policy DuckDB rejects the NULL
        # return and every report task using the UDF fails at query time.
        con = duckdb.connect()
        try:
            register_normalize_brand(con)
            rows = con.execute(
                "select normalize_brand(l) from (values ('Nº 10.935'), ('E.S. 123'), ('4571'), "
                "('  REPSOL '), ('CEPSA Estaciones de Servicio'), (NULL)) t(l)"
            ).fetchall()
            assert [r[0] for r in rows] == [None, None, None, "repsol", "cepsa", None]
        finally:
            con.close()

    def test_registration_is_idempotent_on_a_shared_connection(self):
        # Both report tasks register on the same connection; the second call must not raise.
        con = duckdb.connect()
        try:
            register_normalize_brand(con)
            register_normalize_brand(con)
            assert con.execute("select normalize_brand('Shell')").fetchone()[0] == "shell"
        finally:
            con.close()
