# Unit tests for the scripts/idae_*.py ingest pipeline (importable via the "../scripts" pythonpath
# entry in pyproject.toml). These exercise the pure/near-pure logic -- ambiguity detection, brand
# normalization, WLTP filtering, the "is there new data" check -- without touching GCS or the network.
#
# The full pipeline (fetch -> transform -> curate) is also run manually against the real IDAE CSV and
# production GCS bucket before shipping a catalog update; these tests are the fast, CI-visible
# regression net for the logic that run depends on, not a substitute for that manual pass.
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
import pytest
from idae_build_vehicle_catalog import _check_generation_gap
from idae_build_vehicle_catalog import _clean_variant
from idae_build_vehicle_catalog import _model_year
from idae_build_vehicle_catalog import _resolve_row
from idae_build_vehicle_catalog import build_vehicle
from idae_build_vehicle_catalog import GenerationMismatchError
from idae_build_vehicle_catalog import UnresolvedPairError
from idae_ingest_raw import is_up_to_date
from idae_ingest_raw import resolve_current_csv_url
from idae_transform_vehicle_consumption import normalize_marca
from idae_transform_vehicle_consumption import transform

# --- idae_transform_vehicle_consumption.normalize_marca ---------------------------------


def test_normalize_marca_strips_canarias_suffix():
    assert normalize_marca("Skoda Canarias") == "Skoda"


def test_normalize_marca_collapses_volkswagen_turismos_onto_the_bare_brand():
    assert normalize_marca("Volkswagen Turismos") == "Volkswagen"


def test_normalize_marca_collapses_both_commercial_line_spellings_together():
    # "Volkswagen Comerciales Canarias" -> strip suffix -> "Volkswagen Comerciales" -> aliased.
    # Must land on the same string as the non-Canarias form, and NOT on the passenger brand.
    assert normalize_marca("Volkswagen Comerciales Canarias") == "Volkswagen Vehículos Comerciales"
    assert normalize_marca("Volkswagen Vehículos Comerciales") == "Volkswagen Vehículos Comerciales"


def test_normalize_marca_leaves_real_two_word_brands_alone():
    assert normalize_marca("Alfa Romeo") == "Alfa Romeo"
    assert normalize_marca("Land Rover") == "Land Rover"


# --- idae_transform_vehicle_consumption.transform ----------------------------------------


def _raw_row(**overrides) -> dict:
    base = {
        "categoria": "M1",
        "marca": "SEAT",
        "modelo": "Ibiza",
        "motorizacion": "Gasolina",
        "consumo_mixto_wltp": 5.4,
    }
    return {**base, **overrides}


def test_transform_drops_non_passenger_categories():
    df = pd.DataFrame([_raw_row(categoria="M1"), _raw_row(categoria="N1")])
    result = transform(df)
    assert list(result["categoria"]) == ["M1"]


def test_transform_drops_nedc_only_rows_with_no_wltp_figure():
    df = pd.DataFrame([_raw_row(consumo_mixto_wltp=5.4), _raw_row(consumo_mixto_wltp=None)])
    result = transform(df)
    assert len(result) == 1
    assert result.iloc[0]["consumo_mixto_wltp"] == 5.4


def test_transform_drops_zero_reported_liquid_fuel_rows():
    # A zero WLTP figure on a gasoline/diesel row is IDAE data-entry noise, not a real "burns
    # nothing" car -- must be dropped just like a missing figure.
    df = pd.DataFrame([_raw_row(motorizacion="Gasolina", consumo_mixto_wltp=0.0)])
    result = transform(df)
    assert len(result) == 0


def test_transform_keeps_zero_reported_electric_rows():
    # Electric vehicles legitimately report 0 l/100km -- the zero-drop rule is liquid-fuel-only.
    df = pd.DataFrame([_raw_row(motorizacion="Eléctricos puros", consumo_mixto_wltp=0.0)])
    result = transform(df)
    assert len(result) == 1


def test_transform_normalizes_marca_in_place():
    df = pd.DataFrame([_raw_row(marca="Volkswagen Turismos")])
    result = transform(df)
    assert result.iloc[0]["marca"] == "Volkswagen"


# --- idae_build_vehicle_catalog._resolve_row ----------------------------------------------


def _transformed_row(**overrides) -> dict:
    base = {
        "marca": "Volkswagen",
        "modelo": "Golf",
        "motorizacion": "Gasolina",
        "vehiculo": "MY26 Golf 1.5 TSI 85 kW (115 CV) 6 vel.",
        "consumo_mixto_wltp": 5.4,
        "fecha_alta": "01/01/2020",
        "fecha_actualizacion": "01/01/2020",
    }
    return {**base, **overrides}


def _spec(include: list[str], exclude: list[str] | None = None) -> dict:
    return {"include": include, "exclude": exclude or [], "market_status": "vigente"}


def test_resolve_row_returns_the_single_clean_match():
    df = pd.DataFrame([_transformed_row()])
    row = _resolve_row(df, "Volkswagen", "Golf", "gasoline", _spec(["1.5 TSI"]))
    assert row["vehiculo"] == "MY26 Golf 1.5 TSI 85 kW (115 CV) 6 vel."


def test_resolve_row_raises_when_nothing_matches():
    df = pd.DataFrame([_transformed_row()])
    with pytest.raises(UnresolvedPairError, match="no row matched"):
        _resolve_row(df, "Volkswagen", "Golf", "gasoline", _spec(["2.0 TDI"]))


def test_resolve_row_raises_when_multiple_distinct_rows_match():
    df = pd.DataFrame(
        [
            _transformed_row(vehiculo="MY26 Golf 1.5 TSI 85 kW (115 CV) 6 vel.", consumo_mixto_wltp=5.4),
            _transformed_row(vehiculo="MY26 Golf Life 1.5 TSI 85 kW (115 CV) 6 vel.", consumo_mixto_wltp=5.55),
        ]
    )
    with pytest.raises(UnresolvedPairError, match="ambiguous match"):
        _resolve_row(df, "Volkswagen", "Golf", "gasoline", _spec(["1.5 TSI", "85 kW (115 CV)"]))


def test_resolve_row_raises_when_identical_text_carries_different_consumption():
    # IDAE sometimes repeats a vehiculo string with a different consumo value -- same text is not
    # enough to call it unambiguous, the (vehiculo, consumo) pair must be unique too.
    df = pd.DataFrame(
        [
            _transformed_row(consumo_mixto_wltp=5.4),
            _transformed_row(consumo_mixto_wltp=5.5),
        ]
    )
    with pytest.raises(UnresolvedPairError, match="ambiguous match"):
        _resolve_row(df, "Volkswagen", "Golf", "gasoline", _spec(["1.5 TSI"]))


def test_resolve_row_excludes_tokens_that_should_not_appear():
    df = pd.DataFrame(
        [
            _transformed_row(vehiculo="MY26 Golf 1.5 TSI 85 kW (115 CV) 6 vel."),
            _transformed_row(vehiculo="MY26 Golf Life 1.5 TSI 85 kW (115 CV) 6 vel."),
        ]
    )
    row = _resolve_row(df, "Volkswagen", "Golf", "gasoline", _spec(["1.5 TSI"], exclude=["Life"]))
    assert row["vehiculo"] == "MY26 Golf 1.5 TSI 85 kW (115 CV) 6 vel."


def test_resolve_row_picks_the_most_recently_updated_duplicate():
    # Same (vehiculo, consumo) repeated across historical re-submissions -- the tie-break is
    # fecha_actualizacion (most recently updated), not fecha_alta (first added).
    df = pd.DataFrame(
        [
            _transformed_row(fecha_alta="01/01/2022", fecha_actualizacion="01/01/2022"),
            _transformed_row(fecha_alta="01/01/2019", fecha_actualizacion="01/01/2024"),
        ]
    )
    row = _resolve_row(df, "Volkswagen", "Golf", "gasoline", _spec(["1.5 TSI"]))
    assert row["fecha_actualizacion"] == "01/01/2024"


def test_resolve_row_modelo_substring_does_not_collide_with_a_different_model():
    # "308" matching by substring must not also pick up a "3008" row -- a different Peugeot model.
    # This is the collision _resolve_row's docstring claims is avoided by matching within an
    # already marca-filtered subset; pin it down instead of relying on it happening to hold.
    df = pd.DataFrame(
        [
            _transformed_row(marca="Peugeot", modelo="308", vehiculo="308 Active PureTech 130"),
            _transformed_row(
                marca="Peugeot", modelo="3008", vehiculo="3008 Active PureTech 130", consumo_mixto_wltp=6.9
            ),
        ]
    )
    row = _resolve_row(df, "Peugeot", "308", "gasoline", _spec(["PureTech 130"]))
    assert row["modelo"] == "308"
    assert row["consumo_mixto_wltp"] == 5.4


# --- idae_build_vehicle_catalog._clean_variant / build_vehicle ---------------------------


def test_clean_variant_strips_the_manufacturer_option_code():
    assert _clean_variant("TCe 110kW (150CV) 4X2 EDC (semi: D3 2 M3A 6US)") == "TCe 110kW (150CV) 4X2 EDC"


def test_clean_variant_keeps_other_parenthetical_content():
    assert _clean_variant("1.5 TSI 85 kW (115 CV) 6 vel.") == "1.5 TSI 85 kW (115 CV) 6 vel."


def test_build_vehicle_derives_model_year_from_fecha_alta():
    pair = {"pair_id": "vw-golf", "model": "Volkswagen Golf", "segment": "compacto", "gasoline": _spec(["x"])}
    row = pd.Series(_transformed_row(fecha_alta="15/03/2023"))
    from datetime import date

    vehicle = build_vehicle(pair, "gasoline", row, "https://example.com/idae.csv", date(2026, 8, 20))
    assert vehicle["model_year"] == 2023
    assert vehicle["id"] == "vw-golf-gasoline"
    assert vehicle["source_url"] == "https://example.com/idae.csv"
    assert vehicle["cycle"] == "WLTP"
    assert vehicle["reviewed_on"] == "2026-08-20"


# --- idae_build_vehicle_catalog._model_year / _check_generation_gap ----------------------


def test_model_year_falls_back_to_fecha_alta_when_fecha_comercializacion_is_missing():
    row = pd.Series(_transformed_row(fecha_alta="15/03/2023", fecha_comercializacion=None))
    assert _model_year(row) == 2023


def test_model_year_prefers_fecha_comercializacion_when_present():
    # The actual on-sale date is the semantically correct one -- only ~7.5% of real IDAE rows carry
    # it, but when they do it should win over fecha_alta (when the DB row was merely added).
    row = pd.Series(_transformed_row(fecha_alta="15/03/2023", fecha_comercializacion="01/01/2024"))
    assert _model_year(row) == 2024


def test_check_generation_gap_allows_a_small_facelift_gap():
    pair = {"pair_id": "vw-golf"}
    vehicles = [{"model_year": 2024}, {"model_year": 2025}]
    _check_generation_gap(pair, vehicles)  # does not raise


def test_check_generation_gap_raises_when_pair_spans_too_many_years():
    pair = {"pair_id": "seat-ibiza"}
    vehicles = [{"model_year": 2025}, {"model_year": 2019}]
    with pytest.raises(GenerationMismatchError, match="seat-ibiza"):
        _check_generation_gap(pair, vehicles)


# --- idae_ingest_raw.is_up_to_date / resolve_current_csv_url ------------------------------


def test_is_up_to_date_false_when_nothing_stored():
    assert is_up_to_date(None, "https://x/csv", "Mon, 01 Jan 2026") is False


def test_is_up_to_date_false_when_url_differs():
    stored = {"csv_url": "https://x/old.csv", "last_modified": "Mon, 01 Jan 2026"}
    assert is_up_to_date(stored, "https://x/new.csv", "Mon, 01 Jan 2026") is False


def test_is_up_to_date_false_when_last_modified_differs():
    stored = {"csv_url": "https://x/csv", "last_modified": "Mon, 01 Jan 2026"}
    assert is_up_to_date(stored, "https://x/csv", "Tue, 02 Jan 2026") is False


def test_is_up_to_date_true_when_url_and_last_modified_match():
    stored = {"csv_url": "https://x/csv", "last_modified": "Mon, 01 Jan 2026"}
    assert is_up_to_date(stored, "https://x/csv", "Mon, 01 Jan 2026") is True


def test_resolve_current_csv_url_picks_the_newest_when_the_page_lists_more_than_one():
    # Regression guard: must not just take the first regex match in page-source order.
    html = """
        <a href="/storage/csv/idae-historico-202501.csv">anterior</a>
        <a href="/storage/csv/idae-historico-202606.csv">actual</a>
    """
    fake_response = Mock(text=html)
    fake_response.raise_for_status = Mock()
    with patch("idae_ingest_raw.httpx.get", return_value=fake_response):
        url = resolve_current_csv_url()
    assert url == "https://coches.idae.es/storage/csv/idae-historico-202606.csv"


def test_resolve_current_csv_url_raises_when_no_link_found():
    fake_response = Mock(text="<html>no csv link here</html>")
    fake_response.raise_for_status = Mock()
    with patch("idae_ingest_raw.httpx.get", return_value=fake_response):
        with pytest.raises(RuntimeError, match="Could not find"):
            resolve_current_csv_url()
