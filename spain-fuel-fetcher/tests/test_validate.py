import pytest
from spain_fuel_api.validate import validate_api_response
from tests.fixture import get_response_raw_data


class TestValidateApiResponse:
    def test_valid_response_passes(self):
        validate_api_response(get_response_raw_data())

    def test_non_ok_status_raises(self):
        raw = get_response_raw_data()
        raw["ResultadoConsulta"] = "ERROR"
        with pytest.raises(ValueError, match="non-OK status"):
            validate_api_response(raw)

    def test_missing_lista_raises(self):
        raw = get_response_raw_data()
        raw["ListaEESSPrecio"] = None
        with pytest.raises(ValueError, match="missing or empty"):
            validate_api_response(raw)

    def test_empty_lista_raises(self):
        raw = get_response_raw_data()
        raw["ListaEESSPrecio"] = []
        with pytest.raises(ValueError, match="missing or empty"):
            validate_api_response(raw)

    def test_missing_fecha_raises(self):
        raw = get_response_raw_data()
        del raw["Fecha"]
        with pytest.raises(ValueError, match="missing 'Fecha'"):
            validate_api_response(raw)

    def test_bad_fecha_format_raises(self):
        raw = get_response_raw_data()
        raw["Fecha"] = "2026-04-04"
        with pytest.raises(ValueError, match="unexpected format"):
            validate_api_response(raw)
