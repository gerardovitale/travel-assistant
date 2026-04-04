import json
import subprocess
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest

from data.realtime_client import _transform_to_dataframe
from data.realtime_client import _validate_api_response
from data.realtime_client import fetch_realtime_stations


def _make_raw_api_response(n=3):
    stations = []
    for i in range(n):
        stations.append(
            {
                "C.P.": f"2800{i}",
                "IDEESS": str(4000 + i),
                "IDCCAA": "13",
                "IDMunicipio": str(100 + i),
                "IDProvincia": "28",
                "Tipo Venta": "P",
                "Rótulo": f"Station {i}",
                "Dirección": f"Calle {i}",
                "Municipio": "Madrid",
                "Provincia": "MADRID",
                "Localidad": "MADRID",
                "Latitud": f"40,{4168 + i}",
                "Longitud (WGS84)": f"-3,{7038 + i}",
                "Precio Biodiesel": "",
                "Precio Bioetanol": "",
                "Precio Gas Natural Comprimido": "",
                "Precio Gas Natural Licuado": "",
                "Precio Gases licuados del petróleo": "",
                "Precio Gasoleo A": f"1,{450 + i * 5}",
                "Precio Gasoleo B": "",
                "Precio Gasoleo Premium": "",
                "Precio Gasolina 95 E10": "",
                "Precio Gasolina 95 E5": f"1,{550 + i * 5}",
                "Precio Gasolina 95 E5 Premium": "",
                "Precio Gasolina 98 E10": "",
                "Precio Gasolina 98 E5": "",
                "Precio Hidrogeno": "",
            }
        )
    return {
        "ResultadoConsulta": "OK",
        "Fecha": "04/04/2026 10:30:00",
        "ListaEESSPrecio": stations,
        "Nota": "Test data",
    }


class TestValidateApiResponse:
    def test_valid_response(self):
        raw = _make_raw_api_response()
        _validate_api_response(raw)

    def test_non_ok_status_raises(self):
        raw = _make_raw_api_response()
        raw["ResultadoConsulta"] = "ERROR"
        with pytest.raises(ValueError, match="non-OK status"):
            _validate_api_response(raw)

    def test_empty_stations_raises(self):
        raw = _make_raw_api_response()
        raw["ListaEESSPrecio"] = []
        with pytest.raises(ValueError, match="missing or empty"):
            _validate_api_response(raw)

    def test_missing_stations_raises(self):
        raw = _make_raw_api_response()
        del raw["ListaEESSPrecio"]
        with pytest.raises(ValueError, match="missing or empty"):
            _validate_api_response(raw)

    def test_missing_fecha_raises(self):
        raw = _make_raw_api_response()
        del raw["Fecha"]
        with pytest.raises(ValueError, match="missing 'Fecha'"):
            _validate_api_response(raw)

    def test_bad_fecha_format_raises(self):
        raw = _make_raw_api_response()
        raw["Fecha"] = "2026-04-04"
        with pytest.raises(ValueError, match="unexpected format"):
            _validate_api_response(raw)


class TestTransformToDataframe:
    def test_produces_expected_columns(self):
        raw = _make_raw_api_response()
        df = _transform_to_dataframe(raw)
        from data.entity_maps import get_expected_columns

        assert list(df.columns) == get_expected_columns()

    def test_float_conversion(self):
        raw = _make_raw_api_response(1)
        df = _transform_to_dataframe(raw)
        assert df["diesel_a_price"].iloc[0] == pytest.approx(1.450)
        assert df["latitude"].iloc[0] == pytest.approx(40.4168)

    def test_string_lowercase_and_strip(self):
        raw = _make_raw_api_response(1)
        raw["ListaEESSPrecio"][0]["Provincia"] = "  MADRID  "
        df = _transform_to_dataframe(raw)
        assert df["province"].iloc[0] == "madrid"

    def test_timestamp_is_utc_iso(self):
        raw = _make_raw_api_response(1)
        df = _transform_to_dataframe(raw)
        ts = df["timestamp"].iloc[0]
        assert "+00:00" in ts or ts.endswith("Z")

    def test_empty_price_becomes_nan(self):
        raw = _make_raw_api_response(1)
        df = _transform_to_dataframe(raw)
        assert pd.isna(df["biodiesel_price"].iloc[0])


class TestFetchRealtimeStations:
    @patch("data.realtime_client.MIN_EXPECTED_STATIONS", 1)
    @patch("data.realtime_client.subprocess.run")
    def test_successful_fetch(self, mock_run):
        raw = _make_raw_api_response(3)
        mock_run.return_value = MagicMock(stdout=json.dumps(raw))
        df = fetch_realtime_stations(curl_timeout=10)
        assert df is not None
        assert len(df) == 3

    @patch("data.realtime_client.MIN_EXPECTED_STATIONS", 10)
    @patch("data.realtime_client.subprocess.run")
    def test_undersized_payload_returns_none(self, mock_run):
        raw = _make_raw_api_response(9)
        mock_run.return_value = MagicMock(stdout=json.dumps(raw))
        df = fetch_realtime_stations(curl_timeout=10)
        assert df is None

    @patch("data.realtime_client.time.sleep")
    @patch("data.realtime_client.subprocess.run")
    def test_curl_failure_returns_none(self, mock_run, mock_sleep):
        mock_run.side_effect = subprocess.CalledProcessError(1, "curl")
        df = fetch_realtime_stations(curl_timeout=10)
        assert df is None

    @patch("data.realtime_client.time.sleep")
    @patch("data.realtime_client.subprocess.run")
    def test_curl_timeout_returns_none(self, mock_run, mock_sleep):
        mock_run.side_effect = subprocess.TimeoutExpired("curl", 10)
        df = fetch_realtime_stations(curl_timeout=10)
        assert df is None

    @patch("data.realtime_client.subprocess.run")
    def test_invalid_json_returns_none(self, mock_run):
        mock_run.return_value = MagicMock(stdout="not json")
        df = fetch_realtime_stations(curl_timeout=10)
        assert df is None

    @patch("data.realtime_client.subprocess.run")
    def test_bad_api_status_returns_none(self, mock_run):
        raw = _make_raw_api_response()
        raw["ResultadoConsulta"] = "ERROR"
        mock_run.return_value = MagicMock(stdout=json.dumps(raw))
        df = fetch_realtime_stations(curl_timeout=10)
        assert df is None

    @patch("data.realtime_client.time.sleep")
    @patch("data.realtime_client.MIN_EXPECTED_STATIONS", 1)
    @patch("data.realtime_client.subprocess.run")
    def test_retries_on_failure_then_succeeds(self, mock_run, mock_sleep):
        raw = _make_raw_api_response(3)
        mock_run.side_effect = [
            subprocess.CalledProcessError(1, "curl"),
            MagicMock(stdout=json.dumps(raw)),
        ]
        df = fetch_realtime_stations(curl_timeout=10)
        assert df is not None
        assert len(df) == 3
        mock_sleep.assert_called_once_with(10)
