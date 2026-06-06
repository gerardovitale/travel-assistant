import json
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest
from spain_fuel_api.client import fetch_fuel_stations
from spain_fuel_api.schema import get_expected_columns
from spain_fuel_api.transform import transform_to_dataframe
from tests.fixture import get_response_raw_data


class TestTransformToDataframe:
    def test_produces_expected_columns(self):
        df = transform_to_dataframe(get_response_raw_data())
        assert list(df.columns) == get_expected_columns()

    def test_drops_unmapped_horario_field(self):
        df = transform_to_dataframe(get_response_raw_data())
        assert "Horario" not in df.columns
        assert "horario" not in df.columns

    def test_float_conversion(self):
        df = transform_to_dataframe(get_response_raw_data(1))
        assert df["diesel_a_price"].iloc[0] == pytest.approx(1.450)
        assert df["latitude"].iloc[0] == pytest.approx(40.4168)

    def test_string_lowercase_and_strip(self):
        raw = get_response_raw_data(1)
        raw["ListaEESSPrecio"][0]["Provincia"] = "  MADRID  "
        df = transform_to_dataframe(raw)
        assert df["province"].iloc[0] == "madrid"

    def test_timestamp_is_utc_iso(self):
        df = transform_to_dataframe(get_response_raw_data(1))
        ts = df["timestamp"].iloc[0]
        assert "+00:00" in ts or ts.endswith("Z")

    def test_empty_price_becomes_nan(self):
        df = transform_to_dataframe(get_response_raw_data(1))
        assert pd.isna(df["biodiesel_price"].iloc[0])


class TestFetchFuelStations:
    @patch("spain_fuel_api.fetch.subprocess.run")
    def test_end_to_end_returns_dataframe(self, mock_run):
        raw = get_response_raw_data(3)
        mock_run.return_value = MagicMock(stdout=json.dumps(raw))
        df = fetch_fuel_stations(curl_timeout=10)
        assert list(df.columns) == get_expected_columns()
        assert len(df) == 3

    @patch("spain_fuel_api.fetch.subprocess.run")
    def test_bad_api_status_raises(self, mock_run):
        raw = get_response_raw_data()
        raw["ResultadoConsulta"] = "ERROR"
        mock_run.return_value = MagicMock(stdout=json.dumps(raw))
        with pytest.raises(ValueError, match="non-OK status"):
            fetch_fuel_stations(curl_timeout=10)
