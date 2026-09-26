import pytest
from commodity_api.validate import validate_api_response
from tests.fixture import get_response_raw_data


class TestValidateApiResponse:
    def test_valid_response_passes(self):
        validate_api_response(get_response_raw_data())

    def test_missing_observations_raises(self):
        raw = get_response_raw_data()
        raw["observations"] = None
        with pytest.raises(ValueError, match="missing or empty"):
            validate_api_response(raw)

    def test_empty_observations_raises(self):
        raw = get_response_raw_data()
        raw["observations"] = []
        with pytest.raises(ValueError, match="missing or empty"):
            validate_api_response(raw)

    def test_missing_date_field_raises(self):
        raw = {"observations": [{"value": "82.87"}]}
        with pytest.raises(ValueError, match="missing 'date'/'value'"):
            validate_api_response(raw)

    def test_missing_value_field_raises(self):
        raw = {"observations": [{"date": "2026-01-02"}]}
        with pytest.raises(ValueError, match="missing 'date'/'value'"):
            validate_api_response(raw)

    def test_dot_value_is_not_rejected(self):
        raw = {"observations": [{"date": "2026-01-03", "value": "."}]}
        validate_api_response(raw)
