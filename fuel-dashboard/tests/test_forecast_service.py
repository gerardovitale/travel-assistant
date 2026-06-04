import datetime
from unittest.mock import patch

import pandas as pd
from api.schemas import FuelType
from services.forecast_service import _assign_regimes
from services.forecast_service import _build_response
from services.forecast_service import _build_transition_counts
from services.forecast_service import _confidence_score
from services.forecast_service import _expected_days_in_regime
from services.forecast_service import _normalize_zip
from services.forecast_service import _probability_of_cheaper_regime_within_days
from services.forecast_service import _recommendation
from services.forecast_service import _transition_matrix
from services.forecast_service import get_historical_forecast


def _make_zip_history(days: int, zip_code: str = "28001", province: str = "madrid") -> pd.DataFrame:
    start = datetime.date(2026, 1, 1)
    cycle = [1.40, 1.42, 1.44, 1.47, 1.49, 1.51, 1.54, 1.56, 1.58]
    rows = []
    for offset in range(days):
        rows.append(
            {
                "date": start + datetime.timedelta(days=offset),
                "zip_code": zip_code,
                "province": province,
                "fuel_type": FuelType.diesel_a_price.value,
                "avg_price": cycle[offset % len(cycle)],
                "min_price": cycle[offset % len(cycle)] - 0.02,
                "max_price": cycle[offset % len(cycle)] + 0.02,
                "station_count": 10,
            }
        )
    return pd.DataFrame(rows)


def _make_province_history(days: int, province: str = "madrid") -> pd.DataFrame:
    start = datetime.date(2026, 1, 1)
    cycle = [1.41, 1.43, 1.45, 1.48, 1.50, 1.52, 1.53, 1.55, 1.57]
    rows = []
    for offset in range(days):
        rows.append(
            {
                "date": start + datetime.timedelta(days=offset),
                "province": province,
                "fuel_type": FuelType.diesel_a_price.value,
                "avg_price": cycle[offset % len(cycle)],
                "min_price": cycle[offset % len(cycle)] - 0.02,
                "max_price": cycle[offset % len(cycle)] + 0.02,
                "station_count": 100,
            }
        )
    return pd.DataFrame(rows)


def test_assign_regimes_uses_price_terciles():
    df = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=6, freq="D"),
            "avg_price": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        }
    )

    result = _assign_regimes(df)

    assert result["regime"].tolist() == ["cheap", "cheap", "normal", "normal", "expensive", "expensive"]


def test_build_transition_counts_only_uses_consecutive_days():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-04"]),
            "regime": ["cheap", "normal", "expensive"],
        }
    )

    counts = _build_transition_counts(df)

    assert counts["cheap"]["normal"] == 1
    assert counts["normal"]["expensive"] == 0


def test_probability_of_cheaper_regime_within_days_accounts_for_multi_step_path():
    matrix = {
        "cheap": {"cheap": 1.0, "normal": 0.0, "expensive": 0.0},
        "normal": {"cheap": 0.5, "normal": 0.5, "expensive": 0.0},
        "expensive": {"cheap": 0.0, "normal": 0.5, "expensive": 0.5},
    }

    probability = _probability_of_cheaper_regime_within_days(matrix, "expensive", 2)

    assert probability == 0.75


def test_expected_days_in_regime_uses_self_transition_probability():
    matrix = {
        "cheap": {"cheap": 0.75, "normal": 0.25, "expensive": 0.0},
        "normal": {"cheap": 0.2, "normal": 0.6, "expensive": 0.2},
        "expensive": {"cheap": 0.1, "normal": 0.3, "expensive": 0.6},
    }

    assert _expected_days_in_regime(matrix, "cheap") == 4.0


def test_confidence_score_drops_for_sparse_transitions():
    sparse = {
        "cheap": {"cheap": 1, "normal": 0, "expensive": 0},
        "normal": {"cheap": 0, "normal": 0, "expensive": 0},
        "expensive": {"cheap": 0, "normal": 0, "expensive": 0},
    }
    dense = {
        "cheap": {"cheap": 20, "normal": 10, "expensive": 0},
        "normal": {"cheap": 8, "normal": 12, "expensive": 5},
        "expensive": {"cheap": 3, "normal": 9, "expensive": 11},
    }

    assert _confidence_score(sparse) < _confidence_score(dense)


@patch("services.forecast_service.download_aggregate")
def test_get_historical_forecast_returns_zip_code_forecast(mock_download):
    mock_download.side_effect = lambda name: _make_zip_history(90) if name == "zip_code_daily_stats.parquet" else None

    result = get_historical_forecast(FuelType.diesel_a_price, zip_code="28001")

    assert result.insufficient_data is False
    assert result.geography_type == "zip_code"
    assert result.geography_value == "28001"
    assert result.coverage_days == 90
    assert set(result.next_day_probabilities) == {"cheap", "normal", "expensive"}
    assert result.transition_observations > 0


@patch("services.forecast_service.download_aggregate")
def test_get_historical_forecast_falls_back_to_province_when_zip_history_is_thin(mock_download):
    def _aggregate(name: str):
        if name == "zip_code_daily_stats.parquet":
            return _make_zip_history(20)
        if name == "province_daily_stats.parquet":
            return _make_province_history(90)
        return None

    mock_download.side_effect = _aggregate

    result = get_historical_forecast(FuelType.diesel_a_price, zip_code="28001")

    assert result.insufficient_data is False
    assert result.geography_type == "province"
    assert result.source == "province"
    assert result.geography_value == "madrid"


@patch("services.forecast_service.download_aggregate")
def test_get_historical_forecast_returns_insufficient_data_when_no_usable_history(mock_download):
    mock_download.side_effect = lambda name: _make_zip_history(15) if name == "zip_code_daily_stats.parquet" else None

    result = get_historical_forecast(FuelType.diesel_a_price, zip_code="28001")

    assert result.insufficient_data is True
    assert result.recommendation == "Sin suficiente historico"


@patch("services.forecast_service.download_aggregate")
def test_get_historical_forecast_province_only(mock_download):
    mock_download.side_effect = lambda name: (
        _make_province_history(90) if name == "province_daily_stats.parquet" else None
    )

    result = get_historical_forecast(FuelType.diesel_a_price, province="madrid")

    assert result.insufficient_data is False
    assert result.geography_type == "province"
    assert result.geography_value == "madrid"
    assert result.source == "province"


def test_transition_matrix_zero_row_defaults_to_self_loop():
    counts = {
        "cheap": {"cheap": 10, "normal": 5, "expensive": 0},
        "normal": {"cheap": 0, "normal": 0, "expensive": 0},
        "expensive": {"cheap": 2, "normal": 3, "expensive": 5},
    }

    matrix = _transition_matrix(counts)

    assert matrix["normal"]["normal"] == 1.0
    assert matrix["normal"]["cheap"] == 0.0
    assert matrix["normal"]["expensive"] == 0.0


def test_recommendation_cheap_always_refuels():
    assert _recommendation("cheap", 0.0, 0.0) == "Reposta hoy"
    assert _recommendation("cheap", 0.9, 0.9) == "Reposta hoy"


def test_recommendation_expensive_waits_when_cheaper_likely():
    assert _recommendation("expensive", 0.55, 0.0) == "Puedes esperar"
    assert _recommendation("expensive", 0.0, 0.75) == "Puedes esperar"


def test_recommendation_expensive_refuels_when_cheaper_unlikely():
    assert _recommendation("expensive", 0.3, 0.5) == "Reposta hoy"


def test_recommendation_normal_waits_when_cheaper_likely_within_3d():
    assert _recommendation("normal", 0.6, 0.0) == "Puedes esperar"


def test_recommendation_normal_refuels_when_cheaper_unlikely():
    assert _recommendation("normal", 0.5, 0.9) == "Reposta hoy"


def test_transition_matrix_rows_sum_to_one_without_rounding_drift():
    # 1/3 + 1/3 + 1/3 must stay 1.0; per-entry round(.,4) would give 0.9999 and leak mass.
    counts = {regime: {"cheap": 1, "normal": 1, "expensive": 1} for regime in ("cheap", "normal", "expensive")}

    matrix = _transition_matrix(counts)

    for src in ("cheap", "normal", "expensive"):
        assert abs(sum(matrix[src].values()) - 1.0) < 1e-9


def test_normalize_zip_strips_float_suffix_and_pads_leading_zero():
    assert _normalize_zip("28001.0") == "28001"
    assert _normalize_zip(28001.0) == "28001"
    assert _normalize_zip(1001) == "01001"
    assert _normalize_zip("01001") == "01001"


@patch("services.forecast_service.download_aggregate")
def test_get_historical_forecast_matches_float_typed_zip_column(mock_download):
    history = _make_zip_history(90)
    history["zip_code"] = 28001.0  # float64 column stringifies to "28001.0"

    mock_download.side_effect = lambda name: history if name == "zip_code_daily_stats.parquet" else None

    result = get_historical_forecast(FuelType.diesel_a_price, zip_code="28001")

    assert result.insufficient_data is False
    assert result.geography_value == "28001"
    assert result.coverage_days == 90


@patch("services.forecast_service.download_aggregate")
def test_get_historical_forecast_matches_leading_zero_zip(mock_download):
    history = _make_zip_history(90)
    history["zip_code"] = 1001  # stored without leading zero (e.g. int column)

    mock_download.side_effect = lambda name: history if name == "zip_code_daily_stats.parquet" else None

    result = get_historical_forecast(FuelType.diesel_a_price, zip_code="01001")

    assert result.insufficient_data is False
    assert result.coverage_days == 90


@patch("services.forecast_service.download_aggregate")
def test_get_historical_forecast_rounds_output_to_four_decimals(mock_download):
    mock_download.side_effect = lambda name: _make_zip_history(90) if name == "zip_code_daily_stats.parquet" else None

    result = get_historical_forecast(FuelType.diesel_a_price, zip_code="28001")

    for row in result.transition_matrix.values():
        for prob in row.values():
            assert prob == round(prob, 4)
    for prob in result.next_day_probabilities.values():
        assert prob == round(prob, 4)
    assert result.cheaper_within_3d == round(result.cheaper_within_3d, 4)
    assert result.cheaper_within_7d == round(result.cheaper_within_7d, 4)


def test_build_response_rejects_when_transitions_below_floor():
    # 66 observation days but only 20 consecutive-day transitions (< MIN_TRANSITIONS); the rest are gapped.
    dates = list(pd.date_range("2026-01-01", periods=21, freq="D"))
    dates += list(pd.date_range("2026-03-01", periods=45, freq="3D"))
    prices = [1.40 + (i % 9) * 0.02 for i in range(len(dates))]
    history = pd.DataFrame({"date": pd.to_datetime(dates), "avg_price": prices})

    response = _build_response(history, "province", "madrid", "province")

    assert response.insufficient_data is True
