from aggregator.reports.config import _csv_list
from aggregator.reports.config import REPORT_MIN_APPEARANCES_COMPARISON
from aggregator.reports.config import REPORT_MIN_APPEARANCES_WIN_RATE


# ---- _csv_list ---------------------------------------------------------------


def test_csv_list_parses_comma_separated_values():
    assert _csv_list("__NONEXISTENT__", "a,b,c") == ["a", "b", "c"]


def test_csv_list_strips_whitespace_from_each_entry():
    assert _csv_list("__NONEXISTENT__", " a , b , c ") == ["a", "b", "c"]


def test_csv_list_filters_out_empty_entries():
    assert _csv_list("__NONEXISTENT__", "a,,b") == ["a", "b"]


def test_csv_list_single_entry():
    assert _csv_list("__NONEXISTENT__", "ballenoil") == ["ballenoil"]


def test_csv_list_uses_env_var_when_set(monkeypatch):
    monkeypatch.setenv("AGGREGATOR_REPORT_BRANDS", "cepsa,bp")
    result = _csv_list("AGGREGATOR_REPORT_BRANDS", "ballenoil,repsol,costco")
    assert result == ["cepsa", "bp"]


def test_csv_list_env_var_strips_whitespace(monkeypatch):
    monkeypatch.setenv("AGGREGATOR_REPORT_BRANDS", " cepsa , bp ")
    result = _csv_list("AGGREGATOR_REPORT_BRANDS", "ballenoil")
    assert result == ["cepsa", "bp"]


# ---- default threshold constants --------------------------------------------


def test_default_min_appearances_win_rate_is_30():
    assert REPORT_MIN_APPEARANCES_WIN_RATE == 30


def test_default_min_appearances_comparison_is_10():
    assert REPORT_MIN_APPEARANCES_COMPARISON == 10


def test_min_appearances_win_rate_stricter_than_comparison():
    assert REPORT_MIN_APPEARANCES_WIN_RATE > REPORT_MIN_APPEARANCES_COMPARISON


# ---- downstream modules pick up the constants -------------------------------


def test_brand_win_rate_uses_config_constant():
    from aggregator.reports import brand_win_rate

    assert brand_win_rate.MIN_APPEARANCES is REPORT_MIN_APPEARANCES_WIN_RATE


def test_brand_comparison_uses_config_constant():
    from aggregator.reports import brand_comparison

    assert brand_comparison.MIN_APPEARANCES is REPORT_MIN_APPEARANCES_COMPARISON
