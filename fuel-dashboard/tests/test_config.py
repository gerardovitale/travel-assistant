def test_report_brands_parses_csv_string(monkeypatch):
    monkeypatch.setenv("DASHBOARD_REPORT_BRANDS", "ballenoil,repsol,cepsa")
    from config import Settings

    s = Settings()
    assert s.report_brands == ["ballenoil", "repsol", "cepsa"]


def test_report_brands_strips_whitespace(monkeypatch):
    monkeypatch.setenv("DASHBOARD_REPORT_BRANDS", " ballenoil , repsol ")
    from config import Settings

    s = Settings()
    assert s.report_brands == ["ballenoil", "repsol"]


def test_report_brands_empty_string_produces_empty_list(monkeypatch):
    monkeypatch.setenv("DASHBOARD_REPORT_BRANDS", "")
    from config import Settings

    s = Settings()
    assert s.report_brands == []


def test_report_brands_default_when_env_not_set(monkeypatch):
    monkeypatch.delenv("DASHBOARD_REPORT_BRANDS", raising=False)
    from config import Settings

    s = Settings()
    assert set(s.report_brands) == {"cepsa", "repsol", "ballenoil", "costco"}


def test_report_brands_list_passthrough():
    from config import Settings

    s = Settings(report_brands=["bp", "shell"])
    assert s.report_brands == ["bp", "shell"]


def test_fuel_type_report_ships_disabled_by_default(monkeypatch):
    monkeypatch.delenv("DASHBOARD_REPORT_FUEL_TYPE_ENABLED", raising=False)
    from config import Settings

    s = Settings()
    assert s.report_fuel_type_enabled is False
    assert s.report_fuel_type_default_km_year == 15000


def test_fuel_type_report_flag_reads_env(monkeypatch):
    monkeypatch.setenv("DASHBOARD_REPORT_FUEL_TYPE_ENABLED", "true")
    monkeypatch.setenv("DASHBOARD_REPORT_FUEL_TYPE_DEFAULT_KM_YEAR", "22000")
    from config import Settings

    s = Settings()
    assert s.report_fuel_type_enabled is True
    assert s.report_fuel_type_default_km_year == 22000
