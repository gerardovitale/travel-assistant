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
    assert set(s.report_brands) == {"ballenoil", "repsol", "costco"}


def test_report_brands_list_passthrough():
    from config import Settings

    s = Settings(report_brands=["bp", "shell"])
    assert s.report_brands == ["bp", "shell"]
