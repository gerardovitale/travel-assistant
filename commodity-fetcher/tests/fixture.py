def get_response_raw_data(dates_and_values: list[tuple[str, str]] | None = None) -> dict:
    """Build a raw FRED series-observations response.

    Mirrors the real FRED shape: string date/value pairs, '.' marking non-trading days.
    Defaults to a small run of trading days plus one weekend gap.
    """
    if dates_and_values is None:
        dates_and_values = [
            ("2026-01-02", "82.87"),
            ("2026-01-03", "."),
            ("2026-01-04", "."),
            ("2026-01-05", "83.14"),
        ]
    return {
        "observations": [{"date": date, "value": value} for date, value in dates_and_values],
    }
