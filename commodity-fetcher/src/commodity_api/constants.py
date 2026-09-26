DATA_SOURCE_URL = "https://api.stlouisfed.org/fred/series/observations"
DEFAULT_SERIES_ID = "DCOILBRENTEU"
FRED_DATE_FORMAT = "%Y-%m-%d"
MISSING_VALUE_SENTINEL = "."

SERIES_METADATA = {
    "DCOILBRENTEU": {"commodity": "brent_crude", "unit": "USD/bbl", "source": "FRED"},
}
