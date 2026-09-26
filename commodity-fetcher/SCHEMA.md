# Commodity-price data contract

`commodity-fetcher` is the single source of truth for fetching external commodity/market
time series (starting with Brent crude oil) and normalizing them. This document is the
**data contract** between the source API (FRED) and the two consuming services
(`fuel-ingestor`, `fuel-dashboard`).

- **Source endpoint:** `https://api.stlouisfed.org/fred/series/observations` (FRED —
  Federal Reserve Economic Data).
- **Transport:** plain `requests` (unlike `spain_fuel_api`, FRED does not TLS-fingerprint
  block Python's OpenSSL 3.x stack, so no curl-subprocess workaround is needed here).
- **Auth:** free API key, passed as the `api_key` query parameter. Never read from the
  environment inside this package — callers (fuel-ingestor) own sourcing it.
- **Format:** JSON. All observation values are strings; `"."` marks a non-trading day
  (weekend/holiday — the commodity didn't trade).
- **First series:** `DCOILBRENTEU` — Brent crude, USD-denominated, daily (FRED reports this
  series in dollars per barrel, not euros). Comparing it against Spain's euro pump prices
  means comparing across currencies for now — no FX conversion step exists yet.

## 1. Raw FRED response

```json
{
  "observations": [
    { "date": "2024-01-02", "value": "82.87" },
    { "date": "2024-01-03", "value": "." }
  ]
}
```

| Field                  | Type   | Notes                                           |
| ---------------------- | ------ | ----------------------------------------------- |
| `observations`         | array  | One object per calendar day. Must be non-empty. |
| `observations[].date`  | string | `YYYY-MM-DD`.                                   |
| `observations[].value` | string | Numeric string, or `"."` on a non-trading day.  |

## 2. Output DataFrame (the contract consumed by services)

`transform_to_dataframe` / `fetch_commodity_prices` return a pandas DataFrame with
exactly these 6 columns, in this order (`get_expected_columns()`):

```
date, series_id, commodity, value, unit, source
```

This is a **tidy/long format**, not Brent-specific — additional commodities (EUR/USD FX,
diesel/gasoline product futures, the EU weekly Oil Bulletin) can append rows with a
different `series_id` later without any schema change.

Normalization rules:

- **`date`**: a python `date` object (not a string) — matches the rest of the pipeline's
  convention (`aggregator._snapshot_date`), so DuckDB infers a genuine DATE column when
  this is loaded, consistent with the other aggregates it gets joined against.
- **`value`**: `"." → NaN` via `pd.to_numeric(..., errors="coerce")`, then rows with a
  missing value are **dropped** (not forward-filled) — the stored series stays "trading
  days only" rather than fabricating a price. This means the series has gaps on
  weekends/holidays.
- **`series_id` / `commodity` / `unit` / `source`**: static per-series metadata from
  `get_series_metadata(series_id)` (see `SERIES_METADATA` in `constants.py`), attached to
  every row.

## 3. Public API

```python
from commodity_api import (
    fetch_commodity_prices,   # fetch -> validate -> transform -> DataFrame (raises)
    fetch_raw_data,           # requests + retry -> raw dict (raises after exhaustion)
    validate_api_response,    # raise ValueError on malformed response
    transform_to_dataframe,   # (raw dict, series_id) -> normalized DataFrame
    get_expected_columns,
    get_series_metadata,
    DATA_SOURCE_URL,
    DEFAULT_SERIES_ID,
    SERIES_METADATA,
)
```

`fetch_*` accept `api_key`, `series_id` (default `DCOILBRENTEU`), `observation_start`,
`observation_end`, `connect_timeout`, `read_timeout`, `max_retries`, `retry_base_delay`,
and `exponential_backoff` (default `True`: delays `base * 2**(attempt-1)`; set `False`
for a fixed `base` delay).

### Service-specific responsibilities (NOT in this package)

- **fuel-ingestor**: sourcing `FRED_API_KEY` from the environment, choosing the fetch
  window (a rolling 90-day window, to self-heal missed runs), data-quality checks
  (plausibility range, duplicate `(date, series_id)`), and the GCS Parquet
  merge-write to `aggregates/commodity_prices.parquet`.
- **fuel-dashboard**: joining the commodity series by `date` against fuel-price
  aggregates (`query_national_price_trend`) to build the Tendencias-tab overlay and
  fuel/Brent correlation KPI.
