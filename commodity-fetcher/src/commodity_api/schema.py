# Long-format on purpose: one row per (date, series_id) rather than one column per commodity.
# This lets aggregates/commodity_prices.parquet hold Brent, EUR/USD, product futures, etc. side
# by side as more series_id values are added later, with no schema change. See SCHEMA.md.
from commodity_api.constants import SERIES_METADATA


def get_expected_columns() -> list[str]:
    """Final column order of the transformed DataFrame (the output contract)."""
    return ["date", "series_id", "commodity", "value", "unit", "source"]


def get_series_metadata(series_id: str) -> dict:
    """Static metadata (commodity name, unit, source) attached to every row of a series."""
    return SERIES_METADATA[series_id]
