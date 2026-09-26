import logging

import pandas as pd
from commodity_api.constants import MISSING_VALUE_SENTINEL
from commodity_api.schema import get_expected_columns
from commodity_api.schema import get_series_metadata

logger = logging.getLogger(__name__)


def transform_to_dataframe(raw_data: dict, series_id: str) -> pd.DataFrame:
    """Transform a validated raw FRED response into the normalized output DataFrame.

    FRED marks non-trading days (weekends/holidays) with a '.' value. Those rows are
    dropped rather than forward-filled: forward-filling would fabricate a price on a day
    the commodity didn't actually trade at, and the stored series should stay a faithful
    "trading days only" record. Output columns follow get_expected_columns().
    """
    observations = raw_data["observations"]
    df = pd.DataFrame(observations)[["date", "value"]]

    df["value"] = df["value"].replace(MISSING_VALUE_SENTINEL, pd.NA)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    dropped = df["value"].isna().sum()
    if dropped:
        logger.info("Dropping %d non-trading-day observation(s) for series %s", dropped, series_id)
    df = df.dropna(subset=["value"]).copy()

    # A python date object (not a string) — matches the rest of the pipeline's convention
    # (aggregator._snapshot_date), so DuckDB infers a genuine DATE column when this is
    # loaded, consistent with the other aggregates it gets joined against.
    df["date"] = pd.to_datetime(df["date"]).dt.date

    metadata = get_series_metadata(series_id)
    df["series_id"] = series_id
    df["commodity"] = metadata["commodity"]
    df["unit"] = metadata["unit"]
    df["source"] = metadata["source"]

    return df[get_expected_columns()].reset_index(drop=True)
