import logging
from datetime import timedelta
from typing import Any

import pandas as pd
from aggregator.brand_utils import register_normalize_brand
from aggregator.pipeline.base import TaskConfig
from aggregator.pipeline.gcs import CallableSource
from aggregator.pipeline.gcs import GCSParquetSink
from aggregator.reports.config import REPORT_FUEL_COLS

logger = logging.getLogger(__name__)

BRAND_SCORECARD_BLOB = "aggregates/reports/brand_scorecard.parquet"

WINDOW_DAYS = 90
# Brands with fewer stations nationally on the last day are dropped: one-off independents would
# otherwise add thousands of rows nobody can pick. Costco (~5 stations) is the smallest chain kept.
MIN_NATIONAL_STATIONS = 5

# One row per province × brand × fuel. Every measure is a sum, so the dashboard re-aggregates any
# set of provinces (peninsula, islands, one province) by summing before dividing.
BRAND_SCORECARD_COLUMNS = [
    "province",
    "brand",
    "fuel_type",
    "stations",
    "station_days",
    "price_gap_sum",
    "zip_days",
    "below_market_days",
    "win_days",
    "window_start",
    "window_end",
    "days_covered",
]


def _compute_scorecard_for_fuel(con, fuel_col: str) -> pd.DataFrame:
    fuel_type = fuel_col.replace("_price", "")
    return con.execute(
        f"""
        with stations as (
            select
                dt,
                zip_code,
                province,
                eess_id,
                brand,
                -- Unbranded stations still compete: each counts as its own competitor.
                coalesce(brand, 'eess:' || eess_id) as competitor,
                {fuel_col} as price
            from _brand_scorecard_work
            where {fuel_col} > 0
        ),
        market as (
            select dt, zip_code, sum(price) as price_sum, min(price) as min_price, count(*) as stations
            from stations
            group by dt, zip_code
        ),
        competitor_day as (
            select
                dt,
                zip_code,
                competitor,
                any_value(brand) as brand,
                any_value(province) as province,
                avg(price) as price,
                sum(price) as price_sum,
                min(price) as min_price,
                count(*) as stations
            from stations
            group by dt, zip_code, competitor
        ),
        scored as (
            select
                c.*,
                -- The rest of the zip: a brand never sets the average it is compared with, so a
                -- chain owning most of a zip cannot pull it towards its own price.
                (m.price_sum - c.price_sum) / nullif(m.stations - c.stations, 0) as market_price,
                m.stations as market_stations,
                -- The cheapest competitor wins the zip that day; ties split the credit.
                case
                    when c.min_price = m.min_price then
                        1.0 / sum((c.min_price = m.min_price)::int) over (partition by c.dt, c.zip_code)
                    else 0.0
                end as win_credit
            from competitor_day c
            inner join market m on c.dt = m.dt and c.zip_code = m.zip_code
        ),
        competition as (
            select
                province,
                brand,
                sum(stations) as station_days,
                sum((price - market_price) * stations) as price_gap_sum,
                count(*) as zip_days,
                sum((price < market_price)::int) as below_market_days,
                sum(win_credit) as win_days
            from scored
            -- A zip where the brand is alone has no market to beat.
            where brand is not null and market_stations > stations
            group by province, brand
        ),
        footprint as (
            select province, brand, count(distinct eess_id) as stations
            from stations
            where brand is not null and dt = (select max(dt) from _brand_scorecard_work)
            group by province, brand
        )
        select
            coalesce(f.province, c.province) as province,
            coalesce(f.brand, c.brand) as brand,
            '{fuel_type}' as fuel_type,
            coalesce(f.stations, 0) as stations,
            coalesce(c.station_days, 0) as station_days,
            round(coalesce(c.price_gap_sum, 0), 4) as price_gap_sum,
            coalesce(c.zip_days, 0) as zip_days,
            coalesce(c.below_market_days, 0) as below_market_days,
            round(coalesce(c.win_days, 0), 4) as win_days
        from footprint f
        full outer join competition c on f.province = c.province and f.brand = c.brand
        """
    ).df()


def compute_brand_scorecard(
    con,
    fuel_cols: list[str] | None = None,
    window_days: int = WINDOW_DAYS,
    min_national_stations: int = MIN_NATIONAL_STATIONS,
) -> pd.DataFrame:
    """Each brand against its local market (same zip code, same day) over the last *window_days*.

    Per brand: stations on the last day, the station-weighted price gap to the average of the zip's
    other stations, and how many brand-zip-days it was below that average or the cheapest. Public
    stations only.
    """
    if fuel_cols is None:
        fuel_cols = REPORT_FUEL_COLS

    register_normalize_brand(con)
    window_end = con.execute("select max(cast(timestamp as date)) from fuel_prices").fetchone()[0]
    if window_end is None:
        return pd.DataFrame(columns=BRAND_SCORECARD_COLUMNS)

    price_cols_sql = ", ".join(fuel_cols)
    con.execute("DROP TABLE IF EXISTS _brand_scorecard_work")
    con.execute(
        f"""
        CREATE TEMP TABLE _brand_scorecard_work AS
        WITH rows AS (
            SELECT
                cast(timestamp as date) as dt,
                cast(zip_code as varchar) as zip_code,
                cast(province as varchar) as province,
                cast(eess_id as varchar) as eess_id,
                cast(label as varchar) as label,
                sale_type,
                {price_cols_sql}
            FROM fuel_prices
        ),
        -- normalize_brand is a Python UDF: call it once per distinct label, not once per row.
        brands AS (
            SELECT label, normalize_brand(label) as brand FROM (SELECT DISTINCT label FROM rows)
        )
        SELECT r.dt, r.zip_code, r.province, r.eess_id, b.brand, {price_cols_sql}
        FROM rows r
        LEFT JOIN brands b ON r.label = b.label
        WHERE r.dt > date '{window_end}' - interval {window_days} day
            AND r.zip_code is not null
            -- The dashboard scopes rows by province; a row without one could not be placed.
            AND r.province is not null
            -- Restricted-sale stations (cooperatives, fleets) are not open to drivers.
            AND coalesce(r.sale_type, 'p') = 'p'
        """
    )
    try:
        days_covered = con.execute("select count(distinct dt) from _brand_scorecard_work").fetchone()[0]
        frames = [_compute_scorecard_for_fuel(con, fuel_col) for fuel_col in fuel_cols]
    finally:
        con.execute("DROP TABLE IF EXISTS _brand_scorecard_work")

    frames = [df for df in frames if not df.empty]
    if not frames:
        return pd.DataFrame(columns=BRAND_SCORECARD_COLUMNS)
    result = pd.concat(frames, ignore_index=True)

    national = result.groupby(["brand", "fuel_type"])["stations"].transform("sum")
    result = result[national >= min_national_stations].copy()
    # DuckDB returns the max date as a datetime.date.
    result["window_start"] = (window_end - timedelta(days=window_days - 1)).isoformat()
    result["window_end"] = window_end.isoformat()
    result["days_covered"] = days_covered
    logger.info(f"brand_scorecard_computed rows={len(result)} days_covered={days_covered}")
    return result[BRAND_SCORECARD_COLUMNS].reset_index(drop=True)


def build_task(bucket: Any, con: Any) -> TaskConfig:
    return TaskConfig(
        name="brand_scorecard",
        description="Brand price gap, days below market and wins vs local market, last 90 days by province",
        output_blob=BRAND_SCORECARD_BLOB,
        source=CallableSource(lambda: compute_brand_scorecard(con)),
        sink=GCSParquetSink(bucket, BRAND_SCORECARD_BLOB),
    )
