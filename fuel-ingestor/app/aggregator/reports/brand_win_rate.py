import logging
from typing import Any
from typing import List

import pandas as pd
from aggregator.pipeline.base import TaskConfig
from aggregator.pipeline.gcs import CallableSource
from aggregator.pipeline.gcs import GCSParquetSink
from aggregator.reports.config import REPORT_BRANDS
from aggregator.reports.config import REPORT_DIRECTIONS
from aggregator.reports.config import REPORT_FUEL_COLS
from aggregator.reports.config import REPORT_GEO_COLS
from aggregator.reports.config import REPORT_MIN_APPEARANCES_WIN_RATE

logger = logging.getLogger(__name__)

REPORTS_PREFIX = "aggregates/reports/"
BRAND_WIN_RATE_BLOB = f"{REPORTS_PREFIX}brand_win_rate.parquet"

BRANDS = REPORT_BRANDS
FUEL_COLS = REPORT_FUEL_COLS
GEO_COLS = REPORT_GEO_COLS
DIRECTIONS = REPORT_DIRECTIONS
MIN_APPEARANCES = REPORT_MIN_APPEARANCES_WIN_RATE

BRAND_WIN_RATE_COLUMNS = [
    "brand",
    "direction",
    "geo_level",
    "geo_value",
    "fuel_type",
    "appearances",
    "win_rate_pct",
    "confidence_level",
    "last_updated",
]

_DIRECTION_AGG = {"cheapest": "min", "priciest": "max"}


def _compute_win_rate_for_combination(con, geo_col, fuel_col, brands, direction, min_appearances):
    agg_fn = _DIRECTION_AGG[direction]
    fuel_type = fuel_col.replace("_price", "")
    brand_list = ", ".join(f"'{b}'" for b in brands)

    return con.execute(
        f"""
        with base as (
            select
                cast(timestamp as date) as dt,
                {geo_col},
                label,
                {fuel_col}
            from _brand_win_rate_work
            where {fuel_col} is not null and {fuel_col} > 0
        ),
        brands_of_interest as (
            select * from base where label in ({brand_list})
        ),
        boundary as (
            select dt, {geo_col}, {agg_fn}({fuel_col}) as boundary_price
            from base group by 1, 2
        ),
        joint as (
            select
                b.dt,
                b.{geo_col} as geo_value,
                br.label as brand,
                (b.boundary_price = br.{fuel_col}) as is_at_boundary
            from boundary b
            inner join brands_of_interest br on b.dt = br.dt and b.{geo_col} = br.{geo_col}
        ),
        -- Collapse to brand-day: a brand wins the day if any of its stations hits the boundary.
        -- Prevents multi-station brands from inflating appearances and dominating win_rate_pct.
        brand_day as (
            select
                dt,
                geo_value,
                brand,
                max(is_at_boundary::int) as hit_boundary
            from joint
            group by dt, geo_value, brand
        ),
        -- Distribute 1/N credit equally among brands that tie on the same day/geo,
        -- ensuring the sum of win_rate_pct across all brands cannot exceed 100%.
        tie_adjusted as (
            select
                dt,
                geo_value,
                brand,
                case
                    when hit_boundary = 1 then
                        1.0 / nullif(sum(hit_boundary) over (partition by dt, geo_value), 0)
                    else 0.0
                end as win_credit
            from brand_day
        )
        select
            brand,
            '{direction}' as direction,
            '{geo_col}' as geo_level,
            cast(geo_value as varchar) as geo_value,
            '{fuel_type}' as fuel_type,
            count(*) as appearances,
            round(avg(win_credit) * 100, 2) as win_rate_pct,
            -- high >= 384 (±5% ME at 95% CI); medium >= 100 (rough estimate); low otherwise
            case
                when count(*) >= 384 then 'high'
                when count(*) >= 100 then 'medium'
                else 'low'
            end as confidence_level,
            cast(max(dt) as varchar) as last_updated
        from tie_adjusted
        group by brand, direction, geo_level, geo_value, fuel_type
        -- Sparse brand-geo pairs suppressed below min_appearances; absent from output without a flag
        having count(*) >= {min_appearances}
        """
    ).df()


def compute_brand_win_rate(
    con,
    brands: List[str] = None,
    fuel_cols: List[str] = None,
    geo_cols: List[str] = None,
    directions: List[str] = None,
    min_appearances: int = MIN_APPEARANCES,
) -> pd.DataFrame:
    if brands is None:
        brands = BRANDS
    if fuel_cols is None:
        fuel_cols = FUEL_COLS
    if geo_cols is None:
        geo_cols = GEO_COLS
    if directions is None:
        directions = DIRECTIONS
    invalid = set(directions) - set(_DIRECTION_AGG)
    if invalid:
        raise ValueError(f"Invalid direction(s): {invalid!r}. Valid values: {set(_DIRECTION_AGG)!r}")

    price_cols_sql = ", ".join(fuel_cols)
    geo_cols_sql = ", ".join(geo_cols)

    # Materialize once to avoid one full fuel_prices scan per (direction × geo_col × fuel_col).
    con.execute("DROP TABLE IF EXISTS _brand_win_rate_work")
    con.execute(
        f"""
        CREATE TEMP TABLE _brand_win_rate_work AS
        SELECT
            timestamp,
            {geo_cols_sql},
            lower(cast(label as varchar)) as label,
            {price_cols_sql}
        FROM fuel_prices
        """
    )

    try:
        all_frames = []
        for direction in directions:
            for geo_col in geo_cols:
                for fuel_col in fuel_cols:
                    df = _compute_win_rate_for_combination(con, geo_col, fuel_col, brands, direction, min_appearances)
                    if not df.empty:
                        all_frames.append(df)
                    logger.info(
                        f"brand_win_rate_combination_computed direction={direction!r} geo_col={geo_col!r} "
                        f"fuel_col={fuel_col!r} rows={len(df)}"
                    )
    finally:
        con.execute("DROP TABLE IF EXISTS _brand_win_rate_work")

    if not all_frames:
        return pd.DataFrame(columns=BRAND_WIN_RATE_COLUMNS)

    result = pd.concat(all_frames, ignore_index=True)
    logger.info(f"brand_win_rate_computed total_rows={len(result)}")
    return result[BRAND_WIN_RATE_COLUMNS]


def build_task(
    bucket: Any, con: Any, today: str
) -> TaskConfig:  # today: interface compat only; last_updated derived from max(dt)
    return TaskConfig(
        name="brand_win_rate",
        description="Probability of brand being cheapest or priciest by geo area",
        output_blob=BRAND_WIN_RATE_BLOB,
        source=CallableSource(lambda: compute_brand_win_rate(con)),
        sink=GCSParquetSink(bucket, BRAND_WIN_RATE_BLOB),
    )
