import logging
from typing import Any
from typing import List

import pandas as pd
from aggregator.pipeline.base import TaskConfig
from aggregator.pipeline.gcs import CallableSource
from aggregator.pipeline.gcs import GCSParquetSink
from aggregator.reports.config import REPORT_BRANDS
from aggregator.reports.config import REPORT_FUEL_COLS
from aggregator.reports.config import REPORT_GEO_COLS
from aggregator.reports.config import REPORT_MIN_APPEARANCES_COMPARISON

logger = logging.getLogger(__name__)

BRAND_COMPARISON_BLOB = "aggregates/reports/brand_price_comparison.parquet"

BRANDS = REPORT_BRANDS
FUEL_COLS = REPORT_FUEL_COLS
GEO_COLS = REPORT_GEO_COLS
# Lower than REPORT_MIN_APPEARANCES_WIN_RATE — Costco operates only a handful of stations in Spain
MIN_APPEARANCES = REPORT_MIN_APPEARANCES_COMPARISON

BRAND_COMPARISON_COLUMNS = [
    "brand",
    "geo_level",
    "geo_value",
    "fuel_type",
    "brand_avg_price",
    "market_avg_price",
    "price_delta_pct",
    "days_below_market_pct",
    "appearances",
    "confidence_level",
    "last_updated",
]


def _compute_comparison_for_combination(con, geo_col, fuel_col, brands, min_appearances):
    fuel_type = fuel_col.replace("_price", "")
    brand_list = ", ".join(f"'{b}'" for b in brands)

    return con.execute(
        f"""
        with base as (
            select dt, {geo_col}, label, {fuel_col}
            from _brand_comparison_work
            where {fuel_col} is not null and {fuel_col} > 0
        ),
        brand_daily as (
            select dt, {geo_col}, label as brand, avg({fuel_col}) as brand_avg_price
            from base where label in ({brand_list})
            group by dt, {geo_col}, label
        ),
        market_daily as (
            select dt, {geo_col}, avg({fuel_col}) as market_avg_price
            from base group by dt, {geo_col}
        ),
        joint as (
            select
                b.dt,
                b.brand,
                b.{geo_col} as geo_value,
                b.brand_avg_price,
                m.market_avg_price,
                cast(b.brand_avg_price < m.market_avg_price as int) as is_below_market
            from brand_daily b
            inner join market_daily m on b.dt = m.dt and b.{geo_col} = m.{geo_col}
        )
        select
            brand,
            '{geo_col}' as geo_level,
            cast(geo_value as varchar) as geo_value,
            '{fuel_type}' as fuel_type,
            round(avg(brand_avg_price), 4) as brand_avg_price,
            round(avg(market_avg_price), 4) as market_avg_price,
            -- avg of daily deltas: weights each day equally regardless of station count;
            -- mathematically distinct from (avg_brand - avg_market) / avg_market (ratio of means)
            round(
                avg((brand_avg_price - market_avg_price) / nullif(market_avg_price, 0) * 100),
                2
            ) as price_delta_pct,
            round(avg(is_below_market) * 100.0, 2) as days_below_market_pct,
            count(*) as appearances,
            -- high >= 384 (+-5% margin at 95% CI); medium >= 100 (rough estimate); low otherwise
            case
                when count(*) >= 384 then 'high'
                when count(*) >= 100 then 'medium'
                else 'low'
            end as confidence_level,
            cast(max(dt) as varchar) as last_updated
        from joint
        group by brand, geo_level, geo_value, fuel_type
        -- Sparse brand-geo pairs suppressed below min_appearances; absent from output without a flag
        having count(*) >= {min_appearances}
        """
    ).df()


def compute_brand_price_comparison(
    con,
    brands: List[str] = None,
    fuel_cols: List[str] = None,
    geo_cols: List[str] = None,
    min_appearances: int = MIN_APPEARANCES,
) -> pd.DataFrame:
    if brands is None:
        brands = BRANDS
    if fuel_cols is None:
        fuel_cols = FUEL_COLS
    if geo_cols is None:
        geo_cols = GEO_COLS

    price_cols_sql = ", ".join(fuel_cols)
    geo_cols_sql = ", ".join(geo_cols)

    # Materialize once to avoid one full fuel_prices scan per (geo_col x fuel_col) combination.
    con.execute("DROP TABLE IF EXISTS _brand_comparison_work")
    con.execute(
        f"""
        CREATE TEMP TABLE _brand_comparison_work AS
        SELECT
            cast(timestamp as date) as dt,
            {geo_cols_sql},
            lower(cast(label as varchar)) as label,
            {price_cols_sql}
        FROM fuel_prices
        """
    )

    try:
        all_frames = []
        for geo_col in geo_cols:
            for fuel_col in fuel_cols:
                df = _compute_comparison_for_combination(con, geo_col, fuel_col, brands, min_appearances)
                if not df.empty:
                    all_frames.append(df)
                logger.info(
                    f"brand_comparison_combination_computed geo_col={geo_col!r} fuel_col={fuel_col!r} rows={len(df)}"
                )
    finally:
        con.execute("DROP TABLE IF EXISTS _brand_comparison_work")

    if not all_frames:
        return pd.DataFrame(columns=BRAND_COMPARISON_COLUMNS)

    result = pd.concat(all_frames, ignore_index=True)
    logger.info(f"brand_comparison_computed total_rows={len(result)}")
    return result[BRAND_COMPARISON_COLUMNS]


def build_task(bucket: Any, con: Any, today: str) -> TaskConfig:
    return TaskConfig(
        name="brand_price_comparison",
        description="Brand average price vs market average by geo area",
        output_blob=BRAND_COMPARISON_BLOB,
        source=CallableSource(lambda: compute_brand_price_comparison(con)),
        sink=GCSParquetSink(bucket, BRAND_COMPARISON_BLOB),
    )
