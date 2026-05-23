import concurrent.futures
import logging
import os
import shutil
import tempfile
import time
from datetime import datetime
from datetime import timezone
from typing import List

import duckdb
import pandas as pd
from aggregator.main import _get_bucket
from aggregator.main import _latest_raw_file_per_day
from aggregator.main import _list_raw_parquet_files
from aggregator.main import _upload_parquet_to_gcs
from aggregator.pipeline.base import PipelineResult
from aggregator.pipeline.runner import TaskRunner
from aggregator.reports.brand_comparison import build_task as brand_comparison_task
from aggregator.reports.brand_win_rate import build_task as brand_win_rate_task
from aggregator.shared import _log_event

logger = logging.getLogger(__name__)

BRANDS_TO_ANALYZE = ["ballenoil", "repsol"]
FUEL_COLS = ["gasoline_95_e5_price", "diesel_a_price"]
GEO_COLS = ["zip_code", "locality", "municipality"]
MIN_APPEARANCES = 30

BRAND_COMPETITIVENESS_BLOB = "aggregates/brand_competitiveness.parquet"
BRAND_COMPETITIVENESS_MONTHLY_BLOB = "aggregates/brand_competitiveness_monthly.parquet"
BRAND_COMPETITIVENESS_COLUMNS = [
    "brand",
    "geo_level",
    "geo_value",
    "fuel_type",
    "appearances",
    "win_rate_pct",
    "last_updated",
]
BRAND_COMPETITIVENESS_MONTHLY_COLUMNS = [
    "year_month",
    "brand",
    "geo_level",
    "fuel_type",
    "appearances",
    "win_rate_pct",
]


def _download_parquets_to_dir(bucket, parquet_files):
    tmp_dir = tempfile.mkdtemp(prefix="fuel_parquets_")
    _log_event(logger.info, "downloading_raw_parquets", count=len(parquet_files), tmp_dir=tmp_dir)

    def _download_one(file_name):
        try:
            _log_event(logger.info, "downloading_parquet_file", file=file_name)
            local_path = os.path.join(tmp_dir, os.path.basename(file_name))
            bucket.blob(file_name).download_to_filename(local_path)
            return None
        except Exception as exc:
            _log_event(logger.warning, "raw_parquet_download_failed", file=file_name, error=str(exc))
            return file_name

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(_download_one, parquet_files))

    failed = [f for f in results if f is not None]
    if failed:
        _log_event(logger.warning, "parquet_downloads_partial", failed=len(failed), total=len(parquet_files))
        if len(failed) == len(parquet_files):
            raise RuntimeError(f"All {len(parquet_files)} parquet downloads failed — aborting")

    _log_event(logger.info, "raw_parquets_downloaded", succeeded=len(parquet_files) - len(failed))
    return tmp_dir


def _load_duckdb(parquet_dir):
    con = duckdb.connect()
    con.execute(
        f"create table fuel_prices as select * from read_parquet('{parquet_dir}/*.parquet', union_by_name=true)"
    )
    row_count = con.execute("select count(*) from fuel_prices").fetchone()[0]
    _log_event(logger.info, "duckdb_table_loaded", rows=row_count)
    return con


def _compute_for_combination(con, geo_col, fuel_col, brands, min_appearances, today):
    fuel_type = fuel_col.replace("_price", "")
    brand_list = ", ".join(f"'{b}'" for b in brands)

    overall_df = con.execute(
        f"""
    with base as (
        select
            cast(timestamp as date) as dt,
            {geo_col},
            lower(cast(label as varchar)) as label,
            {fuel_col}
        from fuel_prices
        where {fuel_col} is not null
    ),
    brands_of_interest as (
        select * from base where label in ({brand_list})
    ),
    min_price as (
        select dt, {geo_col}, min({fuel_col}) as min_price
        from base group by 1, 2
    ),
    joint as (
        select
            mp.dt,
            mp.{geo_col} as geo_value,
            br.label as brand,
            mp.min_price = br.{fuel_col} as is_cheapest
        from min_price mp
        inner join brands_of_interest br on mp.dt = br.dt and mp.{geo_col} = br.{geo_col}
    )
    select
        brand,
        '{geo_col}' as geo_level,
        cast(geo_value as varchar) as geo_value,
        '{fuel_type}' as fuel_type,
        count(*) as appearances,
        round(avg(is_cheapest::int) * 100, 2) as win_rate_pct,
        '{today}' as last_updated
    from joint
    group by 1, 2, 3, 4
    having count(*) >= {min_appearances}
    """
    ).df()

    monthly_df = con.execute(
        f"""
    with base as (
        select
            cast(timestamp as date) as dt,
            {geo_col},
            lower(cast(label as varchar)) as label,
            {fuel_col}
        from fuel_prices
        where {fuel_col} is not null
    ),
    brands_of_interest as (
        select * from base where label in ({brand_list})
    ),
    min_price as (
        select dt, {geo_col}, min({fuel_col}) as min_price
        from base group by 1, 2
    ),
    joint as (
        select
            mp.dt,
            br.label as brand,
            mp.min_price = br.{fuel_col} as is_cheapest
        from min_price mp
        inner join brands_of_interest br on mp.dt = br.dt and mp.{geo_col} = br.{geo_col}
    )
    select
        date_trunc('month', dt) as year_month,
        brand,
        '{geo_col}' as geo_level,
        '{fuel_type}' as fuel_type,
        count(*) as appearances,
        round(avg(is_cheapest::int) * 100, 2) as win_rate_pct
    from joint
    group by 1, 2, 3, 4
    order by 1
    """
    ).df()

    _log_event(
        logger.info,
        "combination_computed",
        geo_col=geo_col,
        fuel_col=fuel_col,
        overall_rows=len(overall_df),
        monthly_rows=len(monthly_df),
    )
    return overall_df, monthly_df


def _compute_brand_competitiveness_with_con(con, brands, fuel_cols, geo_cols, min_appearances, today):
    all_overall = []
    all_monthly = []

    for geo_col in geo_cols:
        for fuel_col in fuel_cols:
            overall_df, monthly_df = _compute_for_combination(con, geo_col, fuel_col, brands, min_appearances, today)
            all_overall.append(overall_df)
            all_monthly.append(monthly_df)

    overall = (
        pd.concat(all_overall, ignore_index=True)
        if all_overall
        else pd.DataFrame(columns=BRAND_COMPETITIVENESS_COLUMNS)
    )
    monthly = (
        pd.concat(all_monthly, ignore_index=True)
        if all_monthly
        else pd.DataFrame(columns=BRAND_COMPETITIVENESS_MONTHLY_COLUMNS)
    )
    _log_event(logger.info, "brand_competitiveness_computed", overall_rows=len(overall), monthly_rows=len(monthly))
    return overall, monthly


def compute_brand_competitiveness(
    parquet_dir, brands=None, fuel_cols=None, geo_cols=None, min_appearances=MIN_APPEARANCES
):
    if brands is None:
        brands = BRANDS_TO_ANALYZE
    if fuel_cols is None:
        fuel_cols = FUEL_COLS
    if geo_cols is None:
        geo_cols = GEO_COLS

    today = datetime.now(timezone.utc).date().isoformat()
    con = _load_duckdb(parquet_dir)
    try:
        return _compute_brand_competitiveness_with_con(con, brands, fuel_cols, geo_cols, min_appearances, today)
    finally:
        con.close()


def run_brand_analytics(bucket=None) -> List[PipelineResult]:
    _log_event(logger.info, "brand_analytics_run_start")
    if bucket is None:
        bucket = _get_bucket()

    parquet_files = _list_raw_parquet_files(bucket)
    if not parquet_files:
        _log_event(logger.warning, "brand_analytics_skipped_no_raw_files")
        return []

    parquet_files = _latest_raw_file_per_day(parquet_files)
    _log_event(logger.info, "brand_analytics_raw_files_selected", count=len(parquet_files))

    tmp_dir = _download_parquets_to_dir(bucket, parquet_files)
    try:
        con = _load_duckdb(tmp_dir)
        try:
            today = datetime.now(timezone.utc).date().isoformat()
            fuel_row_count = con.execute("select count(*) from fuel_prices").fetchone()[0]

            legacy_start = time.monotonic()
            overall_df, monthly_df = _compute_brand_competitiveness_with_con(
                con, BRANDS_TO_ANALYZE, FUEL_COLS, GEO_COLS, MIN_APPEARANCES, today
            )
            _upload_parquet_to_gcs(bucket, BRAND_COMPETITIVENESS_BLOB, overall_df)
            _upload_parquet_to_gcs(bucket, BRAND_COMPETITIVENESS_MONTHLY_BLOB, monthly_df)
            legacy_duration = round(time.monotonic() - legacy_start, 2)

            report_results = TaskRunner().run(
                [
                    brand_win_rate_task(bucket, con, today),
                    brand_comparison_task(bucket, con, today),
                ]
            )

            legacy_results = [
                PipelineResult(
                    name="brand_competitiveness",
                    description="Brand cheapest win rate by geo area (overall)",
                    output_blob=BRAND_COMPETITIVENESS_BLOB,
                    input_rows=fuel_row_count,
                    output_rows=len(overall_df),
                    duration_seconds=legacy_duration,
                    status="ok",
                ),
                PipelineResult(
                    name="brand_competitiveness_monthly",
                    description="Brand cheapest win rate by geo area (monthly trend)",
                    output_blob=BRAND_COMPETITIVENESS_MONTHLY_BLOB,
                    input_rows=fuel_row_count,
                    output_rows=len(monthly_df),
                    duration_seconds=legacy_duration,  # computed together with brand_competitiveness
                    status="ok",
                ),
            ]
            _log_event(logger.info, "brand_analytics_run_complete")
            return legacy_results + report_results

        finally:
            con.close()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        _log_event(logger.info, "tmp_dir_cleaned_up", tmp_dir=tmp_dir)
