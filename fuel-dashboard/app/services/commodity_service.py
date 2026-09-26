import pandas as pd
from api.schemas import CommodityCorrelationResponse
from api.schemas import CommodityPoint
from api.schemas import CommodityTrendResponse
from api.schemas import FuelType

from data.duckdb_engine import query_commodity_price_trend
from data.duckdb_engine import query_national_price_trend
from data.gcs_client import download_aggregate

COMMODITY_AGGREGATE = "commodity_prices.parquet"
BRENT_SERIES_ID = "DCOILBRENTEU"
BRENT_COMMODITY_LABEL = "brent_crude"
MIN_CORRELATION_OBSERVATIONS = 14


def get_commodity_trend(days_back: int) -> CommodityTrendResponse:
    """Brent-crude price series for the Tendencias-tab overlay."""
    commodity_df = download_aggregate(COMMODITY_AGGREGATE)
    trend_df = query_commodity_price_trend(commodity_df, BRENT_SERIES_ID, days_back)
    unit = ""
    if commodity_df is not None and not commodity_df.empty:
        brent_rows = commodity_df[commodity_df["series_id"] == BRENT_SERIES_ID]
        if not brent_rows.empty:
            unit = brent_rows["unit"].iloc[0]
    series = [CommodityPoint(date=str(row["date"]), value=float(row["value"])) for _, row in trend_df.iterrows()]
    return CommodityTrendResponse(series=series, commodity=BRENT_COMMODITY_LABEL, unit=unit, days_back=days_back)


def get_fuel_vs_brent_correlation(
    fuel_type: FuelType, days_back: int, province: str | None = None
) -> CommodityCorrelationResponse:
    """Pearson correlation between a fuel's national daily average price and Brent, by date."""
    fuel_df = query_national_price_trend(fuel_type.value, days_back, province)
    commodity_df = download_aggregate(COMMODITY_AGGREGATE)
    brent_df = query_commodity_price_trend(commodity_df, BRENT_SERIES_ID, days_back)

    correlation = None
    insufficient_data = True
    observations = 0
    if not fuel_df.empty and not brent_df.empty:
        merged = pd.merge(
            fuel_df[["date", "avg_price"]],
            brent_df[["date", "value"]],
            on="date",
            how="inner",
        )
        observations = len(merged)
        if observations >= MIN_CORRELATION_OBSERVATIONS:
            correlation = merged["avg_price"].corr(merged["value"])
            insufficient_data = False

    return CommodityCorrelationResponse(
        fuel_type=fuel_type.value,
        commodity=BRENT_COMMODITY_LABEL,
        correlation=correlation,
        window_days=days_back,
        observations=observations,
        insufficient_data=insufficient_data,
    )
