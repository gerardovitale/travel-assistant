import pandas as pd
from api.schemas import BrandKind
from api.schemas import BrandReportFuelType
from api.schemas import BrandScore
from api.schemas import BrandScorecardResponse
from api.schemas import Region

from data.gcs_client import download_aggregate
from data.geojson_loader import is_mainland_province
from data.geojson_loader import normalize_data_province_name

BRAND_SCORECARD_REPORT = "reports/brand_scorecard.parquet"

# Hypermarket chains and unattended/automatic low-cost networks, by normalized brand key (see the
# ingestor's normalize_brand). A business-model tag, not a price verdict: some (Carrefour) are not
# cheaper than their zip code's average, and the scorecard shows that.
LOW_COST_BRANDS = frozenset(
    {
        "alcampo",
        "autonetoil",
        "ballenoil",
        "beroil",
        "bonarea",
        "campsa express",
        "carrefour",
        "costco",
        "e.leclerc",
        "easygas",
        "eroski",
        "esclatoil",
        "gasexpress",
        "petroprix",
        "plenergy",
        "supeco",
    }
)

# Cepsa rebranded to Moeve; the data key stays "cepsa". Brands not listed are title-cased.
BRAND_NAMES = {
    "bonarea": "bonÀrea",
    "bp": "BP",
    "cepsa": "Moeve (Cepsa)",
    "dst": "DST",
    "gm oil": "GM Oil",
    "ham": "HAM",
    "ids": "IDS",
    "q8": "Q8",
    "t9": "T9",
}

# A brand needs this many stations in the scope to be ranked: chains, not a lone station.
MIN_REGION_STATIONS = 10
MIN_PROVINCE_STATIONS = 3
# ...and about a month of zip-days with competitors, so the percentages mean something.
MIN_ZIP_DAYS = 30

_SUMMED = ["stations", "station_days", "price_gap_sum", "zip_days", "below_market_days", "win_days"]


def brand_name(brand: str) -> str:
    return BRAND_NAMES.get(brand) or brand.title()


def _in_scope(provinces: pd.Series, province: str | None, region: Region) -> pd.Series:
    if province:
        return provinces == normalize_data_province_name(province)
    if region == Region.all:
        return pd.Series(True, index=provinces.index)
    mainland = provinces.isin([p for p in provinces.unique() if is_mainland_province(p)])
    return mainland if region == Region.peninsula else ~mainland


def _scores(df: pd.DataFrame, min_stations: int) -> list[BrandScore]:
    totals = df.groupby("brand")[_SUMMED].sum()
    totals = totals[(totals["stations"] >= min_stations) & (totals["zip_days"] >= MIN_ZIP_DAYS)]
    totals = totals.assign(price_gap=totals["price_gap_sum"] / totals["station_days"]).sort_values("price_gap")
    return [
        BrandScore(
            brand=brand,
            name=brand_name(brand),
            kind=BrandKind.low_cost if brand in LOW_COST_BRANDS else BrandKind.traditional,
            stations=int(row["stations"]),
            price_gap=round(row["price_gap"], 4),
            below_market_pct=round(100 * row["below_market_days"] / row["zip_days"], 1),
            win_pct=round(100 * row["win_days"] / row["zip_days"], 1),
        )
        for brand, row in totals.iterrows()
    ]


def get_brand_scorecard(
    fuel_types: list[BrandReportFuelType],
    province: str | None = None,
    region: Region = Region.all,
) -> BrandScorecardResponse | None:
    """Brands ranked by their price gap to the local market over the report window, per fuel.

    None when the report has not been published yet.
    """
    df = download_aggregate(BRAND_SCORECARD_REPORT)
    if df is None:
        return None
    # The window is the report's, the same for every row, so it survives a scope with no rows.
    first = df.iloc[0] if not df.empty else None
    df = df[_in_scope(df["province"], province, region)]
    min_stations = MIN_PROVINCE_STATIONS if province else MIN_REGION_STATIONS
    return BrandScorecardResponse(
        window_start=None if first is None else str(first["window_start"]),
        window_end=None if first is None else str(first["window_end"]),
        days_covered=0 if first is None else int(first["days_covered"]),
        fuels={
            fuel.value: _scores(df[df["fuel_type"] == fuel.value.removesuffix("_price")], min_stations)
            for fuel in dict.fromkeys(fuel_types)
        },
    )
