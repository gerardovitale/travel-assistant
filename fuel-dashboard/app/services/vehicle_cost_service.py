# Running-cost comparison between energy types (the "combustible" report in the Reportes tab).
#
# Answers "is diesel or gasoline cheaper to run?" by combining the curated consumption catalog with
# our own province-level price history. Fuel cost only — no purchase price, maintenance, insurance or
# taxes — so it answers "cheaper to *use*", never "cheaper to *buy*".
#
# The math, for a vehicle with consumption ``c`` (units/100 km) and energy price ``p`` (EUR/unit):
#
#     cost_per_100km = c * p
#
# which is unit-agnostic: (l/100km, EUR/l) and (kWh/100km, EUR/kWh) both hold. For a model pair,
# diesel is cheaper iff ``c_d * p_d < c_g * p_g``, i.e. iff the *price ratio* ``p_d / p_g`` sits below
# the *breakeven ratio* ``c_g / c_d``, a constant of the pair. The price ratio moves by province and by
# day, which is what makes the answer worth publishing.
#
# Note there is deliberately **no annual-km breakeven** here. With fuel-only costs the yearly cost is
# ``cost_per_100km * km / 100`` — linear through the origin — so whichever vehicle is cheaper per
# 100 km is cheaper at every mileage. Annual km sizes the gap, it never flips the winner. A km
# breakeven only exists once a purchase premium enters the model, which this report excludes by
# design; inventing one would be a trick. What *does* flip the answer is the consumption ratio (the
# car), the price ratio (the province), and time.
import pandas as pd
from api.schemas import EnergyType
from services.vehicle_catalog import get_pair
from services.vehicle_catalog import get_vehicle
from services.vehicle_catalog import list_pairs
from services.vehicle_catalog import load_catalog
from services.vehicle_catalog import SEGMENT_LABELS
from services.vehicle_catalog import Vehicle

from data.gcs_client import download_aggregate
from data.geojson_loader import is_mainland_province
from data.geojson_loader import normalize_data_province_name

PROVINCE_AGGREGATE = "province_daily_stats.parquet"

# Which price column in province_daily_stats each energy type reads. This dict is the EV seam:
# adding electricity means adding one entry (routed to a tariff source) and catalog rows — the cost
# and breakeven math below does not change. `electric` is deliberately absent: the repo has no
# electricity price source yet, so electric vehicles are reported as not priceable rather than
# silently priced with a made-up number.
FUEL_COLUMN_BY_ENERGY: dict[EnergyType, str] = {
    EnergyType.gasoline: "gasoline_95_e5_price",
    EnergyType.diesel: "diesel_a_price",
    EnergyType.lpg: "liquefied_petroleum_gases_price",
}

# Below this the verdict is noise rather than a finding: a sub-1% edge is well inside the gap between
# WLTP and real-world consumption.
TIE_MARGIN_PCT = 1.0


def _load_prices() -> pd.DataFrame | None:
    """Province daily prices, cleaned. ``None`` when the aggregate is unavailable."""
    df = download_aggregate(PROVINCE_AGGREGATE)
    if df is None or df.empty:
        return None
    prices = df[["date", "province", "fuel_type", "avg_price", "station_count"]].copy()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices["avg_price"] = pd.to_numeric(prices["avg_price"], errors="coerce")
    prices["station_count"] = pd.to_numeric(prices["station_count"], errors="coerce")
    prices = prices.dropna(subset=["date", "avg_price", "station_count"])
    prices = prices[(prices["avg_price"] > 0) & (prices["station_count"] > 0)]
    return prices if not prices.empty else None


def _daily_series(prices: pd.DataFrame, energy_type: EnergyType, province: str | None) -> pd.DataFrame:
    """Daily EUR/unit for an energy type, station-count weighted. Empty frame when unpriceable."""
    fuel_column = FUEL_COLUMN_BY_ENERGY.get(energy_type)
    if fuel_column is None:
        return pd.DataFrame(columns=["date", "price"])

    subset = prices[prices["fuel_type"] == fuel_column]
    if province is not None:
        subset = subset[subset["province"] == normalize_data_province_name(province)]
    if subset.empty:
        return pd.DataFrame(columns=["date", "price"])

    # Weighted so the national series is a real market average, not an average of province averages.
    subset = subset.assign(_weighted=subset["avg_price"] * subset["station_count"])
    grouped = subset.groupby("date", as_index=False).agg(
        _weighted=("_weighted", "sum"), _stations=("station_count", "sum")
    )
    grouped["price"] = grouped["_weighted"] / grouped["_stations"]
    return grouped[["date", "price"]].sort_values("date").reset_index(drop=True)


def _latest_price(series: pd.DataFrame) -> tuple[float, str] | None:
    if series.empty:
        return None
    last = series.iloc[-1]
    return float(last["price"]), last["date"].date().isoformat()


def _align_on_common_days(gasoline_series: pd.DataFrame, diesel_series: pd.DataFrame) -> pd.DataFrame:
    """Inner-join two price series on date.

    Every pairwise comparison goes through this. Taking each fuel's own latest reading would compare
    a fresh gasoline price against a possibly stale diesel one and then date the verdict with
    whichever is newer — a claim the underlying data does not support.
    """
    return gasoline_series.merge(diesel_series, on="date", suffixes=("_gasoline", "_diesel"))


def _latest_common_prices(
    gasoline_series: pd.DataFrame, diesel_series: pd.DataFrame
) -> tuple[float, float, str] | None:
    """Both prices from the most recent day on which *both* fuels reported."""
    merged = _align_on_common_days(gasoline_series, diesel_series)
    if merged.empty:
        return None
    last = merged.sort_values("date").iloc[-1]
    return float(last["price_gasoline"]), float(last["price_diesel"]), last["date"].date().isoformat()


def _cost_per_100km(vehicle: Vehicle, price: float) -> float:
    return round(vehicle.consumption * price, 2)


def _verdict(margin_pct: float) -> str:
    if abs(margin_pct) < TIE_MARGIN_PCT:
        return "tie"
    return "diesel" if margin_pct > 0 else "gasoline"


def _vehicle_payload(vehicle: Vehicle, price: float | None, price_date: str | None) -> dict:
    return {
        "vehicle_id": vehicle.id,
        "pair_id": vehicle.pair_id,
        "label": vehicle.label,
        "model": vehicle.model,
        "variant": vehicle.variant,
        "segment": vehicle.segment,
        "energy_type": vehicle.energy_type.value,
        "consumption": vehicle.consumption,
        "consumption_unit": vehicle.consumption_unit,
        "price_per_unit": round(price, 3) if price is not None else None,
        "price_date": price_date,
        "cost_per_100km": _cost_per_100km(vehicle, price) if price is not None else None,
    }


def _pair_breakeven_ratio(pair) -> float | None:
    by_energy = {v.energy_type: v for v in pair.vehicles}
    gasoline, diesel = by_energy.get(EnergyType.gasoline), by_energy.get(EnergyType.diesel)
    if gasoline is None or diesel is None:
        return None
    return round(_breakeven_ratio(gasoline, diesel), 4)


def get_vehicle_options() -> dict:
    """Catalog options for the picker.

    Never returns ``None``: the catalog is a committed asset, not a downloaded aggregate, so this
    endpoint stays useful even when the price parquet is unavailable. ``unpriceable_energy_types``
    is how the UI admits that, say, electricity has no price source yet instead of quietly hiding it.
    """
    catalog = load_catalog()
    priceable = set(FUEL_COLUMN_BY_ENERGY)
    return {
        "version": catalog.version,
        "source": catalog.source,
        "source_url": catalog.source_url,
        "notes": catalog.notes,
        "segments": [{"id": key, "label": label} for key, label in SEGMENT_LABELS.items()],
        "pairs": [
            {
                "pair_id": pair.pair_id,
                "model": pair.model,
                "segment": pair.segment,
                "segment_label": SEGMENT_LABELS[pair.segment],
                "model_year": pair.model_year,
                # None when the pair has no gasoline/diesel couple to compare — the catalog only
                # requires two energy types, so a diesel+electric pair is valid and must not 500 here.
                "breakeven_ratio": _pair_breakeven_ratio(pair),
                "vehicles": [
                    {
                        "vehicle_id": v.id,
                        "label": v.label,
                        "variant": v.variant,
                        "energy_type": v.energy_type.value,
                        "consumption": v.consumption,
                        "consumption_unit": v.consumption_unit,
                        "price_available": v.energy_type in priceable,
                    }
                    for v in pair.vehicles
                ],
            }
            for pair in list_pairs()
        ],
        "unpriceable_energy_types": sorted(e.value for e in EnergyType if e not in priceable),
    }


def get_vehicle_costs(vehicle_ids: list[str], province: str | None = None) -> list[dict] | None:
    """EUR/100 km per vehicle, all priced on one shared day. ``None`` when prices are unavailable.

    These rows sit next to the verdict card in the UI, so they must be dated the same way it is:
    every priceable vehicle is priced on the latest day on which *all* of them reported. Energy
    types with no price source (electricity) are excluded from that intersection rather than
    nulling out the vehicles that can be priced.
    """
    prices = _load_prices()
    if prices is None:
        return None

    vehicles = [v for v in (get_vehicle(vehicle_id) for vehicle_id in vehicle_ids) if v is not None]
    series_by_energy = {v.energy_type: _daily_series(prices, v.energy_type, province) for v in vehicles}
    priceable = {energy: series for energy, series in series_by_energy.items() if not series.empty}

    common_dates: set | None = None
    for series in priceable.values():
        dates = set(series["date"])
        common_dates = dates if common_dates is None else common_dates & dates
    shared_date = max(common_dates) if common_dates else None

    rows = []
    for vehicle in vehicles:
        price, price_date = None, None
        series = priceable.get(vehicle.energy_type)
        if series is not None and shared_date is not None:
            on_day = series[series["date"] == shared_date]
            if not on_day.empty:
                price = float(on_day.iloc[0]["price"])
                price_date = shared_date.date().isoformat()
        rows.append(_vehicle_payload(vehicle, price, price_date))
    return rows


def _breakeven_ratio(gasoline: Vehicle, diesel: Vehicle) -> float:
    return gasoline.consumption / diesel.consumption


def _breakeven_metrics(gasoline: Vehicle, diesel: Vehicle, gasoline_price: float, diesel_price: float) -> dict:
    """The full pairwise comparison at one pair of prices.

    Single source of truth: the national verdict, the per-province ranking and the UI-test fixtures
    all go through here, so the tie band and the derived figures cannot drift apart.
    """
    breakeven_ratio = _breakeven_ratio(gasoline, diesel)
    price_ratio = diesel_price / gasoline_price
    margin_pct = (breakeven_ratio - price_ratio) / breakeven_ratio * 100
    gasoline_cost = _cost_per_100km(gasoline, gasoline_price)
    diesel_cost = _cost_per_100km(diesel, diesel_price)
    return {
        "price_ratio": round(price_ratio, 4),
        "breakeven_ratio": round(breakeven_ratio, 4),
        # The headline: the diesel price at which the two cost the same, given today's gasoline price.
        "breakeven_diesel_price": round(gasoline_price * breakeven_ratio, 3),
        # How much the diesel price could still rise before the verdict flips. Positive = headroom.
        "diesel_headroom_eur_l": round(gasoline_price * breakeven_ratio - diesel_price, 3),
        # The falsifiable version of the claim: the real-world diesel consumption at which the two
        # tie. Reframes the WLTP caveat as "diesel wins while it actually burns under X l/100km".
        "breakeven_diesel_consumption": round(gasoline.consumption * gasoline_price / diesel_price, 2),
        "margin_pct": round(margin_pct, 1),
        "winner": _verdict(margin_pct),
        "cost_gasoline_per_100km": gasoline_cost,
        "cost_diesel_per_100km": diesel_cost,
        "cost_gap_per_100km": round(abs(gasoline_cost - diesel_cost), 2),
    }


def get_pair_breakeven(pair_id: str, province: str | None = None) -> dict | None:
    """Current verdict for a model pair. ``None`` when the pair or its prices are unavailable."""
    pair = get_pair(pair_id)
    gasoline, diesel = pair.get(EnergyType.gasoline), pair.get(EnergyType.diesel)
    if gasoline is None or diesel is None:
        return None

    prices = _load_prices()
    if prices is None:
        return None

    aligned = _latest_common_prices(
        _daily_series(prices, EnergyType.gasoline, province),
        _daily_series(prices, EnergyType.diesel, province),
    )
    if aligned is None:
        return None
    gasoline_price, diesel_price, price_date = aligned

    return {
        "pair_id": pair_id,
        "model": gasoline.model,
        "segment": gasoline.segment,
        "province": province,
        "gasoline": _vehicle_payload(gasoline, gasoline_price, price_date),
        "diesel": _vehicle_payload(diesel, diesel_price, price_date),
        **_breakeven_metrics(gasoline, diesel, gasoline_price, diesel_price),
        # One date for both figures: they come from the same day by construction.
        "price_date": price_date,
    }


def get_breakeven_by_province(pair_id: str, mainland_only: bool = True) -> dict | None:
    """Latest verdict per province, best-for-diesel first. ``None`` when data is unavailable.

    Non-mainland provinces (Canarias, Baleares, Ceuta, Melilla — see ``is_mainland_province``) are
    excluded by default: their fuel tax regime differs enough that they would top or bottom any
    ranking for a reason that has nothing to do with the engine choice.
    """
    pair = get_pair(pair_id)
    gasoline, diesel = pair.get(EnergyType.gasoline), pair.get(EnergyType.diesel)
    if gasoline is None or diesel is None:
        return None

    prices = _load_prices()
    if prices is None:
        return None

    breakeven_ratio = _breakeven_ratio(gasoline, diesel)
    empty = {
        "pair_id": pair_id,
        "model": gasoline.model,
        "breakeven_ratio": round(breakeven_ratio, 4),
        "rows": [],
        "provinces_dropped": 0,
    }

    columns = [FUEL_COLUMN_BY_ENERGY[EnergyType.gasoline], FUEL_COLUMN_BY_ENERGY[EnergyType.diesel]]
    subset = prices[prices["fuel_type"].isin(columns)]
    if mainland_only:
        # Resolve per distinct province, not per row: the province column runs to tens of thousands
        # of rows and is_mainland_province re-normalises the name on every call.
        mainland = {p for p in subset["province"].unique() if is_mainland_province(p)}
        subset = subset[subset["province"].isin(mainland)]
    if subset.empty:
        return empty

    provinces_seen = subset["province"].nunique()
    # Align on days where BOTH fuels reported, then take each province's most recent such day. A
    # province whose diesel series stopped weeks ago must not have its stale price compared against
    # today's gasoline and then be dated as if it were current.
    wide = subset.pivot_table(index=["province", "date"], columns="fuel_type", values="avg_price", aggfunc="last")
    counts = subset.pivot_table(index=["province", "date"], columns="fuel_type", values="station_count", aggfunc="last")
    if any(column not in wide.columns for column in columns):
        return {**empty, "provinces_dropped": provinces_seen}

    both_reported = wide.dropna(subset=columns)
    if both_reported.empty:
        return {**empty, "provinces_dropped": provinces_seen}

    latest = both_reported.reset_index().sort_values("date").groupby("province", as_index=False).last()
    latest = latest.merge(counts.reset_index(), on=["province", "date"], suffixes=("", "_stations"))
    # Provinces with no day where both fuels reported are dropped and counted, never interpolated.
    dropped = provinces_seen - len(latest)

    rows = []
    for _, price_row in latest.iterrows():
        gasoline_price = float(price_row[columns[0]])
        diesel_price = float(price_row[columns[1]])
        rows.append(
            {
                "province": price_row["province"],
                "gasoline_price": round(gasoline_price, 3),
                "diesel_price": round(diesel_price, 3),
                **_breakeven_metrics(gasoline, diesel, gasoline_price, diesel_price),
                # Surfaced so a verdict resting on a handful of stations reads as such.
                "station_count": int(min(price_row[f"{columns[0]}_stations"], price_row[f"{columns[1]}_stations"])),
                "price_date": price_row["date"].date().isoformat(),
            }
        )
    return {
        "pair_id": pair_id,
        "model": gasoline.model,
        "breakeven_ratio": round(breakeven_ratio, 4),
        "rows": sorted(rows, key=lambda r: r["margin_pct"], reverse=True),
        "provinces_dropped": dropped,
    }


def get_breakeven_history(pair_id: str, province: str | None = None, days: int = 365) -> dict | None:
    """Daily price ratio against the pair's fixed breakeven ratio, plus where it crossed."""
    pair = get_pair(pair_id)
    gasoline, diesel = pair.get(EnergyType.gasoline), pair.get(EnergyType.diesel)
    if gasoline is None or diesel is None:
        return None

    prices = _load_prices()
    if prices is None:
        return None

    gasoline_series = _daily_series(prices, EnergyType.gasoline, province)
    diesel_series = _daily_series(prices, EnergyType.diesel, province)
    if gasoline_series.empty or diesel_series.empty:
        return None

    merged = gasoline_series.merge(diesel_series, on="date", suffixes=("_gasoline", "_diesel"))
    if merged.empty:
        return None
    cutoff = merged["date"].max() - pd.Timedelta(days=max(1, days) - 1)
    merged = merged[merged["date"] >= cutoff].reset_index(drop=True)

    breakeven_ratio = _breakeven_ratio(gasoline, diesel)
    merged["price_ratio"] = merged["price_diesel"] / merged["price_gasoline"]
    # Same tie band as the headline verdict. A bare `price_ratio < breakeven_ratio` would count a
    # day sitting 0.4% below breakeven as a diesel win while the verdict card calls it a draw —
    # two contradictory claims about the same day, on the same screen.
    merged["margin_pct"] = (breakeven_ratio - merged["price_ratio"]) / breakeven_ratio * 100
    merged["winner"] = merged["margin_pct"].map(_verdict)
    merged["diesel_wins"] = merged["winner"] == "diesel"

    # A crossover is the first day of each run where the verdict differs from the previous day.
    flips = merged["winner"] != merged["winner"].shift()
    crossovers = [
        {"date": row["date"].date().isoformat(), "winner": row["winner"]}
        for _, row in merged[flips.fillna(False) & (merged.index > 0)].iterrows()
    ]

    series = [
        {
            "date": row["date"].date().isoformat(),
            "price_ratio": round(float(row["price_ratio"]), 4),
            "cost_gasoline_per_100km": _cost_per_100km(gasoline, float(row["price_gasoline"])),
            "cost_diesel_per_100km": _cost_per_100km(diesel, float(row["price_diesel"])),
        }
        for _, row in merged.iterrows()
    ]

    return {
        "pair_id": pair_id,
        "model": gasoline.model,
        "province": province,
        "breakeven_ratio": round(breakeven_ratio, 4),
        "pct_days_diesel_wins": round(float(merged["diesel_wins"].mean()) * 100, 1),
        # Reported so the percentages account for every day: diesel + gasoline + tie = 100.
        "pct_days_tie": round(float((merged["winner"] == "tie").mean()) * 100, 1),
        "days": len(merged),
        # Zero flips is the honest headline when it happens — do not dramatise a race that never ran.
        "flips": len(crossovers),
        "crossovers": crossovers,
        "series": series,
    }
