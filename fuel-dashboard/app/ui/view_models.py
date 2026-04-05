from collections import defaultdict
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

import pandas as pd
from api.schemas import AlternativePlan
from api.schemas import FUEL_SINGLETONS
from api.schemas import FuelGroup
from api.schemas import HistoricalPeriod
from api.schemas import StationResult
from api.schemas import TrendPeriod
from api.schemas import TrendPoint
from api.schemas import TripPlan
from api.schemas import ZoneResult

FUEL_DISPLAY_NAMES: Dict[str, str] = {
    "diesel_a_price": "Diesel A",
    "diesel_b_price": "Diesel B",
    "diesel_premium_price": "Diesel Premium",
    "gasoline_95_e5_price": "Gasolina 95 E5",
    "gasoline_95_e10_price": "Gasolina 95 E10",
    "gasoline_95_e5_premium_price": "Gasolina 95 E5 Premium",
    "gasoline_98_e5_price": "Gasolina 98 E5",
    "gasoline_98_e10_price": "Gasolina 98 E10",
    "biodiesel_price": "Biodiesel",
    "bioethanol_price": "Bioetanol",
    "compressed_natural_gas_price": "Gas Natural Comprimido",
    "liquefied_natural_gas_price": "Gas Natural Licuado",
    "liquefied_petroleum_gases_price": "Gases Licuados del Petroleo",
    "hydrogen_price": "Hidrogeno",
}

FUEL_VARIANT_SHORT_NAMES: Dict[str, str] = {
    "diesel_a_price": "Estandar",
    "diesel_b_price": "B",
    "diesel_premium_price": "Premium",
    "gasoline_95_e5_price": "E5",
    "gasoline_95_e10_price": "E10",
    "gasoline_95_e5_premium_price": "Premium",
    "gasoline_98_e5_price": "E5",
    "gasoline_98_e10_price": "E10",
    "biodiesel_price": "Biodiesel",
    "bioethanol_price": "Bioetanol",
    "compressed_natural_gas_price": "Comprimido",
    "liquefied_natural_gas_price": "Licuado",
}


@dataclass(frozen=True)
class NavigationItem:
    key: str
    label: str
    description: str


PRIMARY_NAV_ITEMS: List[NavigationItem] = [
    NavigationItem("search", "Buscar", "Estaciones cercanas"),
    NavigationItem("trip", "Viaje", "Planifica tu ruta"),
    NavigationItem("insights", "Insights", "Tendencias y mapas"),
]

INSIGHT_SECTION_CARDS: List[NavigationItem] = [
    NavigationItem("trends", "Tendencias", "Sigue la evolucion de precios por codigo postal."),
    NavigationItem("zones", "Comparar zonas", "Descubre provincias, distritos y municipios mas competitivos."),
    NavigationItem("historical", "Analisis historico", "Consulta rankings, patrones semanales y volatilidad."),
    NavigationItem("quality", "Calidad de datos", "Revisa cobertura, huecos y metricas operativas."),
]

SEARCH_MODE_OPTIONS: Dict[str, str] = {
    "best_by_address": "Mejor opción",
    "nearest_by_address": "Mas cercano",
    "cheapest_by_address": "Mas barato",
    "cheapest_by_zip": "Mas barato por CP",
}

TREND_PERIOD_LABELS: Dict[str, str] = {
    TrendPeriod.week.value: "7 dias",
    TrendPeriod.month.value: "30 dias",
    TrendPeriod.quarter.value: "90 dias",
    TrendPeriod.half_year.value: "6 meses",
    TrendPeriod.year.value: "12 meses",
}


@dataclass(frozen=True)
class SearchModeMeta:
    query_label: str
    query_placeholder: str
    helper_text: str
    requires_radius: bool
    success_metric_label: str
    action_label: str
    empty_state_hint: str
    requires_consumption: bool = False


SEARCH_MODE_META: Dict[str, SearchModeMeta] = {
    "cheapest_by_zip": SearchModeMeta(
        query_label="Codigo postal",
        query_placeholder="Ejemplo: 28001",
        helper_text="Introduce un codigo postal para comparar estaciones dentro de esa zona.",
        requires_radius=False,
        success_metric_label="Mejor precio",
        action_label="Buscar estaciones",
        empty_state_hint="Prueba con otro codigo postal o tipo de combustible.",
    ),
    "nearest_by_address": SearchModeMeta(
        query_label="Direccion o referencia",
        query_placeholder="Ejemplo: Gran Via 1, Madrid",
        helper_text="Usa una direccion o tu ubicacion para encontrar la parada mas cercana.",
        requires_radius=False,
        success_metric_label="Distancia minima",
        action_label="Buscar estaciones",
        empty_state_hint="Prueba con otra direccion o amplia tu punto de partida.",
    ),
    "cheapest_by_address": SearchModeMeta(
        query_label="Direccion o referencia",
        query_placeholder="Ejemplo: Calle Alcala 45, Madrid",
        helper_text="Busca alrededor de una direccion y prioriza el precio por litro.",
        requires_radius=True,
        success_metric_label="Mejor precio",
        action_label="Buscar estaciones",
        empty_state_hint="Prueba otro radio o una direccion diferente.",
    ),
    "best_by_address": SearchModeMeta(
        query_label="Direccion o referencia",
        query_placeholder="Ejemplo: Atocha, Madrid",
        helper_text="Estima el coste total de repostar y desplazarte para recomendar la mejor opción.",
        requires_radius=True,
        success_metric_label="Mejor coste total",
        action_label="Buscar estaciones",
        empty_state_hint="Amplia el radio o revisa los ajustes del vehiculo.",
        requires_consumption=True,
    ),
}


BEST_OPTION_METHODOLOGY_LINES: List[str] = [
    "La mejor opción se calcula con el coste total estimado de repostar en cada estacion:",
    "",
    "Coste total = precio x (litros del deposito + combustible del viaje ida y vuelta)",
    "",
    "El combustible del viaje = 2 x distancia (km) x consumo (l/100km) / 100.",
    "Esto incluye el coste real de ir a la estacion y volver.",
    "",
    "La estacion con menor coste total se recomienda primero.",
    "Tu consumo y la distancia determinan automaticamente",
    "cuando compensa ir mas lejos por un precio mas bajo.",
    "",
    "Ejemplo: con 7 l/100km y deposito de 40L, una estacion a 5 km",
    "gasta 1.05 EUR extra en combustible del viaje (ida y vuelta).",
    "Si su precio es 0.03 EUR/L mas barato, ahorras 1.20 EUR en el deposito,",
    "con un ahorro neto de 0.15 EUR.",
]


def fuel_label(fuel_type: str) -> str:
    return FUEL_DISPLAY_NAMES.get(fuel_type, fuel_type.replace("_price", "").replace("_", " ").title())


def search_mode_metadata(mode: str) -> SearchModeMeta:
    return SEARCH_MODE_META.get(mode, SEARCH_MODE_META["cheapest_by_zip"])


def format_price(price: Optional[float]) -> str:
    if price is None:
        return "-"
    return f"{price:.3f} EUR/L"


def format_currency(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.2f} EUR"


def format_distance(distance_km: Optional[float]) -> str:
    if distance_km is None:
        return "-"
    return f"{distance_km:.2f} km"


def format_delta(delta: float) -> str:
    sign = "+" if delta >= 0 else ""
    return f"{sign}{delta:.3f} EUR/L"


def format_percentage(value: Optional[float], decimals: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.{decimals}f}%"


def station_summary(stations: Sequence[StationResult]) -> Dict[str, Any]:
    if not stations:
        return {
            "count": 0,
            "best_price": None,
            "avg_price": None,
            "best_station_label": None,
            "min_distance_km": None,
            "max_distance_km": None,
            "best_estimated_cost": None,
        }

    distances = [s.distance_km for s in stations if s.distance_km is not None]
    costs = [s.estimated_total_cost for s in stations if s.estimated_total_cost is not None]
    prices = [s.price for s in stations]
    best_station = min(stations, key=lambda s: s.price)
    return {
        "count": len(stations),
        "best_price": min(prices),
        "avg_price": sum(prices) / len(prices),
        "best_station_label": best_station.label,
        "min_distance_km": min(distances) if distances else None,
        "max_distance_km": max(distances) if distances else None,
        "best_estimated_cost": min(costs) if costs else None,
    }


def trend_kpis(trend_data: Sequence[TrendPoint]) -> Dict[str, Optional[float]]:
    if not trend_data:
        return {
            "current_avg_price": None,
            "min_price": None,
            "max_price": None,
            "delta_avg_price": None,
            "period_avg": None,
            "pct_change": None,
        }

    avg_prices = [p.avg_price for p in trend_data]
    period_avg = sum(avg_prices) / len(avg_prices)
    first = avg_prices[0]
    pct_change = ((avg_prices[-1] - first) / first * 100) if first else None
    return {
        "current_avg_price": avg_prices[-1],
        "min_price": min(p.min_price for p in trend_data),
        "max_price": max(p.max_price for p in trend_data),
        "delta_avg_price": avg_prices[-1] - avg_prices[0],
        "period_avg": period_avg,
        "pct_change": pct_change,
    }


def trend_summary_cards(metrics: Dict[str, Optional[float]]) -> List[Dict[str, str]]:
    current = metrics["current_avg_price"]
    period_avg = metrics["period_avg"]
    delta_val = metrics["delta_avg_price"]
    pct_change = metrics["pct_change"]

    current_color = ""
    current_desc = ""
    if current is not None and period_avg is not None:
        current_color = "text-green-600" if current <= period_avg else "text-red-600"
        current_desc = f"vs promedio {format_price(period_avg)}"

    delta_text = format_delta(delta_val) if delta_val is not None else "-"
    delta_color = ""
    delta_icon = ""
    delta_desc = ""
    if delta_val is not None:
        delta_color = "text-red-600" if delta_val > 0 else "text-green-600"
        delta_icon = "arrow_upward" if delta_val > 0 else "arrow_downward"
    if pct_change is not None:
        delta_desc = f"{pct_change:+.1f}%"

    return [
        {
            "label": "Promedio actual",
            "value": format_price(current),
            "color": current_color,
            "description": current_desc,
        },
        {
            "label": "Minimo del periodo",
            "value": format_price(metrics["min_price"]),
            "color": "text-green-600",
        },
        {
            "label": "Maximo del periodo",
            "value": format_price(metrics["max_price"]),
            "color": "text-red-600",
        },
        {
            "label": "Variacion",
            "value": delta_text,
            "delta": delta_desc,
            "delta_color": delta_color,
            "delta_icon": delta_icon,
        },
    ]


FUEL_GROUP_DISPLAY_NAMES: Dict[str, str] = {
    "diesel": "Diesel",
    "gasoline_95": "Gasolina 95",
    "gasoline_98": "Gasolina 98",
    "biofuel": "Biocombustible",
    "natural_gas": "Gas Natural",
}


SEARCH_FUEL_OPTIONS: Dict[str, str] = {}
for _fg in FuelGroup:
    SEARCH_FUEL_OPTIONS[f"group:{_fg.value}"] = FUEL_GROUP_DISPLAY_NAMES.get(_fg.value, _fg.value)
for _ft in FUEL_SINGLETONS:
    SEARCH_FUEL_OPTIONS[f"single:{_ft.value}"] = FUEL_DISPLAY_NAMES.get(_ft.value, _ft.value)


def fuel_group_label(group: str) -> str:
    return FUEL_GROUP_DISPLAY_NAMES.get(group, group.replace("_", " ").title())


def _latest_group_snapshot(group_trends: Dict[str, List[TrendPoint]]) -> Dict[str, Any]:
    by_date: Dict[str, Dict[str, float]] = defaultdict(dict)
    for fuel_type, points in group_trends.items():
        for point in points:
            by_date[point.date][fuel_type] = point.avg_price

    comparable_dates = [date for date, prices in by_date.items() if len(prices) >= 2]
    if not comparable_dates:
        return {"date": None, "variants": []}

    snapshot_date = max(comparable_dates)
    variants = [
        {"fuel_type": fuel_type, "label": fuel_label(fuel_type), "current_avg": price}
        for fuel_type, price in by_date[snapshot_date].items()
    ]
    return {"date": snapshot_date, "variants": variants}


def group_trend_kpis(group_trends: Dict[str, List[TrendPoint]]) -> Dict[str, Any]:
    if not group_trends:
        return {"variant_count": 0, "variants": [], "premium_spread": None, "snapshot_date": None}

    snapshot = _latest_group_snapshot(group_trends)
    variants = snapshot["variants"]

    if not variants:
        return {"variant_count": 0, "variants": [], "premium_spread": None, "snapshot_date": None}

    variants.sort(key=lambda v: v["current_avg"])
    cheapest = variants[0]["current_avg"]
    most_expensive = variants[-1]["current_avg"]
    premium_spread = most_expensive - cheapest

    return {
        "variant_count": len(variants),
        "variants": variants,
        "snapshot_date": snapshot["date"],
        "cheapest_label": variants[0]["label"],
        "cheapest_price": cheapest,
        "most_expensive_label": variants[-1]["label"],
        "most_expensive_price": most_expensive,
        "premium_spread": premium_spread,
    }


def group_trend_summary_cards(kpis: Dict[str, Any]) -> List[Dict[str, str]]:
    if not kpis.get("variants"):
        return []

    cards = [
        {
            "label": "Variantes encontradas",
            "value": str(kpis["variant_count"]),
        },
        {
            "label": "Mas barato",
            "value": format_price(kpis.get("cheapest_price")),
            "color": "text-green-600",
            "description": kpis.get("cheapest_label", ""),
        },
        {
            "label": "Mas caro",
            "value": format_price(kpis.get("most_expensive_price")),
            "color": "text-red-600",
            "description": kpis.get("most_expensive_label", ""),
        },
    ]
    spread = kpis.get("premium_spread")
    if spread is not None:
        cards.append(
            {
                "label": "Diferencia premium",
                "value": f"{spread:.3f} EUR/L",
                "color": "text-blue-600",
                "description": "entre la variante mas barata y la mas cara",
            }
        )
    return cards


@dataclass(frozen=True)
class DailySpread:
    date: str
    spread: float
    max_variant: str
    min_variant: str


def compute_daily_spread(group_trends: Dict[str, List[TrendPoint]]) -> List[DailySpread]:
    if len(group_trends) < 2:
        return []

    by_date: Dict[str, Dict[str, float]] = defaultdict(dict)
    for fuel_type, points in group_trends.items():
        for p in points:
            by_date[p.date][fuel_type] = p.avg_price

    result = []
    for date in sorted(by_date):
        prices = by_date[date]
        if len(prices) < 2:
            continue
        max_ft = max(prices, key=prices.__getitem__)
        min_ft = min(prices, key=prices.__getitem__)
        result.append(
            DailySpread(
                date=date,
                spread=prices[max_ft] - prices[min_ft],
                max_variant=max_ft,
                min_variant=min_ft,
            )
        )
    return result


def spread_kpis(daily_spreads: List[DailySpread]) -> Dict[str, Any]:
    if not daily_spreads:
        return {
            "current_spread": None,
            "avg_spread": None,
            "max_spread": None,
            "max_spread_date": None,
            "min_spread": None,
            "min_spread_date": None,
            "spread_trend": None,
        }

    spreads = [ds.spread for ds in daily_spreads]
    avg_spread = sum(spreads) / len(spreads)
    max_entry = max(daily_spreads, key=lambda ds: ds.spread)
    min_entry = min(daily_spreads, key=lambda ds: ds.spread)

    spread_trend = "stable"
    if len(daily_spreads) >= 14:
        first_avg = sum(ds.spread for ds in daily_spreads[:7]) / 7
        last_avg = sum(ds.spread for ds in daily_spreads[-7:]) / 7
        diff = last_avg - first_avg
        if diff > 0.001:
            spread_trend = "widening"
        elif diff < -0.001:
            spread_trend = "narrowing"

    return {
        "current_spread": daily_spreads[-1].spread,
        "avg_spread": avg_spread,
        "max_spread": max_entry.spread,
        "max_spread_date": max_entry.date,
        "min_spread": min_entry.spread,
        "min_spread_date": min_entry.date,
        "spread_trend": spread_trend,
    }


_SPREAD_TREND_LABELS = {
    "widening": "Se esta ampliando",
    "narrowing": "Se esta reduciendo",
    "stable": "Estable",
}


def spread_summary_cards(kpis: Dict[str, Any]) -> List[Dict[str, str]]:
    if kpis.get("current_spread") is None:
        return []

    trend_label = _SPREAD_TREND_LABELS.get(kpis.get("spread_trend", ""), "")
    trend_color = ""
    if kpis.get("spread_trend") == "widening":
        trend_color = "text-red-600"
    elif kpis.get("spread_trend") == "narrowing":
        trend_color = "text-green-600"

    return [
        {
            "label": "Diferencia actual",
            "value": format_price(kpis["current_spread"]),
            "color": "text-blue-600",
        },
        {
            "label": "Diferencia promedio",
            "value": format_price(kpis["avg_spread"]),
        },
        {
            "label": "Maxima diferencia",
            "value": format_price(kpis["max_spread"]),
            "color": "text-red-600",
            "description": kpis.get("max_spread_date", ""),
        },
        {
            "label": "Minima diferencia",
            "value": format_price(kpis["min_spread"]),
            "color": "text-green-600",
            "description": kpis.get("min_spread_date", ""),
        },
        {
            "label": "Tendencia",
            "value": trend_label,
            "color": trend_color,
        },
    ]


def monthly_spread_pattern(daily_spreads: List[DailySpread]) -> Optional[pd.DataFrame]:
    if len(daily_spreads) < 15:
        return None

    rows = [{"month": ds.date[:7], "spread": ds.spread} for ds in daily_spreads]
    df = pd.DataFrame(rows)
    monthly = (
        df.groupby("month")
        .agg(
            avg_spread=("spread", "mean"),
            min_spread=("spread", "min"),
            max_spread=("spread", "max"),
            count=("spread", "count"),
        )
        .reset_index()
    )
    monthly = monthly[monthly["count"] >= 15]
    if len(monthly) < 3:
        return None
    return monthly.drop(columns=["count"])


def zone_kpis(zones: Sequence[ZoneResult]) -> Dict[str, Any]:
    if not zones:
        return {
            "zone_count": 0,
            "cheapest_zip": None,
            "cheapest_avg_price": None,
            "province_avg_price": None,
            "total_stations": 0,
            "savings_potential": None,
        }

    cheapest_zone = min(zones, key=lambda z: z.avg_price)
    most_expensive_zone = max(zones, key=lambda z: z.avg_price)
    province_avg = sum(z.avg_price for z in zones) / len(zones)
    total_stations = sum(int(z.station_count) for z in zones)
    savings_potential = most_expensive_zone.avg_price - cheapest_zone.avg_price
    return {
        "zone_count": len(zones),
        "cheapest_zip": cheapest_zone.zip_code,
        "cheapest_avg_price": cheapest_zone.avg_price,
        "province_avg_price": province_avg,
        "total_stations": total_stations,
        "savings_potential": savings_potential,
    }


def zone_summary_cards(metrics: Dict[str, Any]) -> List[Dict[str, str]]:
    zone_count = metrics["zone_count"]
    total_stations = metrics["total_stations"]
    cheapest_avg = metrics["cheapest_avg_price"]
    province_avg = metrics["province_avg_price"]

    savings_desc = ""
    if cheapest_avg is not None and province_avg is not None:
        diff = province_avg - cheapest_avg
        savings_desc = f"{diff:+.3f} EUR/L vs provincial"

    savings_potential = metrics.get("savings_potential")

    return [
        {
            "label": "Zonas analizadas",
            "value": str(zone_count),
            "description": f"{total_stations} estaciones en total",
        },
        {
            "label": "CP mas barato",
            "value": metrics["cheapest_zip"] or "-",
            "description": format_price(cheapest_avg),
        },
        {
            "label": "Mejor promedio",
            "value": format_price(cheapest_avg),
            "color": "text-green-600",
            "description": savings_desc,
        },
        {
            "label": "Ahorro potencial",
            "value": f"{savings_potential:.3f} EUR/L" if savings_potential is not None else "-",
            "color": "text-green-600",
            "description": "diferencia entre zonas",
        },
    ]


def _truncate(text: Optional[str], max_len: int = 30) -> str:
    if not text:
        return "-"
    return text if len(text) <= max_len else text[: max_len - 1] + "…"


def search_recommendation(stations: Sequence[StationResult], mode: str) -> Dict[str, str]:
    """Build a recommendation card dict from the top search result.

    Assumes *stations* are already sorted by the caller (price, distance, or
    estimated total cost depending on *mode*), so ``stations[0]`` is the best match.
    """
    if not stations:
        return {
            "title": "Sin recomendacion",
            "headline": "No hay estaciones disponibles para comparar.",
            "detail": "Prueba otra ubicacion o cambia la estrategia de busqueda.",
            "caption": "",
        }

    recommended = stations[0]
    avg_price = sum(station.price for station in stations) / len(stations)
    price_savings_vs_avg = avg_price - recommended.price

    if mode == "nearest_by_address":
        second_best = stations[1] if len(stations) > 1 else None
        distance_gap = None
        if second_best and recommended.distance_km is not None and second_best.distance_km is not None:
            distance_gap = second_best.distance_km - recommended.distance_km
        caption = (
            f"Llega {distance_gap:.2f} km antes que la siguiente alternativa."
            if distance_gap is not None and distance_gap > 0
            else "Ideal si priorizas rapidez y acceso."
        )
        return {
            "title": "Recomendacion principal",
            "headline": f"{recommended.label} es la estacion mas cercana.",
            "detail": f"A {format_distance(recommended.distance_km)} de tu punto de partida.",
            "caption": caption,
        }

    if mode == "best_by_address":
        second_best = stations[1] if len(stations) > 1 else None
        cost_advantage = None
        if (
            second_best
            and recommended.estimated_total_cost is not None
            and second_best.estimated_total_cost is not None
        ):
            cost_advantage = second_best.estimated_total_cost - recommended.estimated_total_cost
        detail = f"Coste total estimado {format_currency(recommended.estimated_total_cost)}."
        caption = (
            f"Ahorras {cost_advantage:.2f} EUR frente a la siguiente opción."
            if cost_advantage is not None and cost_advantage > 0
            else "Equilibra precio por litro y coste de desplazamiento."
        )
        return {
            "title": "Mejor opción",
            "headline": f"{recommended.label} compensa mejor el viaje y el repostaje.",
            "detail": detail,
            "caption": caption,
        }

    zone_suffix = " en tu codigo postal" if mode == "cheapest_by_zip" else " cerca de tu ubicacion"
    caption = (
        f"Ahorro medio de {price_savings_vs_avg:.3f} EUR/L frente al conjunto analizado."
        if price_savings_vs_avg > 0
        else "Es la opción mas competitiva dentro de los resultados cargados."
    )
    return {
        "title": "Mejor precio detectado",
        "headline": f"{recommended.label} ofrece el mejor precio{zone_suffix}.",
        "detail": f"Precio actual: {format_price(recommended.price)}.",
        "caption": caption,
    }


def search_summary_cards(summary: Dict[str, Any], mode: str) -> List[Dict[str, str]]:
    count = summary["count"]
    cards: List[Dict[str, str]] = [
        {
            "label": "Estaciones",
            "value": str(count),
            "description": f"de {count} resultado{'s' if count != 1 else ''}",
        },
        {
            "label": "Mejor precio",
            "value": format_price(summary["best_price"]),
            "color": "text-green-600",
            "description": _truncate(summary.get("best_station_label")),
        },
        {
            "label": "Precio promedio",
            "value": format_price(summary.get("avg_price")),
        },
    ]
    if mode == "best_by_address":
        best_cost = summary["best_estimated_cost"]
        cards.append(
            {
                "label": "Mejor coste total",
                "value": "-" if best_cost is None else f"{best_cost:.2f} EUR",
                "color": "text-green-600",
                "description": "incluye repostaje y desplazamiento",
            }
        )
    else:
        cards.append({"label": "Distancia minima", "value": format_distance(summary["min_distance_km"])})
    return cards


def trip_kpis(trip_plan: TripPlan) -> Dict[str, Any]:
    hours = int(trip_plan.duration_minutes // 60)
    mins = int(trip_plan.duration_minutes % 60)
    return {
        "total_distance": f"{trip_plan.total_distance_km:.0f} km",
        "duration": f"{hours}h {mins}min",
        "num_stops": len(trip_plan.stops),
        "total_cost": f"{trip_plan.total_fuel_cost:.2f} EUR",
        "savings": f"{trip_plan.savings_eur:.2f} EUR",
        "total_liters": f"{trip_plan.total_fuel_liters:.1f} L",
        "fuel_at_destination": f"{trip_plan.fuel_at_destination_pct:.0f}%",
        "total_detour": f"{sum(s.detour_minutes for s in trip_plan.stops):.0f} min",
    }


def trip_summary_cards(trip_plan: TripPlan) -> List[Dict[str, str]]:
    kpis = trip_kpis(trip_plan)
    return [
        {
            "label": "Distancia total",
            "value": kpis["total_distance"],
            "description": kpis["duration"],
        },
        {
            "label": "Paradas",
            "value": str(kpis["num_stops"]),
            "description": f"{kpis['total_liters']} a repostar",
        },
        {
            "label": "Coste total combustible",
            "value": kpis["total_cost"],
            "color": "text-blue-600",
        },
        {
            "label": "Ahorro estimado",
            "value": kpis["savings"],
            "color": "text-green-600",
            "description": "vs precio mediano de la ruta",
        },
        {
            "label": "Desvio total",
            "value": kpis["total_detour"],
        },
        {
            "label": "Combustible en destino",
            "value": kpis["fuel_at_destination"],
        },
    ]


def trip_recommendation(trip_plan: TripPlan) -> Dict[str, str]:
    if not trip_plan.stops:
        return {
            "title": "Ruta validada",
            "headline": "Puedes completar el trayecto sin parar a repostar.",
            "detail": f"Distancia estimada: {trip_plan.total_distance_km:.0f} km.",
            "caption": f"Llegarias con {trip_plan.fuel_at_destination_pct:.0f}% de combustible restante.",
        }

    first_stop = trip_plan.stops[0]
    return {
        "title": "Plan recomendado",
        "headline": f"{len(trip_plan.stops)} parada(s) recomendada(s) para completar la ruta.",
        "detail": (
            f"Primera parada sugerida: {first_stop.station.label} "
            f"({first_stop.detour_minutes:.1f} min de desvio, {format_price(first_stop.station.price)})."
        ),
        "caption": f"Desvio total estimado: {sum(stop.detour_minutes for stop in trip_plan.stops):.0f} min.",
    }


def alternative_plan_cards(plan: AlternativePlan) -> List[Dict[str, str]]:
    return [
        {
            "label": "Paradas",
            "value": str(plan.num_stops),
        },
        {
            "label": "Coste total",
            "value": f"{plan.total_fuel_cost:.2f} EUR",
            "color": "text-blue-600",
        },
        {
            "label": "Litros totales",
            "value": f"{plan.total_fuel_liters:.1f} L",
        },
        {
            "label": "Desvio total",
            "value": f"{plan.total_detour_minutes:.0f} min",
        },
        {
            "label": "Combustible en destino",
            "value": f"{plan.fuel_at_destination_pct:.0f}%",
        },
    ]


# --- Historical analytics ---

SPANISH_DAY_NAMES = {
    0: "Lunes",
    1: "Martes",
    2: "Miercoles",
    3: "Jueves",
    4: "Viernes",
    5: "Sabado",
    6: "Domingo",
}

HISTORICAL_PERIOD_LABELS = {
    HistoricalPeriod.quarter: "90 dias",
    HistoricalPeriod.half_year: "6 meses",
    HistoricalPeriod.year: "12 meses",
}


def province_ranking_kpis(df) -> List[Dict[str, str]]:
    if df.empty:
        return []
    cheapest = df.iloc[0]
    most_expensive = df.iloc[-1]
    diff = most_expensive["avg_price"] - cheapest["avg_price"]
    return [
        {
            "label": "Provincia mas barata",
            "value": str(cheapest["province"]).title(),
            "color": "text-green-600",
            "description": format_price(cheapest["avg_price"]),
        },
        {
            "label": "Provincia mas cara",
            "value": str(most_expensive["province"]).title(),
            "color": "text-red-600",
            "description": format_price(most_expensive["avg_price"]),
        },
        {
            "label": "Diferencia max-min",
            "value": f"{diff:.3f} EUR/L",
            "description": "entre la mas barata y la mas cara",
        },
        {
            "label": "Provincias analizadas",
            "value": str(len(df)),
        },
    ]


def best_day_advice(df) -> Optional[str]:
    """Return a short tip about the cheapest day to refuel, or None if data is insufficient."""
    if df.empty or len(df) < 7:
        return None
    cheapest_idx = df["avg_price"].idxmin()
    most_expensive_idx = df["avg_price"].idxmax()
    cheapest = df.loc[cheapest_idx]
    most_expensive = df.loc[most_expensive_idx]
    diff = most_expensive["avg_price"] - cheapest["avg_price"]
    if diff < 0.001:
        return None
    cheapest_day = SPANISH_DAY_NAMES.get(int(cheapest["day_of_week"]), "?")
    expensive_day = SPANISH_DAY_NAMES.get(int(most_expensive["day_of_week"]), "?")
    return (
        f"Consejo: los {cheapest_day.lower()} suelen ser mas baratos "
        f"({diff:.3f} EUR/L menos que los {expensive_day.lower()}, el dia mas caro)."
    )


def day_of_week_kpis(df) -> List[Dict[str, str]]:
    if df.empty:
        return []
    cheapest_idx = df["avg_price"].idxmin()
    most_expensive_idx = df["avg_price"].idxmax()
    cheapest = df.loc[cheapest_idx]
    most_expensive = df.loc[most_expensive_idx]
    diff = most_expensive["avg_price"] - cheapest["avg_price"]
    weeks = int(df["count_days"].iloc[0]) if not df.empty else 0
    return [
        {
            "label": "Dia mas barato",
            "value": SPANISH_DAY_NAMES.get(int(cheapest["day_of_week"]), "?"),
            "color": "text-green-600",
            "description": format_price(cheapest["avg_price"]),
        },
        {
            "label": "Dia mas caro",
            "value": SPANISH_DAY_NAMES.get(int(most_expensive["day_of_week"]), "?"),
            "color": "text-red-600",
            "description": format_price(most_expensive["avg_price"]),
        },
        {
            "label": "Ahorro potencial",
            "value": f"{diff:.4f} EUR/L",
            "color": "text-green-600",
            "description": "entre el dia mas barato y el mas caro",
        },
        {
            "label": "Semanas analizadas",
            "value": str(weeks),
        },
    ]


def brand_ranking_kpis(df) -> List[Dict[str, str]]:
    if df.empty:
        return []
    cheapest = df.iloc[0]
    most_expensive = df.iloc[-1]
    diff = most_expensive["avg_price"] - cheapest["avg_price"]
    return [
        {
            "label": "Marca mas barata",
            "value": str(cheapest["brand"]).title(),
            "color": "text-green-600",
            "description": format_price(cheapest["avg_price"]),
        },
        {
            "label": "Marca mas cara",
            "value": str(most_expensive["brand"]).title(),
            "color": "text-red-600",
            "description": format_price(most_expensive["avg_price"]),
        },
        {
            "label": "Diferencia max-min",
            "value": f"{diff:.3f} EUR/L",
            "description": "entre la mas barata y la mas cara",
        },
        {
            "label": "Marcas analizadas",
            "value": str(len(df)),
        },
    ]


def volatility_kpis(df) -> List[Dict[str, str]]:
    if df.empty:
        return []

    most_stable = df.iloc[0]
    least_stable = df.iloc[-1]
    median_cv = df["coefficient_of_variation"].median()

    return [
        {
            "label": "Zona mas estable",
            "value": str(most_stable["zip_code"]),
            "color": "text-green-600",
            "description": (
                f"{str(most_stable['province']).title()} | "
                f"CV {format_percentage(most_stable['coefficient_of_variation'])}"
            ),
        },
        {
            "label": "Zona mas volatil",
            "value": str(least_stable["zip_code"]),
            "color": "text-red-600",
            "description": (
                f"{str(least_stable['province']).title()} | "
                f"CV {format_percentage(least_stable['coefficient_of_variation'])}"
            ),
        },
        {
            "label": "CV mediano",
            "value": format_percentage(median_cv),
            "description": "dispersion relativa del precio medio diario",
        },
        {
            "label": "Zonas analizadas",
            "value": str(len(df)),
        },
    ]


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------


def format_data_size(size_bytes: int) -> str:
    """Format bytes as human-readable MB or GB."""
    if size_bytes >= 1_073_741_824:
        return f"{size_bytes / 1_073_741_824:.1f} GB"
    return f"{size_bytes / 1_048_576:.1f} MB"


def data_inventory_kpis(inventory: dict) -> list[dict]:
    """Build KPI cards for the data inventory."""
    return [
        {
            "label": "Dias de datos",
            "value": str(inventory["num_days"]),
        },
        {
            "label": "Meses de datos",
            "value": str(inventory["num_months"]),
        },
        {
            "label": "Anos de datos",
            "value": str(inventory["num_years"]),
        },
        {
            "label": "Tamano aproximado",
            "value": format_data_size(inventory["total_size_bytes"]),
        },
    ]


def latest_day_kpis(latest_stats: dict) -> list[dict]:
    """Build KPI cards for the latest day's snapshot metrics."""
    max_date = latest_stats["max_date"]
    date_str = max_date.isoformat() if max_date else "-"
    return [
        {
            "label": "Ultima fecha disponible",
            "value": date_str,
        },
        {
            "label": "Estaciones cargadas",
            "value": str(latest_stats["unique_stations"]),
        },
        {
            "label": "Provincias",
            "value": str(latest_stats["unique_provinces"]),
        },
        {
            "label": "Comunidades autonomas",
            "value": str(latest_stats["unique_communities"]),
        },
        {
            "label": "Localidades",
            "value": str(latest_stats["unique_localities"]),
        },
        {
            "label": "Tipos de combustible",
            "value": str(latest_stats["unique_fuel_types"]),
        },
    ]


def missing_days_kpis(missing_days: list[str]) -> list[dict]:
    """Build KPI cards summarising missing ingestion days."""
    count = len(missing_days)
    most_recent = missing_days[-1] if missing_days else "-"
    return [
        {
            "label": "Dias sin datos",
            "value": str(count),
            "color": "text-red-600" if count > 0 else "text-green-600",
        },
        {
            "label": "Ultimo dia sin datos",
            "value": most_recent,
        },
    ]
