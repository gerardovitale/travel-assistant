from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

from api.schemas import AlternativePlan
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

SEARCH_MODE_OPTIONS: Dict[str, str] = {
    "cheapest_by_zip": "Mas barato por codigo postal",
    "nearest_by_address": "Mas cercano por direccion",
    "cheapest_by_address": "Mas barato cerca de direccion",
    "best_by_address": "Mejor opcion",
}

TREND_PERIOD_LABELS: Dict[str, str] = {
    TrendPeriod.week.value: "7 dias",
    TrendPeriod.month.value: "30 dias",
    TrendPeriod.quarter.value: "90 dias",
}


@dataclass(frozen=True)
class SearchModeMeta:
    query_label: str
    query_placeholder: str
    helper_text: str
    requires_radius: bool
    success_metric_label: str
    requires_consumption: bool = False


SEARCH_MODE_META: Dict[str, SearchModeMeta] = {
    "cheapest_by_zip": SearchModeMeta(
        query_label="Codigo postal",
        query_placeholder="Ejemplo: 28001",
        helper_text="Introduce un codigo postal de Espana para comparar precios.",
        requires_radius=False,
        success_metric_label="Mejor precio",
    ),
    "nearest_by_address": SearchModeMeta(
        query_label="Direccion o referencia",
        query_placeholder="Ejemplo: Gran Via 1, Madrid",
        helper_text="Usa una direccion para ver las estaciones mas cercanas.",
        requires_radius=False,
        success_metric_label="Distancia minima",
    ),
    "cheapest_by_address": SearchModeMeta(
        query_label="Direccion o referencia",
        query_placeholder="Ejemplo: Calle Alcala 45, Madrid",
        helper_text="Busca estaciones cercanas y ordena por precio.",
        requires_radius=True,
        success_metric_label="Mejor precio",
    ),
    "best_by_address": SearchModeMeta(
        query_label="Direccion o referencia",
        query_placeholder="Ejemplo: Atocha, Madrid",
        helper_text="Calcula el coste total (repostaje + viaje ida y vuelta) para encontrar la opcion mas barata.",
        requires_radius=True,
        success_metric_label="Mejor puntuacion",
        requires_consumption=True,
    ),
}


SCORE_METHODOLOGY_LINES: List[str] = [
    "La puntuacion (0-10) se basa en el coste total real de repostar en cada estacion:",
    "",
    "Coste total = precio x (litros del deposito + combustible del viaje ida y vuelta)",
    "",
    "El combustible del viaje = 2 x distancia (km) x consumo (l/100km) / 100.",
    "Esto incluye el coste real de ir a la estacion y volver.",
    "",
    "La estacion con menor coste total recibe un 10; la de mayor coste un 0.",
    "No hay pesos arbitrarios: tu consumo y la distancia determinan",
    "automaticamente cuando compensa ir mas lejos por un precio mas bajo.",
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


def format_distance(distance_km: Optional[float]) -> str:
    if distance_km is None:
        return "-"
    return f"{distance_km:.2f} km"


def format_delta(delta: float) -> str:
    sign = "+" if delta >= 0 else ""
    return f"{sign}{delta:.3f} EUR/L"


def station_summary(stations: Sequence[StationResult]) -> Dict[str, Any]:
    if not stations:
        return {
            "count": 0,
            "best_price": None,
            "avg_price": None,
            "best_station_label": None,
            "min_distance_km": None,
            "max_distance_km": None,
            "best_score": None,
            "best_estimated_cost": None,
        }

    distances = [s.distance_km for s in stations if s.distance_km is not None]
    scores = [s.score for s in stations if s.score is not None]
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
        "best_score": max(scores) if scores else None,
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
        best_score = summary["best_score"]
        cards.append(
            {
                "label": "Mejor coste total",
                "value": "-" if best_cost is None else f"{best_cost:.2f} EUR",
                "color": "text-green-600",
                "description": "-" if best_score is None else f"Puntuacion: {best_score:.1f}/10",
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
