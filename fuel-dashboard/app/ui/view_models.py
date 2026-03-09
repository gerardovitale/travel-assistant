from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

from api.schemas import StationResult
from api.schemas import TrendPeriod
from api.schemas import TrendPoint
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
    "best_by_address": "Mejor opcion (precio + distancia)",
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
        helper_text="Combina precio y distancia para recomendar la mejor opcion.",
        requires_radius=True,
        success_metric_label="Mejor puntuacion",
    ),
}


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
        }

    distances = [s.distance_km for s in stations if s.distance_km is not None]
    scores = [s.score for s in stations if s.score is not None]
    prices = [s.price for s in stations]
    best_station = min(stations, key=lambda s: s.price)
    return {
        "count": len(stations),
        "best_price": min(prices),
        "avg_price": sum(prices) / len(prices),
        "best_station_label": best_station.label,
        "min_distance_km": min(distances) if distances else None,
        "max_distance_km": max(distances) if distances else None,
        "best_score": min(scores) if scores else None,
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
    mode_meta = search_mode_metadata(mode)
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
        best_score = summary["best_score"]
        cards.append(
            {"label": mode_meta.success_metric_label, "value": "-" if best_score is None else f"{best_score:.2f}"}
        )
    else:
        cards.append({"label": "Distancia minima", "value": format_distance(summary["min_distance_km"])})
    return cards
