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
            "min_distance_km": None,
            "max_distance_km": None,
            "best_score": None,
        }

    distances = [s.distance_km for s in stations if s.distance_km is not None]
    scores = [s.score for s in stations if s.score is not None]
    return {
        "count": len(stations),
        "best_price": min(s.price for s in stations),
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
        }

    avg_prices = [p.avg_price for p in trend_data]
    return {
        "current_avg_price": avg_prices[-1],
        "min_price": min(p.min_price for p in trend_data),
        "max_price": max(p.max_price for p in trend_data),
        "delta_avg_price": avg_prices[-1] - avg_prices[0],
    }


def zone_kpis(zones: Sequence[ZoneResult]) -> Dict[str, Any]:
    if not zones:
        return {"zone_count": 0, "cheapest_zip": None, "cheapest_avg_price": None, "province_avg_price": None}

    cheapest_zone = min(zones, key=lambda z: z.avg_price)
    province_avg = sum(z.avg_price for z in zones) / len(zones)
    return {
        "zone_count": len(zones),
        "cheapest_zip": cheapest_zone.zip_code,
        "cheapest_avg_price": cheapest_zone.avg_price,
        "province_avg_price": province_avg,
    }


def search_summary_cards(summary: Dict[str, Any], mode: str) -> List[Dict[str, str]]:
    mode_meta = search_mode_metadata(mode)
    cards = [
        {"label": "Estaciones", "value": str(summary["count"])},
        {"label": "Mejor precio", "value": format_price(summary["best_price"])},
    ]
    if mode == "best_by_address":
        best_score = summary["best_score"]
        cards.append(
            {"label": mode_meta.success_metric_label, "value": "-" if best_score is None else f"{best_score:.2f}"}
        )
    else:
        cards.append({"label": "Distancia minima", "value": format_distance(summary["min_distance_km"])})
    cards.append({"label": "Distancia maxima", "value": format_distance(summary["max_distance_km"])})
    return cards
