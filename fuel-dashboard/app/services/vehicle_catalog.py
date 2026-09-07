# Curated vehicle-consumption catalog backing the fuel-type report (Reportes tab).
#
# The catalog is a committed JSON file, regenerated (not hand-written) from IDAE's public "Base de
# Datos de Consumo de Carburante y Emisiones" by the scripts/idae_*.py pipeline: model selection is
# still an editorial choice (which pairs to include), but every consumption figure is IDAE's own
# WLTP-declared value for that exact trim, traceable via each vehicle's ``source_url``. See
# idae-consumption-ingest-task.md and scripts/idae_vehicle_pairs.py for how pairs are chosen.
#
# Vehicles are grouped by ``pair_id`` so the comparison is like-for-like (same model, same generation,
# different engine). An electric row added under an existing ``pair_id`` extends the comparison to
# three ways without any change here.
import json
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Literal

from api.schemas import EnergyType
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

_CATALOG_PATH = Path(__file__).resolve().parents[1] / "data" / "vehicle_catalog.json"

# Consumption is always "units per 100 km"; only the unit changes per energy source. This is what
# keeps `cost_per_100km = consumption * price_per_unit` valid for liquid fuels and electricity alike.
CONSUMPTION_UNIT_BY_ENERGY: dict[EnergyType, str] = {
    EnergyType.gasoline: "l/100km",
    EnergyType.diesel: "l/100km",
    EnergyType.lpg: "l/100km",
    EnergyType.electric: "kWh/100km",
}

# Loose plausibility ceiling per unit, not a precision bound: `Field(gt=0)` alone would let a stray
# data-entry typo (a misplaced decimal point, e.g. IDAE reporting 54.0 instead of 5.4) through
# untouched. Wide enough to cover everything from a city car to a heavy 4x4/van (still M1) or a
# thirsty early EV, tight enough to catch an order-of-magnitude error.
MAX_PLAUSIBLE_CONSUMPTION_BY_ENERGY: dict[EnergyType, float] = {
    EnergyType.gasoline: 20.0,
    EnergyType.diesel: 20.0,
    EnergyType.lpg: 20.0,
    EnergyType.electric: 40.0,
}

SEGMENT_LABELS: dict[str, str] = {
    "utilitario": "Utilitario",
    "compacto": "Compacto",
    "berlina": "Berlina",
    "suv_compacto": "SUV compacto",
    "suv_grande": "SUV grande",
}


class Vehicle(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    id: str
    pair_id: str
    model: str
    variant: str
    model_year: int
    segment: str
    energy_type: EnergyType
    consumption: float = Field(gt=0)
    consumption_unit: str
    # Where this row can be verified. IDAE has no per-vehicle permalink, so every vehicle from a
    # given ingest run carries the same catalog-wide CSV URL -- not a deep link, but exactly where
    # the number can be checked by searching for `variant`.
    source_url: str
    # Fixed to WLTP for now: the ingest script drops NEDC-only IDAE rows before they ever reach
    # this model, so mixing cycles is a structural impossibility, not just a convention. If NEDC
    # support is ever wanted, widen this to an enum.
    cycle: Literal["WLTP"]
    # When a human last confirmed this pairing is still reasonable -- set by the ingest script at
    # run time, not derived from IDAE's own dates.
    reviewed_on: date
    # Hand-set by the curator (scripts/idae_vehicle_pairs.py), not inferred from IDAE's dates, and
    # -- unlike consumption/source_url -- not sourced from IDAE at all: it's the curator's general
    # knowledge as of that file's per-entry comment date, and can go stale silently. The UI should
    # not imply you can go buy a discontinued diesel, but treat this field as a hint, not a fact.
    market_status: Literal["vigente", "descontinuado"]

    @property
    def label(self) -> str:
        return f"{self.model} {self.variant}"

    @model_validator(mode="after")
    def _validate_unit_and_segment(self) -> "Vehicle":
        expected_unit = CONSUMPTION_UNIT_BY_ENERGY[self.energy_type]
        if self.consumption_unit != expected_unit:
            raise ValueError(f"{self.id}: {self.energy_type.value} must use {expected_unit}")
        if self.segment not in SEGMENT_LABELS:
            raise ValueError(f"{self.id}: unknown segment {self.segment!r}")
        if not self.source_url.startswith("http"):
            raise ValueError(f"{self.id}: source_url must be a real link, got {self.source_url!r}")
        max_consumption = MAX_PLAUSIBLE_CONSUMPTION_BY_ENERGY[self.energy_type]
        if self.consumption > max_consumption:
            raise ValueError(f"{self.id}: consumption {self.consumption} exceeds plausible ceiling {max_consumption}")
        return self


class VehiclePair(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    pair_id: str
    model: str
    segment: str
    model_year: int
    vehicles: list[Vehicle]


class VehicleCatalog(BaseModel):
    version: str
    source: str
    source_url: str
    notes: str = ""
    vehicles: list[Vehicle]

    @model_validator(mode="after")
    def _validate_pairs(self) -> "VehicleCatalog":
        ids = [v.id for v in self.vehicles]
        duplicates = {i for i in ids if ids.count(i) > 1}
        if duplicates:
            raise ValueError(f"duplicate vehicle ids: {sorted(duplicates)}")

        by_pair: dict[str, list[Vehicle]] = {}
        for vehicle in self.vehicles:
            by_pair.setdefault(vehicle.pair_id, []).append(vehicle)
        for pair_id, members in by_pair.items():
            energy_types = {m.energy_type for m in members}
            if len(energy_types) < 2:
                raise ValueError(f"pair {pair_id!r} needs at least two energy types to be comparable")
            if len(energy_types) != len(members):
                raise ValueError(f"pair {pair_id!r} has more than one vehicle per energy type")
        return self


@lru_cache(maxsize=1)
def load_catalog() -> VehicleCatalog:
    """Parse and validate the shipped catalog.

    Lazily cached rather than parsed at import so a malformed file fails the request that needs it
    instead of taking the whole app down at startup. A unit test validates the shipped file.
    """
    with open(_CATALOG_PATH, encoding="utf-8") as f:
        return VehicleCatalog(**json.load(f))


def get_vehicle(vehicle_id: str) -> Vehicle | None:
    return next((v for v in load_catalog().vehicles if v.id == vehicle_id), None)


def get_pair(pair_id: str) -> dict[EnergyType, Vehicle]:
    """Return the pair's variants keyed by energy type; empty dict when the pair is unknown."""
    return {v.energy_type: v for v in load_catalog().vehicles if v.pair_id == pair_id}


def list_pairs() -> list[VehiclePair]:
    """Model pairs ordered by segment (as declared in SEGMENT_LABELS) then model name."""
    grouped: dict[str, list[Vehicle]] = {}
    for vehicle in load_catalog().vehicles:
        grouped.setdefault(vehicle.pair_id, []).append(vehicle)

    segment_order = list(SEGMENT_LABELS)
    pairs = []
    for pair_id, members in grouped.items():
        # Sorted outside the constructor: inside it, the field's declared type makes the lambda
        # parameter infer as `Vehicle | Mapping[str, Any]` and `.energy_type` stops resolving.
        ordered = sorted(members, key=lambda v: v.energy_type.value)
        pairs.append(
            VehiclePair(
                pair_id=pair_id,
                model=members[0].model,
                segment=members[0].segment,
                model_year=members[0].model_year,
                vehicles=ordered,
            )
        )
    return sorted(pairs, key=lambda p: (segment_order.index(p.segment), p.model))
