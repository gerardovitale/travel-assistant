# Curated vehicle-consumption catalog backing the fuel-type report (Reportes tab).
#
# The catalog is a committed JSON file, not ingested data: the model selection is an editorial choice
# and the consumption figures are manufacturer-declared WLTP values. Both facts are surfaced in the UI
# (``version`` / ``source_url``) so the reader can judge them.
#
# Vehicles are grouped by ``pair_id`` so the comparison is like-for-like (same model, same generation,
# different engine). An electric row added under an existing ``pair_id`` extends the comparison to
# three ways without any change here.
import json
from functools import lru_cache
from pathlib import Path

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
