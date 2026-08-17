import pytest
from api.schemas import EnergyType
from pydantic import ValidationError
from services.vehicle_catalog import CONSUMPTION_UNIT_BY_ENERGY
from services.vehicle_catalog import get_pair
from services.vehicle_catalog import get_vehicle
from services.vehicle_catalog import list_pairs
from services.vehicle_catalog import load_catalog
from services.vehicle_catalog import SEGMENT_LABELS
from services.vehicle_catalog import VehicleCatalog


def _vehicle(**overrides) -> dict:
    base = {
        "id": "test-gas",
        "pair_id": "test",
        "model": "Test",
        "variant": "1.0",
        "model_year": 2022,
        "segment": "compacto",
        "energy_type": "gasoline",
        "consumption": 6.0,
        "consumption_unit": "l/100km",
    }
    return {**base, **overrides}


def _catalog(vehicles: list[dict]) -> dict:
    return {"version": "test", "source": "test", "source_url": "https://example.com", "vehicles": vehicles}


# --- shipped catalog -------------------------------------------------------------------


def test_shipped_catalog_parses():
    catalog = load_catalog()
    assert catalog.version
    assert len(catalog.vehicles) >= 2


def test_shipped_catalog_does_not_claim_a_source_it_does_not_have():
    # The figures are unverified estimates. Until they are replaced from a real dataset (see
    # idae-consumption-ingest-task.md) the catalog must not carry a citation, and the source line
    # must say so — a link that implies provenance is worse than none.
    catalog = load_catalog()
    if not catalog.source_url:
        assert "sin verificar" in catalog.source.lower()
    else:
        assert catalog.source_url.startswith("http")


def test_shipped_catalog_ids_are_unique():
    ids = [v.id for v in load_catalog().vehicles]
    assert len(ids) == len(set(ids))


def test_shipped_catalog_pairs_have_gasoline_and_diesel():
    for pair in list_pairs():
        energy_types = {v.energy_type for v in pair.vehicles}
        assert EnergyType.gasoline in energy_types
        assert EnergyType.diesel in energy_types


def test_shipped_catalog_consumption_is_positive_and_unit_matches():
    for vehicle in load_catalog().vehicles:
        assert vehicle.consumption > 0
        assert vehicle.consumption_unit == CONSUMPTION_UNIT_BY_ENERGY[vehicle.energy_type]


def test_shipped_catalog_segments_are_known():
    for vehicle in load_catalog().vehicles:
        assert vehicle.segment in SEGMENT_LABELS


def test_shipped_catalog_diesel_variants_consume_less_than_gasoline():
    # The whole report rests on this: diesel engines burn fewer litres. A pair where it doesn't hold
    # is a data-entry error, not an interesting finding.
    for pair in list_pairs():
        by_energy = {v.energy_type: v for v in pair.vehicles}
        assert by_energy[EnergyType.diesel].consumption < by_energy[EnergyType.gasoline].consumption


# --- validation ------------------------------------------------------------------------


def test_duplicate_ids_rejected():
    with pytest.raises(ValidationError, match="duplicate vehicle ids"):
        VehicleCatalog(**_catalog([_vehicle(), _vehicle(energy_type="diesel", consumption=5.0)]))


def test_pair_with_single_energy_type_rejected():
    with pytest.raises(ValidationError, match="at least two energy types"):
        VehicleCatalog(**_catalog([_vehicle()]))


def test_pair_with_two_vehicles_of_same_energy_type_rejected():
    vehicles = [
        _vehicle(id="a"),
        _vehicle(id="b"),
        _vehicle(id="c", energy_type="diesel", consumption=5.0),
    ]
    with pytest.raises(ValidationError, match="more than one vehicle per energy type"):
        VehicleCatalog(**_catalog(vehicles))


def test_zero_consumption_rejected():
    with pytest.raises(ValidationError):
        VehicleCatalog(**_catalog([_vehicle(consumption=0)]))


def test_wrong_unit_for_energy_type_rejected():
    with pytest.raises(ValidationError, match="must use l/100km"):
        VehicleCatalog(**_catalog([_vehicle(consumption_unit="kWh/100km")]))


def test_electric_vehicle_requires_kwh_unit():
    # Guards the EV seam: an electric row added later must declare kWh/100km.
    with pytest.raises(ValidationError, match="must use kWh/100km"):
        VehicleCatalog(**_catalog([_vehicle(energy_type="electric", consumption_unit="l/100km")]))


def test_electric_vehicle_with_kwh_unit_accepted():
    vehicles = [
        _vehicle(),
        _vehicle(id="test-diesel", energy_type="diesel", consumption=5.0),
        _vehicle(id="test-ev", energy_type="electric", consumption=16.0, consumption_unit="kWh/100km"),
    ]
    catalog = VehicleCatalog(**_catalog(vehicles))
    assert len(catalog.vehicles) == 3


def test_unknown_segment_rejected():
    with pytest.raises(ValidationError, match="unknown segment"):
        VehicleCatalog(**_catalog([_vehicle(segment="monovolumen")]))


# --- lookups ---------------------------------------------------------------------------


def test_get_vehicle_returns_none_for_unknown_id():
    assert get_vehicle("does-not-exist") is None


def test_get_vehicle_returns_catalog_entry():
    known_id = load_catalog().vehicles[0].id
    vehicle = get_vehicle(known_id)
    assert vehicle is not None
    assert vehicle.id == known_id


def test_get_pair_returns_empty_dict_for_unknown_pair():
    assert get_pair("does-not-exist") == {}


def test_get_pair_keys_by_energy_type():
    pair_id = list_pairs()[0].pair_id
    pair = get_pair(pair_id)
    assert set(pair) >= {EnergyType.gasoline, EnergyType.diesel}
    assert pair[EnergyType.diesel].pair_id == pair_id


def test_list_pairs_orders_by_segment_then_model():
    segment_order = list(SEGMENT_LABELS)
    keys = [(segment_order.index(p.segment), p.model) for p in list_pairs()]
    assert keys == sorted(keys)


# --- catalog options / EV seam ---------------------------------------------------------


def test_default_pair_setting_exists_in_catalog():
    from config import settings

    assert settings.report_fuel_type_default_pair in {p.pair_id for p in list_pairs()}


def test_vehicle_options_exposes_a_breakeven_ratio_per_pair():
    from services.vehicle_cost_service import get_vehicle_options

    options = get_vehicle_options()
    for pair in options["pairs"]:
        assert pair["breakeven_ratio"] > 1  # diesel burns fewer litres, so c_g / c_d > 1


def test_vehicle_options_flags_electricity_as_unpriceable():
    from services.vehicle_cost_service import get_vehicle_options

    assert "electric" in get_vehicle_options()["unpriceable_energy_types"]


def test_pair_breakeven_ratio_is_none_without_a_gasoline_diesel_couple(monkeypatch):
    # The catalog only requires two energy types, so diesel+electric is a valid pair. It must
    # degrade to a null ratio, not blow up the options endpoint.
    from services.vehicle_cost_service import _pair_breakeven_ratio

    catalog = VehicleCatalog(
        **_catalog(
            [
                _vehicle(id="d", energy_type="diesel", consumption=4.5),
                _vehicle(id="e", energy_type="electric", consumption=16.0, consumption_unit="kWh/100km"),
            ]
        )
    )
    pair = type("P", (), {"vehicles": catalog.vehicles})()
    assert _pair_breakeven_ratio(pair) is None
