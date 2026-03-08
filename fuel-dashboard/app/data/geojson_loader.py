import json
from pathlib import Path
from typing import Dict
from typing import Optional

_GEOJSON_DIR = Path(__file__).resolve().parent / "geojson"

_provinces_geojson: Optional[dict] = None
_provinces_name_lookup: Optional[Dict[str, str]] = None
_districts_geojson: Optional[dict] = None

_DATA_TO_GEOJSON_OVERRIDES: Dict[str, str] = {
    "coruña (a)": "A Coruña",
    "rioja (la)": "La Rioja",
    "palmas (las)": "Las Palmas",
    "balears (illes)": "Illes Balears",
    "valencia / valència": "València/Valencia",
    "castellón / castelló": "Castelló/Castellón",
    "gipuzkoa": "Gipuzkoa/Guipúzcoa",
    "bizkaia": "Bizkaia/Vizcaya",
    "araba/álava": "Araba/Álava",
    "alicante": "Alacant/Alicante",
}

_NON_MAINLAND_PROVINCES = {
    "Las Palmas",
    "Santa Cruz De Tenerife",
    "Illes Balears",
    "Ceuta",
    "Melilla",
}


def load_provinces_geojson() -> dict:
    global _provinces_geojson, _provinces_name_lookup
    if _provinces_geojson is None:
        path = _GEOJSON_DIR / "spain-provinces.geojson"
        with open(path, encoding="utf-8") as f:
            _provinces_geojson = json.load(f)
        _provinces_name_lookup = {}
        for feature in _provinces_geojson["features"]:
            geojson_name = feature["properties"]["name"]
            _provinces_name_lookup[geojson_name.lower()] = geojson_name
        _provinces_name_lookup.update(_DATA_TO_GEOJSON_OVERRIDES)
    return _provinces_geojson


def get_geojson_province_name(data_province: str) -> Optional[str]:
    load_provinces_geojson()
    return _provinces_name_lookup.get(data_province.lower())


def load_madrid_districts() -> dict:
    global _districts_geojson
    if _districts_geojson is None:
        path = _GEOJSON_DIR / "distritos-madrid.geojson"
        with open(path, encoding="utf-8") as f:
            _districts_geojson = json.load(f)
    return _districts_geojson
