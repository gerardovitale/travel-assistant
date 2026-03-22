import json
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional

_GEOJSON_DIR = Path(__file__).resolve().parent / "geojson"

_provinces_geojson: Optional[dict] = None
_provinces_name_lookup: Optional[Dict[str, str]] = None
_districts_geojson: Optional[dict] = None
_postal_code_index: Optional[Dict[str, dict]] = None

ZIP_CODE_PROPERTY = "COD_POSTAL"

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


def _ensure_postal_index() -> Dict[str, dict]:
    global _postal_code_index
    if _postal_code_index is None:
        path = _GEOJSON_DIR / "spain-postal-codes.geojson"
        if not path.exists():
            _postal_code_index = {}
            return _postal_code_index
        with open(path, encoding="utf-8") as f:
            geojson = json.load(f)
        _postal_code_index = {}
        for feature in geojson["features"]:
            code = str(feature["properties"].get(ZIP_CODE_PROPERTY, "")).strip()
            if code:
                _postal_code_index[code] = {
                    "type": "Feature",
                    "properties": {ZIP_CODE_PROPERTY: code},
                    "geometry": feature["geometry"],
                }
    return _postal_code_index


def load_postal_code_boundary(zip_code: str) -> Optional[dict]:
    index = _ensure_postal_index()
    return index.get(zip_code)


def load_postal_codes_for_zip_list(zip_codes: List[str]) -> dict:
    """Return a GeoJSON FeatureCollection containing only features matching the given zip codes."""
    index = _ensure_postal_index()
    features = []
    for zc in zip_codes:
        feature = index.get(zc)
        if feature is not None:
            features.append(feature)
    return {"type": "FeatureCollection", "features": features}


def load_madrid_districts() -> dict:
    global _districts_geojson
    if _districts_geojson is None:
        path = _GEOJSON_DIR / "distritos-madrid.geojson"
        with open(path, encoding="utf-8") as f:
            _districts_geojson = json.load(f)
    return _districts_geojson
