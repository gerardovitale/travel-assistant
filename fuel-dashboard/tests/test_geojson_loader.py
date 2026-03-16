import json
from unittest.mock import patch

import data.geojson_loader as loader_module


SAMPLE_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"COD_POSTAL": "28001"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-3.71, 40.41], [-3.70, 40.41], [-3.70, 40.42], [-3.71, 40.42], [-3.71, 40.41]]],
            },
        },
        {
            "type": "Feature",
            "properties": {"COD_POSTAL": "08001"},
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [
                    [[[2.16, 41.38], [2.17, 41.38], [2.17, 41.39], [2.16, 41.39], [2.16, 41.38]]],
                ],
            },
        },
    ],
}


def _reset_cache():
    loader_module._postal_code_index = None


def test_load_postal_code_boundary_returns_feature(tmp_path):
    _reset_cache()
    geojson_path = tmp_path / "spain-postal-codes.geojson"
    geojson_path.write_text(json.dumps(SAMPLE_GEOJSON), encoding="utf-8")

    with patch.object(loader_module, "_GEOJSON_DIR", tmp_path):
        result = loader_module.load_postal_code_boundary("28001")

    assert result is not None
    assert result["properties"]["COD_POSTAL"] == "28001"
    assert result["geometry"]["type"] == "Polygon"
    _reset_cache()


def test_load_postal_code_boundary_returns_none_for_unknown(tmp_path):
    _reset_cache()
    geojson_path = tmp_path / "spain-postal-codes.geojson"
    geojson_path.write_text(json.dumps(SAMPLE_GEOJSON), encoding="utf-8")

    with patch.object(loader_module, "_GEOJSON_DIR", tmp_path):
        result = loader_module.load_postal_code_boundary("99999")

    assert result is None
    _reset_cache()


def test_load_postal_code_boundary_missing_file(tmp_path):
    _reset_cache()
    with patch.object(loader_module, "_GEOJSON_DIR", tmp_path):
        result = loader_module.load_postal_code_boundary("28001")

    assert result is None
    _reset_cache()


def test_load_postal_code_boundary_multipolygon(tmp_path):
    _reset_cache()
    geojson_path = tmp_path / "spain-postal-codes.geojson"
    geojson_path.write_text(json.dumps(SAMPLE_GEOJSON), encoding="utf-8")

    with patch.object(loader_module, "_GEOJSON_DIR", tmp_path):
        result = loader_module.load_postal_code_boundary("08001")

    assert result is not None
    assert result["geometry"]["type"] == "MultiPolygon"
    _reset_cache()


MULTI_ZIP_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"COD_POSTAL": "28001"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-3.71, 40.41], [-3.70, 40.41], [-3.70, 40.42], [-3.71, 40.42], [-3.71, 40.41]]],
            },
        },
        {
            "type": "Feature",
            "properties": {"COD_POSTAL": "28002"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-3.70, 40.42], [-3.69, 40.42], [-3.69, 40.43], [-3.70, 40.43], [-3.70, 40.42]]],
            },
        },
        {
            "type": "Feature",
            "properties": {"COD_POSTAL": "08001"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[2.16, 41.38], [2.17, 41.38], [2.17, 41.39], [2.16, 41.39], [2.16, 41.38]]],
            },
        },
    ],
}


def test_load_postal_codes_for_zip_list_returns_matching(tmp_path):
    _reset_cache()
    geojson_path = tmp_path / "spain-postal-codes.geojson"
    geojson_path.write_text(json.dumps(MULTI_ZIP_GEOJSON), encoding="utf-8")

    with patch.object(loader_module, "_GEOJSON_DIR", tmp_path):
        result = loader_module.load_postal_codes_for_zip_list(["28001", "28002"])

    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 2
    codes = {f["properties"]["COD_POSTAL"] for f in result["features"]}
    assert codes == {"28001", "28002"}
    _reset_cache()


def test_load_postal_codes_for_zip_list_skips_unknown(tmp_path):
    _reset_cache()
    geojson_path = tmp_path / "spain-postal-codes.geojson"
    geojson_path.write_text(json.dumps(MULTI_ZIP_GEOJSON), encoding="utf-8")

    with patch.object(loader_module, "_GEOJSON_DIR", tmp_path):
        result = loader_module.load_postal_codes_for_zip_list(["28001", "99999"])

    assert len(result["features"]) == 1
    assert result["features"][0]["properties"]["COD_POSTAL"] == "28001"
    _reset_cache()


def test_load_postal_codes_for_zip_list_empty_input(tmp_path):
    _reset_cache()
    geojson_path = tmp_path / "spain-postal-codes.geojson"
    geojson_path.write_text(json.dumps(MULTI_ZIP_GEOJSON), encoding="utf-8")

    with patch.object(loader_module, "_GEOJSON_DIR", tmp_path):
        result = loader_module.load_postal_codes_for_zip_list([])

    assert result["features"] == []
    _reset_cache()
