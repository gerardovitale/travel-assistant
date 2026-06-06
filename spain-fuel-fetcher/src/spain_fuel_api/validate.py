from datetime import datetime

from spain_fuel_api.constants import DATA_SOURCE_DATETIME_FORMAT


def validate_api_response(raw_data: dict) -> None:
    """Validate the raw government API response structure. Raises ValueError on problems."""
    status = raw_data.get("ResultadoConsulta")
    if status != "OK":
        raise ValueError(f"API returned non-OK status: {status!r}")

    stations = raw_data.get("ListaEESSPrecio")
    if not stations or not isinstance(stations, list):
        raise ValueError(f"API response missing or empty 'ListaEESSPrecio' (got {type(stations).__name__})")

    fecha = raw_data.get("Fecha")
    if not fecha:
        raise ValueError("API response missing 'Fecha' field")
    try:
        datetime.strptime(fecha, DATA_SOURCE_DATETIME_FORMAT)
    except ValueError:
        raise ValueError(f"API 'Fecha' field has unexpected format: {fecha!r}")
