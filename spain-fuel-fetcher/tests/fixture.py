def get_response_raw_data(n: int = 3) -> dict:
    """Build a raw government API response with n station records.

    Mirrors the real API shape: Spanish field names, comma-decimal strings, and the
    'Horario' field (present in raw, intentionally dropped by the transform).
    """
    stations = []
    for i in range(n):
        stations.append(
            {
                "C.P.": f"2800{i}",
                "IDEESS": str(4000 + i),
                "IDCCAA": "13",
                "IDMunicipio": str(100 + i),
                "IDProvincia": "28",
                "Tipo Venta": "P",
                "Rótulo": f"Station {i}",
                "Dirección": f"Calle {i}",
                "Horario": "L-D: 24H",
                "Municipio": "Madrid",
                "Provincia": "MADRID",
                "Localidad": "MADRID",
                "Latitud": f"40,{4168 + i}",
                "Longitud (WGS84)": f"-3,{7038 + i}",
                "Precio Biodiesel": "",
                "Precio Bioetanol": "",
                "Precio Gas Natural Comprimido": "",
                "Precio Gas Natural Licuado": "",
                "Precio Gases licuados del petróleo": "",
                "Precio Gasoleo A": f"1,{450 + i * 5}",
                "Precio Gasoleo B": "",
                "Precio Gasoleo Premium": "",
                "Precio Gasolina 95 E10": "",
                "Precio Gasolina 95 E5": f"1,{550 + i * 5}",
                "Precio Gasolina 95 E5 Premium": "",
                "Precio Gasolina 98 E10": "",
                "Precio Gasolina 98 E5": "",
                "Precio Hidrogeno": "",
            }
        )
    return {
        "ResultadoConsulta": "OK",
        "Fecha": "04/04/2026 10:30:00",
        "ListaEESSPrecio": stations,
        "Nota": "Test data",
    }
