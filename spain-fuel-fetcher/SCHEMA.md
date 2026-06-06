# Spain fuel-price data contract

`spain-fuel-fetcher` is the single source of truth for fetching the Spain government
fuel-price API and normalizing it. This document is the **data contract** between the
government API and the two consuming services (`fuel-ingestor`, `fuel-dashboard`).

- **Source endpoint:**
  `https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/`
- **Transport:** `curl` subprocess (Python's OpenSSL 3.x is blocked by the server's TLS
  fingerprinting; curl's handshake is accepted). Forced `--tlsv1.2 --tls-max 1.2`.
- **Format:** JSON. All values are strings; decimals use a comma (`,`) separator
  (Spanish locale).

## 1. Raw government API response

Top-level object:

| Field               | Type   | Notes                                                                    |
| ------------------- | ------ | ------------------------------------------------------------------------ |
| `ResultadoConsulta` | string | `"OK"` on success; anything else is rejected by `validate_api_response`. |
| `Fecha`             | string | Snapshot time, format `%d/%m/%Y %H:%M:%S`, timezone **Europe/Madrid**.   |
| `ListaEESSPrecio`   | array  | One object per station (see below). Must be a non-empty list.            |
| `Nota`              | string | Free-text metadata note (ignored).                                       |

Each `ListaEESSPrecio` station object (Spanish field names):

| Raw field                            | Output column                     | Type                                 |
| ------------------------------------ | --------------------------------- | ------------------------------------ |
| `C.P.`                               | `zip_code`                        | string                               |
| `IDEESS`                             | `eess_id`                         | string                               |
| `IDCCAA`                             | `ccaa_id`                         | string                               |
| `IDMunicipio`                        | `municipality_id`                 | string                               |
| `IDProvincia`                        | `province_id`                     | string                               |
| `Tipo Venta`                         | `sale_type`                       | string (`P` public / `R` restricted) |
| `Rótulo`                             | `label`                           | string (brand)                       |
| `Dirección`                          | `address`                         | string                               |
| `Municipio`                          | `municipality`                    | string                               |
| `Provincia`                          | `province`                        | string                               |
| `Localidad`                          | `locality`                        | string                               |
| `Latitud`                            | `latitude`                        | float (comma-decimal)                |
| `Longitud (WGS84)`                   | `longitude`                       | float (comma-decimal)                |
| `Precio Biodiesel`                   | `biodiesel_price`                 | float (comma-decimal, may be empty)  |
| `Precio Bioetanol`                   | `bioethanol_price`                | float                                |
| `Precio Gas Natural Comprimido`      | `compressed_natural_gas_price`    | float                                |
| `Precio Gas Natural Licuado`         | `liquefied_natural_gas_price`     | float                                |
| `Precio Gases licuados del petróleo` | `liquefied_petroleum_gases_price` | float                                |
| `Precio Gasoleo A`                   | `diesel_a_price`                  | float                                |
| `Precio Gasoleo B`                   | `diesel_b_price`                  | float                                |
| `Precio Gasoleo Premium`             | `diesel_premium_price`            | float                                |
| `Precio Gasolina 95 E10`             | `gasoline_95_e10_price`           | float                                |
| `Precio Gasolina 95 E5`              | `gasoline_95_e5_price`            | float                                |
| `Precio Gasolina 95 E5 Premium`      | `gasoline_95_e5_premium_price`    | float                                |
| `Precio Gasolina 98 E10`             | `gasoline_98_e10_price`           | float                                |
| `Precio Gasolina 98 E5`              | `gasoline_98_e5_price`            | float                                |
| `Precio Hidrogeno`                   | `hydrogen_price`                  | float                                |

> `Horario` (opening hours) is present in the raw response but intentionally **dropped**
> by the transform — it is not part of the output contract.

## 2. Output DataFrame (the contract consumed by services)

`transform_to_dataframe` / `fetch_fuel_stations` return a pandas DataFrame with exactly
these 28 columns, in this order (`get_expected_columns()`):

```
timestamp, zip_code, eess_id, ccaa_id, municipality_id, province_id, sale_type,
label, address, municipality, province, locality, latitude, longitude,
biodiesel_price, bioethanol_price, compressed_natural_gas_price,
liquefied_natural_gas_price, liquefied_petroleum_gases_price, diesel_a_price,
diesel_b_price, diesel_premium_price, gasoline_95_e10_price, gasoline_95_e5_price,
gasoline_95_e5_premium_price, gasoline_98_e10_price, gasoline_98_e5_price,
hydrogen_price
```

Normalization rules:

- **`timestamp`**: derived from `Fecha`, localized from Europe/Madrid to **UTC**, emitted
  as an ISO-8601 string (e.g. `2024-10-09T20:12:15+00:00`). Constant per snapshot.
- **String columns**: lowercased and stripped.
- **Float columns** (`get_float_columns()` — the 2 coordinates + 14 price columns):
  comma→dot conversion then `pd.to_numeric(..., errors="coerce")`. Empty or invalid
  values become `NaN`.

## 3. Public API

```python
from spain_fuel_api import (
    fetch_fuel_stations,      # fetch -> validate -> transform -> DataFrame (raises)
    fetch_raw_data,           # curl + retry -> raw dict (raises after exhaustion)
    validate_api_response,    # raise ValueError on malformed response
    transform_to_dataframe,   # raw dict -> normalized DataFrame
    get_renaming_map,
    get_float_columns,
    get_expected_columns,
    DATA_SOURCE_URL,
    DATA_SOURCE_DATETIME_FORMAT,
    DATA_SOURCE_TIMEZONE,
)
```

`fetch_*` accept `curl_timeout`, `connect_timeout`, `max_retries`, `retry_base_delay`,
and `exponential_backoff` (default `True`: delays `base * 2**(attempt-1)`; set `False`
for a fixed `base` delay).

### Service-specific responsibilities (NOT in this package)

- **fuel-ingestor**: `validate_dataframe` (station-count / price-range / coordinate-range
  data-quality checks) and GCS Parquet upload.
- **fuel-dashboard**: graceful-degradation wrapper (`fetch_realtime_stations` returns
  `None` on failure) and the `MIN_EXPECTED_STATIONS` floor.
