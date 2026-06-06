import logging
from datetime import datetime
from datetime import timezone

import pandas as pd
from spain_fuel_api.constants import DATA_SOURCE_DATETIME_FORMAT
from spain_fuel_api.constants import DATA_SOURCE_TIMEZONE
from spain_fuel_api.schema import get_expected_columns
from spain_fuel_api.schema import get_float_columns
from spain_fuel_api.schema import get_renaming_map

logger = logging.getLogger(__name__)


def transform_to_dataframe(raw_data: dict) -> pd.DataFrame:
    """Transform a validated raw API response into the normalized output DataFrame.

    String columns are lowercased and stripped; float columns are converted from
    comma-decimal strings to floats (invalid values become NaN); a UTC ISO timestamp
    is derived from the API 'Fecha' field. Output columns follow get_expected_columns().
    """
    data = raw_data["ListaEESSPrecio"]
    renaming_map = get_renaming_map()
    float_columns = get_float_columns()

    df = pd.DataFrame(data).rename(columns=renaming_map)[list(renaming_map.values())]

    for column in df.columns:
        df[column] = df[column].str.lower().str.strip()
        if column in float_columns:
            df[column] = pd.to_numeric(df[column].str.replace(",", "."), errors="coerce")

    fecha_str = raw_data["Fecha"]
    datetime_obj = datetime.strptime(fecha_str, DATA_SOURCE_DATETIME_FORMAT)
    utc_datetime_obj = DATA_SOURCE_TIMEZONE.localize(datetime_obj).astimezone(timezone.utc)
    df["timestamp"] = utc_datetime_obj.isoformat()

    return df[get_expected_columns()]
