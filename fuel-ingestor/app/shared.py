import pandas as pd

FUEL_PRICE_COLUMNS = [
    "biodiesel_price",
    "bioethanol_price",
    "compressed_natural_gas_price",
    "liquefied_natural_gas_price",
    "liquefied_petroleum_gases_price",
    "diesel_a_price",
    "diesel_b_price",
    "diesel_premium_price",
    "gasoline_95_e10_price",
    "gasoline_95_e5_price",
    "gasoline_95_e5_premium_price",
    "gasoline_98_e10_price",
    "gasoline_98_e5_price",
    "hydrogen_price",
]


def _snapshot_date(raw_df: pd.DataFrame):
    return pd.to_datetime(raw_df["timestamp"].iloc[0]).date()


def _log_event(log_method, event, **fields):
    if fields:
        details = " ".join(f"{key}={value!r}" for key, value in fields.items())
        log_method(f"{event} {details}")
        return
    log_method(event)
