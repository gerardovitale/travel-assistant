import pandas as pd


def make_stations_df(fuel_type="diesel_a_price", n=5):
    rows = []
    for i in range(n):
        rows.append(
            {
                "label": f"station_{i}",
                "address": f"calle {i}",
                "municipality": "madrid",
                "province": "madrid",
                "zip_code": f"2800{i}",
                "latitude": 40.4168 + i * 0.01,
                "longitude": -3.7038 + i * 0.01,
                fuel_type: 1.50 + i * 0.05,
                "distance_km": 1.0 + i * 0.5,
            }
        )
    return pd.DataFrame(rows)


SAMPLE_FUEL_TYPE = "diesel_a_price"


def make_group_stations_df(primary_fuel="diesel_a_price", all_fuels=None, n=3):
    if all_fuels is None:
        all_fuels = [primary_fuel, "diesel_b_price", "diesel_premium_price"]
    rows = []
    for i in range(n):
        row = {
            "label": f"station_{i}",
            "address": f"calle {i}",
            "municipality": "madrid",
            "province": "madrid",
            "zip_code": f"2800{i}",
            "latitude": 40.4168 + i * 0.01,
            "longitude": -3.7038 + i * 0.01,
            "distance_km": 1.0 + i * 0.5,
        }
        for ft in all_fuels:
            if ft == primary_fuel:
                row[ft] = 1.50 + i * 0.05
            else:
                row[ft] = 1.45 + i * 0.05 if i % 2 == 0 else None
        rows.append(row)
    return pd.DataFrame(rows)
