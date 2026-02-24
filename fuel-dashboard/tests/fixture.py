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
