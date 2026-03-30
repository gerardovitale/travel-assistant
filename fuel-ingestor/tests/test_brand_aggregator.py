import pandas as pd
from aggregator import compute_brand_daily_stats


def _make_brand_raw_df(labels=None, n_per_brand=12):
    """Create a raw DataFrame with multiple brands, each with enough stations to pass MIN_STATION_COUNT."""
    if labels is None:
        labels = ["repsol", "shell", "bp"]
    rows = []
    for i, label in enumerate(labels):
        for j in range(n_per_brand):
            rows.append(
                {
                    "timestamp": "2026-03-22T05:48:06",
                    "eess_id": f"{i * 100 + j}",
                    "municipality_id": "101",
                    "province_id": "28",
                    "label": label,
                    "province": "madrid",
                    "municipality": "Madrid",
                    "locality": "Madrid",
                    "diesel_a_price": 1.40 + i * 0.05 + j * 0.001,
                    "gasoline_95_e5_price": 1.55 + i * 0.03 + j * 0.001,
                }
            )
    return pd.DataFrame(rows)


class TestComputeBrandDailyStats:

    def test_computes_stats_per_brand_and_fuel_type(self):
        raw_df = _make_brand_raw_df()
        result = compute_brand_daily_stats(raw_df)

        assert not result.empty
        assert set(result.columns) == {
            "date",
            "brand",
            "fuel_type",
            "avg_price",
            "min_price",
            "max_price",
            "station_count",
        }

        # Should have entries for each brand and fuel type combination
        brands = result["brand"].unique()
        assert "repsol" in brands
        assert "shell" in brands
        assert "bp" in brands

        fuel_types = result["fuel_type"].unique()
        assert "diesel_a_price" in fuel_types
        assert "gasoline_95_e5_price" in fuel_types

    def test_filters_out_numbered_station_ids(self):
        raw_df = _make_brand_raw_df(labels=["repsol", "Nº 10.935", "shell"])
        result = compute_brand_daily_stats(raw_df)

        brands = result["brand"].unique()
        assert "repsol" in brands
        assert "shell" in brands
        # Numbered station IDs should be filtered out
        for brand in brands:
            assert "10.935" not in str(brand)

    def test_filters_brands_below_min_station_count(self):
        raw_df = _make_brand_raw_df(labels=["repsol", "shell"], n_per_brand=12)
        # Add a brand with very few stations (below MIN_STATION_COUNT)
        small_brand = pd.DataFrame(
            {
                "timestamp": ["2026-03-22T05:48:06"] * 3,
                "eess_id": ["900", "901", "902"],
                "municipality_id": ["101"] * 3,
                "province_id": ["28"] * 3,
                "label": ["tiny_brand"] * 3,
                "province": ["madrid"] * 3,
                "municipality": ["Madrid"] * 3,
                "locality": ["Madrid"] * 3,
                "diesel_a_price": [1.45, 1.46, 1.47],
                "gasoline_95_e5_price": [1.55, 1.56, 1.57],
            }
        )
        raw_df = pd.concat([raw_df, small_brand], ignore_index=True)
        result = compute_brand_daily_stats(raw_df)

        brands = result["brand"].unique()
        assert "tiny_brand" not in brands
        assert "repsol" in brands

    def test_normalizes_brand_names_case(self):
        raw_df = _make_brand_raw_df(labels=["REPSOL", "Shell", "  BP  "])
        result = compute_brand_daily_stats(raw_df)

        brands = result["brand"].unique()
        assert "repsol" in brands
        assert "shell" in brands
        assert "bp" in brands

    def test_empty_raw_df_returns_empty(self):
        raw_df = pd.DataFrame(columns=["timestamp", "label", "province", "diesel_a_price"])
        result = compute_brand_daily_stats(raw_df)
        assert result.empty
