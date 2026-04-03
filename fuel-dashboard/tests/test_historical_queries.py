import datetime
from unittest.mock import patch

import pandas as pd
from api.schemas import FuelType
from ui.view_models import brand_ranking_kpis
from ui.view_models import day_of_week_kpis
from ui.view_models import province_ranking_kpis
from ui.view_models import SPANISH_DAY_NAMES
from ui.view_models import volatility_kpis

from data.geojson_loader import normalize_data_province_name


def _make_province_daily_stats():
    """Sample province_daily_stats aggregate."""
    rows = []
    for province, avg in [("madrid", 1.45), ("barcelona", 1.48), ("sevilla", 1.42)]:
        for day_offset in range(5):
            rows.append(
                {
                    "date": datetime.date(2026, 3, 18) + datetime.timedelta(days=day_offset),
                    "province": province,
                    "fuel_type": "gasoline_95_e5_price",
                    "avg_price": avg + day_offset * 0.001,
                    "min_price": avg - 0.05,
                    "max_price": avg + 0.05,
                    "station_count": 100,
                }
            )
    return pd.DataFrame(rows)


def _make_day_of_week_stats():
    """Sample day_of_week_stats aggregate."""
    rows = []
    for dow in range(7):
        rows.append(
            {
                "day_of_week": dow,
                "fuel_type": "gasoline_95_e5_price",
                "province": "__national__",
                "sum_price": 1.45 + dow * 0.001,
                "count_days": 52,
                "min_daily_avg": 1.40,
                "max_daily_avg": 1.55,
            }
        )
    return pd.DataFrame(rows)


class TestProvinceRankingKpis:

    def test_returns_4_cards(self):
        df = pd.DataFrame(
            {
                "province": ["sevilla", "madrid", "barcelona"],
                "avg_price": [1.42, 1.45, 1.48],
                "min_price": [1.37, 1.40, 1.43],
                "max_price": [1.47, 1.50, 1.53],
                "total_observations": [500, 500, 500],
            }
        )
        cards = province_ranking_kpis(df)
        assert len(cards) == 4
        assert "Sevilla" in cards[0]["value"]
        assert "Barcelona" in cards[1]["value"]

    def test_empty_df_returns_empty(self):
        cards = province_ranking_kpis(pd.DataFrame())
        assert cards == []


class TestDayOfWeekKpis:

    def test_returns_4_cards(self):
        df = _make_day_of_week_stats()
        df["avg_price"] = df["sum_price"] / df["count_days"]
        cards = day_of_week_kpis(df)
        assert len(cards) == 4
        # Monday (day_of_week=0) should be cheapest since it has lowest sum_price
        assert cards[0]["value"] == SPANISH_DAY_NAMES[0]

    def test_empty_df_returns_empty(self):
        cards = day_of_week_kpis(pd.DataFrame())
        assert cards == []


class TestQueryProvinceRanking:

    def test_query_filters_by_fuel_type_and_groups_by_province(self):
        from data.duckdb_engine import query_province_ranking

        df = _make_province_daily_stats()
        result = query_province_ranking(df, "gasoline_95_e5_price", 90)

        assert len(result) == 3
        assert "province" in result.columns
        assert "avg_price" in result.columns
        # Should be sorted by avg_price ASC
        assert result.iloc[0]["province"] == "sevilla"
        assert result.iloc[-1]["province"] == "barcelona"


class TestQueryDayOfWeekPattern:

    def test_query_returns_7_rows_for_national(self):
        from data.duckdb_engine import query_day_of_week_pattern

        df = _make_day_of_week_stats()
        result = query_day_of_week_pattern(df, "gasoline_95_e5_price", None)

        assert len(result) == 7
        assert "day_of_week" in result.columns
        assert "avg_price" in result.columns


class TestProvinceNormalization:

    def test_normalize_data_province_name_maps_public_names(self):
        assert normalize_data_province_name("A Coruña") == "coruña (a)"
        assert normalize_data_province_name("Illes Balears") == "balears (illes)"
        assert normalize_data_province_name("La Rioja") == "rioja (la)"

    @patch("data.duckdb_engine.query_day_of_week_pattern")
    @patch("data.gcs_client.download_aggregate")
    def test_get_day_of_week_pattern_normalizes_public_province_name(self, mock_download_aggregate, mock_query):
        from services.station_service import get_day_of_week_pattern

        mock_download_aggregate.return_value = _make_day_of_week_stats()
        mock_query.return_value = pd.DataFrame()

        get_day_of_week_pattern(FuelType.gasoline_95_e5_price, "A Coruña")

        assert mock_query.call_args.args[2] == "coruña (a)"


def _make_brand_daily_stats():
    """Sample brand_daily_stats aggregate."""
    rows = []
    for brand, avg in [("repsol", 1.42), ("cepsa", 1.45), ("shell", 1.48)]:
        for day_offset in range(5):
            rows.append(
                {
                    "date": datetime.date(2026, 3, 18) + datetime.timedelta(days=day_offset),
                    "brand": brand,
                    "fuel_type": "gasoline_95_e5_price",
                    "avg_price": avg + day_offset * 0.001,
                    "min_price": avg - 0.05,
                    "max_price": avg + 0.05,
                    "station_count": 50,
                }
            )
    return pd.DataFrame(rows)


def _make_zip_code_daily_stats():
    rows = []
    for zip_code, avg in [("28001", 1.45), ("08001", 1.48)]:
        for day_offset in range(5):
            rows.append(
                {
                    "date": datetime.date.today() - datetime.timedelta(days=4 - day_offset),
                    "zip_code": zip_code,
                    "province": "madrid" if zip_code == "28001" else "barcelona",
                    "fuel_type": "gasoline_95_e5_price",
                    "avg_price": avg + day_offset * 0.001,
                    "min_price": avg - 0.05,
                    "max_price": avg + 0.05,
                    "station_count": 25,
                }
            )
    return pd.DataFrame(rows)


def _make_zip_code_volatility_stats(days=70):
    rows = []
    start_date = datetime.date.today() - datetime.timedelta(days=days - 1)
    for day_offset in range(days):
        current_date = start_date + datetime.timedelta(days=day_offset)
        rows.extend(
            [
                {
                    "date": current_date,
                    "zip_code": "28001",
                    "province": "madrid",
                    "fuel_type": "gasoline_95_e5_price",
                    "avg_price": 1.5000 + ((day_offset % 3) - 1) * 0.0010,
                    "min_price": 1.4950,
                    "max_price": 1.5050,
                    "station_count": 5,
                },
                {
                    "date": current_date,
                    "zip_code": "41001",
                    "province": "sevilla",
                    "fuel_type": "gasoline_95_e5_price",
                    "avg_price": 1.5000 + ((day_offset % 5) - 2) * 0.0100,
                    "min_price": 1.4500,
                    "max_price": 1.5500,
                    "station_count": 6,
                },
                {
                    "date": current_date,
                    "zip_code": "07001",
                    "province": "balears (illes)",
                    "fuel_type": "gasoline_95_e5_price",
                    "avg_price": 1.4700 + ((day_offset % 4) - 2) * 0.0040,
                    "min_price": 1.4500,
                    "max_price": 1.4900,
                    "station_count": 4,
                },
            ]
        )

    for day_offset in range(40):
        rows.append(
            {
                "date": start_date + datetime.timedelta(days=day_offset),
                "zip_code": "50001",
                "province": "zaragoza",
                "fuel_type": "gasoline_95_e5_price",
                "avg_price": 1.4900 + ((day_offset % 4) - 2) * 0.0030,
                "min_price": 1.4700,
                "max_price": 1.5100,
                "station_count": 5,
            }
        )

    for day_offset in range(days):
        rows.append(
            {
                "date": start_date + datetime.timedelta(days=day_offset),
                "zip_code": "46001",
                "province": "valencia / valència",
                "fuel_type": "gasoline_95_e5_price",
                "avg_price": 1.5100 + ((day_offset % 4) - 2) * 0.0020,
                "min_price": 1.5000,
                "max_price": 1.5200,
                "station_count": 2,
            }
        )

    return pd.DataFrame(rows)


class TestBrandRankingKpis:

    def test_returns_4_cards(self):
        df = pd.DataFrame(
            {
                "brand": ["repsol", "cepsa", "shell"],
                "avg_price": [1.42, 1.45, 1.48],
                "min_price": [1.37, 1.40, 1.43],
                "max_price": [1.47, 1.50, 1.53],
                "total_observations": [500, 500, 500],
            }
        )
        cards = brand_ranking_kpis(df)
        assert len(cards) == 4
        assert "Repsol" in cards[0]["value"]
        assert "Shell" in cards[1]["value"]

    def test_empty_df_returns_empty(self):
        cards = brand_ranking_kpis(pd.DataFrame())
        assert cards == []


class TestQueryBrandRanking:

    def test_query_filters_by_fuel_type_and_groups_by_brand(self):
        from data.duckdb_engine import query_brand_ranking

        df = _make_brand_daily_stats()
        result = query_brand_ranking(df, "gasoline_95_e5_price", 90)

        assert len(result) == 3
        assert "brand" in result.columns
        assert "avg_price" in result.columns
        # Should be sorted by avg_price ASC
        assert result.iloc[0]["brand"] == "repsol"
        assert result.iloc[-1]["brand"] == "shell"

    def test_respects_top_n_limit(self):
        from data.duckdb_engine import query_brand_ranking

        df = _make_brand_daily_stats()
        result = query_brand_ranking(df, "gasoline_95_e5_price", 90, top_n=2)
        assert len(result) == 2

    def test_weights_avg_price_by_station_count(self):
        from data.duckdb_engine import query_brand_ranking

        today = datetime.date.today()
        df = pd.DataFrame(
            [
                {
                    "date": today - datetime.timedelta(days=1),
                    "brand": "volatile",
                    "fuel_type": "gasoline_95_e5_price",
                    "avg_price": 1.0,
                    "min_price": 0.99,
                    "max_price": 1.01,
                    "station_count": 1,
                },
                {
                    "date": today,
                    "brand": "volatile",
                    "fuel_type": "gasoline_95_e5_price",
                    "avg_price": 2.0,
                    "min_price": 1.99,
                    "max_price": 2.01,
                    "station_count": 100,
                },
                {
                    "date": today - datetime.timedelta(days=1),
                    "brand": "steady",
                    "fuel_type": "gasoline_95_e5_price",
                    "avg_price": 1.8,
                    "min_price": 1.79,
                    "max_price": 1.81,
                    "station_count": 50,
                },
                {
                    "date": today,
                    "brand": "steady",
                    "fuel_type": "gasoline_95_e5_price",
                    "avg_price": 1.8,
                    "min_price": 1.79,
                    "max_price": 1.81,
                    "station_count": 50,
                },
            ]
        )

        result = query_brand_ranking(df, "gasoline_95_e5_price", 30)

        assert list(result["brand"]) == ["steady", "volatile"]
        volatile_avg = result.loc[result["brand"] == "volatile", "avg_price"].iloc[0]
        assert abs(volatile_avg - (201.0 / 101.0)) < 1e-9


class TestQueryBrandPriceTrend:

    def test_returns_daily_prices_for_selected_brands(self):
        from data.duckdb_engine import query_brand_price_trend

        df = _make_brand_daily_stats()
        result = query_brand_price_trend(df, "gasoline_95_e5_price", 90, ["repsol", "shell"])

        assert not result.empty
        assert set(result.columns) == {"date", "brand", "avg_price"}
        brands = result["brand"].unique()
        assert "repsol" in brands
        assert "shell" in brands
        assert "cepsa" not in brands

    def test_empty_brands_list_returns_empty(self):
        from data.duckdb_engine import query_brand_price_trend

        df = _make_brand_daily_stats()
        result = query_brand_price_trend(df, "gasoline_95_e5_price", 90, [])
        assert result.empty


class TestQueryZipCodePriceTrend:

    def test_returns_daily_prices_for_selected_zip_code(self):
        from data.duckdb_engine import query_zip_code_price_trend

        df = _make_zip_code_daily_stats()
        result = query_zip_code_price_trend(df, "28001", "gasoline_95_e5_price", 90)

        assert not result.empty
        assert set(result.columns) == {"date", "avg_price", "min_price", "max_price"}
        assert len(result) == 5
        assert result["avg_price"].iloc[0] < result["avg_price"].iloc[-1]

    def test_filters_out_other_zip_codes(self):
        from data.duckdb_engine import query_zip_code_price_trend

        df = _make_zip_code_daily_stats()
        result = query_zip_code_price_trend(df, "99999", "gasoline_95_e5_price", 90)

        assert result.empty


class TestVolatilityKpis:

    def test_returns_4_cards(self):
        df = pd.DataFrame(
            {
                "zip_code": ["28001", "41001"],
                "province": ["madrid", "sevilla"],
                "avg_price": [1.50, 1.51],
                "std_dev_price": [0.001, 0.010],
                "coefficient_of_variation": [0.0007, 0.0066],
                "min_price": [1.499, 1.48],
                "max_price": [1.501, 1.54],
                "price_range": [0.002, 0.06],
                "observation_days": [70, 70],
                "avg_station_count": [5, 6],
            }
        )

        cards = volatility_kpis(df)

        assert len(cards) == 4
        assert cards[0]["value"] == "28001"
        assert "Madrid" in cards[0]["description"]

    def test_empty_df_returns_empty(self):
        assert volatility_kpis(pd.DataFrame()) == []


class TestQueryVolatilityByZone:

    def test_query_computes_metrics_and_orders_by_lowest_cv(self):
        from data.duckdb_engine import query_volatility_by_zone

        df = _make_zip_code_volatility_stats()
        result = query_volatility_by_zone(df, "gasoline_95_e5_price", 90, mainland_only=False)

        assert not result.empty
        assert set(result.columns) == {
            "zip_code",
            "province",
            "avg_price",
            "std_dev_price",
            "coefficient_of_variation",
            "min_price",
            "max_price",
            "price_range",
            "observation_days",
            "avg_station_count",
        }
        assert result.iloc[0]["zip_code"] == "28001"
        assert result.iloc[-1]["zip_code"] == "41001"
        assert result.iloc[0]["coefficient_of_variation"] < result.iloc[-1]["coefficient_of_variation"]

    def test_query_excludes_non_mainland_low_coverage_and_low_station_zones(self):
        from data.duckdb_engine import query_volatility_by_zone

        df = _make_zip_code_volatility_stats()
        result = query_volatility_by_zone(df, "gasoline_95_e5_price", 90, mainland_only=True)

        zip_codes = set(result["zip_code"])
        assert "07001" not in zip_codes
        assert "50001" not in zip_codes
        assert "46001" not in zip_codes
        assert zip_codes == {"28001", "41001"}
