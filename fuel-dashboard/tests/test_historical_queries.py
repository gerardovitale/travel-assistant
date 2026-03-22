import datetime
from unittest.mock import patch

import pandas as pd
from api.schemas import FuelType
from ui.view_models import day_of_week_kpis
from ui.view_models import province_ranking_kpis
from ui.view_models import SPANISH_DAY_NAMES

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
