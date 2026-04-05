from unittest.mock import MagicMock
from unittest.mock import patch

from api.schemas import StationResult
from ui.components import station_results_table


def test_station_results_table_omits_score_column_and_cell_data():
    table = MagicMock()
    table.classes.return_value = table
    table.props.return_value = table

    station = StationResult(
        label="station_1",
        address="calle 1",
        municipality="madrid",
        province="madrid",
        zip_code="28001",
        latitude=40.4168,
        longitude=-3.7038,
        price=1.45,
        distance_km=1.2,
        score=8.5,
        estimated_total_cost=59.12,
    )

    with patch("ui.components.ui.table", return_value=table) as mock_table:
        station_results_table([station], "best_by_address")

    columns = mock_table.call_args.kwargs["columns"]
    rows = mock_table.call_args.kwargs["rows"]

    assert "score" not in [column["name"] for column in columns]
    assert "score" not in rows[0]
