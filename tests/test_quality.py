from datetime import datetime
from unittest.mock import Mock
from unittest.mock import patch

from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

from tests.config_test import BaseTestCase
from travel_assistant.quality import NotDataFrameError
from travel_assistant.quality import QUALITY_SCHEMA
from travel_assistant.quality import collect_metrics
from travel_assistant.quality import data_quality_metrics


class TestQuality(BaseTestCase):

    @patch("travel_assistant.quality.collect_metrics")
    def test_data_quality_metrics_decorator(self, mock_collect_metrics: Mock):
        test_schema = StructType([
            StructField("column1", StringType(), True),
            StructField("column2", StringType(), True),
        ])
        test_data = [
            ("a", None,),
            ("b", None,),
            ("c", None,),
            ("d", None,),
        ]

        @data_quality_metrics("table_name")
        def test_func():
            return self.spark.createDataFrame(test_data, test_schema)

        expected_df = self.spark.createDataFrame(test_data, test_schema)

        actual_df = test_func()

        mock_collect_metrics.assert_called_once()
        self.assert_spark_dataframes_equal(actual_df, expected_df)

    @patch("travel_assistant.quality.collect_metrics")
    def test_data_quality_metrics_decorator_when_func_does_not_return_df(self, mock_collect_metrics: Mock):
        mock_collect_metrics.side_effect = NotDataFrameError(
            "PySpark DataFrame was expected, instead got <class 'NoneType'>."
        )

        @data_quality_metrics("table_name")
        def test_func():
            return None

        self.assertRaises(NotDataFrameError, test_func)

    @patch("travel_assistant.quality.datetime")
    def test_collect_metrics(self, mock_datetime: Mock):
        mock_datetime.now.return_value = datetime(2024, 5, 25)

        test_schema = StructType([
            StructField("column1", StringType(), True),
            StructField("column2", StringType(), True),
            StructField("column3", StringType(), True),
        ])
        test_data = [
            ("a", None, "a"),
            ("b", None, "a"),
            ("c", None, "a"),
            ("d", None, None),
        ]
        test_df = self.spark.createDataFrame(test_data, test_schema)

        expected_data = [
            (datetime(2024, 5, 25), "Column", "column1", "completeness", 1.0),
            (datetime(2024, 5, 25), "Column", "column2", "completeness", 0.0),
            (datetime(2024, 5, 25), "Column", "column3", "completeness", 0.75),
            (datetime(2024, 5, 25), "DataFrame", "size", "row_number", 4.0),
        ]
        expected_df = self.spark.createDataFrame(expected_data, QUALITY_SCHEMA)

        actual_df = collect_metrics(test_df)

        self.assert_spark_dataframes_equal(actual_df, expected_df)

    @patch("travel_assistant.quality.datetime")
    def test_collect_metrics_when_df_is_empty(self, mock_datetime: Mock):
        mock_datetime.now.return_value = datetime(2024, 5, 25)
        test_schema = StructType([
            StructField("column1", StringType(), True),
            StructField("column2", StringType(), True),
            StructField("column3", StringType(), True),
        ])
        test_data = []
        test_df = self.spark.createDataFrame(test_data, test_schema)

        expected_data = [
            (datetime(2024, 5, 25), "Column", "column1", "completeness", 0.0),
            (datetime(2024, 5, 25), "Column", "column2", "completeness", 0.0),
            (datetime(2024, 5, 25), "Column", "column3", "completeness", 0.0),
            (datetime(2024, 5, 25), "DataFrame", "size", "row_number", 0.0),
        ]
        expected_df = self.spark.createDataFrame(expected_data, QUALITY_SCHEMA)

        actual_df = collect_metrics(test_df)

        self.assert_spark_dataframes_equal(actual_df, expected_df)

    def test_collect_metrics_when_invalid_output_is_passed(self):
        self.assertRaises(NotDataFrameError, collect_metrics, "invalid_output")
        self.assertRaises(NotDataFrameError, collect_metrics, None)
