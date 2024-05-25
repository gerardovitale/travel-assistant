from unittest import TestCase

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class BaseTestCase(TestCase):
    @staticmethod
    def setup_test_spark_session():
        builder = (
            SparkSession.builder.master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        return (
            configure_spark_with_delta_pip(builder)
            .appName("unit-tests")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.maxResultSize", "500m")
            .getOrCreate()
        )

    @staticmethod
    def assert_spark_dataframes_equal(expected_df: DataFrame, actual_df: DataFrame):
        assert actual_df is not None, "The actual_df is None"
        assert expected_df.schema == actual_df.schema, "Schema mismatch"
        expected_rows = expected_df.orderBy(expected_df.columns).collect()
        actual_rows = actual_df.orderBy(actual_df.columns).collect()
        assert expected_rows == actual_rows, "Data mismatch"

    @classmethod
    def setUpClass(cls):
        cls.spark = cls.setup_test_spark_session()
        cls.maxDiff = None

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.spark = None
