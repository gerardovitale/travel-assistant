import pytz
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

CET_TIMEZONE = pytz.timezone("Europe/Madrid")


def get_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("financial-assistant")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "1g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
