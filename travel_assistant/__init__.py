import pytz
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

CET_TIMEZONE = pytz.timezone("Europe/Madrid")

TABLE_PATH = "../data/spain-fuel-price"
PARTITION_COLS = ["date", "hour"]

FUEL_PRICE_URL = (
    "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
)


def get_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("travel-assistant")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "1g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
