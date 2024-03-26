import os

from delta import configure_spark_with_delta_pip
from pyspark import SparkConf
from pyspark.sql import SparkSession


class Config:
    def __init__(self) -> None:
        self.PROJECT_ID = os.getenv("PROJECT_ID")
        self.INSTANCE_NAME = os.getenv("INSTANCE_NAME")
        self.GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        self.DATA_SOURCE_URL = os.getenv("DATA_SOURCE_URL")
        self.DESTINATION_PATH = os.getenv("DATA_DESTINATION_BUTKET") if os.getenv("PROD") else "data/spain-fuel-price"
        self.PARTITION_COLS = ["date", "hour"]

    def get_spark_session(self) -> SparkSession:
        spark_conf = SparkConf()
        spark_conf.setAppName(self.INSTANCE_NAME)
        spark_conf.setMaster("local[*]")
        spark_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        spark_conf.set("spark.driver.memory", "2g")
        spark_conf.set("spark.executor.memory", "1g")
        spark_conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        spark_conf.set("spark.hadoop.fs.gs.project.id", self.PROJECT_ID)
        spark_conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        spark_conf.set(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile", self.GOOGLE_APPLICATION_CREDENTIALS
        )
        builder = SparkSession.builder.config(conf=spark_conf)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
