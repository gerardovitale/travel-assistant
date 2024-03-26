import os

import pytz
from delta import configure_spark_with_delta_pip
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
        builder = (
            SparkSession.builder.master("local[*]")
            .appName(self.INSTANCE_NAME)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "1g")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .config("spark.hadoop.fs.gs.project.id", self.PROJECT_ID)
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", self.GOOGLE_APPLICATION_CREDENTIALS)
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
