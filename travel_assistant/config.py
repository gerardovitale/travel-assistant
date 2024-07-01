import json
import logging
import os
import tempfile

from delta import configure_spark_with_delta_pip
from google.cloud import secretmanager
from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Config(metaclass=Singleton):
    def __init__(self) -> None:
        self.is_prod_env = True if os.getenv("PROD") else False
        logger.info("Setting Config Obj for: ENV = {0}".format("PROD" if self.is_prod_env else "TEST"))
        self.G_CLOUD_PROJECT_ID = os.getenv("G_CLOUD_PROJECT_ID")
        self.G_CLOUD_COMPUTE_INSTANCE_NAME = os.getenv("G_CLOUD_COMPUTE_INSTANCE_NAME")
        self.DATA_SOURCE_URL = os.getenv("DATA_SOURCE_URL")
        self.DESTINATION_PATH = os.getenv("DATA_DESTINATION_BUCKET") if self.is_prod_env else "data/spain-fuel-price"
        self.PARTITION_COLS = ["date", "hour"]

    def get_spark_session(self) -> SparkSession:
        spark_conf = SparkConf()
        spark_conf.setAppName(self.G_CLOUD_COMPUTE_INSTANCE_NAME)
        spark_conf.setMaster("local[*]")
        spark_conf.set("spark.driver.memory", "2g")
        spark_conf.set("spark.executor.memory", "1g")
        spark_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        spark_conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        spark_conf.set("spark.hadoop.fs.gs.project.id", self.G_CLOUD_PROJECT_ID)
        spark_conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        if self.is_prod_env:
            self.set_google_storage_connector_config(spark_conf)
        builder = SparkSession.builder.config(conf=spark_conf)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    def set_google_storage_connector_config(self, spark_conf: SparkConf) -> None:
        logger.info("Getting Google Storage Connector Config")
        credentials = self.get_google_storage_connector_keyfile()
        logger.info("Setting Google Storage Connector Keyfile to the Spark Config")
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(json.dumps(credentials).encode("UTF-8"))
            spark_conf.set(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                temp_file.name,
            )

    def get_google_storage_connector_keyfile(self) -> str:
        logger.info("Fetching Google Storage Connector Keyfile")
        client = secretmanager.SecretManagerServiceClient()
        request = {"name": client.secret_version_path(self.G_CLOUD_PROJECT_ID, os.getenv("G_CLOUD_COMPUTE_SECRET_NAME"), "latest")}
        response = client.access_secret_version(request)
        logger.info("Google Storage Connector Keyfile Fetched")
        secret_payload = response.payload.data.decode("UTF-8")
        return json.loads(secret_payload)
