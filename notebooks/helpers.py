from io import BytesIO
from typing import List

import pandas as pd
import seaborn as sns
from google.cloud import storage
from pyspark.sql import SparkSession

BUCKET = "spain-fuel-prices"
LABEL_LIST = [
    "repsol",
    "cepsa",
    "bp",
    "shell",
    "galp",
    "disa",
    "ballenoil",
    "carrefour",
    "plenoil",
    "petroprix",
    "costco",
]
SEABORN_PALETTE = "colorblind"
SEABORN_FIGURE_FIGSIZE = (12, 10)

sns.set_palette(SEABORN_PALETTE)
sns.set(rc={"figure.figsize": SEABORN_FIGURE_FIGSIZE})


def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("spain-fuel-prices")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def list_blobs(bucket_name):
    storage_client = storage.Client()
    return storage_client.list_blobs(bucket_name)


def read_blobs(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open("rb") as f:
        return f.read()


def read_parquet_as_pd_df(bucket_name: str, limit: int = None) -> pd.DataFrame:
    df_list = []
    bucket_list = list(list_blobs(bucket_name))
    if limit:
        print(f"Setting limit to {limit}")
        bucket_list = bucket_list[:limit]

    for b in bucket_list:
        if not b.name.endswith(".parquet"):
            print(f"Skipping non-parquet file: {b.name}")
            continue
        print(f"Reading {b.name}")
        raw_data = read_blobs(bucket_name, b.name)
        df = pd.read_parquet(BytesIO(raw_data))
        df_list.append(df)

    return pd.concat(df_list).reset_index(drop=True)


def preprocess_spain_fuel_price_df(df: pd.DataFrame, label_list: List[str]) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date
    for label in label_list:
        df.loc[df["label"].str.contains(label), "label"] = label
    return df
