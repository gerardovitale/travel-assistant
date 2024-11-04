from io import StringIO
from typing import List

import pandas as pd
from google.cloud import storage
from pyspark.sql import SparkSession


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
    with blob.open("r") as f:
        return f.read()


def read_csv_as_pd_df(bucket_name: str, limit: int = None) -> pd.DataFrame:
    df_list = []
    bucket_list = list_blobs(bucket_name)
    if limit:
        print(f"Setting limit to {limit}")
        bucket_list = bucket_list[:limit]

    for b in bucket_list:
        print(f"Reading {b.name}")
        string_csv_data = read_blobs(bucket_name, b.name)
        csv_data = StringIO(string_csv_data)
        df = pd.read_csv(csv_data)
        if "date" in df.columns:
            print(f"Dropping Date column from {b.name}")
            df.drop(columns=["date"], inplace=True)
        df_list.append(df)

    return pd.concat(df_list).reset_index().drop(columns=["index"])


def preprocess_spain_fuel_price_df(df: pd.DataFrame, label_list: List[str]) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date
    for label in label_list:
        df.loc[df["label"].str.contains(label), "label"] = label
    return df
