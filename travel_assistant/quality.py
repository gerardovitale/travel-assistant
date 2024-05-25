from datetime import datetime
from functools import wraps

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

QUALITY_SCHEMA = StructType([
    StructField("datetime", TimestampType(), False),
    StructField("entity", StringType(), False),
    StructField("instance", StringType(), False),
    StructField("metric_name", StringType(), False),
    StructField("value", DoubleType(), False),
])


def data_quality_metrics(table_name: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            df = func(*args, **kwargs)

            if not isinstance(df, DataFrame):
                raise ValueError("The decorated function must return a PySpark DataFrame")

            metrics = collect_metrics(df)
            metrics.show(truncate=False)

            return df

        return wrapper

    return decorator


def collect_metrics(df: DataFrame) -> DataFrame:
    processing_dt = datetime.now()
    metric_rows = [(processing_dt, "DataFrame", "size", "row_number", float(df.count()))]
    for column in df.columns:
        completeness = df.filter(col(column).isNotNull()).count() / df.count()
        metric_rows.append((processing_dt, "Column", column, "completeness", completeness))

    spark = SparkSession.builder.getOrCreate()
    metrics_df = spark.createDataFrame(metric_rows, QUALITY_SCHEMA)
    return metrics_df
