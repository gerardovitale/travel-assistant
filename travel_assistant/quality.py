import logging
from datetime import datetime
from datetime import timezone
from functools import wraps

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

BASE_QUALITY_PATH = "data/data-quality-metrics"
QUALITY_SCHEMA = StructType([
    StructField("processing_time", StringType(), False),
    StructField("event_time", StringType(), True),
    StructField("entity", StringType(), False),
    StructField("instance", StringType(), False),
    StructField("metric_name", StringType(), False),
    StructField("value", DoubleType(), False),
])


class NotDataFrameError(Exception):
    pass


def data_quality_metrics(table_name: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"Calling `{func.__name__}` with `{[arg for arg in args]}`.")
            df = func(*args, **kwargs)
            logger.info(f"Collecting data quality metrics for `{table_name}`.")
            metrics = collect_metrics(df)
            logger.info(f"Data quality metrics for `{table_name}` collected.")
            metrics.show(truncate=False)
            write_data_quality_metrics(metrics, table_name)
            logger.info(f"Data quality metrics for `{table_name}` written.")
            return df

        return wrapper

    return decorator


def collect_metrics(df: DataFrame) -> DataFrame:
    if not isinstance(df, DataFrame):
        logger.error(f"PySpark DataFrame was expected, instead got {type(df)}.")
        raise NotDataFrameError(f"PySpark DataFrame was expected, instead got {type(df)}.")

    total_rows = float(df.count())
    logger.info(f"Processing DataFrame with the following total rows: {total_rows:,}.")
    processing_time = datetime.now().astimezone(timezone.utc).isoformat()
    event_time = df.select("timestamp").first().timestamp if "timestamp" in df.columns else None
    metric_rows = [(processing_time, event_time, "DataFrame", "size", "RowNumber", total_rows)]

    for column in df.columns:
        completeness = df.filter(col(column).isNotNull()).count() / (total_rows if total_rows > 0 else 1.0)
        metric_rows.append((processing_time, event_time, "Column", column, "Completeness", completeness))

    return (
        SparkSession.builder.getOrCreate()
        .createDataFrame(metric_rows, QUALITY_SCHEMA)
    )


def write_data_quality_metrics(metrics: DataFrame, table_name: str) -> None:
    table_path = f"{BASE_QUALITY_PATH}/{table_name}"
    logger.info(f"Writing data quality metrics to `{table_path}`.")
    try:
        (metrics.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .save(table_path)
         )
    except Exception as e:
        logger.error(f"Failed to write data quality metrics to `{table_path}`.")
        logger.error(e)
