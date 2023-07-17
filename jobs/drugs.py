import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from utils.common import read_data

DRUGS = "drugs"


def _extract_data(spark: SparkSession, config) -> DataFrame:
    return read_data(spark, config, DRUGS)


def _transform_data(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df.withColumn("drug", F.lower(F.col("drug")))
    )


def run_job(spark: SparkSession, config: dict) -> DataFrame:
    return _transform_data(_extract_data(spark, config))
