import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from utils.common import clean_hex

from utils.common import read_data, get_std_date_udf

PUBMED = "pubmed"


def _extract_data(spark: SparkSession, config: dict) -> DataFrame:
    return read_data(spark, config, PUBMED)


def _transform_data(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .withColumn("date", get_std_date_udf("date"))
        .withColumn("journal", clean_hex("journal"))
        .withColumn("journal", F.lower(F.col("journal")))
        .withColumn("title", clean_hex("title"))
        .withColumn("title", F.lower(F.col("title")))
        .withColumn("type", lit(PUBMED))
        .withColumn("id", F.when(F.col("id") != "", F.col("id")).otherwise(F.monotonically_increasing_id()))
    )


def run_job(spark: SparkSession, config: dict) -> DataFrame:
    return _transform_data(_extract_data(spark, config))
