import pyspark.sql.functions as F
from utils.common import read_data, get_std_date_udf, clean_hex, clean_whitespace
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit


CLINICAL_TRIALS = "clinical_trials"


def _extract_data(spark: SparkSession, config: dict) -> DataFrame:
    return read_data(spark, config, CLINICAL_TRIALS)


def _transform_data(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .withColumn("date", get_std_date_udf("date"))
        .withColumn("journal", clean_hex("journal"))
        .withColumn("journal", clean_whitespace("journal"))
        .withColumn("journal", F.lower(F.col("journal")))
        .withColumn("title", clean_hex("scientific_title"))
        .withColumn("title", clean_whitespace("title"))
        .withColumn("title", F.lower(F.col("title")))
        .withColumn("type", lit(CLINICAL_TRIALS))
        .drop(F.col("scientific_title"))
    )


def run_job(spark: SparkSession, config: dict) -> DataFrame:
    return _transform_data(_extract_data(spark, config))
