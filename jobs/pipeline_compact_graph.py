import os
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from jobs import clinical_trials, pubmed, drugs


def _extract_data(spark: SparkSession, config: dict) -> DataFrame:
    contains_drug = F.col("title").contains(F.col("drug"))
    drugs_df = drugs.run_job(spark, config)
    clinical_trials_df = clinical_trials.run_job(spark, config)
    pubmed_df = pubmed.run_job(spark, config)
    drugs_clinical_df = drugs_df.join(clinical_trials_df, contains_drug, how="inner")
    drugs_pubmed_df = drugs_df.join(pubmed_df, contains_drug, how="inner")
    union_df = drugs_clinical_df.unionByName(drugs_pubmed_df)

    return union_df


# def _transform_data(input_df: DataFrame) -> DataFrame:
#     title_date_map = F.create_map(F.lit("title"), "title", F.lit("date"), "date")
#
#     return (
#         input_df
#         .withColumn(
#             "pubmed",
#             F.when(F.col("type") == "pubmed", title_date_map))
#         .withColumn(
#             "clinical_trials",
#             F.when(F.col("type") == "clinical_trials", title_date_map))
#         .withColumn(
#             "journal",
#             F.create_map(F.lit("title"), "journal", F.lit("date"), "date"))
#         .groupBy("atccode", "drug")
#         .agg(
#             F.collect_list("pubmed").alias("pubmed"),
#             F.collect_list("clinical_trials").alias("clinical_trials"),
#             F.collect_list("journal").alias("journal")
#         )
#     )

def _transform_data(input_df: DataFrame) -> DataFrame:
    title_date_map = F.create_map(F.lit("id"), "id", F.lit("title"), "title", F.lit("date"), "date")

    return (
        input_df
        .withColumn(
            "pubmed",
            F.when(F.col("type") == "pubmed", title_date_map))
        .withColumn(
            "clinical_trials",
            F.when(F.col("type") == "clinical_trials", title_date_map))
        .withColumn(
            "journal",
            F.create_map(F.lit("title"), "journal", F.lit("date"), "date"))
        .groupBy("atccode", "drug")
        .agg(
            F.collect_list("pubmed").alias("pubmed"),
            F.collect_list("clinical_trials").alias("clinical_trials"),
            F.collect_list("journal").alias("journal")
        )
    )


def _load_data(input_df: DataFrame, output_path: str):
    input_df.write.mode("overwrite").json(output_path)


def run_job(spark: SparkSession, config: dict):
    output_path = os.path.join(config.get('data_output_path'), config.get('output_folder_compact'))
    _load_data(_transform_data(_extract_data(spark, config)), output_path)

