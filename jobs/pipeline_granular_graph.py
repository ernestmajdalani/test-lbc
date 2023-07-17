import pyspark.sql.functions as F
import os
from pyspark.sql import DataFrame, SparkSession
from jobs import clinical_trials, pubmed, drugs


def _extract_data(spark: SparkSession, config: dict) -> DataFrame:
    contains_drug = F.col("title").contains(F.col("drug"))

    drugs_df = drugs.run_job(spark, config)
    clinical_trials_df = clinical_trials.run_job(spark, config)
    pubmed_df = pubmed.run_job(spark, config)
    drugs_clinical_df = drugs_df.join(clinical_trials_df, contains_drug, how="inner")
    drugs_pubmed_df = drugs_df.join(pubmed_df, contains_drug, how="inner")
    union_dfs = (
        drugs_clinical_df
        .unionByName(drugs_pubmed_df)
        .withColumnsRenamed({
            "atccode": "source_id",
            "drug": "source_name",
            "id": "destination_id",
            "type": "edge_type"
        }))

    return union_dfs


def _load_data(df: DataFrame, path: str):
    df.write.mode("overwrite").json(path)


def run_job(spark: SparkSession, config: dict):
    output_path = os.path.join(config.get('data_output_path'), config.get('output_folder_granular'))
    _load_data(_extract_data(spark, config), output_path)


